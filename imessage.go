// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2021 Tulir Asokan
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

package main

import (
	"time"

	log "maunium.net/go/maulogger/v2"

	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
)

type iMessageHandler struct {
	bridge *IMBridge
	log    log.Logger
	stop   chan struct{}
}

func NewiMessageHandler(bridge *IMBridge) *iMessageHandler {
	return &iMessageHandler{
		bridge: bridge,
		log:    bridge.Log.Sub("iMessage"),
		stop:   make(chan struct{}),
	}
}

func (imh *iMessageHandler) Start() {
	messages := imh.bridge.IM.MessageChan()
	readReceipts := imh.bridge.IM.ReadReceiptChan()
	typingNotifications := imh.bridge.IM.TypingNotificationChan()
	chats := imh.bridge.IM.ChatChan()
	contacts := imh.bridge.IM.ContactChan()
	messageStatuses := imh.bridge.IM.MessageStatusChan()
	for {
		select {
		case msg := <-messages:
			imh.HandleMessage(msg)
		case rr := <-readReceipts:
			imh.HandleReadReceipt(rr)
		case notif := <-typingNotifications:
			imh.HandleTypingNotification(notif)
		case chat := <-chats:
			imh.HandleChat(chat)
		case contact := <-contacts:
			imh.HandleContact(contact)
		case status := <-messageStatuses:
			imh.HandleMessageStatus(status)
		case <-imh.stop:
			return
		}
	}
}

// resolveChatGUIDWithCorrelationIdentifier takes a GUID/UUID pair, and determines whether a different, pre-existing chat GUID should be used instead.
// if a pre-existing chat is found, and a portal exists with the incoming GUID, the portal will be tombstoned and forgotten.
func (imh *iMessageHandler) resolveChatGUIDWithCorrelationIdentifier(guid *pb.GUID, correlationID string) (resolved *pb.GUID) {
	resolved = guid
	if !imh.bridge.IM.Capabilities().Correlation || len(correlationID) == 0 {
		// no correlation, passthrough
		return
	}
	if guid.IsGroup {
		// we don't correlate groups right now, passthrough
		return
	}
	strGUID := guid.ToString()
	if portal := imh.bridge.DB.Portal.GetByCorrelationID(correlationID); portal != nil {
		// there's already a portal with this correlation ID
		if portal.GUID != strGUID {
			// the existing portal has a different GUID
			if existingPortal := imh.bridge.DB.Portal.GetByGUID(strGUID); existingPortal != nil {
				// the incoming GUID has an existing portal
				if len(existingPortal.MXID) == 0 {
					// its just a row, delete it
					existingPortal.Delete()
				} else {
					// tombstone it
					imh.bridge.newDummyPortal(existingPortal).mergeIntoPortal(portal.MXID, "This room has been deduplicated.")
				}
			}
		}
		parsed := pb.ParseIdentifier(portal.GUID)
		resolved = &parsed
		return
	}
	imh.bridge.DB.Portal.StoreCorrelation(strGUID, correlationID)
	return
}

// resolveIdentifiers takes a chat GUID, sender GUID, and correlation ID, and maps the GUIDs to pre-existing GUIDs if possible.
// this preserves consistency and makes the sender appear to come from the same person, avoiding issues where the DM sender
// is not a participant and cannot join the portal.
func (imh *iMessageHandler) resolveIdentifiers(guid *pb.GUID, correlationID string, senderID *pb.GUID, senderCorrelationID string, fromMe bool) (newGUID *pb.GUID, newSender *pb.GUID) {
	if !imh.bridge.IM.Capabilities().Correlation {
		// no correlation
		return guid, senderID
	}
	if senderID != nil && len(senderCorrelationID) > 0 {
		// store the correlation for this sender
		imh.bridge.DB.Puppet.StoreCorrelation(senderID.ToString(), correlationID)
	}
	if guid.IsGroup || len(correlationID) == 0 {
		// todo: correlate group senders, requires knowledge of who is in the portal, this is not easily accessible right now.
		return guid, senderID
	}
	newGUID = imh.resolveChatGUIDWithCorrelationIdentifier(guid, correlationID)
	return newGUID, senderID
}

func (imh *iMessageHandler) HandleMessage(msg *pb.Message) {
	// TODO trace log
	//imh.log.Debugfln("Received incoming message: %+v", msg)
	msg.ChatGUID, msg.Sender = imh.resolveIdentifiers(msg.GetChatGUID(), msg.GetCorrelations().GetChat(), msg.GetSender(), msg.GetCorrelations().GetSender(), msg.IsFromMe)
	portal := imh.bridge.GetPortalByGUID(msg.ChatGUID.ToString())
	if len(portal.MXID) == 0 {
		portal.log.Infoln("Creating Matrix room to handle message")
		err := portal.CreateMatrixRoom(nil, nil)
		if err != nil {
			imh.log.Warnfln("Failed to create Matrix room to handle message: %v", err)
			return
		}
	}
	portal.Messages <- msg
}

func (imh *iMessageHandler) HandleMessageStatus(status *pb.SendMessageStatus) {
	status.ChatGUID = imh.resolveChatGUIDWithCorrelationIdentifier(status.ChatGUID, status.GetCorrelations().GetChat())
	portal := imh.bridge.GetPortalByGUID(status.ChatGUID.ToString())
	if len(portal.GUID) == 0 {
		imh.log.Debugfln("Ignoring message status for message from unknown portal %s/%s", status.GUID, status.ChatGUID)
		return
	}
	portal.MessageStatuses <- status
}

func (imh *iMessageHandler) HandleReadReceipt(rr *pb.ReadReceipt) {
	rr.ChatGUID, rr.SenderGUID = imh.resolveIdentifiers(rr.GetChatGUID(), rr.GetCorrelations().GetChat(), rr.GetSenderGUID(), rr.GetCorrelations().GetSender(), rr.GetIsFromMe())
	portal := imh.bridge.GetPortalByGUID(rr.ChatGUID.ToString())
	if len(portal.MXID) == 0 {
		return
	}
	portal.ReadReceipts <- rr
}

func (imh *iMessageHandler) HandleTypingNotification(notif *pb.TypingNotification) {
	notif.ChatGUID = imh.resolveChatGUIDWithCorrelationIdentifier(notif.GetChatGUID(), *notif.GetCorrelations().Chat)
	portal := imh.bridge.GetPortalByGUID(notif.ChatGUID.ToString())
	if len(portal.MXID) == 0 {
		return
	}
	_, err := portal.MainIntent().UserTyping(portal.MXID, notif.Typing, 60*time.Second)
	if err != nil {
		action := "typing"
		if !notif.Typing {
			action = "not typing"
		}
		portal.log.Warnln("Failed to mark %s as %s in %s: %v", portal.MainIntent().UserID, action, portal.MXID, err)
	}
}

func (imh *iMessageHandler) HandleChat(chat *pb.ChatInfo) {
	chat.GUID = imh.resolveChatGUIDWithCorrelationIdentifier(chat.GUID, chat.GetCorrelationID())
	portal := imh.bridge.GetPortalByGUID(chat.GUID.ToString())
	if len(portal.MXID) > 0 {
		portal.log.Infoln("Syncing Matrix room to handle chat command")
		portal.SyncWithInfo(chat)
	} else if !chat.GetNoCreateRoom() {
		portal.log.Infoln("Creating Matrix room to handle chat command")
		err := portal.CreateMatrixRoom(chat, nil)
		if err != nil {
			imh.log.Warnfln("Failed to create Matrix room to handle chat command: %v", err)
			return
		}
	}
}

func (imh *iMessageHandler) HandleContact(contact *pb.Contact) {
	puppet := imh.bridge.GetPuppetByGUID(contact.UserGUID.ToString())
	if len(puppet.MXID) > 0 {
		puppet.log.Infoln("Syncing Puppet to handle contact command")
		puppet.SyncWithContact(contact)
	}
}

func (imh *iMessageHandler) Stop() {
	close(imh.stop)
}
