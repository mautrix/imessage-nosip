package main

import (
	"database/sql"
	"fmt"
	"html"
	"strings"
	"sync"
	"time"

	"go.mau.fi/mautrix-imessage-nosip/core"
	"go.mau.fi/mautrix-imessage-nosip/imessage"
	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
	log "maunium.net/go/maulogger/v2"
	"maunium.net/go/mautrix"
	"maunium.net/go/mautrix/event"
	"maunium.net/go/mautrix/id"
)

type BridgeMode string

var (
	SynapseMode BridgeMode = "synapse"
	HungryMode  BridgeMode = "hungry"
)

func bridgeMode() BridgeMode {
	return SynapseMode
}

type PreparedEvent struct {
	GUID string

	Puppet  core.IPuppet
	Type    event.Type
	Content interface{}
	Extra   map[string]interface{}

	Original *pb.Message
	part     int
	/// When present, the event cannot be backfilled until the designated event is bridged
	AvatarChange *pb.Message
	NameChange   *pb.Message
	MemberChange *pb.Message
}

type IncomingMessageIngress struct {
	Bridge *IMBridge
	Portal *Portal

	PreparedEvents []PreparedEvent
	log            log.Logger
	backfillLock   sync.Mutex
}

func NewIncomingMessageIngress(portal *Portal) *IncomingMessageIngress {
	return &IncomingMessageIngress{
		Bridge:         portal.bridge,
		Portal:         portal,
		PreparedEvents: make([]PreparedEvent, 0),
		log:            portal.log.Sub("Incoming"),
	}
}

func (mt *IncomingMessageIngress) ProcessNewMessage(msg *pb.Message) {
	mt.addMessage(msg)
	mt.Backfill(true)
}

func (mt *IncomingMessageIngress) addMessage(msg *pb.Message) {
	puppet := mt.Portal.getPuppetForMessage(msg)
	for part, item := range msg.Items {
		prepared := PreparedEvent{
			GUID:     item.GUID,
			Puppet:   puppet,
			Type:     event.EventMessage,
			Extra:    make(map[string]interface{}),
			Original: msg,
			part:     part,
		}

		switch item.X.(type) {
		case *pb.Item_Text:
			content := &event.MessageEventContent{
				MsgType: event.MsgText,
			}

			item := item.GetText()
			body := item.GetText()
			body = strings.ReplaceAll(body, "\ufffc", "")
			subject := item.GetSubject()

			if len(subject) > 0 {
				content.Body = fmt.Sprintf("**%s**\n%s", subject, body)
				content.Format = event.FormatHTML
				content.FormattedBody = fmt.Sprintf("<strong>%s</strong><br>%s", html.EscapeString(subject), html.EscapeString(body))
			} else {
				content.Body = body
			}
			mt.Portal.SetReply(content, msg)
			prepared.Content = content
		case *pb.Item_Attachment:
			var err error
			prepared.Content, prepared.Extra, err = mt.Portal.handleIMAttachment(msg, item.GetAttachment().Attachment, puppet.GetIntent())
			if err != nil {
				prepared.Content = &event.MessageEventContent{
					MsgType: event.MsgNotice,
					Body:    err.Error(),
				}
				prepared.Extra = make(map[string]interface{})
			}
		case *pb.Item_Typing:
			continue
		case *pb.Item_Plugin:
			plugin := item.GetPlugin()
			if richLink := plugin.GetRichLink(); richLink != nil {
				mt.log.Debugfln("Handling rich link in iMessage %s", msg.GUID)
				linkPreview := mt.Portal.convertRichLinkToBeeper(richLink)
				fallbackText := plugin.GetFallbackText()
				if linkPreview != nil {
					prepared.Extra["com.beeper.linkpreviews"] = []*BeeperLinkPreview{linkPreview}
					mt.log.Debugfln("Link preview metadata converted for %s", msg.GUID)
					if len(fallbackText) == 0 {
						fallbackText = linkPreview.MatchedURL
					}
					if len(fallbackText) == 0 {
						fallbackText = linkPreview.CanonicalURL
					}
				}
				content := &event.MessageEventContent{
					MsgType: event.MsgText,
					Body:    fallbackText,
				}
				mt.Portal.SetReply(content, msg)
				prepared.Content = content
			} else {
				mt.log.Debugfln("Dropping message %s because it has no rich link", msg.GUID)
			}
		case *pb.Item_GroupNameChange:
			prepared.NameChange = msg
		case *pb.Item_GroupAvatarChange:
			prepared.AvatarChange = msg
		case *pb.Item_GroupParticipantChange:
			prepared.MemberChange = msg
		case *pb.Item_Tapback:
			tapback := item.GetTapback()
			prepared.Type = event.EventReaction

			lookupRelationID := func() id.EventID {
				if chatGUID, tapbackTargetGUID := msg.GetChatGUID(), tapback.GetTapback().GetTarget(); chatGUID != nil && len(tapbackTargetGUID) != 0 {
					row := mt.Bridge.DB.Message.GetByGUID2(msg.ChatGUID.ToString(), tapback.GetTapback().GetTarget())
					if row != nil {
						return row.MXID
					}
				}
				return id.EventID("")
			}

			prepared.Content = &event.ReactionEventContent{
				RelatesTo: event.RelatesTo{
					// This does not work for backfilling in synapse mode!
					EventID: lookupRelationID(),
					Type:    event.RelAnnotation,
					Key:     (imessage.TapbackType)(tapback.Tapback.Type).Emoji(),
				},
			}
		case *pb.Item_Phantom:
			mt.log.Debugfln("Witnessed phantom item %s/%s", item.GetPhantom().GetTypeString(), item.GetPhantom().GetDebugDescription())
		}
		if len(msg.Service) > 0 {
			prepared.Extra[bridgeInfoService] = msg.Service
		}
		if msg.MessageMetadata != nil {
			prepared.Extra["com.beeper.message_metadata"] = msg.GetMessageMetadata().ConvertToMap()
		}
		prepared.Extra["com.beeper.proto.network_message_origin"] = msg.ChatGUID.ToString()
		prepared.Extra["com.beeper.proto.network_message_sender"] = msg.Sender.ToString()
		prepared.Extra["com.beeper.proto.network_message_id"] = msg.GUID
		mt.PreparedEvents = append(mt.PreparedEvents, prepared)
	}
}

func (pm *PreparedEvent) realizeAvatarChange(portal *Portal) *id.EventID {
	change := pm.AvatarChange.GetItems()[0].GetGroupAvatarChange()
	if avatar := change.GetNewAvatar(); avatar != nil && change.GetAction() == pb.GroupActionType_GroupActionAdd {
		return portal.UpdateAvatar(avatar, pm.Puppet.GetIntent())
	} else if change.GetAction() == pb.GroupActionType_GroupActionRemove {
		// TODO
	} else {
		portal.log.Warnfln("Unexpected group action type %d in avatar change item", change.GetAction())
	}
	return nil
}

func (pm *PreparedEvent) realizeMemberChange(portal *Portal) *id.EventID {
	change := pm.MemberChange.GetItems()[0].GetGroupParticipantChange()
	target := change.GetTarget()
	if len(target) == 0 {
		return nil
	}
	puppet := portal.bridge.GetPuppetByLocalID(target)
	puppet.Sync()
	if change.Action == pb.GroupActionType_GroupActionAdd {
		return portal.setMembership(pm.Puppet.GetIntent(), puppet, event.MembershipJoin, pm.MemberChange.Time.AsTime().UnixMilli())
	} else if change.Action == pb.GroupActionType_GroupActionRemove {
		// TODO make sure this won't break anything and enable it
		//return portal.setMembership(intent, puppet, event.MembershipLeave, dbMessage.Timestamp)
	} else {
		portal.log.Warnfln("Unexpected group action type %d in member change item", change.Action)
	}
	return nil
}

const firstEventIDPrefix = "__mautrix_nosip_first_event_id_"
const nextBatchGUIDStartPrefix = "__mautrix_nosip_next_batch_guid_start_"
const nextBatchIDPrefix = "__mautrix_nosip_next_batch_id_"

func (mt *IncomingMessageIngress) firstEventIDKey() string {
	return firstEventIDPrefix + mt.Portal.MXID.String()
}

func (mt *IncomingMessageIngress) loadFirstEventID() id.EventID {
	return id.EventID(mt.Portal.bridge.DB.KV.Get(mt.firstEventIDKey()))
}

func (mt *IncomingMessageIngress) createFirstEvent() {
	firstEventResp, err := mt.Portal.MainIntent().SendMessageEvent(mt.Portal.MXID, PortalCreationDummyEvent, struct{}{})
	if err != nil {
		mt.log.Errorln("Failed to send dummy event to mark portal creation:", err)
	} else {
		mt.Portal.bridge.DB.KV.Set(mt.firstEventIDKey(), firstEventResp.EventID.String())
	}
}

func (mt *IncomingMessageIngress) loadNextBatchGUID() string {
	return ""
}

func (mt *IncomingMessageIngress) nextBatchIDKey() string {
	return nextBatchIDPrefix + mt.Portal.MXID.String()
}

func (mt *IncomingMessageIngress) loadNextBatchID() id.BatchID {
	return id.BatchID(mt.Portal.bridge.DB.KV.Get(mt.nextBatchIDKey()))
}

func (mt *IncomingMessageIngress) saveNextBatchID(bid id.BatchID) {
	mt.Portal.bridge.DB.KV.Set(mt.nextBatchIDKey(), bid.String())
}

const maxBatchCount = 4096

func (mt *IncomingMessageIngress) sendNextBatch(isForward, isLatest bool, prevEventID id.EventID) (resp *mautrix.RespBatchSend, beforeFirstMessageTimestampMillis int64) {
	var req mautrix.ReqBatchSend

	if !isForward {
		firstEventID := mt.loadFirstEventID()
		nextBatchID := mt.loadNextBatchID()
		if firstEventID != "" || nextBatchID != "" {
			req.PrevEventID = firstEventID
			req.BatchID = nextBatchID
		} else {
			mt.log.Warnfln("Can't backfill %d messages through %s to chat: first event ID not known", 0, mt.Portal.MXID)
			return nil, 0
		}
	} else {
		req.PrevEventID = prevEventID
	}

	messages := make([]PreparedEvent, 0)

	capped := false
	for index, msg := range mt.PreparedEvents {
		if len(messages) >= maxBatchCount {
			// STOP IT!!!! NO MORE.
			mt.PreparedEvents = mt.PreparedEvents[index:]
			capped = true
			break
		}

		// already backfilled
		if mt.Bridge.DB.Message.GetByGUID2(mt.Portal.GUID, msg.GUID) != nil {
			continue
		}

		// insert the message at the front of the array for reverse chronological insertion
		messages = append(messages, msg)
	}
	if !capped {
		mt.PreparedEvents = make([]PreparedEvent, 0)
	}

	mt.log.Infofln("Preparing to backfill %d messages to %s", len(messages), mt.Portal.GUID)

	beforeFirstMessageTimestampMillis = int64(messages[len(messages)-1].GetMessageTimestamp()) - 1

	addedMembers := make(map[id.UserID]struct{})
	addMember := func(puppet core.IPuppet) {
		if _, alreadyAdded := addedMembers[puppet.GetMXID()]; alreadyAdded {
			return
		}
		mt.log.Debugfln("Adding puppet %s to state events for backfill", puppet.GetMXID())
		mxid := puppet.GetMXID().String()
		content := event.MemberEventContent{
			Membership:  event.MembershipJoin,
			Displayname: puppet.GetDisplayname(),
			AvatarURL:   puppet.GetAvatarURL().CUString(),
		}
		inviteContent := content
		inviteContent.Membership = event.MembershipInvite
		req.StateEventsAtStart = append(req.StateEventsAtStart, &event.Event{
			Type:      event.StateMember,
			Sender:    mt.Portal.MainIntent().UserID,
			StateKey:  &mxid,
			Timestamp: beforeFirstMessageTimestampMillis,
			Content:   event.Content{Parsed: &inviteContent},
		}, &event.Event{
			Type:      event.StateMember,
			Sender:    puppet.GetMXID(),
			StateKey:  &mxid,
			Timestamp: beforeFirstMessageTimestampMillis,
			Content:   event.Content{Parsed: &content},
		})
		addedMembers[puppet.GetMXID()] = struct{}{}
	}

	// The messages are ordered newest to oldest, so iterate them in reverse order.
	for _, msg := range messages {
		puppet, intent := msg.Puppet, msg.Puppet.GetIntent()
		if !intent.IsCustomPuppet && !mt.Portal.bridge.StateStore.IsInRoom(mt.Portal.MXID, puppet.GetMXID()) {
			addMember(puppet)
		}

		wrapped, err := msg.Wrap(mt.Portal)
		if err != nil {
			panic("Failed to wrap!")
		}
		mt.log.Debugfln("Wrapped %s in chat %s", msg.GUID, mt.Portal.GUID)
		req.Events = append(req.Events, wrapped)
	}

	if len(req.BatchID) == 0 || isForward {
		mt.log.Debugln("Sending a dummy event to avoid forward extremity errors with backfill")
		_, err := mt.Portal.MainIntent().SendMessageEvent(mt.Portal.MXID, PreBackfillDummyEvent, struct{}{})
		if err != nil {
			mt.log.Warnln("Error sending pre-backfill dummy event:", err)
		}
	}

	var err error
	resp, err = mt.Portal.MainIntent().BatchSend(mt.Portal.MXID, &req)
	if err != nil {
		mt.log.Errorln("Error batch sending messages:", err)
		return
	} else {
		txn, err := mt.Bridge.DB.Begin()
		if err != nil {
			mt.log.Errorln("Failed to start transaction to save batch messages:", err)
			return
		}

		// Do the following block in the transaction
		{
			// portal.finishBatch(txn, resp.EventIDs, infos)
			for idx, eventID := range resp.EventIDs {
				msg := messages[idx]
				msg.Insert(mt.Portal, eventID, txn)
			}
			mt.Portal.Update(txn)
		}

		err = txn.Commit()
		mt.saveNextBatchID(resp.NextBatchID)
		if err != nil {
			mt.log.Errorln("Failed to commit transaction to save batch messages:", err)
			return
		}
		return resp, beforeFirstMessageTimestampMillis
	}
}

var (
	PortalCreationDummyEvent = event.Type{Type: "fi.mau.dummy.portal_created", Class: event.MessageEventType}
	PreBackfillDummyEvent    = event.Type{Type: "fi.mau.dummy.pre_backfill", Class: event.MessageEventType}

	HistorySyncMarker = event.Type{Type: "org.matrix.msc2716.marker", Class: event.MessageEventType}

	BackfillStatusEvent = event.Type{Type: "com.beeper.backfill_status", Class: event.StateEventType}
)

func (mt *IncomingMessageIngress) Backfill(forward bool) {
	var (
		resp                              *mautrix.RespBatchSend
		beforeFirstMessageTimestampMillis int64
	)
	var forwardPrevID id.EventID
	// var timeEnd, timeStart *time.Time
	var isLatestEvents bool
	// portal.latestEventBackfillLock.Lock()
	if forward {
		// TODO this overrides the TimeStart set when enqueuing the backfill
		//      maybe the enqueue should instead include the prev event ID
		lastMessage := mt.Bridge.DB.Message.GetLastInChat(mt.Portal.GUID)
		forwardPrevID = lastMessage.MXID
		// start := lastMessage.Time().Add(1 * time.Second)
		// timeStart = &start
		// Sending events at the end of the room (= latest events)
		isLatestEvents = true
	} else {
		firstMessage := mt.Bridge.DB.Message.GetFirstInChat(mt.Portal.GUID)
		if firstMessage != nil {
			// end := firstMessage.Time().Add(-1 * time.Second)
			// timeEnd = &end
			// mt.log.Debugfln("Limiting backfill to end at %v", end)
		} else {
			// Portal is empty -> events are latest
			isLatestEvents = true
		}
	}

	var insertionEventIds []id.EventID
	for len(mt.PreparedEvents) > 0 {
		resp, beforeFirstMessageTimestampMillis = mt.sendNextBatch(forward, isLatestEvents, forwardPrevID)
		if resp != nil && (resp.BaseInsertionEventID != "" || !isLatestEvents) {
			insertionEventIds = append(insertionEventIds, resp.BaseInsertionEventID)
		}
	}
	if len(insertionEventIds) > 0 {
		mt.sendPostBackfillDummy(
			time.Unix(beforeFirstMessageTimestampMillis, 0),
			insertionEventIds[0])
	}
}

func (pm *PreparedEvent) GetMessageTimestamp() int64 {
	return pm.Original.Time.AsTime().UnixMilli()
}

func (mt *IncomingMessageIngress) sendPostBackfillDummy(lastTimestamp time.Time, insertionEventId id.EventID) {
	resp, err := mt.Portal.MainIntent().SendMessageEvent(mt.Portal.MXID, HistorySyncMarker, map[string]interface{}{
		"org.matrix.msc2716.marker.insertion": insertionEventId,
		//"m.marker.insertion":                  insertionEventId,
	})
	if err != nil {
		mt.log.Errorln("Error sending post-backfill dummy event:", err)
		return
	}
	msg := mt.Bridge.DB.Message.New()
	msg.ChatGUID = mt.Portal.GUID
	msg.MXID = resp.EventID
	msg.GUID = resp.EventID.String()
	msg.GUID2 = resp.EventID.String()
	msg.Timestamp = lastTimestamp.Add(1 * time.Second).UnixNano()
	msg.Insert(nil)
}

func (pm *PreparedEvent) Wrap(portal *Portal) (*event.Event, error) {
	eventType, content, err := portal.wrapMessage(pm.Puppet.GetIntent(), pm.Type, pm.Content, pm.Extra)
	if err != nil {
		return nil, err
	}
	return &event.Event{
		Sender:    pm.Puppet.GetIntent().UserID,
		Type:      eventType,
		Timestamp: pm.GetMessageTimestamp(),
		Content:   content,
	}, nil
}

func (pm *PreparedEvent) Insert(portal *Portal, mxID id.EventID, txn *sql.Tx) {
	dbMessage := portal.bridge.DB.Message.New()
	dbMessage.ChatGUID = portal.GUID
	dbMessage.GUID = pm.Original.GUID
	dbMessage.Part = pm.part
	dbMessage.GUID2 = pm.GUID
	dbMessage.MXID = mxID
	dbMessage.Timestamp = pm.Original.Time.AsTime().UnixNano() / 1e6
	dbMessage.Insert(txn)
}
