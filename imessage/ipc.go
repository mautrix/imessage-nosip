// mautrix-imessage - A Matrix-iMessage puppeting bridge.
// Copyright (C) 2022 Tulir Asokan
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

package imessage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	log "maunium.net/go/maulogger/v2"

	"go.mau.fi/mautrix-imessage-nosip/ipc"
	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
)

type APIWith interface {
	API
	Set(*ipc.Processor)
}

type iOSConnector struct {
	IPC                 *ipc.Processor
	bridge              Bridge
	log                 log.Logger
	messageChan         chan *pb.Message
	receiptChan         chan *pb.ReadReceipt
	typingChan          chan *pb.TypingNotification
	chatChan            chan *pb.ChatInfo
	contactChan         chan *pb.Contact
	messageStatusChan   chan *pb.SendMessageStatus
	isNoSIP             bool
	mergeChats          bool
	path                string
	args                []string
	proc                *exec.Cmd
	procLog             log.Logger
	printPayloadContent bool
	pingInterval        time.Duration
	stopPinger          chan bool
}

func NewiOSConnector(bridge Bridge) APIWith {
	logger := bridge.GetLog().Sub("iMessage").Sub("Mac-noSIP")
	processLogger := bridge.GetLog().Sub("iMessage").Sub("Barcelona")
	return &iOSConnector{
		log:                 logger,
		bridge:              bridge,
		messageChan:         make(chan *pb.Message, 256),
		receiptChan:         make(chan *pb.ReadReceipt, 32),
		typingChan:          make(chan *pb.TypingNotification, 32),
		chatChan:            make(chan *pb.ChatInfo, 32),
		contactChan:         make(chan *pb.Contact, 2048),
		messageStatusChan:   make(chan *pb.SendMessageStatus, 32),
		isNoSIP:             bridge.GetConnectorConfig().Platform == "mac-nosip",
		mergeChats:          bridge.GetConnectorConfig().ChatMerging,
		path:                bridge.GetConnectorConfig().IMRestPath,
		args:                bridge.GetConnectorConfig().IMRestArgs,
		procLog:             processLogger,
		printPayloadContent: bridge.GetConnectorConfig().LogIPCPayloads,
		pingInterval:        time.Duration(bridge.GetConnectorConfig().PingInterval) * time.Second,
		stopPinger:          make(chan bool, 8),
	}
}

func (ios *iOSConnector) Set(proc *ipc.Processor) {
	ios.IPC = proc
}

func (ios *iOSConnector) Start(readyCallback func()) error {
	ios.log.Debugln("Preparing to execute", ios.path)
	args := ios.args
	if ios.mergeChats {
		args = append(args, "--enable-merged-chats")
	}
	ios.proc = exec.Command(ios.path, args...)

	if runtime.GOOS == "ios" {
		ios.log.Debugln("Running Barcelona connector on iOS, temp files will be world-readable")
		TempFilePermissions = 0644
		TempDirPermissions = 0755
	}

	stdout, err := ios.proc.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to get subprocess stdout pipe: %w", err)
	}
	stdin, err := ios.proc.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to get subprocess stdin pipe: %w", err)
	}

	ipcProc := ipc.NewCustomProcessor(stdin, stdout, ios.log, ios.printPayloadContent)
	ios.Set(ipcProc)

	go func() {
		ipcProc.Loop()
		if ios.proc.ProcessState.Exited() {
			ios.log.Errorfln("Barcelona died with exit code %d, exiting bridge...", ios.proc.ProcessState.ExitCode())
			os.Exit(ios.proc.ProcessState.ExitCode())
		}
	}()

	err = ios.proc.Start()
	if err != nil {
		return fmt.Errorf("failed to start imessage-rest: %w", err)
	}

	ios.log.Debugln("Process started, PID", ios.proc.Process.Pid)

	go ios.pingLoop(ipcProc)

	ios.IPC.SetHandler((*pb.Payload_Message)(nil), ios.handleIncomingMessage)
	ios.IPC.SetHandler((*pb.Payload_ReadReceipt)(nil), ios.handleIncomingReadReceipt)
	ios.IPC.SetHandler((*pb.Payload_TypingNotification)(nil), ios.handleIncomingTypingNotification)
	ios.IPC.SetHandler((*pb.Payload_Chat)(nil), ios.handleIncomingChat)
	ios.IPC.SetHandler((*pb.Payload_BridgeStatus)(nil), ios.handleIncomingStatus)
	ios.IPC.SetHandler((*pb.Payload_Contact)(nil), ios.handleIncomingContact)
	ios.IPC.SetHandler((*pb.Payload_SendMessageStatus)(nil), ios.handleIncomingSendMessageStatus)
	ios.IPC.SetHandler((*pb.Payload_Log)(nil), ios.HandleIncomingLog)
	readyCallback()
	return nil
}

func (ios *iOSConnector) Stop() {
	if ios.proc == nil || ios.proc.ProcessState == nil || ios.proc.ProcessState.Exited() {
		ios.log.Debugln("Barcelona subprocess not running when Stop was called")
		return
	}
	ios.stopPinger <- true
	err := ios.proc.Process.Signal(syscall.SIGTERM)
	if err != nil && !errors.Is(err, os.ErrProcessDone) {
		ios.log.Warnln("Failed to send SIGTERM to Barcelona process:", err)
	}
	time.AfterFunc(3*time.Second, func() {
		err = ios.proc.Process.Kill()
		if err != nil && !errors.Is(err, os.ErrProcessDone) {
			ios.log.Warnln("Failed to kill Barcelona process:", err)
		}
	})
	err = ios.proc.Wait()
	if err != nil {
		ios.log.Warnln("Error waiting for Barcelona process:", err)
	}
}

func (ios *iOSConnector) pingLoop(ipcProc *ipc.Processor) {
	for {
		pong, err := ipcProc.RequestAsync(&pb.Payload_Ping{Ping: &pb.Ping{}})
		if err != nil {
			ios.log.Fatalln("Failed to send ping to Barcelona")
			os.Exit(254)
		}
		timeout := time.After(ios.pingInterval)
		select {
		case <-ios.stopPinger:
			return
		case <-timeout:
			ios.log.Fatalfln("Didn't receive pong from Barcelona within %s", ios.pingInterval)
			os.Exit(255)
		case <-pong:
		}
		select {
		case <-timeout:
		case <-ios.stopPinger:
			return
		}
	}
}

func (ios *iOSConnector) postprocessMessage(message *pb.Message) {
	if !message.IsFromMe {
		if ios.Capabilities().MergedChats {
			message.Sender.Service = "iMessage"
		}
	}
}

func (ios *iOSConnector) handleIncomingMessage(data ipc.RawMessage) interface{} {
	message := data.(*pb.Payload_Message).Message
	ios.postprocessMessage(message)
	select {
	case ios.messageChan <- message:
	default:
		ios.log.Warnln("Incoming message buffer is full")
	}
	return nil
}

func (ios *iOSConnector) handleIncomingReadReceipt(data ipc.RawMessage) interface{} {
	receipt := data.(*pb.Payload_ReadReceipt).ReadReceipt

	select {
	case ios.receiptChan <- receipt:
	default:
		ios.log.Warnln("Incoming receipt buffer is full")
	}
	return nil
}

func (ios *iOSConnector) handleIncomingTypingNotification(data ipc.RawMessage) interface{} {
	notif := data.(*pb.Payload_TypingNotification).TypingNotification
	select {
	case ios.typingChan <- notif:
	default:
		ios.log.Warnln("Incoming typing notification buffer is full")
	}
	return nil
}

func (ios *iOSConnector) handleIncomingChat(data ipc.RawMessage) interface{} {
	chat := data.(*pb.Payload_Chat).Chat
	select {
	case ios.chatChan <- chat:
	default:
		ios.log.Warnln("Incoming chat buffer is full")
	}
	return nil
}

func (ios *iOSConnector) handleIncomingStatus(data ipc.RawMessage) interface{} {
	state := data.(*pb.Payload_BridgeStatus).BridgeStatus
	ios.bridge.SendBridgeStatus(state)
	return nil
}

func (ios *iOSConnector) handleIncomingContact(data ipc.RawMessage) interface{} {
	contact := data.(*pb.Payload_Contact).Contact
	select {
	case ios.contactChan <- contact:
	default:
		ios.log.Warnln("Incoming contact buffer is full")
	}
	return nil
}

func (ios *iOSConnector) handleIncomingSendMessageStatus(data ipc.RawMessage) interface{} {
	status := data.(*pb.Payload_SendMessageStatus).SendMessageStatus
	select {
	case ios.messageStatusChan <- status:
	default:
		ios.log.Warnln("Incoming send message status buffer is full")
	}
	return nil
}

func guidTo(id string) *pb.GUID {
	guid := pb.GUID(pb.ParseIdentifier(id))
	return &guid
}

func (ios *iOSConnector) GetMessagesSinceDate(chatID string, minDate time.Time) ([]*pb.Message, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), &pb.Payload_GetMessagesAfter{&pb.GetMessagesAfterRequest{
		ChatGUID:  guidTo(chatID),
		Timestamp: timestamppb.New(minDate),
	}}, &resp)
	messages := resp.GetMessages().GetMessages()
	for _, msg := range messages {
		ios.postprocessMessage(msg)
	}
	return messages, err
}

func (ios *iOSConnector) GetMessagesWithLimit(chatID string, limit int) ([]*pb.Message, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), &pb.Payload_GetRecentMessages{&pb.GetRecentMessagesRequest{
		ChatGUID: guidTo(chatID),
		Limit:    int64(limit),
	}}, &resp)
	messages := resp.GetMessages().GetMessages()
	for _, msg := range messages {
		ios.postprocessMessage(msg)
	}
	return messages, err
}

func (ios *iOSConnector) GetChatsWithMessagesAfter(minDate time.Time) (resp []string, err error) {
	var ipcResp *pb.Payload
	err = ios.IPC.Request(context.Background(), &pb.Payload_GetChats{&pb.GetChatsRequest{
		MinTimestamp: timestamppb.New(minDate),
	}}, &ipcResp)
	if err != nil {
		return nil, err
	}
	guids := ipcResp.GetChatList().GetChats()
	resp = make([]string, len(guids))
	for i, guid := range guids {
		resp[i] = guid.ToString()
	}
	return resp, err
}

func (ios *iOSConnector) GetMessagesWithQuery(query *pb.HistoryQuery) ([]*pb.Message, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), &pb.Payload_HistoryQuery{query}, &resp)
	messages := resp.GetMessages().GetMessages()
	for _, msg := range messages {
		ios.postprocessMessage(msg)
	}
	return messages, err
}

func (ios *iOSConnector) MessageChan() <-chan *pb.Message {
	return ios.messageChan
}

func (ios *iOSConnector) ReadReceiptChan() <-chan *pb.ReadReceipt {
	return ios.receiptChan
}

func (ios *iOSConnector) TypingNotificationChan() <-chan *pb.TypingNotification {
	return ios.typingChan
}

func (ios *iOSConnector) ChatChan() <-chan *pb.ChatInfo {
	return ios.chatChan
}

func (ios *iOSConnector) ContactChan() <-chan *pb.Contact {
	return ios.contactChan
}

func (ios *iOSConnector) MessageStatusChan() <-chan *pb.SendMessageStatus {
	return ios.messageStatusChan
}

func (ios *iOSConnector) GetContactInfo(identifier string) (*pb.Contact, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), &pb.Payload_GetContact_{GetContact_: &pb.GetContactRequest{Identifier: identifier}}, &resp)
	return resp.GetContact(), err
}

func (ios *iOSConnector) GetContactList() ([]*pb.Contact, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), nil, &resp)
	return resp.GetContacts().GetContacts(), err
}

func (ios *iOSConnector) GetChatInfo(chatID string) (*pb.ChatInfo, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), &pb.Payload_GetChat_{GetChat_: &pb.GetChatRequest{ChatGUID: guidTo(chatID)}}, &resp)
	return resp.GetChat(), err
}

func (ios *iOSConnector) GetGroupAvatar(chatID string) (*pb.Attachment, error) {
	var resp *pb.Payload
	err := ios.IPC.Request(context.Background(), &pb.Payload_GetChatAvatar{GetChatAvatar: &pb.GetChatAvatarRequest{ChatGUID: guidTo(chatID)}}, &resp)
	return resp.GetAttachment(), err
}

func makeReplyTarget(replyTo string, replyToPart int) *pb.MessageTarget {
	if len(replyTo) == 0 {
		return nil
	}
	return &pb.MessageTarget{GUID: replyTo, Part: int64(replyToPart)}
}

func (ios *iOSConnector) SendMessage2(chatID, replyTo string, replyToPart int, metadata *pb.Mapping, parts []pb.SendMessagePartType) (*pb.SendResponse, error) {
	var resp *pb.Payload

	formedParts := make([]*pb.SendMessagePart, len(parts))

	for index, part := range parts {
		formedParts[index] = &pb.SendMessagePart{
			Part: part,
		}
	}

	req := &pb.SendMessageRequest{
		ChatGUID:    guidTo(chatID),
		ReplyTarget: makeReplyTarget(replyTo, replyToPart),
		Metadata:    metadata,
		Parts:       formedParts,
	}

	err := ios.IPC.Request(context.Background(), &pb.Payload_SendMessage{req}, &resp)
	return resp.GetSendResponse(), err
}

func (ios *iOSConnector) SendMessage(chatID, text string, replyTo string, replyToPart int, richLink *pb.RichLink, metadata *pb.Mapping) (*pb.SendResponse, error) {
	return ios.SendMessage2(chatID, replyTo, replyToPart, metadata, []pb.SendMessagePartType{
		&pb.SendMessagePart_Text{Text: &pb.SendTextMessageRequest{
			Text:     text,
			RichLink: richLink,
		}},
	})
}

func (ios *iOSConnector) SendFile(chatID, text, filename string, pathOnDisk string, replyTo string, replyToPart int, mimeType string, voiceMemo bool, metadata *pb.Mapping) (*pb.SendResponse, error) {
	return ios.SendMessage2(chatID, replyTo, replyToPart, metadata, []pb.SendMessagePartType{
		&pb.SendMessagePart_Media{Media: &pb.SendMediaMessageRequest{
			Text: text,
			Attachment: &pb.Attachment{
				FileName:   filename,
				PathOnDisk: pathOnDisk,
				MimeType:   &mimeType,
			},
			IsAudioMessage: voiceMemo,
		}},
	})
}

func (ios *iOSConnector) SendFileCleanup(sendFileDir string) {
	_ = os.RemoveAll(sendFileDir)
}

func (ios *iOSConnector) SendTapback(chatID, targetGUID string, targetPart int, tapback pb.TapbackType) (*pb.SendResponse, error) {
	return ios.SendMessage2(chatID, "", 0, nil, []pb.SendMessagePartType{
		&pb.SendMessagePart_Tapback{Tapback: &pb.SendTapbackMessageRequest{
			Target: makeReplyTarget(targetGUID, targetPart),
			Type:   tapback,
		}},
	})
}

func (ios *iOSConnector) SendReadReceipt(chatID, readUpTo string) error {
	return ios.IPC.Send(&pb.Payload_SendReadReceipt{SendReadReceipt: &pb.SendReadReceiptRequest{
		ChatGUID: guidTo(chatID),
		ReadUpTo: readUpTo,
	}})
}

func (ios *iOSConnector) SendTypingNotification(chatID string, typing bool) error {
	return ios.IPC.Send(&pb.Payload_SetTyping{SetTyping: &pb.SetTypingRequest{
		ChatGUID: guidTo(chatID),
		Typing:   typing,
	}})
}

func (ios *iOSConnector) PreStartupSyncHook() (hookResp *pb.StartupSyncHookResponse, err error) {
	var resp *pb.Payload
	err = ios.IPC.Request(context.Background(), &pb.Payload_PreStartupSync{}, &resp)
	hookResp = resp.GetSyncHookResponse()
	return
}

func (ios *iOSConnector) ResolveIdentifier(identifier string) (resolved string, err error) {
	var resp *pb.Payload
	err = ios.IPC.Request(context.Background(), &pb.Payload_ResolveIdentifier{ResolveIdentifier: &pb.ResolveIdentifierRequest{
		Identifier: identifier,
	}}, &resp)
	resolved = resp.GetResolveIdentifierResponse().GUID.ToString()
	return
}

func (ios *iOSConnector) PrepareDM(guid string) error {
	return ios.IPC.Request(context.Background(), &pb.Payload_PrepareDM{&pb.PrepareDMRequest{GUID: guidTo(guid)}}, nil)
}

func (ios *iOSConnector) Capabilities() ConnectorCapabilities {
	return ConnectorCapabilities{
		MessageSendResponses:    true,
		SendTapbacks:            true,
		SendReadReceipts:        true,
		SendTypingNotifications: true,
		SendCaptions:            false,
		BridgeState:             false,
		MergedChats:             true,
		ChatBridgeResult:        false,
	}
}
