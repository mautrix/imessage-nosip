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
	"go.mau.fi/mautrix-imessage-nosip/ipc"
	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
)

const (
	ReqSendMessage         ipc.Command = "send_message"
	ReqSendMedia           ipc.Command = "send_media"
	ReqSendTapback         ipc.Command = "send_tapback"
	ReqSendReadReceipt     ipc.Command = "send_read_receipt"
	ReqSetTyping           ipc.Command = "set_typing"
	ReqGetChats            ipc.Command = "get_chats"
	ReqGetChat             ipc.Command = "get_chat"
	ReqGetChatAvatar       ipc.Command = "get_chat_avatar"
	ReqGetContact          ipc.Command = "get_contact"
	ReqGetContactList      ipc.Command = "get_contact_list"
	ReqGetMessagesAfter    ipc.Command = "get_messages_after"
	ReqGetRecentMessages   ipc.Command = "get_recent_messages"
	ReqPreStartupSync      ipc.Command = "pre_startup_sync"
	ReqResolveIdentifier   ipc.Command = "resolve_identifier"
	ReqPrepareDM           ipc.Command = "prepare_dm"
	ReqMessageBridgeResult ipc.Command = "message_bridge_result"
	ReqChatBridgeResult    ipc.Command = "chat_bridge_result"
	ReqPing                ipc.Command = "ping"
)

type SendReadReceiptRequest pb.SendReadReceiptRequest

type SetTypingRequest pb.SetTypingRequest

type GetChatRequest pb.GetChatRequest

type GetChatsRequest pb.GetChatsRequest

type GetContactRequest pb.GetContactRequest

type GetContactListResponse pb.ContactList

type GetRecentMessagesRequest pb.GetRecentMessagesRequest

type GetMessagesAfterRequest pb.GetMessagesAfterRequest

type ResolveIdentifierRequest pb.ResolveIdentifierRequest

type ResolveIdentifierResponse pb.ResolveIdentifierResponse

type PrepareDMRequest pb.PrepareDMRequest
