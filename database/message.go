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

package database

import (
	"database/sql"
	"time"

	log "maunium.net/go/maulogger/v2"

	"maunium.net/go/mautrix/id"
	"maunium.net/go/mautrix/util/dbutil"
)

type MessageQuery struct {
	db  *Database
	log log.Logger
}

func (mq *MessageQuery) New() *Message {
	return &Message{
		table: table{
			db:  mq.db,
			log: mq.log,
		},
	}
}

func (mq *MessageQuery) GetIDsSince(chat string, since time.Time) (messages []string) {
	rows, err := mq.db.Query("SELECT guid FROM message WHERE chat_guid=$1 AND timestamp>=$2 AND part=0 ORDER BY timestamp ASC", chat, since.Unix()*1000)
	if err != nil || rows == nil {
		return nil
	}
	defer rows.Close()
	for rows.Next() {
		var msgID string
		err = rows.Scan(&msgID)
		if err != nil {
			mq.log.Errorln("Database scan failed:", err)
		} else {
			messages = append(messages, msgID)
		}
	}
	return
}

func (mq *MessageQuery) GetLastByGUID(chat string, guid string) *Message {
	return mq.get("SELECT chat_guid, guid2, guid, part, mxid, sender_guid, timestamp "+
		"FROM message WHERE chat_guid=$1 AND guid=$2 ORDER BY part DESC LIMIT 1", chat, guid)
}

func (mq *MessageQuery) GetByGUID(chat string, guid string, part int) *Message {
	return mq.get("SELECT chat_guid, guid2, guid, part, mxid, sender_guid, timestamp "+
		"FROM message WHERE chat_guid=$1 AND guid=$2 AND part=$3", chat, guid, part)
}

func (mq *MessageQuery) GetByGUID2(chat string, guid2 string) *Message {
	return mq.get("SELECT chat_guid, guid2, guid, part, mxid, sender_guid, timestamp "+
		"FROM message WHERE chat_guid=$1 AND guid2=$2", chat, guid2)
}

func (mq *MessageQuery) GetByMXID(mxid id.EventID) *Message {
	return mq.get("SELECT chat_guid, guid2, guid, part, mxid, sender_guid, timestamp "+
		"FROM message WHERE mxid=$1", mxid)
}

func (mq *MessageQuery) getAtEndInChat(chat string, order string) *Message {
	msg := mq.get("SELECT chat_guid, guid2, guid, part, mxid, sender_guid, timestamp "+
		"FROM message WHERE chat_guid=$1 ORDER BY timestamp "+order+" LIMIT 1", chat)
	if msg == nil || msg.Timestamp == 0 {
		// Old db, we don't know what the last message is.
		return nil
	}
	return msg
}

func (mq *MessageQuery) GetFirstInChat(chat string) *Message {
	return mq.getAtEndInChat(chat, "ASC")
}

func (mq *MessageQuery) GetLastInChat(chat string) *Message {
	return mq.getAtEndInChat(chat, "DESC")
}

func (mq *MessageQuery) get(query string, args ...interface{}) *Message {
	row := mq.db.QueryRow(query, args...)
	if row == nil {
		return nil
	}
	return mq.New().Scan(row)
}

type Message struct {
	table

	ChatGUID   string
	GUID2      string
	GUID       string
	Part       int
	MXID       id.EventID
	SenderGUID string
	Timestamp  int64
}

func (msg *Message) Time() time.Time {
	// Add 1 ms to avoid rounding down
	return time.Unix(msg.Timestamp/1000, ((msg.Timestamp%1000)+1)*int64(time.Millisecond))
}

func (msg *Message) Scan(row dbutil.Scannable) *Message {
	err := row.Scan(&msg.ChatGUID, &msg.GUID2, &msg.GUID, &msg.Part, &msg.MXID, &msg.SenderGUID, &msg.Timestamp)
	if err != nil {
		if err != sql.ErrNoRows {
			msg.log.Errorln("Database scan failed:", err)
		}
		return nil
	}
	return msg
}

func (msg *Message) Insert(txn *sql.Tx) {
	_, err := msg.exec(txn, "INSERT INTO message (chat_guid, guid2, guid, part, mxid, sender_guid, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		msg.ChatGUID, msg.GUID2, msg.GUID, msg.Part, msg.MXID, msg.SenderGUID, msg.Timestamp)
	if err != nil {
		msg.log.Warnfln("Failed to insert %s.%d@%s: %v", msg.GUID, msg.Part, msg.ChatGUID, err)
	}
}

func (msg *Message) Delete() {
	_, err := msg.db.Exec("DELETE FROM message WHERE (chat_guid=$1 AND guid=$2) OR (guid2=$3)", msg.ChatGUID, msg.GUID, msg.GUID2)
	if err != nil {
		msg.log.Warnfln("Failed to delete %s.%d@%s: %v", msg.GUID, msg.Part, msg.ChatGUID, err)
	}
}
