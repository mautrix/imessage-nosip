-- v15: Add guid2 column

ALTER TABLE message RENAME TO old_message;
ALTER TABLE tapback RENAME TO old_tapback;

CREATE TABLE message (
	chat_guid     TEXT REFERENCES portal(guid) ON DELETE CASCADE ON UPDATE CASCADE,
	guid2 		  TEXT UNIQUE,
	guid          TEXT,
	part          INTEGER,
	mxid          TEXT NOT NULL UNIQUE,
	sender_guid   TEXT NOT NULL,
	timestamp     BIGINT NOT NULL,
	PRIMARY KEY (chat_guid, guid, part)
);

INSERT INTO message SELECT chat_guid, NULL, guid, part, mxid, sender_guid, timestamp FROM old_message;

CREATE TABLE tapback (
	chat_guid    TEXT,
	message_guid TEXT,
	message_part INTEGER,
	sender_guid  TEXT,
	type         INTEGER NOT NULL,
	mxid         TEXT NOT NULL UNIQUE, guid TEXT DEFAULT NULL,
    guid         TEXT DEFAULT NULL,
	PRIMARY KEY (chat_guid, message_guid, message_part, sender_guid),
	FOREIGN KEY (chat_guid, message_guid, message_part) REFERENCES message(chat_guid, guid, part) ON DELETE CASCADE ON UPDATE CASCADE
);

INSERT INTO tapback SELECT chat_guid, message_guid, message_part, sender_guid, type, mxid, guid FROM old_tapback;

