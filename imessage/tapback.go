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

package imessage

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
)

const (
	TapbackLove TapbackType = iota + 2000
	TapbackLike
	TapbackDislike
	TapbackLaugh
	TapbackEmphasis
	TapbackQuestion

	TapbackRemoveOffset = 1000
)

type Tapback struct {
	TargetGUID string      `json:"target_guid"`
	TargetPart int         `json:"-"`
	Remove     bool        `json:"-"`
	Type       TapbackType `json:"type"`
}

var (
	ErrUnknownNormalTapbackTarget = errors.New("unrecognized formatting of normal tapback target")
	ErrInvalidTapbackTargetPart   = errors.New("tapback target part index is invalid")
	ErrUnknownTapbackTargetType   = errors.New("unrecognized tapback target type")
)

func (tapback *Tapback) Parse() (*Tapback, error) {
	if tapback.Type >= 3000 && tapback.Type < 4000 {
		tapback.Type -= TapbackRemoveOffset
		tapback.Remove = true
	}
	if strings.HasPrefix(tapback.TargetGUID, "bp:") {
		tapback.TargetGUID = tapback.TargetGUID[len("bp:"):]
	} else if strings.HasPrefix(tapback.TargetGUID, "p:") {
		targetParts := strings.Split(tapback.TargetGUID[len("p:"):], "/")
		if len(targetParts) == 2 {
			var err error
			tapback.TargetPart, err = strconv.Atoi(targetParts[0])
			if err != nil {
				return nil, fmt.Errorf("%w: '%s' (%v)", ErrInvalidTapbackTargetPart, tapback.TargetGUID, err)
			}
			tapback.TargetGUID = targetParts[1]
		} else {
			return nil, fmt.Errorf("%w: '%s'", ErrUnknownNormalTapbackTarget, tapback.TargetGUID)
		}
	} else {
		return nil, fmt.Errorf("%w: '%s'", ErrUnknownTapbackTargetType, tapback.TargetGUID)
	}
	return tapback, nil
}

type TapbackType pb.TapbackType

func TapbackFromEmoji(emoji string) pb.TapbackType {
	switch []rune(emoji)[0] {
	case '\u2665', '\u2764', '\U0001f499', '\U0001f49a', '\U0001f90e', '\U0001f5a4', '\U0001f90d', '\U0001f9e1',
		'\U0001f49b', '\U0001f49c', '\U0001f496', '\u2763', '\U0001f495', '\U0001f49f':
		// '♥', '❤', '💙', '💚', '🤎', '🖤', '🤍', '🧡', '💛', '💜', '💖', '❣', '💕', '💟'
		return pb.TapbackType_TapbackLove
	case '\U0001f44d': // '👍'
		return pb.TapbackType_TapbackLike
	case '\U0001f44e': // '👎'
		return pb.TapbackType_TapbackDislike
	case '\U0001f602', '\U0001f639', '\U0001f606', '\U0001f923': // '😂', '😹', '😆', '🤣'
		return pb.TapbackType_TapbackLaugh
	case '\u2755', '\u2757', '\u203c': // '❕', '❗', '‼',
		return pb.TapbackType_TapbackEmphasis
	case '\u2753', '\u2754': // '❓', '❔'
		return pb.TapbackType_TapbackQuestion
	default:
		return 0
	}
}

func (amt TapbackType) String() string {
	return amt.Emoji()
}

func (amt TapbackType) Emoji() string {
	switch (pb.TapbackType)(amt) {
	case 0:
		return ""
	case pb.TapbackType_TapbackLove:
		fallthrough
	case pb.TapbackType_TapbackRemoveLove:
		return "\u2764\ufe0f" // "❤️"
	case pb.TapbackType_TapbackLike:
		fallthrough
	case pb.TapbackType_TapbackRemoveLike:
		return "\U0001f44d\ufe0f" // "👍️"
	case pb.TapbackType_TapbackDislike:
		fallthrough
	case pb.TapbackType_TapbackRemoveDislike:
		return "\U0001f44e\ufe0f" // "👎️"
	case pb.TapbackType_TapbackLaugh:
		fallthrough
	case pb.TapbackType_TapbackRemoveLaugh:
		return "\U0001f602" // "😂"
	case pb.TapbackType_TapbackEmphasis:
		fallthrough
	case pb.TapbackType_TapbackRemoveEmphasis:
		return "\u203c\ufe0f" // "‼️"
	case pb.TapbackType_TapbackQuestion:
		fallthrough
	case pb.TapbackType_TapbackRemoveQuestion:
		return "\u2753\ufe0f" // "❓️"
	default:
		return "\ufffd" // "�"
	}
}
