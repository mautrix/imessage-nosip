package imessage

import (
	"strings"

	"go.mau.fi/mautrix-imessage-nosip/ipc"
	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
	log "maunium.net/go/maulogger/v2"
)

type LogLine struct {
	Message  string                 `json:"message"`
	Level    string                 `json:"level"`
	Module   string                 `json:"module"`
	Metadata map[string]interface{} `json:"metadata"`
}

func getLevelFromName(name string) log.Level {
	switch strings.ToUpper(name) {
	case "DEBUG":
		return log.LevelDebug
	case "INFO":
		return log.LevelInfo
	case "WARN":
		return log.LevelWarn
	case "ERROR":
		return log.LevelError
	case "FATAL":
		return log.LevelFatal
	default:
		return log.Level{Name: name, Color: -1, Severity: 1}
	}
}

func (ios *iOSConnector) HandleIncomingLog(data ipc.RawMessage) interface{} {
	message := data.(*pb.Payload_Log).Log
	logger := ios.procLog.Subm(message.Module, message.Metadata.ConvertToMap())
	logger.Log(getLevelFromName(message.Level), message.Message)
	return nil
}
