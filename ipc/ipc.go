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

package ipc

import (
	"context"
	"fmt"
	"io"
	"os"
	"reflect"
	"runtime/debug"
	"sync"
	"sync/atomic"

	// pio "github.com/gogo/protobuf/io"
	pb "github.com/open-imcore/barcelona-mautrix-protocol/mautrix-nosip-protobuf"
	log "maunium.net/go/maulogger/v2"
)

const (
	CommandResponse = "response"
	CommandError    = "error"
)

var (
	ErrUnknownCommand    = pb.Payload_Error{&pb.Error{Code: "unknown-command", Message: "Unknown command"}}
	ErrSizeLimitExceeded = pb.Payload_Error{&pb.Error{Code: "size_limit_exceeded"}}
	ErrTimeoutError      = pb.Payload_Error{&pb.Error{Code: "timeout"}}
	ErrUnsupportedError  = pb.Payload_Error{&pb.Error{Code: "unsupported"}}
)

type Command string

type Message pb.Payload

type OutgoingMessage pb.Payload
type IPCError pb.Error

func (err *IPCError) Error() string {
	return fmt.Sprintf("%s: %s", err.Code, err.Message)
}

func (err *IPCError) Is(other error) bool {
	otherErr, ok := other.(*IPCError)
	if !ok {
		return false
	}
	return otherErr.Code == err.Code
}

type RawMessage pb.PayloadCommand
type HandlerFunc func(message RawMessage) interface{}

type HandlerID interface{}

type Processor struct {
	log    log.Logger
	lock   *sync.Mutex
	stdout WriteCloser
	stdin  ReadCloser

	handlers   map[HandlerID]HandlerFunc
	waiters    map[int64]chan<- *pb.Payload
	waiterLock sync.Mutex
	reqID      int64

	printPayloadContent bool
}

func newProcessor(lock *sync.Mutex, output io.Writer, input io.Reader, logger log.Logger, printPayloadContent bool) *Processor {
	return &Processor{
		lock:                lock,
		log:                 logger,
		stdout:              NewDelimitedWriter(output),
		stdin:               NewDelimitedReader(input, 16384*64),
		handlers:            make(map[HandlerID]HandlerFunc),
		waiters:             make(map[int64]chan<- *pb.Payload),
		printPayloadContent: printPayloadContent,
	}
}

func NewCustomProcessor(output io.Writer, input io.Reader, logger log.Logger, printPayloadContent bool) *Processor {
	return newProcessor(&sync.Mutex{}, output, input, logger.Sub(""), printPayloadContent)
}

func NewStdioProcessor(logger log.Logger, printPayloadContent bool) *Processor {
	return newProcessor(&logger.(*log.BasicLogger).StdoutLock, os.Stdout, os.Stdin, logger.Sub(""), printPayloadContent)
}

func (ipc *Processor) Loop() {
	for {
		payload := pb.Payload{}
		if err := ipc.stdin.ReadMsg(&payload); err != nil {
			ipc.log.Warnfln("Dropping corrupted input")
			continue
		}

		preflect := payload.ProtoReflect()
		descriptor := preflect.Descriptor()
		oneofs := descriptor.Oneofs().Get(0)
		which := preflect.WhichOneof(oneofs)

		if payload.GetError() == nil && payload.GetLog() == nil {
			ipc.log.Debugfln("Received command: %s/%d", which.Name(), payload.ID)
		}

		if payload.IsResponse || payload.GetError() != nil {
			ipc.waiterLock.Lock()
			waiter, ok := ipc.waiters[payload.ID]
			if !ok {
				ipc.log.Warnln("Nothing waiting for  response to %d", payload.ID)
			} else {
				delete(ipc.waiters, payload.ID)
				waiter <- &payload
			}
			ipc.waiterLock.Unlock()
		} else {
			handler, ok := ipc.handlers[reflect.TypeOf(payload.Command)]
			if !ok {
				ipc.respond(payload.ID, &ErrUnknownCommand)
			} else {
				go ipc.callHandler(&payload, handler)
			}
		}
	}
}

func (ipc *Processor) Send(data pb.PayloadCommand) error {
	ipc.lock.Lock()
	err := ipc.stdout.WriteMsg(&pb.Payload{ID: -1, Command: data})
	ipc.lock.Unlock()
	return err
}

func (ipc *Processor) RequestAsync(data pb.PayloadCommand) (<-chan *pb.Payload, error) {
	respChan := make(chan *pb.Payload, 1)
	reqID := atomic.AddInt64(&ipc.reqID, 1)
	ipc.waiterLock.Lock()
	ipc.waiters[reqID] = respChan
	ipc.waiterLock.Unlock()
	ipc.lock.Lock()
	payload := pb.Payload{ID: reqID, Command: data, IsResponse: false}

	err := ipc.stdout.WriteMsg(&payload)
	ipc.lock.Unlock()
	if err != nil {
		ipc.waiterLock.Lock()
		delete(ipc.waiters, reqID)
		ipc.waiterLock.Unlock()
		close(respChan)
	}
	return respChan, err
}

func (ipc *Processor) Request(ctx context.Context, reqData pb.PayloadCommand, respData **pb.Payload) error {
	respChan, err := ipc.RequestAsync(reqData)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	select {
	case rawData := <-respChan:
		if respErr := rawData.GetError(); respErr != nil {
			return fmt.Errorf("%s: %s", respErr.Code, respErr.Message)
		}
		if respData != nil {
			*respData = rawData
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("context finished: %w", ctx.Err())
	}
}

func (ipc *Processor) callHandler(msg *pb.Payload, handler HandlerFunc) {
	defer func() {
		err := recover()
		if err != nil {
			ipc.log.Errorfln("Panic in  handler for %s: %v:\n%s", msg.Command, err, string(debug.Stack()))
			ipc.respondError(msg.ID, err)
		}
	}()
	resp := handler(msg.Command)
	if err, isErr := resp.(error); isErr {
		ipc.respondError(msg.ID, err)
	} else if command, isCommand := resp.(pb.PayloadCommand); isCommand {
		ipc.respond(msg.ID, command)
	} else if resp != nil {
		ipc.log.Warnfln("Attempt to respond to payload %d with unsupported data %v", msg.ID, resp)
	}
}

func (ipc *Processor) respondError(id int64, respErr interface{}) {
	var resp pb.Error
	if ipcErr, isRealError := respErr.(pb.Error); isRealError {
		resp = ipcErr
	} else if err, isError := respErr.(error); isError {
		resp = pb.Error{
			Code:    "error",
			Message: err.Error(),
		}
	} else {
		resp = pb.Error{
			Code:    "error",
			Message: fmt.Sprintf("%v", respErr),
		}
	}
	ipc.respond(id, &pb.Payload_Error{&resp})
}

func (ipc *Processor) respond(id int64, response pb.PayloadCommand) {
	if id == 0 && response == nil {
		// No point in replying
		return
	}
	ipc.lock.Lock()
	err := ipc.stdout.WriteMsg(&pb.Payload{ID: id, Command: response, IsResponse: true})
	ipc.lock.Unlock()
	if err != nil {
		ipc.log.Errorln("Failed to encode  response: %v", err)
	}
}

func (ipc *Processor) SetHandler(command interface{}, handler HandlerFunc) {
	ipc.handlers[reflect.TypeOf(command)] = handler
}
