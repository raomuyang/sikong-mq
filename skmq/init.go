package skmq

import (
	"github.com/sikong-mq/skmq/process"
	"github.com/sikong-mq/skmq/base"
)

var (
	Trace = process.Trace
	Info = process.Info
	Warn = process.Warn
	Err = process.Err
)

var (
	DLHandler DeadLetterHandlerFunc
)

func init() {
	DLHandler = func(message base.Message) {
		err := process.DeleteMessage(message.MsgId)
		if err != nil {
			Warn.Printf("Delete dead letter failed, message: " + message.MsgId)
		}
	}
}
