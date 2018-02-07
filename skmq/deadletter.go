package skmq

import (
	"github.com/sikong-mq/skmq/base"
)

type DeadLetterHandler interface {
	Process(message base.Message)
}

type DefaultDeadLetterHandler struct{}

func (handler *DefaultDeadLetterHandler) Process(message base.Message) {
	Info.Printf("Process dead letter via default handler: %v \n", message)
	err := msgCache.DeleteMessage(message.MsgId)
	if err != nil {
		Warn.Printf("Delete dead letter failed, message: " + message.MsgId)
	}
}

func SetDeadLetterHandler(handler DeadLetterHandler) {
	deadLetterHandler = handler
}
