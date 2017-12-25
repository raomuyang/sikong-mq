package skmq

import (
	"github.com/sikong-mq/skmq/base"
)

type DeadLetterHandlerFunc func(message base.Message)

func SetDeadLetterHander(handlerFunc DeadLetterHandlerFunc)  {
	DLHandler = handlerFunc
}