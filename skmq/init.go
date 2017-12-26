package skmq

import (
	"github.com/sikong-mq/skmq/process"
)

var (
	Trace = process.Trace
	Info = process.Info
	Warn = process.Warn
	Err = process.Err
)

var (
	deadLetterHandler DeadLetterHandler
)

func init() {
	v := interface{}(&DefaultDeadLetterHandler{})
	deadLetterHandler = v.(DeadLetterHandler)
}
