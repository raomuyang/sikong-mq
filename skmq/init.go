package skmq

import (
	"github.com/raomuyang/sikong-mq/skmq/base"
	"github.com/raomuyang/sikong-mq/skmq/exchange"
	"github.com/raomuyang/sikong-mq/skmq/process"
)

var (
	Trace = process.Trace
	Info  = process.Info
	Warn  = process.Warn
	Err   = process.Err
)

var (
	deadLetterHandler DeadLetterHandler

	Configuration   *base.MQConfig
	DBConfiguration *base.DBConfig

	msgHandler   process.MessageHandler
	msgCache     process.Cache
	dataExchange exchange.Exchange
)

func init() {
	Configuration = &base.MQConfig{
		RetryTimes:   5,
		ACKTimeout:   60000,
		Rate:         1000,
		ListenerHost: "127.0.0.1",
		ListenerPort: "1734"}

	DBConfiguration = &base.DBConfig{
		Address: "127.0.0.1:6379",
		DB:      1}

}

func init() {
	v := interface{}(&DefaultDeadLetterHandler{})
	deadLetterHandler = v.(DeadLetterHandler)
}
