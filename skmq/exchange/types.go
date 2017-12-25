package exchange

import (
	"github.com/sikong-mq/skmq/process"
	"github.com/sikong-mq/skmq/base"
)

type MQConfig base.MQConfig
type DBConfig base.DBConfig

var (
	Trace = process.Trace
	Info = process.Info
	Warn = process.Warn
	Err = process.Err
)
