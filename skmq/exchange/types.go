package exchange

import (
	"github.com/raomuyang/sikong-mq/skmq/base"
	"github.com/raomuyang/sikong-mq/skmq/process"
)

type MQConfig base.MQConfig
type DBConfig base.DBConfig

var (
	Trace = process.Trace
	Info  = process.Info
	Warn  = process.Warn
	Err   = process.Err
)
