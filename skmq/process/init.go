package process

import (
	"github.com/sikong-mq/skmq/base"
)

var (
	Configuration   *base.MQConfig
	DBConfiguration *base.DBConfig
)

func init() {
	Configuration = &base.MQConfig{
		RetryTimes:   5,
		ACKTimeout:   60000,
		Rate:         1000,
		ListenerHost: "127.0.0.1",
		ListenerPort: "1734"}

	DBConfiguration = &base.DBConfig{
		Address:      "127.0.0.1:6379",
		DB:           1}

}