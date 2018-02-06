package base


type MQConfig struct {
	// The max retry times of message push to consumer
	RetryTimes int `json:"retry_times"`

	// Millisecond, message ack timeout, enter to retry queue
	ACKTimeout int `json:"ack_timeout"`

	// A rate limiter will distributes permits at a configurable rate (n/second)
	Rate float64 `json:"rate"`

	ListenerHost string `json:"listener_host"`

	ListenerPort string `json:"listener_port"`

	LogLevel int `json:"log_level"`
}

type DBConfig struct {
	// host:port
	Address string `json:"address"`

	// Optional(If open the password authentication)
	Password string `json:"password"`

	// Maximum number of idle connections in the pool.
	MaxIdle int `json:"max_idle"`

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int `json:"max_active"`

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout int `json:"idle_timeout"`

	// If MPending is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool `json:"wait"`

	// Redis db index
	DB int `json:"db"`

	ReadTimeout int `json:"read_timeout"`

	WriteTimeout int `json:"write_timeout"`

	DialConnectTimeout int `json:"dial_connect_timeout"`
}
