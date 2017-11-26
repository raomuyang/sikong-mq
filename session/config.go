package session

type MQConfig struct {
	// The max retry times of message push to consumer
	RetryTimes int

	ACKTimeout int

	// To limit the queue size,
	// a rate limiter will distributes permits at a configurable rate
	QueueSize int

	ListenerHost string

	ListenerPort string
}

type DBConfig struct {
	// host:port
	Address string

	// Optional(If open the password authentication)
	Password string

	// Maximum number of idle connections in the pool.
	MaxIdle int

	// Maximum number of connections allocated by the pool at a given time.
	// When zero, there is no limit on the number of connections in the pool.
	MaxActive int

	// Close connections after remaining idle for this duration. If the value
	// is zero, then idle connections are not closed. Applications should set
	// the timeout to a value less than the server's timeout.
	IdleTimeout int

	// If MPending is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	// Redis db index
	DB int

	ReadTimeout int

	WriteTimeout int

	DialConnectTimeout int
}

var (
	Configuration   *MQConfig
	DBConfiguration *DBConfig
)

func init() {
	Configuration = &MQConfig{
		RetryTimes:   5,
		ACKTimeout:   3200,
		QueueSize:    10000,
		ListenerHost: "127.0.0.1",
		ListenerPort: "1734"}

	DBConfiguration = &DBConfig{
		Address:      "127.0.0.1:6379",
		DB:           1}

}
