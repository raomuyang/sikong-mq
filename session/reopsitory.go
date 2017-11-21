package session

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"strconv"
)

var (
	Pool *redis.Pool
)

func init() {
	Pool = &redis.Pool{
	}
}

type DBConfig struct {
	// host:port
	Address string

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

	// If Wait is true and the pool is at the MaxActive limit, then Get() waits
	// for a connection to be returned to the pool before returning.
	Wait bool

	DB int

	ReadTimeout int

	WriteTimeout int

	DialConnectTimeout int
}

func InitDBConfig(config DBConfig) *redis.Pool {
	Pool.MaxIdle = config.MaxIdle
	Pool.MaxActive = config.MaxActive
	Pool.Wait = config.Wait
	Pool.IdleTimeout = time.Duration(config.IdleTimeout) * time.Millisecond

	optionals := make([]redis.DialOption, 0)
	if config.DialConnectTimeout > 0 {
		optionals = append(optionals,
			redis.DialConnectTimeout(time.Duration(1000)*time.Millisecond))
	}
	if config.WriteTimeout+config.ReadTimeout > 0 {
		optionals = append(optionals,
			redis.DialReadTimeout(time.Duration(config.ReadTimeout)*time.Millisecond),
			redis.DialWriteTimeout(time.Duration(config.WriteTimeout)*time.Millisecond))
	}
	if len(config.Password) > 0 {
		optionals = append(optionals,
			redis.DialPassword(config.Password))
	}

	Pool.Dial = func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", config.Address, optionals...)
		if err != nil {
			return nil, err
		}
		c.Do("SELECT", config.DB)
		return c, nil
	}
	Pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
		_, err := c.Do("PING")
		if err != nil {
			println(err)
		}
		return err
	}

	return Pool
}

/**
	Save the information of consumer host,
	and it will be register in set: rec-set/application-id
 */
func SaveRecipientInfo(recipientInfo RecipientInfo) error {
	dbConn := Pool.Get()

	_, err := dbConn.Do("HMSET",
		recipientInfo.RecipientId,
		KAppId, recipientInfo.ApplicationId,
		KHost, recipientInfo.Host,
		KPort, recipientInfo.Port,
		KStatus, Alive)
	if err != nil {
		return err
	}
	key := KRecipientSet + "/" + recipientInfo.ApplicationId
	_, err = dbConn.Do("SADD", key, recipientInfo.RecipientId)
	return err
}

/**
	Return a consumer list
 */
func FindRecipients(applicationId string) ([]*RecipientInfo, error) {
	dbConn := Pool.Get()
	set := KRecipientSet + "/" + applicationId
	rep, err := redis.Strings(dbConn.Do("SMEMBERS", set))
	if err != nil {
		return nil, err
	}

	var list []*RecipientInfo
	for i := range rep {
		id := rep[i]
		result, err := redis.StringMap(dbConn.Do("HGETALL", id))
		if err != nil {
			continue
		}
		rec := RecipientInfo{
			ApplicationId: applicationId,
			RecipientId:   id,
			Host:          result[KHost],
			Port:          result[KPort],
			Status:        result[KStatus]}

		list = append(list, &rec)
	}
	if len(list) == 0 {
		return nil, err
	}
	return list, nil
}

/**
	Save message entity to redis, index by messageId
 */
func PushMessage(message Message) error {
	dbConn := Pool.Get()
	_, err := dbConn.Do("HMSET",
		message.MsgId,
		KAppId, message.AppID,
		KContent, message.Content,
		KRetried, message.Retried,
		KStatus, message.Status)
	if err != nil {
		return err
	}
	_, err = dbConn.Do("LPUSH", KMessageQueue, message.MsgId)
	return err
}

/**
	Get message info by message id
 */
func GetMessageInfo(msgId string) (*Message, error) {
	dbConn := Pool.Get()
	base, err := redis.Strings(dbConn.Do("HMGET", msgId, KAppId, KStatus, KRetried))
	if err != nil {
		return nil, err
	}
	if len(base) == 0 {
		return nil, NoSuchMessage{MsgId: msgId}
	}
	retried, err := strconv.Atoi(base[2])
	if err != nil {
		return nil, AttrTypeError{Type: "int", Value: base[2]}
	}
	message := Message{MsgId: msgId, AppID: base[0], Status: base[1], Retried: retried}
	content, err := redis.ByteSlices(dbConn.Do("HMGET", msgId, KContent))
	if err != nil {
		return nil, err
	}
	if len(content) == 0 {
		return nil, NoSuchMessage{MsgId: msgId, Detail: "content not found."}
	}
	message.Content = content[0]
	return &message, nil
}

func DeleteMessage(id string) {

}

func FoundMessage(id string) *Message {
	return nil
}
