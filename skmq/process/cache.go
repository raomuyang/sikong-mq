package process

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"strconv"
	"fmt"
	"strings"
	"github.com/sikong-mq/skmq/skerr"
	"github.com/sikong-mq/skmq/base"
)

var (
	Pool      *redis.Pool
	Locker	  *RedisLock
)


func init() {
	Pool = &redis.Pool{
	}
	Locker = &RedisLock{Pool:Pool}
}

func InitDBConfig(config base.DBConfig) *redis.Pool {
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
			Warn.Println(err)
		}
		return err
	}
	return Pool
}

/**
	Save the information of consumer host,
	and it will be register in set: rec-set/application-id
 */
func SaveRecipientInfo(recipientInfo base.RecipientInfo) error {
	dbConn := Pool.Get()
	if len(recipientInfo.Status) == 0 {
		recipientInfo.Status = base.Alive
	}
	err := AddApplication(recipientInfo.ApplicationId)
	if err != nil {
		return err
	}
	_, err = dbConn.Do("HMSET",
		recipientInfo.RecipientId,
		base.KAppId, recipientInfo.ApplicationId,
		base.KHost, recipientInfo.Host,
		base.KPort, recipientInfo.Port,
		base.KWeight, recipientInfo.Weight,
		base.KStatus, recipientInfo.Status)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: err.Error()}
	}
	key := base.KRecipientSet + "/" + recipientInfo.ApplicationId
	_, err = dbConn.Do("SADD", key, recipientInfo.RecipientId)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: err.Error()}
	}
	return nil
}

func UpdateRecipient(recipientInfo base.RecipientInfo) error {
	dbConn := Pool.Get()

	old, err := GetRecipientById(recipientInfo.RecipientId)
	if old != nil && strings.Compare(old.RecipientId, recipientInfo.RecipientId) == 0 {
		set := base.KRecipientSet + "/" + recipientInfo.ApplicationId
		_, err = dbConn.Do("SREM", set, old.RecipientId)
		if err != nil {
			return skerr.UnknownDBOperationException{Detail: "remove old recipient failed, " + err.Error()}
		}
	}

	err = SaveRecipientInfo(recipientInfo)
	if err != nil {
		return err
	}

	if strings.Compare(base.Alive, recipientInfo.Status) != 0 {
		set := base.KRecipientSet + "/" + recipientInfo.ApplicationId
		_, err := dbConn.Do("SREM", set, recipientInfo.RecipientId)
		if err != nil {
			return skerr.UnknownDBOperationException{Detail: "Remove from " + set + ": " + err.Error() }
		}

		theMap := base.KRecentMap + "/" + recipientInfo.ApplicationId
		_, err = dbConn.Do("HDEL", theMap, recipientInfo.RecipientId)
		if err != nil {
			return skerr.UnknownDBOperationException{Detail: "Remove from " + theMap + ": " + err.Error() }
		}
	}

	return nil
}

/**
	Return a consumer list
 */
func FindRecipients(applicationId string) ([]*base.RecipientInfo, error) {
	dbConn := Pool.Get()
	set := base.KRecipientSet + "/" + applicationId
	rep, err := redis.Strings(dbConn.Do("SMEMBERS", set))
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: err.Error()}
	}

	var list []*base.RecipientInfo
	for i := range rep {
		id := rep[i]
		rec, err := GetRecipientById(id)
		if err != nil {
			Err.Println("List recipient member error:", err)
			continue
		}
		list = append(list, rec)
	}

	return list, nil
}

func GetRecipientById(recipientId string) (*base.RecipientInfo, error) {
	dbConn := Pool.Get()
	result, err := redis.StringMap(dbConn.Do("HGETALL", recipientId))
	if err != nil {
		return nil, err
	}
	weight, err := strconv.Atoi(result[base.KWeight])
	if err != nil {
		return nil, skerr.AttrTypeError{Type: "int", Value: result[base.KWeight]}
	}
	rec := base.RecipientInfo{
		ApplicationId: result[base.KAppId],
		RecipientId:   recipientId,
		Host:          result[base.KHost],
		Port:          result[base.KPort],
		Weight:		   weight,
		Status:        result[base.KStatus]}
	return &rec, nil
}

/**
	Returns the called times of each consumer(id)
 */
func RecentlyAssignedRecord(applicationId string) (map[string] int, error) {
	dbConn := Pool.Get()
	theMap := base.KRecentMap + "/" + applicationId
	result, err := redis.StringMap(dbConn.Do("HGETALL", theMap))
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "Get recent " +
			"assigned map exception: " + err.Error()}
	}
	recently := make(map[string] int)
	for i := range result {
		value, err := strconv.Atoi(result[i])
		if err != nil {
			return nil, skerr.AttrTypeError{Type: "int", Value: result[i]}
		}
		recently[i] = value
	}
	return recently, nil
}

func UpdateRecipientAssigned(recipient base.RecipientInfo) (int, error) {
	dbConn := Pool.Get()
	theMap := base.KRecentMap + "/" + recipient.ApplicationId
	times, err := redis.Int(dbConn.Do("HINCRBY", theMap, recipient.RecipientId, 1))
	if err != nil {
		return -1, skerr.UnknownDBOperationException{Detail: "Recipient assigned time " +
			"update increment failed: " + err.Error()}
	}
	return times, nil
}

func ResetRecipientAssignedRecord(applicationId string) error {
	dbConn := Pool.Get()
	theMap := base.KRecentMap + "/" + applicationId
	_, err := dbConn.Do("DEL", theMap)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: "Reset recent " +
			"assigned record failed: " + err.Error()}
	}
	return nil
}
 

/**
	Get message info by message id
 */
func GetMessageInfo(msgId string) (*base.Message, error) {
	dbConn := Pool.Get()

	baseResult, err := redis.Strings(dbConn.Do("HMGET", msgId, base.KAppId, base.KStatus,
		base.KRetried, base.KType))
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "get message info: " + err.Error()}
	}
	if len(baseResult) == 0 || baseResult[0] == "" {
		return nil, skerr.NoSuchMessage{MsgId: msgId}
	}
	retried, err := strconv.Atoi(baseResult[2])
	if err != nil {
		return nil, skerr.AttrTypeError{Type: "int", Value: baseResult[2]}
	}

	msgType := baseResult[3]

	message := base.Message{
		MsgId: msgId,
		AppID: baseResult[0],
		Status: baseResult[1],
		Retried: retried,
		Type: msgType}
	content, err := redis.ByteSlices(dbConn.Do("HMGET", msgId, base.KContent))
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "Get message content: " + err.Error()}
	}
	if len(content) == 0 {
		return nil, skerr.NoSuchMessage{MsgId: msgId, Detail: "content not found."}
	}
	message.Content = content[0]
	return &message, nil
}

func DeleteMessage(msgId string) error {
	Trace.Println("delete message", msgId)
	dbConn := Pool.Get()
	_, err := dbConn.Do("DEL", msgId)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: "Delete message: " + err.Error()}
	}
	_, err = dbConn.Do("HDEL", base.KMessageMap, msgId)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: "Remove from message retry set: " + err.Error()}
	}

	Locker.Unlock(msgId + base.MSaved)
	return nil
}

func UpdateMessageStatus(msgId, status string) error {
	dbConn := Pool.Get()
	result, err := redis.String(dbConn.Do("HGET", msgId, base.KStatus))
	if len(result) != 0 {
		_, err = dbConn.Do("HSET", msgId, base.KStatus, status)
		if err != nil {
			return skerr.UnknownDBOperationException{Detail: "Update message status: " + err.Error()}
		}
		return nil
	}
	return skerr.NoSuchMessage{MsgId: msgId, Detail: "Update message status failed."}
}

/**
	Save message entity to redis, index by messageId
 */
func MessageEnqueue(message base.Message) error {
	dbConn := Pool.Get()

	// 抛弃已存在的message
	result, err := Locker.TryLock(message.MsgId + base.MSaved)
	//result, err := redis.String(dbConn.Do("HGET", message.MsgId, base.KStatus))
	if !result {
		return skerr.MsgAlreadyExists{MsgId: message.MsgId}
	}
	_, err = dbConn.Do("HMSET",
		message.MsgId,
		base.KAppId, message.AppID,
		base.KContent, message.Content,
		base.KType, message.Type,
		base.KRetried, message.Retried,
		base.KStatus, message.Status)
	Trace.Printf("message enqueue, messageId: %s, %s: %s, type: %s\n",
		message.MsgId, base.KAppId, message.AppID, message.Type)
	if err != nil {
		Locker.Unlock(message.MsgId)
		return skerr.UnknownDBOperationException{Detail: "Set message exception: " + err.Error()}
	}

	_, err = dbConn.Do("LPUSH", base.KMessageQueue, message.MsgId)
	if err != nil {
		Locker.Unlock(message.MsgId)
		return skerr.UnknownDBOperationException{Detail: "Message enqueue failed: " + err.Error()}
	}

	return nil
}

/**
	对于重试队列的出队列，当时间未达到重试时间时，函数会发生阻塞
 */
func MessageDequeue(queue string) (*base.Message, error) {
	dbConn := Pool.Get()
	result, err := dbConn.Do("BRPOP", queue, 30)
	if err != nil {
		if strings.Contains(err.Error(), "nil") {
			return nil, nil
		}
		return nil, skerr.UnknownDBOperationException{Detail: "Pop msgId from queue failed: " + err.Error()}
	}

	if result == nil {
		return nil, nil
	}
	m, err:= redis.StringMap(result, err)
	msgId := m[queue]
	msg, err := GetMessageInfo(msgId)

	if err != nil {
		return nil, err
	}

	if strings.Compare(base.KMessageRetryQueue, queue) == 0 {

		// sleep
		lasttime, err := redis.Int(dbConn.Do("HGET", base.KMessageMap, msgId))
		if err == nil && lasttime != 0 {
			sleep := time.Now().UnixNano() - int64(lasttime) + int64(base.RetrySleep)
			if sleep > 0 {
				time.Sleep(time.Duration(sleep))
			}
		}
	}

	if strings.Compare(base.KDeadLetterQueue, queue) != 0 {
		_, err = dbConn.Do("HMSET", base.KMessageMap, msgId, time.Now().UnixNano())
		if err != nil {
			// rollback
			_, err2 := dbConn.Do("RPUSH", queue, msgId)
			return nil, skerr.UnknownDBOperationException{
				Detail: fmt.Sprintf("Add message id to set: %s/%s",
					err.Error(), err2.Error())}
		}
		err = UpdateMessageStatus(msgId, base.MSending)
		if err != nil {
			return nil, err
		}
	}

	Locker.Unlock(msgId)
	return msg, nil
}


/**
	信息重发次数递增
 */
func MessageEntryRetryQueue(msgId string) (*base.Message, error) {

	msg, err := GetMessageInfo(msgId)
	if err != nil {
		return nil, err
	}
	if msg.Retried+1 > Configuration.RetryTimes {
		return nil, skerr.MessageDead{MsgId: msg.MsgId, Status: msg.Status, Retried: msg.Retried}
	}

	// 避免多次入队列
	result, err := Locker.TryLock(msgId)
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "Lock resource failed: " + err.Error()}
	}
	if !result {
		return msg, nil
	}


	dbConn := Pool.Get()
	_, err = dbConn.Do("HINCRBY", msgId, base.KRetried, 1)
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "Increasing failed: " + err.Error()}
	}

	_, err = dbConn.Do("HMSET", base.KMessageMap, msgId, time.Now().UnixNano())
	if err != nil {
		return nil, skerr.UnknownDBOperationException{
			Detail: fmt.Sprintf("Add message id to set: %s/%s",
				err.Error(), err.Error())}
	}
	_, err = dbConn.Do("LPUSH", base.KMessageRetryQueue, msgId)
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "Message enqueue failed: " + err.Error()}
	}

	return msg, nil
}

/**
	Dead letter enqueue
 */
func DeadLetterEnqueue(msgId string) error {

	dbConn := Pool.Get()
	_, err := dbConn.Do("HDEL", base.KMessageMap, msgId)
	Trace.Println("HDEL", base.KMessageMap, msgId)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: "delete message record failed, " + err.Error()}
	}

	err = UpdateMessageStatus(msgId, base.MDead)
	switch err.(type) {
	case skerr.NoSuchMessage:
		return nil
	case nil:
	default:
		return err
	}

	_, err = dbConn.Do("LPUSH", base.KDeadLetterQueue, msgId)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: "dead Letter enqueue failed, " + err.Error()}
	}
	return nil
}

func MessagePostRecords() (map[string] int, error) {
	dbConn := Pool.Get()
	result, err := dbConn.Do("HGETALL", base.KMessageMap)
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "get message post records failed, " + err.Error()}
	}
	if result == nil {
		 return make(map[string]int, 0), nil
	}
	msgMap, err := redis.IntMap(result, err)
	if err != nil {
		return nil, skerr.UnknownDBOperationException{Detail: "convert result failed, " + err.Error()}
	}
	return msgMap, nil
}

func AddApplication(appId string) error  {
	_, err := Pool.Get().Do("SADD", base.KAppSet, appId)
	if err != nil {
		return skerr.UnknownDBOperationException{Detail: "application register failed, " + err.Error()}
	}
	return nil
}

func GetApps() []string {
	list, _ := redis.Strings(Pool.Get().Do("SMEMBERS", base.KAppSet))
	return list
}

type RedisLock struct {
	Pool       *redis.Pool
	expireTime int
	prefix     string

}

func (redisLock *RedisLock) TryLock(requestId string) (bool, error) {
	dbConn := redisLock.Pool.Get()
	rep, err := dbConn.Do("SET", redisLock.Prefix() + requestId, "Locked", "NX", "EX", redisLock.ExpireTime())
	if err != nil {
		return false, err
	}

	if rep == nil {
		return false, nil
	}
	result, err := redis.String(rep, err)
	if result == "OK" {
		return true, nil
	}
	return false, err
}


func (redisLock *RedisLock) Unlock(requestId string) error {
	dbConn := redisLock.Pool.Get()
	_, err := dbConn.Do("DEL", redisLock.Prefix() + requestId)
	return err
}

func (redisLock *RedisLock) ExpireTime() int {
	if redisLock.expireTime == 0 {
		// default one day
		return 60 * 60 * 24
	}
	return redisLock.expireTime
}

func (redisLock *RedisLock) SetExpireTime(expireTime int) {
	redisLock.expireTime = expireTime
}

func (redisLock *RedisLock) Prefix() string {
	if len(redisLock.prefix) == 0 {
		return "rds-lock-"
	}
	return redisLock.prefix
}

func (redisLock *RedisLock) SetPrefix(prefix string)  {
	redisLock.prefix = prefix
}

