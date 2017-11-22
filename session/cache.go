package session

import (
	"github.com/garyburd/redigo/redis"
	"time"
	"strconv"
	"fmt"
	"strings"
)

var (
	Pool *redis.Pool
)

func init() {
	Pool = &redis.Pool{
	}
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
			fmt.Println(err)
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
	if len(recipientInfo.Status) == 0 {
		recipientInfo.Status = Alive
	}
	_, err := dbConn.Do("HMSET",
		recipientInfo.RecipientId,
		KAppId, recipientInfo.ApplicationId,
		KHost, recipientInfo.Host,
		KPort, recipientInfo.Port,
		KWeight, recipientInfo.Weight,
		KStatus, recipientInfo.Status)
	if err != nil {
		return UnknownDBOperationException{Detail: err.Error()}
	}
	key := KRecipientSet + "/" + recipientInfo.ApplicationId
	_, err = dbConn.Do("SADD", key, recipientInfo.RecipientId)
	if err != nil {
		return UnknownDBOperationException{Detail: err.Error()}
	}
	return nil
}

func UpdateRecipient(recipientInfo RecipientInfo) error {
	dbConn := Pool.Get()
	err := SaveRecipientInfo(recipientInfo)
	if err != nil {
		return err
	}

	if strings.Compare(Alive, recipientInfo.Status) != 0 {
		set := KRecipientSet + "/" + recipientInfo.ApplicationId
		_, err := dbConn.Do("SREM", set, recipientInfo.RecipientId)
		if err != nil {
			return UnknownDBOperationException{Detail: "Remove from " + set + ": " + err.Error() }
		}

		theMap := KRecentMap + "/" + recipientInfo.ApplicationId
		_, err = dbConn.Do("HDEL", theMap, recipientInfo.RecipientId)
		if err != nil {
			return UnknownDBOperationException{Detail: "Remove from " + theMap + ": " + err.Error() }
		}
	}

	return nil
}

/**
	Return a consumer list
 */
func FindRecipients(applicationId string) ([]*RecipientInfo, error) {
	dbConn := Pool.Get()
	set := KRecipientSet + "/" + applicationId
	rep, err := redis.Strings(dbConn.Do("SMEMBERS", set))
	if err != nil {
		return nil, UnknownDBOperationException{Detail: err.Error()}
	}

	var list []*RecipientInfo
	for i := range rep {
		id := rep[i]
		result, err := redis.StringMap(dbConn.Do("HGETALL", id))
		if err != nil {
			// TODO log
			fmt.Println("List recipient member error:", err)
			continue
		}
		weight, err := strconv.Atoi(result[KWeight])
		if err != nil {
			return nil, AttrTypeError{Type: "int", Value: result[KWeight]}
		}
		rec := RecipientInfo{
			ApplicationId: applicationId,
			RecipientId:   id,
			Host:          result[KHost],
			Port:          result[KPort],
			Weight:		   weight,
			Status:        result[KStatus]}

		list = append(list, &rec)
	}

	return list, nil
}

/**
	Returns the called times of each consumer(id)
 */
func RecentlyAssignedRecord(applicationId string) (map[string] int, error) {
	dbConn := Pool.Get()
	theMap := KRecentMap + "/" + applicationId
	result, err := redis.StringMap(dbConn.Do("HGETALL", theMap))
	if err != nil {
		return nil, UnknownDBOperationException{Detail: "Get recent " +
			"assigned map exception: " + err.Error()}
	}
	recently := make(map[string] int)
	for i := range result {
		value, err := strconv.Atoi(result[i])
		if err != nil {
			return nil, AttrTypeError{Type: "int", Value: result[i]}
		}
		recently[i] = value
	}
	return recently, nil
}

func UpdateRecipientAssigned(recipient RecipientInfo) (int, error) {
	dbConn := Pool.Get()
	theMap := KRecentMap + "/" + recipient.ApplicationId
	fmt.Println("HINCRBY", theMap, recipient.RecipientId, 1)
	times, err := redis.Int(dbConn.Do("HINCRBY", theMap, recipient.RecipientId, 1))
	if err != nil {
		return -1, UnknownDBOperationException{Detail: "Recipient assigned time " +
			"update increment failed: " + err.Error()}
	}
	return times, nil
}

func ResetRecipientAssignedRecord(applicationId string) error {
	dbConn := Pool.Get()
	theMap := KRecentMap + "/" + applicationId
	_, err := dbConn.Do("DEL", theMap)
	if err != nil {
		return UnknownDBOperationException{Detail: "Reset recent " +
			"assigned record failed: " + err.Error()}
	}
	return nil
}
 

/**
	Get message info by message id
 */
func GetMessageInfo(msgId string) (*Message, error) {
	dbConn := Pool.Get()
	base, err := redis.Strings(dbConn.Do("HMGET", msgId, KAppId, KStatus, KRetried))
	if err != nil {
		return nil, UnknownDBOperationException{"Get message info: " + err.Error()}
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
		return nil, UnknownDBOperationException{Detail: "Get message content: " + err.Error()}
	}
	if len(content) == 0 {
		return nil, NoSuchMessage{MsgId: msgId, Detail: "content not found."}
	}
	message.Content = content[0]
	return &message, nil
}

func DeleteMessage(msgId string) error {
	dbConn := Pool.Get()
	_, err := dbConn.Do("DEL", msgId)
	if err != nil {
		return UnknownDBOperationException{"Delete message: " + err.Error()}
	}
	_, err = dbConn.Do("SREM", KRetrySet, msgId)
	if err != nil {
		return UnknownDBOperationException{"Remove from message retry set: " + err.Error()}
	}
	return nil
}

func UpdateMessageStatus(msgId, status string) error {
	dbConn := Pool.Get()
	result, err := redis.String(dbConn.Do("HGET", msgId, KStatus))
	if len(result) != 0 {
		_, err = dbConn.Do("HSET", msgId, KStatus, status)
		if err != nil {
			return UnknownDBOperationException{Detail: "Update message status: " + err.Error()}
		}
		return nil
	}
	return NoSuchMessage{MsgId: msgId, Detail: "Update message status failed."}
}

/**
	Save message entity to redis, index by messageId
 */
func MessageEnqueue(message Message) error {
	dbConn := Pool.Get()

	// 抛弃已存在的message
	result, err := redis.String(dbConn.Do("HGET", message.MsgId, KStatus))
	if len(result) != 0 {
		return MsgAlreadyExists{MsgId: message.MsgId, Status: result}
	}
	_, err = dbConn.Do("HMSET",
		message.MsgId,
		KAppId, message.AppID,
		KContent, message.Content,
		KRetried, message.Retried,
		KStatus, message.Status)
	if err != nil {
		return UnknownDBOperationException{Detail: "Set message exception: " + err.Error()}
	}

	_, err = dbConn.Do("LPUSH", KMessageQueue, message.MsgId)
	if err != nil {
		return UnknownDBOperationException{Detail: "Message enqueue failed: " + err.Error()}
	}

	return nil
}

func MessageDequeue() (*Message, error) {
	dbConn := Pool.Get()
	result, err := redis.Strings(dbConn.Do("BRPOP", KMessageQueue, 30))
	if err != nil {
		return nil, UnknownDBOperationException{Detail: "Pop msgId from queue failed: " + err.Error()}
	}

	if len(result) < 2 {
		return nil, nil
	}

	msgId := result[1]
	msg, err := GetMessageInfo(msgId)

	if err != nil {
		return nil, err
	}
	_, err = dbConn.Do("SADD", KRetrySet, msgId)
	if err != nil {
		return nil, UnknownDBOperationException{Detail: "Add message id to set: " + err.Error()}
	}

	err = UpdateMessageStatus(msgId, MSending)
	if err != nil {
		return nil, err
	}

	return msg, nil
}

func MessageDequeueAgain(msgId string) (*Message, error) {
	msg, err := GetMessageInfo(msgId)
	if err != nil {
		return nil, err
	}
	if msg.Retried+1 > Configuration.RetryTimes {
		return nil, MessageDeliveryFailed{MsgId: msg.MsgId, Status: msg.Status, Retried: msg.Retried}
	}
	dbConn := Pool.Get()
	_, err = dbConn.Do("HINCRBY", msgId, KRetried, 1)
	if err != nil {
		return nil, UnknownDBOperationException{Detail: "Increasing failed: " + err.Error()}
	}
	return msg, nil
}
