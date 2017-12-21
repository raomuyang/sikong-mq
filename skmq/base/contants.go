package base

import "time"

/**
	DB Key
 */
const (
	KMsgId = "msgid"
	KAppId = "appid"
	KHost  = "host"
	KPort  = "port"
	KStatus = "status"
	KType = "type"
	KContent = "content"
	KRetried = "retried"
	KWeight = "weight"
)

/**
	queue/set name
 */
const (
	// recipients
	KRecipientSet    = "sk-rec-s"

	// recent invoke information
	KRecentMap       = "sk-recently"

	// msg wait queue
	KMessageQueue    = "sk-msg-q"

	// msg send time
	KMessageMap      = "sk-msg-record"

	// msg retry queue
	KMessageRetryQueue = "sk-rty-q"

	KDeadLetterQueue = "sk-dl-q"

	KAppSet = "sk-app-s"

	KStatusLockSet = "sk-lock-s"
)

/**
	Sikong-mq Protocol:
 */
const (
	PING		  = "ping"
	PONG		  = "pong"

	PAppID        = "appid"
	PMsgId        = "msgid"
	PRequestType  = "type"
	PContent      = "content"
	Delim         = "\r\n"
	End			  = Delim + Delim
	Separator     = "="

	TopicMsg = "topic"
	QueueMsg = "queue"

	// Consumer reply
	MPush		= "push"
	MResponse 	= "resp"
	MAckMsg     = "ack"
	MRejectMsg  = "reject"
	MArrivedMsg = "arrived"

	RegisterMsg = "register"
)

/**
	Message status value
 */
const (
	MPending = "pending"
	MSending = "sending"
	MArrived = "arrived"
	MAck     = "ack"
	MReject  = "reject"
	MError   = "error"
	MDead    = "dead"
)

const (
	Alive = "alive"
	Lost = "lost"
)

const (
	ConnectTimeOut = time.Minute
	RetrySleep = time.Minute
)

const (
	DefaultConf = "conf.json"
	DefaultDBConf = "dbconf.json"
)
