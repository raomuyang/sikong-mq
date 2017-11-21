package session

/**
	DB Key
 */
const (
	KMsgId = "msgid"
	KAppId = "appid"
	KHost  = "host"
	KPort  = "port"
	KStatus = "status"
	KContent = "content"
	KRetried = "retried"
)

/**
	queue/set name
 */
var (
	KRecipientSet = "rec-set"
	KMessageQueue = "msg-q"
	KRetryQueue  = "rty-q"
)

/**
	Sikong-mq Protocol:
 */
const (
	AppID       = "appid"
	MsgId       = "msgid"
	RequestType = "type"
	Content     = "content"
	TopicType   = "topic"
	QueueType   = "queue"
	QueryType   = "query"
	Delim       = "\r\n"
	Separator   = "="
)

/**
	Message status value
 */
const (
	Wait   = "wait"
	Ack    = "ack"
	Reject = "reject"
	Error  = "error"
)

const (
	Alive = "alive"
	Lost = "lost"
)
