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
	KWeight = "weight"
)

/**
	queue/set name
 */
const (
	KRecipientSet = "sk-rec-set"
	KMessageQueue = "sk-msg-q"
	KRetrySet  = "sk-rty-s"
	KRecentMap = "sk-recently"
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
	Pending = "pending"
	Sending = "sending"
	Ack     = "ack"
	Reject  = "reject"
	Error   = "error"
)

const (
	Alive = "alive"
	Lost = "lost"
)
