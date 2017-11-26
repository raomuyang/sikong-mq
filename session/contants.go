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
	PAppID        = "appid"
	PMsgId        = "msgid"
	PRequestType  = "type"
	PContent      = "content"
	Delim         = "\r\n"
	End			  = "\r\n\r\n"
	Separator     = "="

	TopicMsg = "topic"
	QueueMsg = "queue"

	MQueryMsg = "query"
	MAckMsg = "ack"
	MRejectMsg = "reject"

	// Notify system the transaction is ready
	MReadyMsg = "ready"
	MDiscard = "discard"

	RegisterMsg = "register"
)

/**
	Message status value
 */
const (
	MPending = "pending"
	MReady 	 = "Ready"
	MSending = "sending"
	MAck     = "ack"
	MReject  = "reject"
	MError   = "error"
)

const (
	Alive = "alive"
	Lost = "lost"
)
