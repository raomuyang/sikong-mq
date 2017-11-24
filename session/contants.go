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
	// recipients
	KRecipientSet    = "sk-rec-set"

	// recent invoke information
	KRecentMap       = "sk-recently"

	// msg wait queue
	KMessageQueue    = "sk-msg-q"

	// msg send time
	KMessageMap      = "sk-msg-time-m"

	// msg retry queue
	KMessageRetryQueue = "sk-rty-q"

	KDeadLetterQueue = "sk-dl-q"
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
	MDead	 = "dead"
)

const (
	Alive = "alive"
	Lost = "lost"
)
