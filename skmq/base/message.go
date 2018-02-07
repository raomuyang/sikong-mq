package base

/**
模仿Http协议，每个请求行均以\r\n间隔
*/
type Message struct {
	MsgId   string
	AppID   string
	Type    string
	Content []byte
	Status  string
	Retried int
}
