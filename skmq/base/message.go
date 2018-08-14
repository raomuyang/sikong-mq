package base

/**
模仿Http协议，每个请求行均以\r\n间隔
*/
type Message struct {
	MsgId   string `json:"msgid"`
	AppID   string `json:"appid"`
	Type    string `json:"type"`
	Content []byte `json:"content"`
	Status  string `json:"status"`
	Retried int    `json:"-"`
}
