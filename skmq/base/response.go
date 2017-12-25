package base

type Response struct {
	Status     string `json:"status"`
	Content    string `json:"content"`
	Disconnect bool   `json:"-"`
}
