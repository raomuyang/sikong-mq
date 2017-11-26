package session

type RecipientInfo struct {
	RecipientId string `json:"id"`
	ApplicationId string `json:"app_id"`
	Host string `json:"host"`
	Port string `json:"port"`
	Status string `json:"-"`
	Weight int `json:"weight"`
}