package skmq

import (
	"encoding/json"
	"strings"
	"bytes"
	"fmt"
)

type Response struct {
	Status     string `json:"status"`
	Content    string `json:"content"`
	Disconnect bool   `json:"-"`
}

func processNewMsg(message Message, out chan<- Response)  {
	content, err := saveMessage(message)
	status := MAck
	if err != nil {
		status = MReject
		content = "Message enqueue failed"
	}
	out <- Response{Status: status, Content: content}
}


func processRegisterMsg(message Message, out chan<- Response)  {
	err := recipientRegister(message)
	status := MAck
	var content = "Recipient register successful."
	if err != nil {
		Warn.Println(err)
		status = MReject
		content = err.Error()
	}
	out <- Response{Status: status, Content: content}
}

func processArrivedMsg(message Message, out chan<- Response)  {
	disconnect := true
	err := UpdateMessageStatus(message.MsgId, MArrived)
	status := MAck
	if err != nil {
		status = MError
		disconnect = false
		Warn.Printf("arrived ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- Response{Status: status, Disconnect: disconnect}
}

func processAckMsg(message Message, out chan<- Response)  {
	err := DeleteMessage(message.MsgId)
	status := MAck
	if err != nil {
		status = MError
		Warn.Printf("message ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- Response{Status: status}
}

func processErrorMsg(message Message, out chan<- Response)  {
	err := DeadLetterEnqueue(message.MsgId)
	status := MAck
	disconnect := true
	if err != nil {
		status = MError
		disconnect = false
		Err.Printf("process error ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- Response{Status: status, Disconnect: disconnect}
}

func processRejectedMsg(message Message, out chan<- Response)  {
	err := checkRejectedMsg(message.MsgId)
	status := MAck
	disconnect := true
	if err != nil {
		status = MError
		disconnect = false
		Warn.Printf("reject ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- Response{Status: status, Disconnect: disconnect}

}

/**
	If the message retransmission times is no more than limit,
	it will be entries another queue to wait retry.
	otherwise it will be entries the dead letter queue
 */
func checkRejectedMsg(msgId string) error {

	_, err := MessageEntryRetryQueue(msgId)
	switch err.(type) {
	case UnknownDBOperationException:
		Warn.Println(err)
	case NoSuchMessage:
		Warn.Println(err)
		return nil
	case MessageDead:
		Warn.Println(err)
		return DeadLetterEnqueue(msgId)
	case nil:
		return nil
	}
	return err
}

/**
	Recipient register
 */
func recipientRegister(message Message) error {
	consumer := RecipientInfo{}
	err := json.Unmarshal(message.Content, &consumer)
	if err != nil {
		return InvalidParameters{Content: string(message.Content)}
	}

	return SaveRecipientInfo(consumer)
}


func saveMessage(message Message) (string, error) {
	err := MessageEnqueue(message)
	switch err.(type) {
	case MsgAlreadyExists:
		return err.Error(), nil
	case nil:
		return "Message enqueue successful", nil
	default:
		return "Message enqueue failed.", err
	}
}

/**
	这里使用四个换行（\r\n\r\n）来间隔一段消息解决tcp消息粘包问题，
	每个参数之间用两个换行（\r\n）间隔
 */
func DecodeMessage(input <-chan []byte) <-chan Message {
	msgChan := make(chan Message, ProcessBuf)
	go func() {
		defer func() {
			p := recover()
			if p != nil {
				Err.Println("Decode message error:", p)
			}
		}()
		message := Message{Status: MPending, Retried: 0}
		var line []byte
		for {
			buf, ok := <-input
			if !ok {
				close(msgChan)
				break
			}
			line = append(line, buf[:]...)
			buf = []byte{}

			split := bytes.Split(line, []byte(Delim))
			for i := 0; i < len(split)-1; i++ {
				sub := split[i]
				if len(sub) > 0 {
					spIndex := bytes.Index(sub, []byte(Separator))
					if spIndex < 0 {
						// 心跳信息
						if bytes.Compare([]byte(PING), sub) == 0 {
							msgChan <- Message{Type: PING}
							continue
						} else {
							Err.Println("MError: param separator not found")
							panic(StreamReadError{sub, "param separator not found"})
						}
					}

					key := fmt.Sprintf("%s", sub[:spIndex])
					if strings.Compare(PContent, key) == 0 {
						value := sub[spIndex+1:]
						message.Content = value
						continue
					}

					value := fmt.Sprintf("%s", sub[spIndex+1:])
					switch key {
					case PMsgId:
						message.MsgId = value
					case PAppID:
						message.AppID = value
					case PRequestType:
						message.Type = value
					default:
						Warn.Printf("No such parameter: %s.\n", key)

					}
				} else {
					// 丢弃没有类型的不完整信息
					if len(message.Type) > 0 {
						msgChan <- message
						message = Message{Status: MPending}
					}
				}
			}
			line = split[len(split)-1]
		}
	}()
	return msgChan
}

func EncodeMessage(message Message) []byte {
	list := []string{
		PRequestType, Separator, message.Type, Delim}
	if len(message.AppID) > 0 {
		list = append(list, PAppID, Separator, message.AppID, Delim)
	}
	if len(message.MsgId) > 0 {
		list = append(list, PMsgId, Separator, message.MsgId, Delim)
	}
	header := strings.Join(list, "")
	content := append([]byte(PContent+Separator), message.Content...)
	return append(append([]byte(header), content...), []byte(End)...)
}