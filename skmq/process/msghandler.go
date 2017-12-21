package process

import (
	"encoding/json"
	"strings"
	"bytes"
	"fmt"
	"github.com/sikong-mq/skmq/skerr"
	"github.com/sikong-mq/skmq/base"
)

const MessageChanBuf = 8

func processNewMsg(message base.Message, out chan<- base.Response)  {
	content, err := saveMessage(message)
	status := base.MAck
	if err != nil {
		status = base.MReject
		content = "Message enqueue failed"
	}
	out <- base.Response{Status: status, Content: content}
}


func processRegisterMsg(message base.Message, out chan<- base.Response)  {
	err := recipientRegister(message)
	status := base.MAck
	var content = "Recipient register successful."
	if err != nil {
		Warn.Println(err)
		status = base.MReject
		content = err.Error()
	}
	out <- base.Response{Status: status, Content: content}
}

func processArrivedMsg(message base.Message, out chan<- base.Response)  {
	disconnect := true
	err := UpdateMessageStatus(message.MsgId, base.MArrived)
	status := base.MAck
	if err != nil {
		status = base.MError
		disconnect = false
		Warn.Printf("arrived ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status, Disconnect: disconnect}
}

func processAckMsg(message base.Message, out chan<- base.Response)  {
	err := DeleteMessage(message.MsgId)
	status := base.MAck
	if err != nil {
		status = base.MError
		Warn.Printf("message ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status}
}

func processErrorMsg(message base.Message, out chan<- base.Response)  {
	err := DeadLetterEnqueue(message.MsgId)
	status := base.MAck
	disconnect := true
	if err != nil {
		status = base.MError
		disconnect = false
		Err.Printf("process error ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status, Disconnect: disconnect}
}

func processRejectedMsg(message base.Message, out chan<- base.Response)  {
	err := checkRejectedMsg(message.MsgId)
	status := base.MAck
	disconnect := true
	if err != nil {
		status = base.MError
		disconnect = false
		Warn.Printf("reject ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status, Disconnect: disconnect}

}

/**
	If the message retransmission times is no more than limit,
	it will be entries another queue to wait retry.
	otherwise it will be entries the dead letter queue
 */
func checkRejectedMsg(msgId string) error {

	_, err := MessageEntryRetryQueue(msgId)
	switch err.(type) {
	case skerr.UnknownDBOperationException:
		Warn.Println(err)
	case skerr.NoSuchMessage:
		Warn.Println(err)
		return nil
	case skerr.MessageDead:
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
func recipientRegister(message base.Message) error {
	consumer := base.RecipientInfo{}
	err := json.Unmarshal(message.Content, &consumer)
	if err != nil {
		return skerr.InvalidParameters{Content: string(message.Content)}
	}

	return SaveRecipientInfo(consumer)
}


func saveMessage(message base.Message) (string, error) {
	err := MessageEnqueue(message)
	switch err.(type) {
	case skerr.MsgAlreadyExists:
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
func DecodeMessage(input <-chan []byte) <-chan base.Message {
	msgChan := make(chan base.Message, MessageChanBuf)
	go func() {
		defer func() {
			p := recover()
			if p != nil {
				Err.Println("Decode message error:", p)
			}
		}()
		message := base.Message{Status: base.MPending, Retried: 0}
		var line []byte
		for {
			buf, ok := <-input
			if !ok {
				close(msgChan)
				break
			}
			line = append(line, buf[:]...)
			buf = []byte{}

			split := bytes.Split(line, []byte(base.Delim))
			for i := 0; i < len(split)-1; i++ {
				sub := split[i]
				if len(sub) > 0 {
					spIndex := bytes.Index(sub, []byte(base.Separator))
					if spIndex < 0 {
						// 心跳信息
						if bytes.Compare([]byte(base.PING), sub) == 0 {
							msgChan <- base.Message{Type: base.PING}
							continue
						} else {
							Err.Println("MError: param separator not found")
							panic(skerr.StreamReadError{sub, "param separator not found"})
						}
					}

					key := fmt.Sprintf("%s", sub[:spIndex])
					if strings.Compare(base.PContent, key) == 0 {
						value := sub[spIndex+1:]
						message.Content = value
						continue
					}

					value := fmt.Sprintf("%s", sub[spIndex+1:])
					switch key {
					case base.PMsgId:
						message.MsgId = value
					case base.PAppID:
						message.AppID = value
					case base.PRequestType:
						message.Type = value
					default:
						Warn.Printf("No such parameter: %s.\n", key)

					}
				} else {
					// 丢弃没有类型的不完整信息
					if len(message.Type) > 0 {
						msgChan <- message
						message = base.Message{Status: base.MPending}
					}
				}
			}
			line = split[len(split)-1]
		}
	}()
	return msgChan
}

func EncodeMessage(message base.Message) []byte {
	list := []string{
		base.PRequestType, base.Separator, message.Type, base.Delim}
	if len(message.AppID) > 0 {
		list = append(list, base.PAppID, base.Separator, message.AppID, base.Delim)
	}
	if len(message.MsgId) > 0 {
		list = append(list, base.PMsgId, base.Separator, message.MsgId, base.Delim)
	}
	header := strings.Join(list, "")
	content := append([]byte(base.PContent + base.Separator), message.Content...)
	return append(append([]byte(header), content...), []byte(base.End)...)
}

func HandleMessage(msgChan <-chan base.Message) <-chan base.Response {
	out := make(chan base.Response, MessageChanBuf)
	go func() {
		for {
			message, ok := <-msgChan
			if !ok {
				Trace.Println("Handler: message channel closed.")
				break
			}
			Info.Printf("Handler: handle message %s/%s[%s] \n", message.AppID, message.MsgId, message.Type)
			switch message.Type {
			case base.RegisterMsg:
				Warn.Println("Handler: mesage rejected: " + message.MsgId)
				processRegisterMsg(message, out)
			case base.MArrivedMsg:
				Info.Println("Handler: mesage deliveried successfully: " + message.MsgId)
				processArrivedMsg(message, out)
			case base.MAckMsg:
				Info.Println("Handler: message ack, " + message.MsgId)
				processAckMsg(message, out)
			case base.MError:
				Warn.Printf("Msg %s error\n", message.MsgId)
				processErrorMsg(message, out)
			case base.MRejectMsg:
				Warn.Printf("Msg %s rejected\n", message.MsgId)
				processRejectedMsg(message, out)
			case base.PING:
				out <- base.Response{Status: base.PONG}
			default:
				Info.Println("Save message " + message.MsgId)
				processNewMsg(message, out)
			}
		}
		close(out)
	}()
	return out
}