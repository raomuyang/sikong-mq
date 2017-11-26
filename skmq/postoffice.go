package skmq

import (
	"net"
	"fmt"
	"bytes"
	"errors"
	"encoding/json"
	"time"
	"strings"
)

type RecipientInfo struct {
	RecipientId string `json:"id"`
	ApplicationId string `json:"app_id"`
	Host string `json:"host"`
	Port string `json:"port"`
	Status string `json:"-"`
	Weight int `json:"weight"`
}

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

type Response struct {
	Status  string `json:"status"`
	Content string `json:"content"`
	Disconnect bool `json:"-"`
}

type Result struct {
	MsgId        string
	Err          error
	FinishedTime time.Time
}

const (
	ProcessBuf = 8
	SendBuf = 1 << 5
)

var stopServer bool = false

func OpenServer() {
	InitDBConfig(*DBConfiguration)
	laddr := Configuration.ListenerHost + ":" + Configuration.ListenerPort
	fmt.Println("Open message queue server, listen " + laddr)
	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	for {
		if stopServer {
			fmt.Println("Shutdown server...")
			break
		}
		connect, err := listener.Accept()
		fmt.Printf("accept: %v\n", connect)
		if err != nil {
			panic(err)
		}
		communication(connect)
	}
}

func StopServer() {
	stopServer = true
}

func communication(connect net.Conn) {
	go func() {

		defer func() {
			err := recover()
			if err != nil {
				// TODO log
				fmt.Printf("Error: %v\n", err)
			}
		}()
		reply(connect, handleMessage(DecodeMessage(ReadStream(connect))))
	}()
}

func messageQueue()  {
	sendQueue := make(chan Message, SendBuf)
	go func() {
		for {
			if stopServer {
				close(sendQueue)
				break
			}
			msg, err := MessageDequeue()
			if err != nil {
				fmt.Println("Error: " + err.Error())
				time.Sleep(10 * time.Second)
				continue
			}
			sendQueue <- *msg
		}
	}()

	go func() {
		for  {
			msg, ok := <-sendQueue
			if !ok {
				fmt.Println("Stop dequeue.")
				break
			}
			delivery(msg)
		}
	}()
}

func delivery(message Message) {
	go func() {
		conn, err := DeliveryMessage(message.AppID, EncodeMessage(message))
		if err != nil {
			fmt.Println("Error: " + err.Error())
			return
		}
		reply(conn, handleMessage(DecodeMessage(ReadStream(conn))))
	}()
}

func reply(connect net.Conn, repChan <-chan Response) {
	disconnect := false
	for {
		response, ok := <-repChan
		if !ok {
			fmt.Println("message handler close the channel")
			break
		}

		rs, err := json.Marshal(response)
		if err != nil {
			panic(errors.New("json marshal struct failed"))
		}
		err = SendMessage(connect, rs)

		if err != nil {
			// TODO log
			fmt.Println(err)
		}

		disconnect = response.Disconnect || disconnect
	}

	if disconnect {
		err := connect.Close()
		if err != nil {
			// TODO log
			fmt.Println("Close connect failed, " + err.Error())
		}
	}

}

func handleMessage(msgChan <-chan Message) <-chan Response {
	out := make(chan Response, ProcessBuf)
	go func() {
		for {
			message, ok := <-msgChan
			if !ok {
				fmt.Println("Message channel closed.")
				break
			}
			fmt.Printf("Handle message: %s-%s/%s \n", message.AppID, message.MsgId, message.Type)
			switch message.Type {
			case RegisterMsg:
				err := processRegisterMsg(message)
				status := MAck
				var content = "Recipient register successful."
				if err != nil {
					// TODO log
					status = MReject
					content = err.Error()
				}
				out <- Response{Status: status, Content: content}
			case MArrivedMsg:
				disconnect := true
				err := arrive(message)
				status := MAck
				if err != nil {
					status = MError
					disconnect = false
				}
				out <- Response{Status: status, Disconnect: disconnect}
			case MAckMsg:
				err := ack(message)
				status := MAck
				if err != nil {
					status = MError
				}
				out <- Response{Status: status}
			case MRejectMsg:
				err := reject(message)
				status := MAck
				disconnect := true
				if err != nil {
					status = MError
					disconnect = false
				}
				out <- Response{Status: status, Disconnect: disconnect}
			default:
				content, err := saveMessage(message)
				status := MAck
				if err != nil {
					status = MReject
					content = "Message enqueue failed"
				}
				out <- Response{Status: status, Content: content}
			}
		}
		close(out)
	}()
	return out
}


/**
	这里使用四个换行（\r\n\r\n）来间隔一段消息解决tcp消息粘包问题，
	每个参数之间用两个换行（\r\n\r\n）间隔
 */
func DecodeMessage(input <-chan []byte) <-chan Message {
	msgChan := make(chan Message, ProcessBuf)
	go func() {
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
						fmt.Println("MError: param separator not found")
						panic(StreamReadError{sub, "param separator not found"})
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
						fmt.Printf("No such parameter: %s.\n", key)

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

func EncodeMessage(message Message) []byte{
	header := strings.Join([]string{
		PMsgId, Separator, message.MsgId, Delim,
		PRequestType, Separator, message.Type, Delim}, "")
	content := append([]byte(PContent + Separator), message.Content...)
	return append(append([]byte(header), content...), []byte(End)...)
}

/**
	Recipient register
 */
func processRegisterMsg(message Message) error {
	consumer := RecipientInfo{}
	err := json.Unmarshal(message.Content, &consumer)
	if err != nil {
		return InvalidParameters{Content: string(message.Content)}
	}

	return SaveRecipientInfo(consumer)
}

/**
	Recipient ack
 */
func ack(message Message) error {
	return DeleteMessage(message.MsgId)
}

/**
	Recipient reject
 */
func reject(message Message) error {
	return nil
}

func arrive(message Message) error {
	return UpdateMessageStatus(message.MsgId, MArrived)
}

func saveMessage(message Message) (string, error) {
	err := MessageEnqueue(message)
	switch err.(type) {
	case MsgAlreadyExists:
		return err.Error(), nil
	default:
		return "Message enqueue failed.", err
	}
	return "Message enqueue successful", nil

}
