package skmq

import (
	"net"
	"fmt"
	"bytes"
	"errors"
	"encoding/json"
	"time"
	"strings"
	"sync"
)

type RecipientInfo struct {
	RecipientId   string `json:"id"`
	ApplicationId string `json:"app_id"`
	Host          string `json:"host"`
	Port          string `json:"port"`
	Status        string `json:"-"`
	Weight        int    `json:"weight"`
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
	Status     string `json:"status"`
	Content    string `json:"content"`
	Disconnect bool   `json:"-"`
}

const (
	ProcessBuf = 8
	SendBuf    = 1 << 5
)

var stop bool = false

func OpenServer() {
	InitDBConfig(*DBConfiguration)
	laddr := Configuration.ListenerHost + ":" + Configuration.ListenerPort
	fmt.Println("Server: open message queue server, listen " + laddr)

	go schedule()
	go scanTimeoutTasks()
	go heartbeatCyclically()

	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	for {
		if stop {
			fmt.Println("Server: shutdown server...")
			break
		}
		connect, err := listener.Accept()
		fmt.Printf("Server: accept %v\n", connect.RemoteAddr())
		if err != nil {
			panic(err)
		}
		receive(connect)
	}
}

func StopServer() {
	stop = true
}

func heartbeatCyclically() {
	CheckRecipientsAvailable()
	time.Sleep(time.Minute)
}

func schedule() {
	sendQueue := make(chan Message, SendBuf)
	dlQueue := make(chan Message, SendBuf)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		// Msg dequeue
		for {
			if stop {
				wg.Done()
				break
			}
			msg, err := MessageDequeue(KMessageQueue)
			if err != nil {
				fmt.Println("Scheduler: dequeue error, " + err.Error())
				time.Sleep(10 * time.Second)
				continue
			}
			if msg == nil {
				continue
			}
			sendQueue <- *msg
		}
	}()

	go func() {
		// retry-msg dequeue
		for {
			if stop {
				wg.Done()
				break
			}
			msg, err := MessageDequeue(KMessageRetryQueue)
			if err != nil {
				fmt.Println("Scheduler: retry-msg dequeue error, " + err.Error())
				time.Sleep(15 * time.Second)
				continue
			}
			if msg == nil {
				time.Sleep(30 * time.Second)
				continue
			}
			sendQueue <- *msg
		}
	}()

	go func() {
		// dl-msg dequeue
		for {
			if stop {
				close(dlQueue)
				break
			}
			msg, err := MessageDequeue(KDeadLetterQueue)
			if err != nil {
				fmt.Println("Scheduler: dl-msg dequeue error, " + err.Error())
				time.Sleep(30 * time.Second)
				continue
			}
			if msg == nil {
				time.Sleep(60 * time.Second)
				continue
			}
			dlQueue <- *msg
		}
	}()

	go func() {
		// msg delivery
		quit := false
		for {
			if quit {
				fmt.Println("Scheduler: stop.")
				break
			}

			select {
			case msg, ok := <-sendQueue:
				if !ok {
					quit = true
					break
				}
				delivery(msg)
			case msg, ok := <-dlQueue:
				if ok {
					processDeadLetter(msg)
				}
			default:
			}
		}
	}()

	wg.Wait()
	close(sendQueue)
}

// scan ack timeout
func scanTimeoutTasks() {

	for {
		if stop {
			break
		}

		records, err := MessagePostRecords()
		if err != nil {
			fmt.Println("Scanner: get records error, " + err.Error())
			time.Sleep(30 * time.Second)
			continue
		}
		for msgId := range records {
			diff := (time.Now().UnixNano() - int64(records[msgId])) - int64(Configuration.ACKTimeout)*int64(time.Millisecond)
			if diff > 0 || -diff < int64(time.Second) {

				_, err := MessageEntryRetryQueue(msgId)
				switch err.(type) {
				case NoSuchMessage:
					fmt.Println("Scheduler: warning, " + err.Error())
					DeadLetterEnqueue(msgId)
				case MessageDead:
					fmt.Println("Scheduler: " + err.Error())
					DeadLetterEnqueue(msgId)
				case nil:
					fmt.Printf("Scheduler: %s will be retried \n", msgId)
				default:
					fmt.Println("Scheduler: " + err.Error())
				}
			}
		}
		time.Sleep(time.Minute)
	}
}

func processDeadLetter(message Message) {
	DeleteMessage(message.MsgId)
}

func delivery(message Message) {
	message.Type = MPush
	defer func() {
		e := recover()
		if e != nil {
			fmt.Printf("Delivery: panic, %s \n", e)
		}
	}()
	go func() {
		fmt.Printf("Delivery: delivery message %s/%s \n", message.AppID, message.MsgId)
		conn, err := DeliveryMessage(message.AppID, EncodeMessage(message))
		if err != nil {
			fmt.Println("Delivery: error, " + err.Error())
			return
		}
		reply(conn, handleMessage(DecodeMessage(ReadStream(conn))))
	}()
}

func receive(connect net.Conn) {
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

func reply(connect net.Conn, repChan <-chan Response) {
	disconnect := false
	for {
		response, ok := <-repChan
		if !ok {
			fmt.Println("Reply: message handler close the channel")
			break
		}

		if strings.Compare(response.Status, PONG) == 0 {
			ReplyHeartbeat(connect)
			fmt.Println("Reply: PONG")
			continue
		}

		content, err := json.Marshal(response)
		if err != nil {
			panic(errors.New("reply: json marshal struct failed"))
		}
		err = SendMessage(connect, EncodeMessage(Message{Content: content, Type: MResponse}))

		if err != nil {
			// TODO log
			fmt.Println("Reply: " + err.Error())
		}

		disconnect = response.Disconnect || disconnect
	}

	if disconnect {
		err := connect.Close()
		if err != nil {
			// TODO log
			fmt.Println("Reply: close connect failed, " + err.Error())
		}
	}

}

func handleMessage(msgChan <-chan Message) <-chan Response {
	out := make(chan Response, ProcessBuf)
	go func() {
		for {
			message, ok := <-msgChan
			if !ok {
				fmt.Println("Handler: message channel closed.")
				break
			}
			fmt.Printf("Handler: handle message %s/%s[%s] \n", message.AppID, message.MsgId, message.Type)
			switch message.Type {
			case RegisterMsg:
				fmt.Println("Handler: mesage rejected: " + message.MsgId)
				err := recipientRegister(message)
				status := MAck
				var content = "Recipient register successful."
				if err != nil {
					// TODO log
					status = MReject
					content = err.Error()
				}
				out <- Response{Status: status, Content: content}
			case MArrivedMsg:
				fmt.Println("Handler: mesage deliveried successfully: " + message.MsgId)
				disconnect := true
				err := arrive(message)
				status := MAck
				if err != nil {
					status = MError
					disconnect = false
					fmt.Printf("--arrived ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status, Disconnect: disconnect}
			case MAckMsg:
				fmt.Println("Handler: message ack, " + message.MsgId)
				err := ack(message)
				status := MAck
				if err != nil {
					status = MError
					fmt.Printf("--message ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status}
			case MError:
				err := msgError(message)
				status := MAck
				disconnect := true
				if err != nil {
					status = MError
					disconnect = false
					fmt.Printf("--process error ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status, Disconnect: disconnect}
			case MRejectMsg:
				err := reject(message)
				status := MAck
				disconnect := true
				if err != nil {
					status = MError
					disconnect = false
					fmt.Printf("--reject ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status, Disconnect: disconnect}
			case PING:
				out <- Response{Status: PONG}
			default:
				fmt.Println("Save message " + message.MsgId)
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
						// 心跳信息
						if bytes.Compare([]byte(PING), sub) == 0 {
							msgChan <- Message{Type: PING}
							continue
						} else {
							fmt.Println("MError: param separator not found")
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
	return processRejectedMsg(message.MsgId)
}

/**
	Recipient process error, entry dl-queue
	TODO error log
 */
func msgError(message Message) error {
	return DeadLetterEnqueue(message.MsgId)
}

func arrive(message Message) error {
	return UpdateMessageStatus(message.MsgId, MArrived)
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
