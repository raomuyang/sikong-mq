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

var stop = false

func OpenServer() {
	InitDBConfig(*DBConfiguration)
	laddr := Configuration.ListenerHost + ":" + Configuration.ListenerPort
	Info.Println("Server: open message queue server, listen " + laddr)

	go schedule()
	go scanTimeoutTasks()
	go heartbeatCyclically()

	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		Err.Println(err)
		panic(err)
	}
	defer listener.Close()

	rateLimiter, err := CreateSmoothRateLimiter(Configuration.Rate)
	if err != nil {
		panic(err)
	}

	for {
		rateLimiter.Acquire(1)
		if stop {
			Info.Println("Server: shutdown server...")
			break
		}
		connect, err := listener.Accept()
		Info.Printf("Server: accept %v\n", connect.RemoteAddr())
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
	for {
		CheckRecipientsAvailable()
		time.Sleep(time.Minute)
	}
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
				Warn.Println("Scheduler: dequeue error, " + err.Error())
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
				Warn.Println("Scheduler: retry-msg dequeue error, " + err.Error())
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
				Warn.Println("Scheduler: dl-msg dequeue error, " + err.Error())
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
				Trace.Println("Scheduler: stop.")
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
			Warn.Println("Scanner: get records error, " + err.Error())
			time.Sleep(30 * time.Second)
			continue
		}
		for msgId := range records {
			diff := (time.Now().UnixNano() - int64(records[msgId])) - int64(Configuration.ACKTimeout)*int64(time.Millisecond)
			if diff > 0 || -diff < int64(time.Second) {

				_, err := MessageEntryRetryQueue(msgId)
				switch err.(type) {
				case NoSuchMessage:
					Warn.Println("Scheduler: warning, " + err.Error())
					DeadLetterEnqueue(msgId)
				case MessageDead:
					Warn.Println("Scheduler: " + err.Error())
					DeadLetterEnqueue(msgId)
				case nil:
					Info.Printf("Scheduler: %s will be retried \n", msgId)
				default:
					Warn.Println("Scheduler: " + err.Error())
				}
			}
		}
		time.Sleep(time.Minute)
	}
}

// TODO should use it to process expired messages.
func processDeadLetter(message Message) {
	DeleteMessage(message.MsgId)
}

func delivery(message Message) {
	message.Type = MPush
	go func() {
		defer func() {
			e := recover()
			if e != nil {
				Err.Printf("Delivery: panic, %s \n", e)
			}
		}()

		Info.Printf("Delivery: delivery message %s/%s \n", message.AppID, message.MsgId)
		conn, err := DeliveryMessage(message.AppID, EncodeMessage(message))
		if err != nil {
			Err.Println("Delivery: error, " + err.Error())
			return
		}
		conn.SetReadDeadline(time.Now().Add(ConnectTimeOut))
		reply(conn, true, handleMessage(DecodeMessage(ReadStream(conn))))
	}()
}

func receive(connect net.Conn) {
	go func() {

		defer func() {
			p := recover()
			if p != nil {
				Err.Printf("%v\n", p)
			}
		}()
		reply(connect, false, handleMessage(DecodeMessage(ReadStream(connect))))
	}()
}

func reply(connect net.Conn, proactive bool, repChan <-chan Response) {
	disconnect := false
	for {
		response, ok := <-repChan
		if !ok {
			Info.Println("Reply: message handler close the channel")
			disconnect = true
			break
		}
		Trace.Printf("Reply: debug %v\n", response)
		if strings.Compare(response.Status, PONG) == 0 {
			ReplyHeartbeat(connect)
			Trace.Println("Reply: PONG")
			continue
		}

		content, err := json.Marshal(response)
		if err != nil {
			panic(errors.New("reply: json marshal struct failed"))
		}
		err = SendMessage(connect, EncodeMessage(Message{Content: content, Type: MResponse}))

		if err != nil {
			Warn.Println("Reply: " + err.Error())
		}
		if proactive {
			disconnect = true
			break
		}

		disconnect = response.Disconnect || disconnect
	}

	if disconnect {
		Info.Println("Close connect: " + connect.RemoteAddr().String())
		err := connect.Close()
		if err != nil {
			Warn.Println("Reply: close connect failed, " + err.Error())
		}
	} else {
		connect.SetReadDeadline(time.Time{})
	}

}

func handleMessage(msgChan <-chan Message) <-chan Response {
	out := make(chan Response, ProcessBuf)
	go func() {
		for {
			message, ok := <-msgChan
			if !ok {
				Trace.Println("Handler: message channel closed.")
				break
			}
			Info.Printf("Handler: handle message %s/%s[%s] \n", message.AppID, message.MsgId, message.Type)
			switch message.Type {
			case RegisterMsg:
				Warn.Println("Handler: mesage rejected: " + message.MsgId)
				err := recipientRegister(message)
				status := MAck
				var content = "Recipient register successful."
				if err != nil {
					Warn.Println(err)
					status = MReject
					content = err.Error()
				}
				out <- Response{Status: status, Content: content}
			case MArrivedMsg:
				Info.Println("Handler: mesage deliveried successfully: " + message.MsgId)
				disconnect := true
				err := arrive(message)
				status := MAck
				if err != nil {
					status = MError
					disconnect = false
					Warn.Printf("arrived ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status, Disconnect: disconnect}
			case MAckMsg:
				Info.Println("Handler: message ack, " + message.MsgId)
				err := ack(message)
				status := MAck
				if err != nil {
					status = MError
					Warn.Printf("message ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status}
			case MError:
				Warn.Printf("Msg %s error\n", message.MsgId)
				err := msgError(message)
				status := MAck
				disconnect := true
				if err != nil {
					status = MError
					disconnect = false
					Err.Printf("process error ack error: %s/%s, %s \n",
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
					Warn.Printf("reject ack error: %s/%s, %s \n",
						message.AppID, message.MsgId, err.Error())
				}
				out <- Response{Status: status, Disconnect: disconnect}
			case PING:
				out <- Response{Status: PONG}
			default:
				Info.Println("Save message " + message.MsgId)
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
