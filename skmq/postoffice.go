package skmq

import (
	"net"
	"errors"
	"encoding/json"
	"time"
	"strings"
	"sync"
	"github.com/sikong-mq/skmq/ratelimiter"
	"github.com/sikong-mq/skmq/model"
)

type Message model.Message
type RecipientInfo model.RecipientInfo

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

	rateLimiter, err := ratelimiter.CreateTokenBucket(Configuration.Rate)
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

// TODO Should use it to process expired messages.
func processDeadLetter(message Message) {
	DeleteMessage(message.MsgId)
}

// TODO Should distinguish between the topic and queue
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
				processRegisterMsg(message, out)
			case MArrivedMsg:
				Info.Println("Handler: mesage deliveried successfully: " + message.MsgId)
				processArrivedMsg(message, out)
			case MAckMsg:
				Info.Println("Handler: message ack, " + message.MsgId)
				processAckMsg(message, out)
			case MError:
				Warn.Printf("Msg %s error\n", message.MsgId)
				processErrorMsg(message, out)
			case MRejectMsg:
				Warn.Printf("Msg %s rejected\n", message.MsgId)
				processRejectedMsg(message, out)
			case PING:
				out <- Response{Status: PONG}
			default:
				Info.Println("Save message " + message.MsgId)
				processNewMsg(message, out)
			}
		}
		close(out)
	}()
	return out
}

