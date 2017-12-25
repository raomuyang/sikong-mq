package skmq

import (
	"net"
	"errors"
	"encoding/json"
	"time"
	"strings"
	"sync"
	"github.com/sikong-mq/skmq/ratelimiter"
	"github.com/sikong-mq/skmq/base"
	"github.com/sikong-mq/skmq/process"
	"github.com/sikong-mq/skmq/exchange"
	"github.com/sikong-mq/skmq/skerr"
)

const (
	SendBuf    = 1 << 5
)

var (
	stop = false
	mutex sync.Mutex
)


func OpenServer() {
	process.InitDBConfig(*process.DBConfiguration)
	laddr := process.Configuration.ListenerHost + ":" + process.Configuration.ListenerPort
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

	rateLimiter, err := ratelimiter.CreateTokenBucket(process.Configuration.Rate)
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
	mutex.Lock()
	defer mutex.Unlock()

	if stop {
		return
	}
	stop = true
}

func heartbeatCyclically() {
	for {
		exchange.CheckRecipientsAvailable()
		time.Sleep(time.Minute)
	}
}

// scan ack timeout
func scanTimeoutTasks() {

	for {
		if stop {
			break
		}

		records, err := process.MessagePostRecords()
		if err != nil {
			Warn.Println("Scanner: get records error, " + err.Error())
			time.Sleep(30 * time.Second)
			continue
		}
		for msgId := range records {
			diff := (time.Now().UnixNano() - int64(records[msgId])) - int64(process.Configuration.ACKTimeout)*int64(time.Millisecond)
			if diff > 0 || -diff < int64(time.Second) {

				_, err := process.MessageEntryRetryQueue(msgId)
				switch err.(type) {
				case skerr.NoSuchMessage:
					Warn.Println("Scheduler: warning, " + err.Error())
					process.DeadLetterEnqueue(msgId)
				case skerr.MessageDead:
					Warn.Println("Scheduler: " + err.Error())
					process.DeadLetterEnqueue(msgId)
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


func receive(connect net.Conn) {
	go func() {

		defer func() {
			p := recover()
			if p != nil {
				Err.Printf("%v\n", p)
			}
		}()
		reply(connect, false, process.HandleMessage(process.DecodeMessage(process.ReadStream(connect))))
	}()
}

func reply(connect net.Conn, proactive bool, repChan <-chan base.Response) {
	disconnect := false
	for {
		response, ok := <-repChan
		if !ok {
			Info.Println("Reply: message handler close the channel")
			disconnect = true
			break
		}
		Trace.Printf("Reply: debug %v\n", response)
		if strings.Compare(response.Status, base.PONG) == 0 {
			exchange.ReplyHeartbeat(connect)
			Trace.Println("Reply: PONG")
			continue
		}

		content, err := json.Marshal(response)
		if err != nil {
			panic(errors.New("reply: json marshal struct failed"))
		}
		err = process.SendMessage(connect, process.EncodeMessage(base.Message{Content: content, Type: base.MResponse}))

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



