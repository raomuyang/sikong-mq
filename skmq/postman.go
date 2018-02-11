package skmq

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/sikong-mq/skmq/base"
	"github.com/sikong-mq/skmq/process"
)

type Postman struct {
	deliveryQueue   chan base.Message
	deadLetterQueue chan base.Message
	waitGroup       *sync.WaitGroup
}

func InitPostman() *Postman {
	var wg sync.WaitGroup
	wg.Add(2)

	return &Postman{
		deliveryQueue:   make(chan base.Message, SendBuf),
		deadLetterQueue: make(chan base.Message, SendBuf),
		waitGroup:       &wg}
}

func (postman *Postman) schedule() {

	go postman.normalLetterSort(postman.deliveryQueue, postman.waitGroup)

	go postman.retryLetterSort(postman.deliveryQueue, postman.waitGroup)

	go postman.deadLetterSort(postman.deadLetterQueue)

	go postman.letterTransfer(postman.deliveryQueue, postman.deadLetterQueue)

	postman.waitGroup.Wait()
	close(postman.deliveryQueue)
}

func (postman *Postman) normalLetterSort(deliveryQueue chan<- base.Message, waitGroup *sync.WaitGroup) {

	backoff := base.Backoff{}
	// Msg dequeue
	for {
		if stop {
			waitGroup.Done()
			break
		}
		msg, err := msgCache.MessageDequeue(base.KMessageQueue)
		if err != nil {
			Warn.Println("Scheduler: dequeue error, " + err.Error())
			time.Sleep(10 * time.Second)
			continue
		}
		if msg == nil {
			t := backoff.Increase()
			time.Sleep(t)
			Trace.Printf("No message, it will be retried after %s", t)
			continue
		}
		backoff.Reset()
		deliveryQueue <- *msg
	}

}

func (postman *Postman) retryLetterSort(deliveryQueue chan<- base.Message, waitGroup *sync.WaitGroup) {
	backoff := base.Backoff{}
	// retry-msg dequeue
	for {
		if stop {
			waitGroup.Done()
			break
		}
		msg, err := msgCache.MessageDequeue(base.KMessageRetryQueue)
		if err != nil {
			Warn.Println("Scheduler: retry-msg dequeue error, " + err.Error())
			time.Sleep(15 * time.Second)
			continue
		}
		if msg == nil {
			t := backoff.Increase()
			time.Sleep(t)
			continue
		}
		backoff.Reset()
		deliveryQueue <- *msg
	}
}

func (postman *Postman) deadLetterSort(dlQueue chan<- base.Message) {
	backoff := base.Backoff{}
	// dl-msg dequeue
	for {
		if stop {
			close(dlQueue)
			break
		}
		msg, err := msgCache.MessageDequeue(base.KDeadLetterQueue)
		if err != nil {
			Warn.Println("Scheduler: dl-msg dequeue error, " + err.Error())
			time.Sleep(30 * time.Second)
			continue
		}
		if msg == nil {
			t := backoff.Increase()
			time.Sleep(t)
			continue
		}
		backoff.Reset()
		dlQueue <- *msg
	}
}

func (postman *Postman) letterTransfer(deliveryQueue <-chan base.Message, dlQueue <-chan base.Message) {
	// msg delivery
	for {

		closed := false
		select {
		case msg, ok := <-deliveryQueue:
			if !ok {
				closed = true
				break
			}
			postman.delivery(msg)
		case msg, ok := <-dlQueue:
			if !ok {
				closed = true
				break
			}
			postman.processDeadLetter(msg)
		}
		if closed {
			Trace.Println("Letter transfer stopped.")
			break
		}
	}
}

// TODO Should distinguish between the topic and queue
func (postman *Postman) delivery(message base.Message) {
	Info.Printf("Delivery: (%s) message %s/%s \n",
		message.Type, message.AppID, message.MsgId)
	switch message.Type {
	case base.QueueMsg:
		Info.Printf("Queue message: %s \n", message.MsgId)
		postman.queue(message)
		break
	case base.TopicMsg:
		Info.Printf("Topic message: %s \n", message.MsgId)
		postman.topic(message)
	default:
		Warn.Printf("None type: %s\n", message.MsgId)
	}
}

func (postman *Postman) topic(message base.Message) {
	message.Type = base.MPush

	go func() {
		channel, err := dataExchange.BroadcastConnect(message.AppID)
		if err != nil {
			Warn.Printf("Get broadcast connects failed: %v", err)
			return
		}
		for {
			connect, ok := <-channel
			if !ok {
				break
			}
			go postman.broadcastConnection(connect, message)

		}
	}()
}

func (postman *Postman) queue(message base.Message) {
	message.Type = base.MPush
	go func() {
		defer func() {
			e := recover()
			if e != nil {
				Err.Printf("Delivery: panic (%s)  message (%s/%s) \n",
					e, message.AppID, message.MsgId)
			}
		}()

		conn, err := dataExchange.Unicast(message.AppID, process.EncodeMessage(message))
		remote := "nil"
		if conn != nil {
			remote = fmt.Sprintf("%v", conn.RemoteAddr())
		}
		logInfo := fmt.Sprintf("Delivery: message (%s/%s) remote (%s)",
			message.AppID, message.MsgId, remote)
		Info.Println(logInfo)
		if err != nil {
			Err.Printf("%s error (%s) ", logInfo, err.Error())
			return
		}
		conn.SetReadDeadline(time.Now().Add(base.ConnectTimeOut))
		reply(conn, true, msgHandler.HandleMessage(process.DecodeMessage(process.ReadStream(conn))))
	}()
}

func (postman *Postman) broadcastConnection(conn net.Conn, message base.Message) {
	logInfo := fmt.Sprintf("Delivery: broadcast message (%s/%s) remote (%s)",
		message.AppID, message.MsgId, conn.RemoteAddr())
	Info.Println(logInfo)
	err := dataExchange.DeliveryContent(conn, process.EncodeMessage(message))
	if err != nil {
		Warn.Printf("Delivery: ")
	}
	conn.SetReadDeadline(time.Now().Add(base.ConnectTimeOut))
	reply(conn, true, msgHandler.HandleMessage(process.DecodeMessage(process.ReadStream(conn))))

}

func (postman *Postman) processDeadLetter(message base.Message) {
	deadLetterHandler.Process(message)
}
