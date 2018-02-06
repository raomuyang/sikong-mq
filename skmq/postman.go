package skmq

import (
	"github.com/sikong-mq/skmq/base"
	"sync"
	"github.com/sikong-mq/skmq/process"
	"time"
	"github.com/sikong-mq/skmq/exchange"
	"net"
	"fmt"
)

func schedule() {
	deliveryQueue := make(chan base.Message, SendBuf)
	dlQueue := make(chan base.Message, SendBuf)

	var wg sync.WaitGroup
	wg.Add(2)

	go normalLetterSort(deliveryQueue, wg)

	go retryLetterSort(deliveryQueue, wg)

	go deadLetterSort(dlQueue)

	go letterTransfer(deliveryQueue, dlQueue)

	wg.Wait()
	close(deliveryQueue)
}


func normalLetterSort(deliveryQueue chan<- base.Message, waitGroup sync.WaitGroup) {

	backoff := base.Backoff{}
	// Msg dequeue
	for {
		if stop {
			waitGroup.Done()
			break
		}
		msg, err := process.MsgCache.MessageDequeue(base.KMessageQueue)
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


func retryLetterSort(deliveryQueue chan<- base.Message, waitGroup sync.WaitGroup)  {
	backoff := base.Backoff{}
	// retry-msg dequeue
	for {
		if stop {
			waitGroup.Done()
			break
		}
		msg, err := process.MsgCache.MessageDequeue(base.KMessageRetryQueue)
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

func deadLetterSort(dlQueue chan<- base.Message)  {
	backoff := base.Backoff{}
	// dl-msg dequeue
	for {
		if stop {
			close(dlQueue)
			break
		}
		msg, err := process.MsgCache.MessageDequeue(base.KDeadLetterQueue)
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

func letterTransfer(deliveryQueue <-chan base.Message, dlQueue <-chan base.Message)  {
	// msg delivery
	for {

		select {
		case msg := <-deliveryQueue:
			delivery(msg)
		case msg := <-dlQueue:
			processDeadLetter(msg)
		}
	}
}

// TODO Should distinguish between the topic and queue
func delivery(message base.Message) {
	Info.Printf("Delivery: (%s) message %s/%s \n",
		message.Type, message.AppID, message.MsgId)
	switch message.Type {
	case base.QueueMsg:
		Info.Printf("Queue message: %s \n", message.MsgId)
		queue(message)
		break
	case base.TopicMsg:
		Info.Printf("Topic message: %s \n", message.MsgId)
		topic(message)
	default:
		Warn.Printf("None type: %s\n", message.MsgId)
	}
}

func topic(message base.Message)  {
	message.Type = base.MPush

	go func() {
		channel, err := exchange.BroadcastConnect(message.AppID)
		if err != nil {
			Warn.Printf("Get broadcast connects failed: %v", err)
			return
		}
		for {
			connect, ok := <- channel
			if !ok {break}
			go broadcastConnection(connect, message)

		}
	}()
}

func queue(message base.Message)  {
	message.Type = base.MPush
	go func() {
		defer func() {
			e := recover()
			if e != nil {
				Err.Printf("Delivery: panic (%s)  message (%s/%s) \n",
					e, message.AppID, message.MsgId)
			}
		}()

		conn, err := exchange.Unicast(message.AppID, process.EncodeMessage(message))
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
		reply(conn, true, process.HandleMessage(process.DecodeMessage(process.ReadStream(conn))))
	}()
}

func broadcastConnection(conn net.Conn, message base.Message)  {
	logInfo := fmt.Sprintf("Delivery: broadcast message (%s/%s) remote (%s)",
		message.AppID, message.MsgId, conn.RemoteAddr())
	Info.Println(logInfo)
	err := exchange.DeliveryContent(conn, process.EncodeMessage(message))
	if err != nil {
		Warn.Printf("Delivery: ")
	}
	conn.SetReadDeadline(time.Now().Add(base.ConnectTimeOut))
	reply(conn, true, process.HandleMessage(process.DecodeMessage(process.ReadStream(conn))))

}

func processDeadLetter(message base.Message) {
	deadLetterHandler.Process(message)
}
