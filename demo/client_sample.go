package main

import (
	"fmt"
	"github.com/raomuyang/sikong-mq/skmq/base"
	"github.com/raomuyang/sikong-mq/skmq/exchange"
	"github.com/raomuyang/sikong-mq/skmq/process"
	"net"
	"time"
)

func main() {
	ex := exchange.GetExchange(nil)
	content := []byte("Content ")

	testMsg1 := base.Message{
		MsgId:   "Test-msg-id-1",
		AppID:   "test-app-id",
		Type:    base.QueueMsg,
		Content: append(content, []byte("- Msg-1")...)}

	testMsg2 := base.Message{
		MsgId:   "Test-msg-id-2",
		AppID:   "test-app-id",
		Type:    base.TopicMsg,
		Content: append(content, []byte("- Msg-2")...)}

	conn, err := net.Dial("tcp", "127.0.0.1:1734")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	pong := ex.Heartbeat(conn)
	fmt.Println("Response:", pong)

	go func() {
		c := process.ReadStream(conn)
		for {
			bytes, ok := <-c
			if !ok {
				fmt.Println("exit")
				break
			}
			fmt.Printf("%s", bytes)
		}
	}()

	process.SendMessage(conn, process.EncodeMessage(testMsg1))

	process.SendMessage(conn, process.EncodeMessage(testMsg2))

	time.Sleep(30 * time.Second)
}
