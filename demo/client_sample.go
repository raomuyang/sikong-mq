package main

import (
	"github.com/sikong-mq/skmq"
	"net"
	"fmt"
	"time"
)

func main() {
	content := []byte("Content ")

	testMsg1 := skmq.Message{
		MsgId:   "Test-msg-id-1",
		AppID:   "test-app-id",
		Type:    skmq.TopicMsg,
		Content: append(content, []byte("- Msg-1")...)}

	testMsg2 := skmq.Message{
		MsgId:   "Test-msg-id-2",
		AppID:   "test-app-id",
		Type:    skmq.TopicMsg,
		Content: append(content, []byte("- Msg-2")...)}

	conn, err := net.Dial("tcp", "127.0.0.1:1734")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	pong := skmq.Heartbeat(conn)
	fmt.Println("Response:", pong)

	go func() {
		c := skmq.ReadStream(conn)
		for {
			bytes, ok := <-c
			if !ok {
				fmt.Println("exit")
				break
			}
			fmt.Printf("%s", bytes)
		}
	}()


	skmq.SendMessage(conn, skmq.EncodeMessage(testMsg1))

	skmq.SendMessage(conn, skmq.EncodeMessage(testMsg2))

	time.Sleep(30 * time.Second)
}
