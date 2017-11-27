package main

import (
	"github.com/sikong-mq/skmq"
	"encoding/json"
	"fmt"
	"net"
)

var recipient = skmq.RecipientInfo{
	RecipientId:   "localtest",
	ApplicationId: "test-app-id",
	Host:          "127.0.0.1",
	Port:          "9001",
}

func main() {
	register()
	// 启动消息接收服务
	fmt.Println("Start recipient server.")
	server, err := net.Listen("tcp", recipient.Host + ":" + recipient.Port)
	if err != nil {
		panic(err)
	}
	for {
		c, err := server.Accept()
		if err != nil {
			fmt.Println("Accept error:", err.Error())
			continue
		}

		msgChan := skmq.DecodeMessage(skmq.ReadStream(c))
		go func() {
			for {
				msg, ok := <- msgChan
				if !ok {
					break
				}
				fmt.Printf("Received: %v, content: %s \n", msg, msg.Content)
				switch msg.Type {
				case skmq.PING:
					skmq.ReplyHeartbeat(c)
				case skmq.MPush:
					respMsg := skmq.Message{MsgId: msg.MsgId, Type: skmq.MAckMsg}
					skmq.SendMessage(c, skmq.EncodeMessage(respMsg))
				case skmq.MResponse:
					fmt.Printf("This is response: %v\n", msg)
				default:

				}
			}
		}()
	}
}

func register()  {
	content, err := json.Marshal(recipient)
	if err != nil {
		panic(err)
	}

	msg := skmq.Message{
		Type: skmq.RegisterMsg,
		Content: content}

	conn, err := net.Dial("tcp", "127.0.0.1:1734")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	err = skmq.SendMessage(conn, skmq.EncodeMessage(msg))
	buf := make([]byte, 1024)
	read, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Register: %s\n", err)
	}
	fmt.Printf("%s", buf[:read])
}