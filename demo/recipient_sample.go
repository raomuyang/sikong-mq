package main

import (
	"encoding/json"
	"fmt"
	"github.com/raomuyang/sikong-mq/skmq/base"
	"github.com/raomuyang/sikong-mq/skmq/exchange"
	"github.com/raomuyang/sikong-mq/skmq/process"
	"net"
)

var recipient = base.RecipientInfo{
	RecipientId:   "localtest",
	ApplicationId: "test-app-id",
	Host:          "127.0.0.1",
	Port:          "9001",
}

func main() {
	register()
	// 启动消息接收服务
	fmt.Println("Start recipient server.")
	server, err := net.Listen("tcp", recipient.Host+":"+recipient.Port)
	if err != nil {
		panic(err)
	}
	for {
		c, err := server.Accept()
		if err != nil {
			fmt.Println("Accept error:", err.Error())
			continue
		}

		msgChan := process.DecodeMessage(process.ReadStream(c))
		go func() {
			for {
				msg, ok := <-msgChan
				if !ok {
					break
				}
				fmt.Printf("Received: %v, content: %s \n", msg, msg.Content)
				switch msg.Type {
				case base.PING:
					exchange.ReplyHeartbeat(c)
				case base.MPush:
					respMsg := base.Message{MsgId: msg.MsgId, Type: base.MAckMsg}
					process.SendMessage(c, process.EncodeMessage(respMsg))
				case base.MResponse:
					fmt.Printf("This is response: %v\n", msg)
				default:

				}
			}
		}()
	}
}

func register() {
	content, err := json.Marshal(recipient)
	if err != nil {
		panic(err)
	}

	msg := base.Message{
		Type:    base.RegisterMsg,
		Content: content}

	conn, err := net.Dial("tcp", "127.0.0.1:1734")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	err = process.SendMessage(conn, process.EncodeMessage(msg))
	buf := make([]byte, 1024)
	read, err := conn.Read(buf)
	if err != nil {
		fmt.Printf("Register: %s\n", err)
	}
	fmt.Printf("%s", buf[:read])
}
