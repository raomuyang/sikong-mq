package session

import (
	"net"
	"bufio"
	"io"
	"fmt"
	"bytes"
)

const (
	AppID       = "appid"
	MsgId       = "msgid"
	RequestType = "type"
	Content     = "content"
	TopicType   = "topic"
	QueueType   = "queue"
	QueryType   = "query"
	Delim       = "\r\n"
	Separator   = "="
)

/**
	模仿Http协议，每个请求行均以\r\n间隔
 */
type Message struct {
	MessageId     string
	ApplicationID string
	Type          string
	Content       []byte
}

var stopServer bool = false

func OpenServer(address string, port string) {
	laddr := address + ":" + port
	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	for {
		if stopServer {
			break
		}
		connect, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		receiveMessage(connect)
	}
}

func RegisterRecipent(recipient RecipientInfo) {

}

func StopServer() {
	stopServer = true
}

func receiveMessage(connect net.Conn) <-chan Message {
	return handleStream(readStream(connect))
}

func readStream(connect net.Conn) (<-chan []byte) {
	input := make(chan []byte, 4)
	quit := make(chan bool, 0)

	go func() {
		reader := bufio.NewReader(connect)
		buf := make([]byte, 1024)
		for {
			read, err := reader.Read(buf)
			if err != nil {
				if err == io.EOF {
					break
				} else {
					panic(err)
				}
			}
			input <- buf[:read]
		}
		close(input)
		quit <- true
	}()
	return input
}

func handleStream(input <-chan []byte) <-chan Message {
	msgChan := make(chan Message, 1)
	go func() {
		message := Message{}
		var line []byte
		var exit = false
		for {
			select {
			case buf, ok := <-input:
				if !ok {
					exit = true
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
							fmt.Println("Error: param separator not found")
							panic(StreamReadError{sub, "param separator not found"})
						}

						key := sub[:spIndex]
						strKey := fmt.Sprintf("%s", sub[:spIndex])
						if bytes.Equal(key, []byte(Content)) {
							value := sub[spIndex+1:]
							message.Content = value
							break
						}

						value := fmt.Sprintf("%s", sub[spIndex+1:])
						switch strKey {
						case MsgId:
							message.MessageId = value
						case AppID:
							message.ApplicationID = value
						case RequestType:
							message.Type = value
						default:
							fmt.Printf("No such parameter: %s.\n", strKey)

						}
					}
				}
				line = split[len(split)-1]
			}
			if exit {
				msgChan <- message
				break
			}
		}
	}()
	return msgChan
}
