package session

import (
	"net"
	"bufio"
	"io"
	"fmt"
	"bytes"
	"errors"
	"encoding/json"
	"time"
)


/**
	模仿Http协议，每个请求行均以\r\n间隔
 */
type Message struct {
	MsgId   string
	AppID   string
	Type    string
	Content []byte
	Status string
	Retried int
}

type Response struct {
	Status string
	Content string
}

type Result struct {
	MsgId string
	Err error
	FinishedTime time.Time
}




var stopServer bool = false

func OpenServer() {
	laddr := Configuration.ListenerHost + ":" + Configuration.ListenerPort
	listener, err := net.Listen("tcp", laddr)
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	for {
		if stopServer {
			fmt.Println("Shutdown server...")
			break
		}
		connect, err := listener.Accept()
		if err != nil {
			panic(err)
		}
		receiveMessage(connect)
	}
}


func StopServer() {
	stopServer = true
}

func receiveMessage(connect net.Conn) {
	defer func() {
		err := recover()
		if err != nil {
			// TODO log
		}
	}()
	responseMessage(connect, handleMessage(decodeMessage(readStream(connect))))
}

func responseMessage(connect net.Conn, repChan <-chan Response) {
	go func() {
		response, ok := <-repChan
		if !ok {
			panic(errors.New("message handler close the channel"))
		}
		rs, err := json.Marshal(response)
		if err != nil {
			panic(errors.New("json marshal struct failed"))
		}
		rs = append(rs, []byte(Delim)...)
		w, err := connect.Write(rs)
		fmt.Println(w)
		if err != nil {
			// TODO log
			fmt.Println(err)
		}
	}()
}

func handleMessage(msgChan <-chan Message) <-chan Response{
	out := make(chan Response, 1)
	go func() {
		message, ok := <-msgChan
		if !ok {
			panic(MessageHandleError{"Channel closed"})
		}

		switch message.Type {
		case RegisterMsg:
			err := processRegisterMsg(message)
			status := MAck
			var content = "Register successful."
			if err != nil {
				// TODO log
				status = MReject
				content = err.Error()
			}
			out <- Response{Status: status, Content: content}
		case MAckMsg:
			err := ack(message)
			status := MAck
			if err != nil {
				status = MError
			}
			out <- Response{Status: status}

		case MRejectMsg:
			reject(message)
		case MQueryMsg:
			msgStatus, err := queryMessageStatus(message)
			status := MAck
			content := msgStatus
			if err != nil {
				status = MReject
				content = err.Error()
			}
			out <- Response{Status: status, Content: content}
		}
	}()
	return out
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

func decodeMessage(input <-chan []byte) <-chan Message {
	msgChan := make(chan Message, 1)
	go func() {
		message := Message{Status: MPending, Retried: 0}
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
							fmt.Println("MError: param separator not found")
							panic(StreamReadError{sub, "param separator not found"})
						}

						key := sub[:spIndex]
						strKey := fmt.Sprintf("%s", sub[:spIndex])
						if bytes.Equal(key, []byte(PContent)) {
							value := sub[spIndex+1:]
							message.Content = value
							break
						}

						value := fmt.Sprintf("%s", sub[spIndex+1:])
						switch strKey {
						case PMsgId:
							message.MsgId = value
						case PAppID:
							message.AppID = value
						case PRequestType:
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


/**
	Recipient register
 */
func processRegisterMsg(message Message) error {
	return nil
}

/**
	Recipient ack
 */
func ack(message Message) error {
	return nil
}

/**
	Recipient reject
 */
func reject(message Message) error {
	return nil
}

func queryMessageStatus(message Message) (string, error) {
	return "", nil
}
