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
	"strings"
)

/**
	模仿Http协议，每个请求行均以\r\n间隔
 */
type Message struct {
	MsgId   string
	AppID   string
	Type    string
	Content []byte
	Status  string
	Retried int
}

type Response struct {
	Status  string `json:"status"`
	Content string `json:"content"`
}

type Result struct {
	MsgId        string
	Err          error
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
		communication(connect)
	}
}

func StopServer() {
	stopServer = true
}

func communication(connect net.Conn) {
	go func() {

		defer func() {
			err := recover()
			if err != nil {
				// TODO log
			}
		}()
		responseMessage(connect, handleMessage(decodeMessage(readStream(connect))))
	}()
}

func responseMessage(connect net.Conn, repChan <-chan Response) {
	response, ok := <-repChan
	if !ok {
		panic(errors.New("message handler close the channel"))
	}
	rs, err := json.Marshal(response)
	if err != nil {
		panic(errors.New("json marshal struct failed"))
	}
	rs = append(rs, []byte(Delim)...)

	// TODO 如果信息量大一次发送会有问题
	w, err := connect.Write(rs)
	fmt.Println(w)
	if err != nil {
		// TODO log
		fmt.Println(err)
	}
}

func handleMessage(msgChan <-chan Message) <-chan Response {
	out := make(chan Response, 8)
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
		case MReadyMsg:
			err := ready(message)
			status := MAck
			content := ""
			if err != nil {
				status = MError
				content = err.Error()
			}
			out <- Response{Status: status, Content: content}
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
		default:
			err := saveMessage(message)
			status := MAck
			content := "Message enqueue"
			if err != nil {
				status = MReject
				content = "Message enqueue failed"
			}
			out <- Response{Status: status, Content: content}
		}
	}()
	return out
}

func readStream(connect net.Conn) (<-chan []byte) {
	input := make(chan []byte, 8)

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
	}()
	return input
}

func decodeMessage(input <-chan []byte) <-chan Message {
	msgChan := make(chan Message, 8)
	go func() {
		message := Message{Status: MPending, Retried: 0}
		var line []byte
		for {
			buf, ok := <-input
			if !ok {
				close(msgChan)
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

					key := fmt.Sprintf("%s", sub[:spIndex])
					if strings.Compare(PContent, key) == 0 {
						value := sub[spIndex+1:]
						message.Content = value
						continue
					}

					value := fmt.Sprintf("%s", sub[spIndex+1:])
					switch key {
					case PMsgId:
						message.MsgId = value
					case PAppID:
						message.AppID = value
					case PRequestType:
						message.Type = value
					default:
						fmt.Printf("No such parameter: %s.\n", key)

					}
				} else {
					// 丢弃没有类型的不完整信息
					if len(message.Type) > 0 {
						msgChan <- message
						message = Message{Status: MPending}
					}
				}
			}
			line = split[len(split)-1]
		}
	}()
	return msgChan
}

/**
	Recipient register
 */
func processRegisterMsg(message Message) error {
	consumer := RecipientInfo{}
	err := json.Unmarshal(message.Content, &consumer)
	if err != nil {
		return InvalidParameters{Content: string(message.Content)}
	}

	return SaveRecipientInfo(consumer)
}

/**
	Recipient ack
	TODO 如何处理
 */
func ack(message Message) error {
	return DeleteMessage(message.MsgId)
}

/**
	Recipient reject
 */
func reject(message Message) error {
	return nil
}

func ready(message Message) error {
	return UpdateMessageStatus(message.MsgId, MReady)
}

func queryMessageStatus(message Message) (string, error) {
	return "", nil
}

func saveMessage(message Message) error {
	return MessageEnqueue(message)
}
