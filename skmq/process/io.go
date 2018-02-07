package process

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/sikong-mq/skmq/base"
	"github.com/sikong-mq/skmq/skerr"
	"io"
	"net"
	"strings"
)

const (
	channelSize      = 8
	sendBufChunkSize = 1 << 10
	writeChunkSize   = 1 << 13
)

/**
Send stream end with `\r\n\r\n`
*/
func SendMessage(conn net.Conn, buf []byte) error {
	buf = append(buf, []byte(base.End)...)
	return WriteBuffer(conn, buf)
}

func WriteBuffer(conn net.Conn, buf []byte) error {

	length := len(buf)
	position := 0
	for {
		if length == position {
			return nil
		}
		end := position + writeChunkSize
		if end > length {
			end = length
		}
		w, err := conn.Write(buf[position:end])
		if err != nil {
			return err
		}
		position += w
	}
}

func ReadStream(connect net.Conn) <-chan []byte {
	input := make(chan []byte, channelSize)

	go func() {
		reader := bufio.NewReader(connect)
		buf := make([]byte, sendBufChunkSize)
		for {
			read, err := reader.Read(buf)
			if err != nil {
				if err != io.EOF {
					Trace.Println("Stream reader:", err)
				}
				break
			}
			input <- buf[:read]
		}
		close(input)
	}()
	return input
}

/**
这里使用四个换行（\r\n\r\n）来间隔一段消息解决tcp消息粘包问题，
每个参数之间用两个换行（\r\n）间隔
*/
func DecodeMessage(input <-chan []byte) <-chan base.Message {
	msgChan := make(chan base.Message, channelSize)
	go func() {
		defer func() {
			p := recover()
			if p != nil {
				Err.Println("Decode message error:", p)
			}
		}()
		message := base.Message{Status: base.MPending, Retried: 0}
		var line []byte
		for {
			buf, ok := <-input
			if !ok {
				close(msgChan)
				break
			}
			line = append(line, buf[:]...)
			buf = []byte{}

			split := bytes.Split(line, []byte(base.Delim))
			for i := 0; i < len(split)-1; i++ {
				sub := split[i]
				if len(sub) > 0 {
					spIndex := bytes.Index(sub, []byte(base.Separator))
					if spIndex < 0 {
						// 心跳信息
						if bytes.Compare([]byte(base.PING), sub) == 0 {
							msgChan <- base.Message{Type: base.PING}
							continue
						} else {
							Err.Println("MError: param separator not found")
							panic(
								skerr.StreamReadError{
									Input: sub,
									Msg:   "param separator not found"})
						}
					}

					key := fmt.Sprintf("%s", sub[:spIndex])
					if strings.Compare(base.PContent, key) == 0 {
						value := sub[spIndex+1:]
						message.Content = value
						continue
					}

					value := fmt.Sprintf("%s", sub[spIndex+1:])
					switch key {
					case base.PMsgId:
						message.MsgId = value
					case base.PAppID:
						message.AppID = value
					case base.PRequestType:
						message.Type = value
					default:
						Warn.Printf("No such parameter: %s.\n", key)

					}
				} else {
					// 丢弃没有类型的不完整信息
					if len(message.Type) > 0 {
						msgChan <- message
						message = base.Message{Status: base.MPending}
					}
				}
			}
			line = split[len(split)-1]
		}
	}()
	return msgChan
}

func EncodeMessage(message base.Message) []byte {
	list := []string{
		base.PRequestType, base.Separator, message.Type, base.Delim}
	if len(message.AppID) > 0 {
		list = append(list, base.PAppID, base.Separator, message.AppID, base.Delim)
	}
	if len(message.MsgId) > 0 {
		list = append(list, base.PMsgId, base.Separator, message.MsgId, base.Delim)
	}
	header := strings.Join(list, "")
	content := append([]byte(base.PContent+base.Separator), message.Content...)
	return append(append([]byte(header), content...), []byte(base.End)...)
}
