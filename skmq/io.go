package skmq

import (
	"net"
	"bufio"
	"io"
)

const (
	CHANBUF = 8
	SENDCHUNK = 1 << 10
	CHUNK   = 1 << 13
)

/**
	Send stream end with `\r\n\r\n`
 */
func SendMessage(conn net.Conn, buf []byte) (error) {
	buf = append(buf, []byte(End)...)
	return WriteBuffer(conn, buf)
}

func WriteBuffer(conn net.Conn, buf []byte) (error) {

	length := len(buf)
	position := 0
	for {
		if length == position {
			return nil
		}
		end := position + CHUNK
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

func ReadStream(connect net.Conn) (<-chan []byte) {
	input := make(chan []byte, CHANBUF)

	go func() {
		reader := bufio.NewReader(connect)
		buf := make([]byte, SENDCHUNK)
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
