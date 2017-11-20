package session

import (
	"fmt"
	"testing"
	"bytes"
)

const (
	body = "content body" + Delim
	msg = AppID + Separator + "applicationid" + Delim +
		MsgId + Separator + "messageid" + Delim +
		RequestType + Separator + TopicType
)

func testInvokeHandleStream() Message {
	input := make(chan []byte, 4)

	msgChan := handleStream(input)

	contentBody := []byte(body)
	content := append([]byte(Content+Separator), contentBody...)

	buf := append([]byte(msg), []byte(Delim)...)
	buf = append(buf, []byte(content)...)

	position := 0
	for {
		begin := position
		end := position + 10
		if end > len(buf) {
			end = len(buf)
		}
		input <- buf[begin:end]
		position += 10
		if position >= len(buf) {
			break
		}
	}
	close(input)
	messageEntity := <-msgChan
	return messageEntity
}

func TestHandleStream(t *testing.T) {
	messageEntity := testInvokeHandleStream()
	fmt.Printf("%v \n", messageEntity)
	if bytes.Equal([]byte(body), messageEntity.Content) {
		t.Error("Content not equal.")
	}
}

func BenchmarkHandleStream(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testInvokeHandleStream()
	}

}
