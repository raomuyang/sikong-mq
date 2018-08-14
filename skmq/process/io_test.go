package process

import (
	"bytes"
	"fmt"
	"github.com/raomuyang/sikong-mq/skmq/base"
	"strings"
	"testing"
)

var (
	content = []byte("This is content")
	testMsg = base.Message{
		MsgId:   "Test-msg-id",
		AppID:   "test-app-id",
		Type:    base.TopicMsg,
		Content: content}
)

func testInvokeDecodeMessage() []base.Message {
	input := make(chan []byte, 4)
	go func() {

		buf := append(EncodeMessage(testMsg), EncodeMessage(testMsg)...)
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
	}()

	msgChan := DecodeMessage(input)
	var list []base.Message
	for {
		msg, ok := <-msgChan
		if !ok {
			break
		}
		list = append(list, msg)
	}
	return list
}

func TestDecodeMessage(t *testing.T) {
	list := testInvokeDecodeMessage()
	if len(list) != 2 {
		t.Error("Decode failed.")
	}
	fmt.Printf("%v \n", list[0])
	if !bytes.Equal([]byte(content), list[1].Content) ||
		!(strings.Compare(list[0].MsgId, list[1].MsgId) == 0) {
		t.Error("Message not equal.")
	}
}

func BenchmarkDecodeMessage(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testInvokeDecodeMessage()
	}

}
