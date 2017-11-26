package session

import (
	"fmt"
	"testing"
	"bytes"
	"strings"
)

const (
	MsgContent = "content body"
	MsgStr  = PContent + Separator + MsgContent + Delim +
		PAppID + Separator + "applicationid" + Delim +
		PMsgId + Separator + "messageid" + Delim +
		PRequestType + Separator + TopicMsg + End
)

func testInvokeHandleStream() []Message {
	input := make(chan []byte, 4)
	go func() {

		buf := append([]byte(MsgStr), []byte(MsgStr)...)
		buf = append(buf, []byte(End)...)
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

	msgChan := decodeMessage(input)
	var list []Message
	for {
		msg, ok:= <-msgChan
		if !ok {
			break
		}
		list = append(list, msg)
	}
	return list
}

func TestHandleStream(t *testing.T) {
	list := testInvokeHandleStream()
	if len(list) != 2 {
		t.Error("Decode failed.")
	}
	fmt.Printf("%v \n", list[0])
	if !bytes.Equal([]byte(MsgContent), list[1].Content) ||
		!(strings.Compare(list[0].MsgId, list[1].MsgId) == 0) {
		t.Error("Message not equal.")
	}
}

func BenchmarkHandleStream(b *testing.B) {
	for i := 0; i < b.N; i++ {
		testInvokeHandleStream()
	}

}
