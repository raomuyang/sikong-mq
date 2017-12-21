package exchange

import (
	"testing"
	"net"
	"strings"
	"fmt"
	"github.com/sikong-mq/skmq/base"
)

var (
	address = "127.0.0.1:9001"
)
func TestHeartBeta(t *testing.T) {
	server, _ := net.Listen("tcp", address)
	go func() {
		conn, _ := server.Accept()
		buf := make([]byte, 10)
		read, _ := conn.Read(buf)
		t.Log(fmt.Sprintf("Readed(%d): %s", read, buf[:read]))
		val := string(buf[:len(base.PING)])
		if strings.Compare(val, base.PING) != 0 {
			t.Error("Not heartbeat package: " + val)
			return
		}
		err := ReplyHeartbeat(conn)
		t.Log(err)
	}()

	clientConn, _ := net.Dial("tcp", address)
	res := Heartbeat(clientConn)
	if !res {
		t.Error("Heartbeat failed.")
	}
}