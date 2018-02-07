package exchange

import (
	"fmt"
	"github.com/sikong-mq/skmq/base"
	"net"
	"strings"
	"testing"
)

var (
	address = "127.0.0.1:9001"
)

func TestHeartBeta(t *testing.T) {
	exc := GetExchange(nil)
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
		err := exc.ReplyHeartbeat(conn)
		t.Log(err)
	}()

	clientConn, _ := net.Dial("tcp", address)
	res := exc.Heartbeat(clientConn)
	if !res {
		t.Error("Heartbeat failed.")
	}
}
