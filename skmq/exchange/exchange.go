package exchange

import (
	"strings"
	"fmt"
	"net"
	"time"
	"bytes"
	"github.com/sikong-mq/skmq/process"
	"github.com/sikong-mq/skmq/skerr"
	"github.com/sikong-mq/skmq/base"
)


/**
	遍历一次所有的Recipients，将失联的标记为Lost
 */
func CheckRecipientsAvailable() {
	apps := process.GetApps()
	for i := range process.GetApps() {
		appId := apps[i]
		recipients, err := process.FindRecipients(appId)
		if err != nil {
			Warn.Println(err)
			continue
		}
		for r := range recipients {
			recipient := recipients[r]
			address := fmt.Sprintf("%s:%s", recipient.Host, recipient.Port)
			connect, err := net.DialTimeout("tcp", address, base.ConnectTimeOut)
			if err != nil {
				Warn.Printf("Heartbeat: %s, %s\n", address, err.Error())
			}
			result := Heartbeat(connect)
			Trace.Printf("Heartbeat: %s, ack: %v\n", address, result)
			if !result {
				recipient.Status = base.Lost
				err = process.UpdateRecipient(*recipient)
				if err != nil {
					Warn.Println(err)
				}
			}
		}
	}
}

/**
	发送一个心跳包，并检测是否正常返回
	若超时或返回值不正确，则返回false
 */
func Heartbeat(connect net.Conn) bool {
	if connect == nil {
		return false
	}
	defer connect.SetDeadline(time.Time{})

	connect.SetWriteDeadline(time.Now().Add(base.ConnectTimeOut))
	err := process.SendMessage(connect, []byte(base.PING))
	if err != nil {
		return false
	}

	buf := make([]byte, 10)
	connect.SetReadDeadline(time.Now().Add(base.ConnectTimeOut))
	read, err := connect.Read(buf)
	if read < len(base.PONG) {
		Warn.Printf("Unexpected heartbeat response (%d) %s\n", read, buf[:read])
		return false
	}

	return bytes.Equal([]byte(base.PONG), buf[:len(base.PONG)])
}

/**
	发送一个8字节的心跳包
 */
func ReplyHeartbeat(conn net.Conn) error {
	defer conn.SetWriteDeadline(time.Time{})
	content := []byte(base.PONG)
	conn.SetWriteDeadline(time.Now().Add(base.ConnectTimeOut))
	return process.SendMessage(conn, content)
}

func RecipientBalance(appId string) (*base.RecipientInfo, error) {

	recipients, err := process.FindRecipients(appId)
	if err != nil {
		return nil, err
	}

	recently, err := process.RecentlyAssignedRecord(appId)
	if err != nil {
		return nil, err
	}

	var value float64 = 0
	var recipient *base.RecipientInfo
	for i := range recipients {
		r := recipients[i]
		if strings.Compare(base.Alive, r.Status) != 0 {
			continue
		}
		weight := r.Weight + 1
		recent := recently[r.RecipientId]
		v := float64(weight) / float64(recent)
		if v > value {
			value = v
			recipient = r
		}
	}
	if recipient != nil {
		process.UpdateRecipientAssigned(*recipient)
	}
	return recipient, nil
}

/**
	从注册的接收方中挑选一台用于发送，并将建立的连接返回
 */
func Unicast(appId string, content []byte) (net.Conn, error) {

	var conn net.Conn
	for {
		recipient, err := RecipientBalance(appId)
		if err != nil {
			continue
		}
		if recipient == nil {
			break
		}
		conn = getConnect(recipient)
		break

	}

	if conn == nil {
		err := skerr.NoneAvailableRecipient{AppId: appId}
		return nil, err
	}

	err := DeliveryContent(conn, content)
	if err != nil {
		return nil, err
	}
	return conn, nil
}


/**
	Get broadcast connects
 */
func BroadcastConnect(appId string) (<-chan net.Conn, error) {

	connects := make(chan net.Conn)
	recipients, err := process.FindRecipients(appId)
	if err != nil {
		return nil, err
	}

	go func() {
		for i := range recipients {
			r := recipients[i]
			if strings.Compare(base.Alive, r.Status) != 0 {
				continue
			}
			conn := getConnect(r)
			if conn == nil {
				continue
			}
		}
	}()

	return connects, nil
}

func DeliveryContent(conn net.Conn, content []byte) error {
	return process.WriteBuffer(conn, content)
}

/**
	避免阻塞
 */
func RemoveLostRecipient(recipient base.RecipientInfo) {
	go func() {
		recipient.Status = base.Lost
		process.UpdateRecipient(recipient)
	}()
}

/**
	Get connect by recipient info
 */
func getConnect(recipient *base.RecipientInfo) net.Conn {
	address := recipient.Host + ":" + recipient.Port
	Info.Println("Connect target:", address)
	conn, err := net.DialTimeout("tcp", address, base.ConnectTimeOut)
	if err != nil {
		RemoveLostRecipient(*recipient)
		return nil
	}
	return conn
}
