package skmq

import (
	"strings"
	"fmt"
	"net"
	"time"
	"bytes"
)

/**
	If the message retransmission times is no more than limit,
	it will be entries another queue to wait retry.
	otherwise it will be entries the dead letter queue
 */
func processRejectedMsg(msgId string) error {

	_, err := MessageEntryRetryQueue(msgId)
	switch err.(type) {
	case UnknownDBOperationException:
		// TODO log
	case NoSuchMessage:
		// TODO log
	case MessageDead:
		// TODO log
		DeadLetterEnqueue(msgId)
		return nil
	case nil:
		return nil
	}
	return err
}

/**
	遍历一次所有的Recipients，将失联的标记为Lost
 */
func CheckRecipientsAvailable() {
	apps := GetApps()
	for i := range GetApps() {
		appId := apps[i]
		recipients, err := FindRecipients(appId)
		if err != nil {
			// TODO log
			continue
		}
		for r := range recipients {
			recipient := recipients[r]
			address := fmt.Sprintf("%s:%s", recipient.Host, recipient.Port)
			connect, err := net.DialTimeout("tcp", address, ConnectTimeOut)
			if err != nil {
				fmt.Printf("Heartbeat: %s, %s\n", address, err.Error())
			}
			result := Heartbeat(connect)
			fmt.Printf("Heartbeat: %s, ack: %v\n", address, result)
			if !result {
				recipient.Status = Lost
				err = UpdateRecipient(*recipient)
				if err != nil {
					// TODO log
					fmt.Println(err)
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
	defer connect.SetDeadline(time.Time{})

	connect.SetWriteDeadline(time.Now().Add(ConnectTimeOut))
	err := SendMessage(connect, []byte(PING))
	if err != nil {
		return false
	}

	buf := make([]byte, 10)
	connect.SetReadDeadline(time.Now().Add(ConnectTimeOut))
	read, err := connect.Read(buf)
	if read < len(PONG) {
		// TODO log
		return false
	}

	return bytes.Equal([]byte(PONG), buf[:len(PONG)])
}

/**
	发送一个8字节的心跳包
 */
func ReplyHeartbeat(conn net.Conn) error {
	defer conn.SetWriteDeadline(time.Time{})
	content := []byte(PONG)
	conn.SetWriteDeadline(time.Now().Add(ConnectTimeOut))
	return SendMessage(conn, content)
}

func RecipientBalance(appId string) (*RecipientInfo, error) {

	recipients, err := FindRecipients(appId)
	if err != nil {
		return nil, err
	}

	recently, err := RecentlyAssignedRecord(appId)
	if err != nil {
		return nil, err
	}

	var value float64 = 0
	var recipient *RecipientInfo
	for i := range recipients {
		r := recipients[i]
		if strings.Compare(Alive, r.Status) != 0 {
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
	return recipient, nil
}

/**
	从注册的接收方中挑选一台用于发送，并将建立的连接返回
	暂时只支持点到点的消息投递
 */
func DeliveryMessage(appId string, content []byte) (net.Conn, error) {

	var conn net.Conn
	for {
		recipient, err := RecipientBalance(appId)
		if err != nil {
			continue
		}
		if recipient == nil {
			break
		}
		address := recipient.Host + ":" + recipient.Port
		fmt.Println("Delivery target:", address)
		conn, err = net.DialTimeout("tcp", address, ConnectTimeOut)
		if err != nil {
			RemoveLostRecipient(*recipient)
			conn = nil
		}
		break

	}

	if conn == nil {
		err := NoneAvailableRecipient{AppId: appId}
		return nil, err
	}

	err := WriteBuffer(conn, content)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

/**
	避免阻塞
 */
func RemoveLostRecipient(recipient RecipientInfo) {
	go func() {
		recipient.Status = Lost
		UpdateRecipient(recipient)
	}()
}
