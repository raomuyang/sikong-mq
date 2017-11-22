package session

import "fmt"

type StreamReadError struct {
	input []byte
	msg string
}

func (e StreamReadError) Error() string  {
	return fmt.Sprintf("Stream read error, case %s: %s", e.msg, e.input)
}

type UnknownDBOperationException struct {
	Detail string
}

func (e UnknownDBOperationException) Error() string {
	return fmt.Sprintf("Unknown data base update exception, %s", e.Detail)
}
type MsgAlreadyExists struct {
	MsgId string
	Status string
}

func (e MsgAlreadyExists) Error() string  {
	return fmt.Sprintf("Message already exists, ID: %s, Status %s", e.MsgId, e.Status)
}

type AttrTypeError struct {
	Type string
	Value string
}

func (e AttrTypeError) Error() string {
	return fmt.Sprintf("Attribute type error, except: %s, value: %s.", e.Type, e.Value)
}


type NoSuchMessage struct {
	MsgId  string
	Detail string
}

func (e NoSuchMessage) Error() string {
	return fmt.Sprintf("No such message info, messaeg id: %s. %s", e.MsgId, e.Detail)
}

type MessageDeliveryFailed struct {
	MsgId string
	Status string
	Retried int
}

func (e MessageDeliveryFailed) Error() string {
	return fmt.Sprintf("Message delivery failed, " +
		"message id: %s, status: %s, retried: %d", e.MsgId, e.Status, e.Retried)
}
