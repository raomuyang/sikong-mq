package session

import "fmt"

type StreamReadError struct {
	input []byte
	msg string
}

func (e StreamReadError) Error() string  {
	return fmt.Sprintf("stream read error, case %s: %s", e.msg, e.input)
}

type MessageHandleError struct {
	Detail string
}

func (e MessageHandleError) Error() string {
	return "message handle error: " + e.Detail
}

type UnknownDBOperationException struct {
	Detail string
}

func (e UnknownDBOperationException) Error() string {
	return fmt.Sprintf("unknown data base update exception, %s", e.Detail)
}
type MsgAlreadyExists struct {
	MsgId string
	Status string
}

func (e MsgAlreadyExists) Error() string  {
	return fmt.Sprintf("message already exists, ID: %s, Status %s", e.MsgId, e.Status)
}

type AttrTypeError struct {
	Type string
	Value string
}

func (e AttrTypeError) Error() string {
	return fmt.Sprintf("attribute type error, except: %s, value: %s.", e.Type, e.Value)
}


type NoSuchMessage struct {
	MsgId  string
	Detail string
}

func (e NoSuchMessage) Error() string {
	return fmt.Sprintf("no such message info, messaeg id: %s. %s", e.MsgId, e.Detail)
}

type MessageDead struct {
	MsgId string
	Status string
	Retried int
}

func (e MessageDead) Error() string {
	return fmt.Sprintf("message delivery failed, " +
		"message id: %s, status: %s, retried: %d", e.MsgId, e.Status, e.Retried)
}

type InvalidParameters struct {
	Content string
}

func (e InvalidParameters) Error() string {
	return fmt.Sprintf("Invalid parameters: %s", e.Content)
}
