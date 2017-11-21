package session

import "fmt"

type StreamReadError struct {
	input []byte
	msg string
}

func (e StreamReadError) Error() string  {
	return fmt.Sprintf("Stream read error, case %s: %s", e.msg, e.input)
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