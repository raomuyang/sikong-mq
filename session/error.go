package session

import "fmt"

type StreamReadError struct {
	input []byte
	msg string
}

func (e StreamReadError) Error() string  {
	return fmt.Sprintf("Stream read error, case %s: %s", e.msg, e.input)
}