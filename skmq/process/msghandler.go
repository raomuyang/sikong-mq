package process

import (
	"encoding/json"
	"github.com/raomuyang/sikong-mq/skmq/base"
	"github.com/raomuyang/sikong-mq/skmq/skerr"
)

const defaultChannelSize = 8

type MessageHandler interface {
	HandleMessage(msgChan <-chan base.Message) <-chan base.Response
}

type MessageHandlerImpl struct {
	msgCache Cache
}

func GetMessageHandler(msgCache Cache) MessageHandler {
	return &MessageHandlerImpl{msgCache: msgCache}
}

func (msgHandler *MessageHandlerImpl) processNewMsg(message base.Message, out chan<- base.Response) {
	content, err := msgHandler.saveMessage(message)
	Info.Printf("process message: %s, %s, error: %s\n", message.MsgId, content, err)
	status := base.MAck
	if err != nil {
		status = base.MReject
		content = "Message enqueue failed"
	}
	out <- base.Response{Status: status, Content: content}
}

func (msgHandler *MessageHandlerImpl) processRegisterMsg(message base.Message, out chan<- base.Response) {
	err := msgHandler.recipientRegister(message)
	status := base.MAck
	var content = "Recipient register successful."
	if err != nil {
		Warn.Println(err)
		status = base.MReject
		content = err.Error()
	}
	out <- base.Response{Status: status, Content: content}
}

func (msgHandler *MessageHandlerImpl) processArrivedMsg(message base.Message, out chan<- base.Response) {
	disconnect := true
	err := msgHandler.msgCache.UpdateMessageStatus(message.MsgId, base.MArrived)
	status := base.MAck
	if err != nil {
		status = base.MError
		disconnect = false
		Warn.Printf("arrived ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status, Disconnect: disconnect}
}

func (msgHandler *MessageHandlerImpl) processAckMsg(message base.Message, out chan<- base.Response) {
	err := msgHandler.msgCache.DeleteMessage(message.MsgId)
	status := base.MAck
	if err != nil {
		status = base.MError
		Warn.Printf("message ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status}
}

func (msgHandler *MessageHandlerImpl) processErrorMsg(message base.Message, out chan<- base.Response) {
	err := msgHandler.msgCache.DeadLetterEnqueue(message.MsgId)
	status := base.MAck
	disconnect := true
	if err != nil {
		status = base.MError
		disconnect = false
		Err.Printf("process error ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status, Disconnect: disconnect}
}

func (msgHandler *MessageHandlerImpl) processRejectedMsg(message base.Message, out chan<- base.Response) {
	err := msgHandler.checkRejectedMsg(message.MsgId)
	status := base.MAck
	disconnect := true
	if err != nil {
		status = base.MError
		disconnect = false
		Warn.Printf("reject ack error: %s/%s, %s \n",
			message.AppID, message.MsgId, err.Error())
	}
	out <- base.Response{Status: status, Disconnect: disconnect}

}

/**
If the message retransmission times is no more than limit,
it will be entries another queue to wait retry.
otherwise it will be entries the dead letter queue
*/
func (msgHandler *MessageHandlerImpl) checkRejectedMsg(msgId string) error {

	_, err := msgHandler.msgCache.MessageEntryRetryQueue(msgId)
	switch err.(type) {
	case skerr.UnknownDBOperationException:
		Warn.Println(err)
	case skerr.NoSuchMessage:
		Warn.Println(err)
		return nil
	case skerr.MessageDead:
		Warn.Println(err)
		return msgHandler.msgCache.DeadLetterEnqueue(msgId)
	case nil:
		return nil
	}
	return err
}

/**
Recipient register
*/
func (msgHandler *MessageHandlerImpl) recipientRegister(message base.Message) error {
	consumer := base.RecipientInfo{}
	err := json.Unmarshal(message.Content, &consumer)
	if err != nil {
		return skerr.InvalidParameters{Content: string(message.Content)}
	}

	return msgHandler.msgCache.SaveRecipientInfo(consumer)
}

func (msgHandler *MessageHandlerImpl) saveMessage(message base.Message) (string, error) {
	err := msgHandler.msgCache.MessageEnqueue(message)
	switch err.(type) {
	case skerr.MsgAlreadyExists:
		return err.Error(), nil
	case nil:
		return "Message enqueue successful", nil
	default:
		return "Message enqueue failed.", err
	}
}

func (msgHandler *MessageHandlerImpl) HandleMessage(msgChan <-chan base.Message) <-chan base.Response {
	out := make(chan base.Response, defaultChannelSize)
	go func() {
		for {
			message, ok := <-msgChan
			if !ok {
				Trace.Println("Handler: message channel closed.")
				break
			}
			Info.Printf("Handler: handle message %s/%s[%s] \n", message.AppID, message.MsgId, message.Type)
			switch message.Type {
			case base.RegisterMsg:
				Warn.Println("Handler: mesage rejected: " + message.MsgId)
				msgHandler.processRegisterMsg(message, out)
			case base.MArrivedMsg:
				Info.Println("Handler: mesage deliveried successfully: " + message.MsgId)
				msgHandler.processArrivedMsg(message, out)
			case base.MAckMsg:
				Info.Println("Handler: message ack, " + message.MsgId)
				msgHandler.processAckMsg(message, out)
			case base.MError:
				Warn.Printf("Msg %s error\n", message.MsgId)
				msgHandler.processErrorMsg(message, out)
			case base.MRejectMsg:
				Warn.Printf("Msg %s rejected\n", message.MsgId)
				msgHandler.processRejectedMsg(message, out)
			case base.PING:
				out <- base.Response{Status: base.PONG}
			default:
				Info.Println("Save message " + message.MsgId)
				msgHandler.processNewMsg(message, out)
			}
		}
		close(out)
	}()
	return out
}
