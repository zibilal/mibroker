package messagebroker

import (
	"bytes"
	"context"
	"errors"
	"sync"
)

// This is meant to be a layer that wraps message broker or the pubs/subs system
// Ex: Kafka, NSQ, RabbitMQ

type MessageBroker interface {
	PublishMessage(topic string, data MessageEncoder) error
	PublishMessagesWithKey(key MessageEncoder, topic string, data ...MessageEncoder) error
	Subscribe(ctx context.Context, groupId string, topics []string) error
	Cleanup()
	ConsumerCallback(callback MessageProcessFunc)
}

type MessageProcessFunc func(interface{}, context.Context) error

type Message interface {
	SetupMessage(input interface{}) error
	MessageEncoder
}

type MessageEncoder interface {
	Encode() ([]byte, error)
	Length() int
}

type SimpleMessage string

func (k SimpleMessage) Encode() ([]byte, error) {
	buff := bytes.NewBufferString(string(k))
	return buff.Bytes(), nil
}
func (k SimpleMessage) Length() int {
	return len(k)
}

type BaseBrokerMessage struct {
	DataByte []byte `json:"-"`
}

func (b *BaseBrokerMessage) Encode() ([]byte, error) {
	if b.DataByte != nil && len(b.DataByte) > 0 {
		return b.DataByte, nil
	}

	return nil, errors.New("DataByte is empty")
}

func (b *BaseBrokerMessage) Length() int {
	return len(b.DataByte)
}

var messageBroker MessageBroker
var once sync.Once

func AssignMessageBroker(broker MessageBroker) {
	once.Do(func() {
		messageBroker = broker
	})
}

func GetMessageBroker() (MessageBroker, error) {
	if messageBroker == nil {
		return nil, errors.New("assign appropriate message broker first")
	}
	return messageBroker, nil
}

func CleanupMessageBroker() {
	if messageBroker != nil {
		messageBroker.Cleanup()
	}
}
