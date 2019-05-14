package kafka

import (
	"context"
	"fmt"
	"github.com/zibilal/mibroker"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	logger "github.com/zibilal/logwrapper"
)

type KafkaMessageBroker struct {
	brokers           []string
	consumerConfig    *cluster.Config
	producerConfig    *sarama.Config
	consumerConverter messagebroker.MessageProcessFunc
	consumerMessage   chan messagebroker.Message

	producer sarama.SyncProducer
}

func NewKafkaMessageBroker(brokers []string) (*KafkaMessageBroker, error) {
	m := new(KafkaMessageBroker)

	m.consumerConfig = cluster.NewConfig()
	m.consumerConfig.Consumer.Return.Errors = true
	m.consumerConfig.Group.Return.Notifications = true
	m.consumerConfig.Group.Mode = cluster.ConsumerModeMultiplex
	m.consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	m.brokers = brokers
	m.consumerMessage = make(chan messagebroker.Message)

	var err error
	m.producer, err = sarama.NewSyncProducer(m.brokers, m.producerConfig)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (k *KafkaMessageBroker) PublishMessagesWithKey(key messagebroker.MessageEncoder, topic string, data ...messagebroker.MessageEncoder) error {

	for _, d := range data {
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Key:   key,
			Value: d,
		}
		partition, offset, err := k.producer.SendMessage(msg)

		if err != nil {
			logger.Error("[Kafka]message publish to topic ", topic, " is failed, error: ", err)
		} else {
			logger.Info("[Kafka]message publish to topic ", topic, " is successful, partition: ", partition, " offset: ", offset)
		}
	}

	return nil
}

// PublishMessage publishes single message
func (k *KafkaMessageBroker) PublishMessage(topic string, data messagebroker.MessageEncoder) error {

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: data,
	}
	partition, offset, err := k.producer.SendMessage(msg)
	if err != nil {
		logger.Error("[Kafka]message publish to topic ", topic, " is failed, error: ", err)
		return err
	}

	logger.Debug("[Kafka]message publish to topic ", topic, " is successful, partition: ", partition, " offset: ", offset)
	return nil
}

func (k *KafkaMessageBroker) ConsumerCallback(callback messagebroker.MessageProcessFunc) {
	k.consumerConverter = callback
}

func (k *KafkaMessageBroker) Cleanup() {
	err := k.producer.Close()
	if err != nil {
		logger.Fatal("unable to close Kafka Message Broker, err: ", err)
	}
}

func (k *KafkaMessageBroker) Subscribe(ctx context.Context, groupId string, topics []string) error {

	consumer, err := cluster.NewConsumer(k.brokers, groupId, topics, k.consumerConfig)
	if err != nil {
		logger.Fatal("[Subscriber][Fatal]Unable to create new consumer, err: ", err.Error())

		return err
	}
	defer func() {
		_ = consumer.Close()
	}()

	// consume errors
	go func() {
		for err := range consumer.Errors() {
			logger.Info("[Subscriber][Error] ", fmt.Sprintf("%s", err.Error()))
		}
	}()

	// consume notifications
	go func() {
		for ntf := range consumer.Notifications() {
			logger.Info("[Subscriber][Rebalanced] ", fmt.Sprintf("%+v", ntf))
		}
	}()

	// consume the message
	for msg := range consumer.Messages() {
		if k.consumerConverter != nil {

			err := k.consumerConverter(msg, ctx)
			if err != nil {
				logger.Info("Message is processed with error ", err, ", offset not marked")
			} else {
				consumer.MarkOffset(msg, "")
				logger.Info("Message is processed, offset is marked")
			}
		}
	}

	select {
	case <-ctx.Done():
		logger.Info("Cancelled without marking offsets")
		return nil
	}
}
