package kafka

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/zoueature/log"
	"github.com/zoueature/mq"
	"go.uber.org/zap"
	"time"
)

type client struct {
	brokers []string
}

func (c *client) Consume(ctx context.Context, topic, groupId string, handler mq.MessageHandler, concurrency int) error {
	consumer, err := sarama.NewConsumerGroup(c.brokers, groupId, sarama.NewConfig())
	if err != nil {
		return err
	}
	topics := []string{topic}
	err = consumer.Consume(ctx, topics, &messageHandler{handler: handler})
	if err != nil {
		return err
	}
	return nil
}

func (c *client) Push(ctx context.Context, msg mq.Message) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(c.brokers, config)
	if err != nil {
		return err
	}
	defer func() {
		err = producer.Close()
		if err != nil {
			log.Error(ctx, "close kafka producer", zap.Error(err))
			return
		}
	}()
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{
		Topic:     msg.Topic(),
		Value:     sarama.ByteEncoder(msg.Body()),
		Timestamp: time.Now(),
	})
	return err
}
