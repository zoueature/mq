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
	defer func() {
		err := consumer.Close()
		if err != nil {
			log.Error(ctx, "close consumer error", zap.Error(err))
			return
		}
	}()
	topics := []string{topic}
	for {
		breakLoop := false
		select {
		case <-ctx.Done():
			breakLoop = true
			break
		default:
			err = consumer.Consume(ctx, topics, &messageHandler{handler: handler})
			if err != nil {
				return err
			}
		}
		if breakLoop {
			break
		}
	}
	return nil
}

func (c *client) Push(ctx context.Context, msg mq.Message) error {
	producer, err := sarama.NewSyncProducer(c.brokers, sarama.NewConfig())
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
