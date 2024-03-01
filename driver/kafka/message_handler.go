package kafka

import (
	"fmt"
	"github.com/IBM/sarama"
	"github.com/zoueature/mq"
	"github.com/zoueature/mq/types"
)

type messageHandler struct {
	handler mq.MessageHandler
}

func (h messageHandler) Setup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h messageHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	return nil
}

func (h messageHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		fmt.Printf("Message topic:%q partition:%d offset:%d\n", msg.Topic, msg.Partition, msg.Offset)
		h.handler(types.KafkaMessage(*msg))
	}
	return nil
}
