package types

import (
	"encoding/json"
	"fmt"
	"github.com/IBM/sarama"
)

type KafkaMessage sarama.ConsumerMessage

func (k KafkaMessage) GetID() string {
	return fmt.Sprintf("%s-%d-%d", k.Topic, k.Partition, k.Offset)
}

func (k KafkaMessage) GetTimestamp() int64 {
	return k.Timestamp.Unix()
}

func (k KafkaMessage) GetBody() []byte {
	return k.Value
}

func (k KafkaMessage) Unmarshal(container any) error {
	return json.Unmarshal(k.GetBody(), container)
}
