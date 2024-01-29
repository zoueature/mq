package nsq

import (
	"encoding/json"
	"github.com/nsqio/go-nsq"
)

type nsqMessage nsq.Message

func (n nsqMessage) GetID() string {
	return string(n.ID[:])
}

func (n nsqMessage) GetTimestamp() int64 {
	return n.Timestamp
}

func (n nsqMessage) GetBody() []byte {
	return n.Body
}

func (n nsqMessage) Unmarshal(container any) error {
	return json.Unmarshal(n.GetBody(), container)
}
