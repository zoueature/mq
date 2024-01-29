package nsq

import (
	"encoding/json"
	"github.com/nsqio/go-nsq"
	"github.com/spf13/cast"
)

type nsqMessage nsq.Message

func (n nsqMessage) GetID() string {
	return cast.ToString([16]byte(n.ID))
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
