package config

type Mq struct {
	Driver         string `json:"driver" yaml:"driver" desc:"消息队列类型, nsq, kafka, rabbit"`
	ConsumeAddress string `json:"consumeAddress" yaml:"consumeAddress" desc:"消费者链接地址"`
	ProductAddress string `json:"productAddress" yaml:"productAddress" desc:"生产者链接地址"`
}
