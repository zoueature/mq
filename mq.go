package mq

import (
	"context"
	"github.com/zoueature/mq/config"
)

var driverMap = map[DriverId]Driver{}

func RegisterDriver(driver Driver) {
	driverMap[driver.Name()] = driver
}

type DriverId string

type Driver interface {
	Name() DriverId
	New(cfg config.Mq) MQ
}

type MQ interface {
	Consume(ctx context.Context, handler Handler, concurrency int)
	Push(ctx context.Context, msg Message) error
}

// ReceivedMessage 接收到的消息
type ReceivedMessage interface {
	GetID() string
	GetTimestamp() int64
	GetBody() []byte
	Unmarshal(container any) error
}

// Message 用户发送的消息
type Message interface {
	// Topic 所属topic
	Topic() string
	Body() []byte
}

type message struct {
	topic string
	body  []byte
}

func (m message) Topic() string {
	return m.topic
}

func (m message) Body() []byte {
	return m.body
}

func NewMessage(topic string, body []byte) Message {
	return message{
		topic: topic,
		body:  body,
	}
}

type Handler interface {
	// Topic 所属topic
	Topic() string
	// Channel 所属channel， kafka相当于serverId
	Channel() string
	// HandleMessage 消息处理
	HandleMessage(message ReceivedMessage) error
}

type handler struct {
	topic   string
	channel string
	do      func(message ReceivedMessage) error
}

func (h handler) Topic() string {
	return h.topic
}

func (h handler) Channel() string {
	return h.channel
}

func (h handler) HandleMessage(message ReceivedMessage) error {
	return h.do(message)
}

func NewHandler(topic, channel string, msgHandler func(message ReceivedMessage) error) Handler {
	return handler{
		topic:   topic,
		channel: channel,
		do:      msgHandler,
	}
}

// New 实例化MQ客户端
func New(cfg config.Mq) MQ {
	driver, ok := driverMap[DriverId(cfg.Driver)]
	if !ok {
		panic("Driver: " + cfg.Driver + " not register. ")
	}
	return driver.New(cfg)
}
