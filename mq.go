package mq

import (
	"context"
	"github.com/zoueature/config"
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
	Consume(ctx context.Context, topic, channel string, handler MessageHandler, concurrency int) error
	Push(ctx context.Context, msg Message) error
}

// ReceivedMessage 接收到的消息
type ReceivedMessage interface {
	GetID() string
	GetTimestamp() int64
	GetBody() []byte
	Unmarshal(container any) error
}

type MessageHandler func(receivedMessage ReceivedMessage) error

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

// New 实例化MQ客户端
func New(cfg config.Mq) MQ {
	driver, ok := driverMap[DriverId(cfg.Driver)]
	if !ok {
		panic("Driver: " + cfg.Driver + " not register. ")
	}
	return driver.New(cfg)
}
