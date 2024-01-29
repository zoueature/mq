package nsq

import (
	"context"
	"github.com/nsqio/go-nsq"
	"github.com/zoueature/mq"
	"github.com/zoueature/mq/config"
	"log"
)

// nsq 消息队列是基于内存的消息队列， 不要用户关键业务的消息传递
// 如果需要确保消息不能丢的
// 请使用kafka 或者 rabbitMQ

// client
type client struct {
	cfg config.Mq
}

type messageHandler struct {
	mqHandler mq.Handler
}

func (m *messageHandler) HandleMessage(message *nsq.Message) error {
	return m.mqHandler.HandleMessage(nsqMessage(*message))
}

func (c client) Consume(ctx context.Context, handler mq.Handler, concurrency int) {
	cfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(handler.Topic(), handler.Channel(), cfg)
	if err != nil {
		log.Fatal(err)
	}
	consumer.AddConcurrentHandlers(&messageHandler{}, concurrency)

	err = consumer.ConnectToNSQLookupd(c.cfg.ConsumeAddress)
	if err != nil {
		log.Fatal(err)
	}

	<-ctx.Done()
	consumer.Stop()
}

func (c client) Push(ctx context.Context, msg mq.Message) error {
	cfg := nsq.NewConfig()
	// 只有第一次会有网络链接操作，
	producer, err := nsq.NewProducer(c.cfg.ProductAddress, cfg)
	if err != nil {
		log.Fatal(err)
	}
	err = producer.Publish(msg.Topic(), msg.Body())
	if err != nil {
		return err
	}
	return nil
}

type driver string

const drv driver = "nsq"

func (d driver) New(cfg config.Mq) mq.MQ {
	return &client{
		cfg: cfg,
	}
}

func (d driver) Name() mq.DriverId {
	return mq.DriverId(d)
}

func init() {
	mq.RegisterDriver(drv)
}
