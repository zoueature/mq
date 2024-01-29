package nsq

import (
	"context"
	"github.com/zoueature/config"
	"github.com/zoueature/mq"
	"log"
	"testing"
)

func TestSendMsg(t *testing.T) {
	cli := drv.New(config.Mq{
		Driver:         "",
		ConsumeAddress: "192.168.1.202:4161",
		ProductAddress: "192.168.1.202:4150",
	})
	ctx := context.Background()
	err := cli.Push(ctx, mq.NewMessage("test", []byte("Hello World")))
	if err != nil {
		t.Fatal(err)
	}
}

func TestConsumeMsg(t *testing.T) {
	cli := drv.New(config.Mq{
		Driver:         "",
		ConsumeAddress: "192.168.1.202:4161",
		ProductAddress: "192.168.1.202:4150",
	})
	ctx := context.Background()
	cli.Consume(ctx, mq.NewHandler("test", "test-channel", func(message mq.ReceivedMessage) error {
		log.Println(message.GetID())
		log.Println(string(message.GetBody()))
		return nil
	}), 1)
}
