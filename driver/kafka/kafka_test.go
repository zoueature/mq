package kafka

import (
	"context"
	"github.com/zoueature/config"
	"github.com/zoueature/mq"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

func TestKafkaConsume(t *testing.T) {
	mqClient := drv.New(config.Mq{
		Driver: "kafka",
		Brokers: []string{
			"192.168.2.35:9092",
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	err := mqClient.Consume(ctx, "mytopic", "mq-test", func(receivedMessage mq.ReceivedMessage) error {
		println(string(receivedMessage.GetBody()))
		return nil
	}, 1)
	if err != nil {
		t.Error(err)
	}
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGKILL, syscall.SIGINT)
	<-ch
	cancel()
}

func TestKafkaProducer(t *testing.T) {
	mqClient := drv.New(config.Mq{
		Driver: "kafka",
		Brokers: []string{
			"192.168.2.35:9092",
		},
	})
	err := mqClient.Push(context.Background(), mq.NewMessage("mytopic", []byte("sdfbhfjewffdfjfinvckjds")))
	if err != nil {
		t.Error(err)
	}
}
