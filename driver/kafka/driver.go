package kafka

import (
	"github.com/zoueature/config"
	"github.com/zoueature/mq"
)

type driver string

const drv driver = "kafka"

func (d driver) New(cfg config.Mq) mq.MQ {
	return &client{
		brokers: cfg.Brokers,
	}
}

func (d driver) Name() mq.DriverId {
	return mq.DriverId(d)
}

func init() {
	mq.RegisterDriver(drv)
}
