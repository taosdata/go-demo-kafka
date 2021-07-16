package options

import (
	"flag"
	"os"
)

type Broker struct {
	Type  string
	Kafka Kafka
}

func (broker *Broker) Init() {
	if addr := os.Getenv("BROKER_TYPE"); addr != "" {
		broker.Type = addr
	} else if broker.Type == "" {
		broker.Type = "kafka"
	}
	flag.StringVar(&broker.Type, "broker.type", broker.Type, "broker type: kafka")

	broker.Kafka.Init()
}
