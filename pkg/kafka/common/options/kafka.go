package options

import (
	"flag"
	"os"
	"strconv"
)

type Kafka struct {
	Brokers    string
	Version    string
	SASLEnable bool
	User       string
	Password   string
	Offset     int64
}

func (kafka *Kafka) Init() {
	if kafka.Brokers == "" {
		if val := os.Getenv("KAFKA_BROKERS"); val != "" {
			kafka.Brokers = val
		}
	}
	flag.StringVar(&kafka.Brokers, "kafka.brokers", kafka.Brokers, "kafka brokers")

	if kafka.Version == "" {
		if val := os.Getenv("KAFKA_VERSION"); val != "" {
			kafka.Version = val
		}
	}
	flag.StringVar(&kafka.Version, "kafka.version", kafka.Version, "kafka version")

	if !kafka.SASLEnable {
		if val := os.Getenv("KAFKA_SASL_ENABLE"); val != "" {
			kafka.SASLEnable, _ = strconv.ParseBool(val)
		}
	}
	flag.BoolVar(&kafka.SASLEnable, "kafka.saslEnable", kafka.SASLEnable, "kafka sasl enable")

	if kafka.User == "" {
		if val := os.Getenv("KAFKA_USER"); val != "" {
			kafka.User = val
		}
	}
	flag.StringVar(&kafka.User, "kafka.user", kafka.User, "kafka user")

	if kafka.Password == "" {
		if val := os.Getenv("KAFKA_PASSWORD"); val != "" {
			kafka.Password = val
		}
	}
	flag.StringVar(&kafka.Password, "kafka.password", kafka.Password, "kafka password")
	if kafka.Offset == 0 {
		kafka.Offset = -1 // Newest
		if val := os.Getenv("KAFKA_OFFSET"); val != "" {
			kafka.Offset, _ = strconv.ParseInt(val, 0, 64)
		}
	}
	flag.Int64Var(&kafka.Offset, "kafka.offset", kafka.Offset, "-1: Newest, -2: Oldest")
}
