package kafka

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	broker "github.com/taosdata/go-demo-kafka/pkg/kafka/common"
	"github.com/taosdata/go-demo-kafka/pkg/kafka/common/options"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/rcrowley/go-metrics"
	"github.com/sirupsen/logrus"
)

type MQ struct {
	client           sarama.Client
	producer         sarama.AsyncProducer
	OnConnect        broker.OnConnectHandler
	OnConnectionLost broker.ConnectionLostHandler
	logger           logrus.FieldLogger
}

func (mq *MQ) BindRecvQueueChan(topic, queue string, channel interface{}) (broker.Subscriber, error) {
	panic("not supported")
}

func (mq *MQ) Connect(conf *options.Kafka) error {
	addrs := strings.Split(conf.Brokers, ",")
	kafkaConfig := sarama.NewConfig()

	mq.logger.Info("connect to kafka %v", conf)
	if len(conf.Version) > 0 {
		version, err := sarama.ParseKafkaVersion(conf.Version)
		if err != nil {
			fmt.Println(err)
			return fmt.Errorf("parsing Kafka version: %v", err)
		}
		kafkaConfig.Version = version
	}

	if conf.Offset == sarama.OffsetNewest || conf.Offset == sarama.OffsetOldest || conf.Offset > 0 {
		// only newest or oldest or explicit offset are valid
		kafkaConfig.Consumer.Offsets.Initial = conf.Offset
	}
	kafkaConfig.MetricRegistry = metrics.DefaultRegistry

	if conf.SASLEnable {
		kafkaConfig.Net.SASL.Enable = true
		kafkaConfig.Net.SASL.User = conf.User
		kafkaConfig.Net.SASL.Password = conf.Password
	}

	var err error
	mq.client, err = sarama.NewClient(addrs, kafkaConfig)
	if err != nil {
		fmt.Println(err)
		return err
	}
	mq.producer, err = sarama.NewAsyncProducerFromClient(mq.client)
	fmt.Println(err)
	return err
}

func (mq *MQ) Stop() {
	if mq.client != nil && !mq.client.Closed() {
		mq.client.Close()
	}
}

func (mq *MQ) PublishWithKey(topic, key string, data interface{}, headers broker.Properties) error {
	if mq.producer == nil {
		return errors.New("broker not connected")
	}
	jsonData, err := json.Marshal(data)
	if err != nil {
		mq.logger.Error(err)
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(jsonData),
	}
	if key != "" {
		msg.Key = sarama.StringEncoder(key)
	}
	if len(headers) > 0 {
		for k, v := range headers {
			msg.Headers = append(msg.Headers, sarama.RecordHeader{
				Key:   sarama.ByteEncoder(k),
				Value: sarama.ByteEncoder(v),
			})
		}
	}
	mq.producer.Input() <- msg

	return nil
}

func (mq *MQ) Publish(topic string, data interface{}) error {
	return mq.PublishWithKey(topic, "", data, nil)
}

func (mq *MQ) Subscribe(topic string, cb broker.Handler) (broker.Subscriber, error) {
	return mq.QueueSubscribe(topic, strings.ReplaceAll(uuid.New().String(), "-", ""), cb)
}

func (mq *MQ) QueueSubscribe(topic, queue string, cb broker.Handler) (broker.Subscriber, error) {
	if mq.client == nil || mq.client.Closed() {
		return nil, errors.New("kafka client not connect")
	}
	return newConsumer(mq.client, mq.logger, topic, queue, cb)
}

func (mq *MQ) KeyQueueSubscribe(topic, queue string, cb broker.Handler) (broker.Subscriber, error) {
	return mq.QueueSubscribe(topic, queue, cb)
}

func (mq *MQ) String() string {
	return "kafka"
}

func (mq *MQ) TopicRegexSupported() bool {
	return false
}

var Broker *MQ

func Run(conf *options.Kafka, logger logrus.FieldLogger, options broker.Options) broker.MQ {
	mq := &MQ{
		logger:           logger,
		OnConnect:        options.OnConnect,
		OnConnectionLost: options.OnConnectionLost,
	}
	Broker = mq
	func() {
		for {
			err := mq.Connect(conf)
			if err != nil {
				logger.WithError(err).Error("connect to mq broker error")
				fmt.Println("connect to mq broker error")
				time.Sleep(30 * time.Second)
			} else {
				break
			}
		}
		if mq.OnConnect != nil {
			mq.OnConnect()
		}
	}()
	return mq
}
