package common

import "time"

type Subscriber interface {
	Unsubscribe() error
}

type ConnectionLostHandler func(error)
type OnConnectHandler func()

type Options struct {
	OnConnect        OnConnectHandler
	OnConnectionLost ConnectionLostHandler
}

type MQ interface {
	Stop()
	Producer
	Consumer
}

type Producer interface {
	Publish(topic string, data interface{}) error
	PublishWithKey(topic, key string, data interface{}, properties Properties) error
}

type Consumer interface {
	Subscribe(topic string, fn Handler) (Subscriber, error)
	QueueSubscribe(topic, queue string, fn Handler) (Subscriber, error)
	KeyQueueSubscribe(topic, queue string, cb Handler) (Subscriber, error)
	BindRecvQueueChan(topic, queue string, channel interface{}) (Subscriber, error)
}

type RPC interface {
	Request(topic string, v interface{}, vPtr interface{}, timeout time.Duration) error
}

type Handler interface{}
type Properties map[string]string
