package kafka

import (
	"context"
	"errors"
	"reflect"

	broker "github.com/taosdata/go-demo-kafka/pkg/kafka/common"

	"github.com/Shopify/sarama"
	"github.com/sirupsen/logrus"
)

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	client sarama.Client
	logger logrus.FieldLogger
	cb     func(m *sarama.ConsumerMessage) error
	cancel context.CancelFunc
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {

	for message := range claim.Messages() {
		err := consumer.cb(message)
		if err != nil {
			consumer.logger.WithField("timestamp", message.Timestamp).Errorf("consume message error: %v", err)
		} else {
			consumer.logger.WithField("timestamp", message.Timestamp).Debug("message claimed")
			session.MarkMessage(message, "")
		}
	}
	return nil
}

func (consumer *Consumer) Unsubscribe() error {
	if consumer.cancel != nil {
		consumer.cancel()
	}
	return nil
}

func newConsumer(client sarama.Client, logger logrus.FieldLogger, topic, queue string, cb broker.Handler) (*Consumer, error) {
	consumerGroup, err := sarama.NewConsumerGroupFromClient(queue, client)
	if err != nil {
		return nil, err
	}

	if cb == nil {
		return nil, errors.New("kafka: Handler required for EncodedConn Subscription")
	}
	argType, numArgs := argInfo(cb)
	if argType == nil {
		return nil, errors.New("kafka: Handler requires at least one argument")
	}

	cbValue := reflect.ValueOf(cb)
	var emptyMsgType = reflect.TypeOf(&sarama.ConsumerMessage{})
	wantsRaw := argType == emptyMsgType

	kafkaCB := kafkaCB(wantsRaw, argType, numArgs, cbValue)

	ctx, cancel := context.WithCancel(context.Background())
	consumer := &Consumer{
		client: client,
		logger: logger,
		cb:     kafkaCB,
		cancel: cancel,
	}

	go func() {
		for {
			err = consumerGroup.Consume(ctx, []string{topic}, consumer)
			if err != nil {
				logger.Error(err)
				return
			}
			if ctx.Err() != nil {
				logger.Error(ctx.Err())
				return
			}
		}
	}()
	logger.Info("Sarama consumer up and running!...")
	return consumer, nil
}
