package main

import (
	"database/sql"
	"flag"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Shopify/sarama"

	"github.com/taosdata/go-demo-kafka/pkg/kafka/common"
	"github.com/taosdata/go-demo-kafka/pkg/kafka/common/options"
	"github.com/taosdata/go-demo-kafka/pkg/kafka/kafka"
	"github.com/taosdata/go-demo-kafka/pkg/utils"

	_ "github.com/taosdata/driver-go/taosSql"

	"github.com/sirupsen/logrus"
	record "github.com/taosdata/go-demo-kafka/internal"
	"github.com/taosdata/go-demo-kafka/pkg/queue"
)

// Global options
var kafkaOptions options.Kafka
var topic string = "test"
var consumedRecords int64
var log = logrus.New()
var qOption *queue.Option = queue.DefaultOption().SetMaxQueueSize(10000).SetMaxBatchSize(100)
var q *queue.Queue
var workers = 4
var taosuri = "root:taosdata/tcp(tdengine:6030)/test"
var consumedPartions = make([]int, record.MAX_PARTITIONS)

// OnConnect hanlder
func onConnect() {
	log.Println("connected")
}

// OnConnectionLost handler
func onConnectionLost(err error) {
	log.Println("connection lost")
}

// Parallel insert use goroutines
func TaosConsume(q *queue.Queue, taos *sql.DB, workers int) {
	// Setup workers for batch queue
	for i := 0; i < workers; i++ {
		go func(q *queue.Queue) {
			for {
				runtime.Gosched()
				records, err := q.Dequeue()
				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				sqlStr, err := utils.ToTaosBatchInsertSql(records)
				if err != nil {
					log.Errorf("cannot build sql with records: %v", err)
					continue
				}
				runtime.Gosched()
				// fmt.Println(sql)
				res, err := taos.Exec(sqlStr)
				if err != nil {
					log.Errorf("exec query error: %v, the sql command is:\n%s\n", err, sqlStr)
				}

				runtime.Gosched()
				n, _ := res.RowsAffected()
				atomic.AddInt64(&consumedRecords, n)
			}
		}(q)
	}
	// Setup a watcher goroutine for profiling
	go func() {
		var last int64
		for {
			consumed := atomic.LoadInt64(&consumedRecords)
			if consumed == 0 {
				continue
			}
			time.Sleep(1 * time.Second)
			if consumed-last != 0 {
				log.Infof("consumed %d records (%d in last second), consumed partitions:%v\n",
					consumed, consumed-last, consumedPartions)
			}
			last = consumed
		}

	}()
}

// Consume message and push to batch queue
func msgHandler(message *sarama.ConsumerMessage) (err error) {
	var r record.Record
	// for partition messages count in each consumer, you can ignore it
	consumedPartions[message.Partition] += 1
	err = utils.FromKafkaBytes(message.Value, &r)
	if err != nil {
		log.Errorf("Partition:%d Offset:%d Key:%v Err: %v\n", message.Partition, message.Offset, message.Key, err)
		return err
	}
	_, err = q.Enqueue(r)
	return err
}

func main() {
	// Option parsing
	qOption.FlagInit()
	kafkaOptions.Init()
	flag.StringVar(&topic, "topic", topic, "kafka topic to consume")
	flag.IntVar(&workers, "worker", workers, "worker for TDengine insertion")
	flag.Parse()

	// Prepare batch queue
	q = queue.NewWithOption(qOption)

	// Prepare taos connection pool
	taos, err := sql.Open("taosSql", taosuri)
	if err != nil {
		log.Errorf("failed to connect TDengine, err:%v", err)
		return
	}

	// Prepare logger
	logger := log.WithFields(logrus.Fields{
		"topic": topic,
	})

	// Subscribe kafka
	mq := kafka.Run(&kafkaOptions, logger, common.Options{OnConnect: onConnect, OnConnectionLost: onConnectionLost})
	handler, err := mq.QueueSubscribe(topic, topic, msgHandler)
	if err != nil {
		log.Error("subscribe error: ", err)
		os.Exit(1)
	}

	// Consume message object queue to TDengine batch by batch
	TaosConsume(q, taos, workers)

	// Listen to sigterm
	c := make(chan os.Signal, 2)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c
	// Gracefully quit
	log.Info("Terminated, wait for job finishing in queue")
	// 1. stop the queue to not recived data any more
	q.Pause()
	// 2. stop subscribe handler
	handler.Unsubscribe()
	// 3. wait until all consumed messages and their jobs are finished
	for {
		if q.IsEmpty() {
			log.Info("all job in the queue is finished")
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	// 4. stop the message queue
	mq.Stop()
}
