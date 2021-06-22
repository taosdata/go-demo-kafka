package main

import (
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"

	_ "github.com/taosdata/driver-go/taosSql"

	record "github.com/taosdata/go-demo-kafka/internal"
	"github.com/taosdata/go-demo-kafka/pkg/queue"
	"github.com/taosdata/go-demo-kafka/pkg/utils"
)

var consumedRecords int64

func ToasConsume(q *queue.Queue, taos *sql.DB, workers int) {
	// start 4 workers for batch queue
	for i := 0; i < workers; i++ {
		go func(q *queue.Queue) {
			for {
				runtime.Gosched()
				records, err := q.Dequeue()
				if err != nil {
					time.Sleep(200 * time.Millisecond)
					continue
				}
				sql, err := utils.ToTaosBatchInsertSql(records)
				if err != nil {
					fmt.Printf("cannot build sql with records: %v", err)
					continue
				}
				runtime.Gosched()
				// fmt.Println(sql)
				res, err := taos.Exec(sql)
				if err != nil {
					fmt.Printf("exec query error: %v, the sql command is:\n%s\n", err, sql)
				}

				runtime.Gosched()
				n, _ := res.RowsAffected()
				atomic.AddInt64(&consumedRecords, n)
			}
		}(q)
	}
	go func() {
		var last int64
		for {
			consumed := atomic.LoadInt64(&consumedRecords)
			if consumed == 0 {
				continue
			}
			time.Sleep(1 * time.Second)
			// if consumedRecords/1000*1000/1000 == 0 {
			fmt.Printf("consumed %d records (%d in last second)\n", consumed, consumed-last)
			last = consumed
			// }
		}

	}()
}

// kafka consumer
func KafkaConsumer(pc sarama.PartitionConsumer, q *queue.Queue) {
	for msg := range pc.Messages() {
		var r record.Record
		err := utils.FromKafkaBytes(msg.Value, &r)
		if err != nil {
			fmt.Printf("Partition:%d Offset:%d Key:%v Err: %v Queue: %v\n", msg.Partition, msg.Offset, msg.Key, err, q)
		} else {

			res, err := q.Enqueue(r)
			if err != nil {
				fmt.Println(res, err)
				continue
			}
		}

	}
}
func main() {
	// Configs
	workers := 4
	broker := "kafka:9092"
	topic := "test"
	taos, err := sql.Open("taosSql", "root:taosdata/tcp(tdengine:6030)/test")
	if err != nil {
		fmt.Printf("failed to connect TDengine, err:%v\n", err)
		return
	}
	consumer, err := sarama.NewConsumer([]string{broker}, nil)
	option := queue.DefaultOption().SetMaxQueueSize(10000).SetMaxBatchSize(100)
	if err != nil {
		fmt.Printf("fail to start consumer, err:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("fail to get list of partition:err%v\n", err)
		return
	}
	fmt.Println(partitionList)
	q := queue.NewWithOption(option)
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("failed to start consumer for partition %d,err:%v\n", partition, err)
			return
		}
		defer pc.AsyncClose()
		go KafkaConsumer(pc, q)
	}

	ToasConsume(q, taos, workers)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	<-signals
}
