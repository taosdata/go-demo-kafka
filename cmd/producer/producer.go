package main

import (
	"database/sql"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/Shopify/sarama"

	_ "github.com/taosdata/driver-go/taosSql"
	record "github.com/taosdata/go-demo-kafka/internal"
	"github.com/taosdata/go-demo-kafka/pkg/utils"
)

var producedRecords int64

func main() {
	recordConfig := record.DefaultConfig()
	recordConfig.Tables = 100
	recordConfig.Columns = 100
	recordConfig.RecordsPerTable = 1000

	taos, err := sql.Open("taosSql", "root:taosdata/tcp(tdengine:6030)/test")
	if err != nil {
		fmt.Printf("failed to connect TDengine, err:%v\n", err)
		return
	}
	sql := recordConfig.DropTableSql()
	_, err = taos.Exec(sql)
	if err != nil {
		fmt.Printf("failed to drop stable, err:%v\n%s\n", err, sql)
		return
	}
	sql = recordConfig.CreateTableSql()
	_, err = taos.Exec(sql)
	if err != nil {
		fmt.Printf("failed to create stable, err:%v\n%s\n", err, sql)
		return
	}
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewSyncProducer([]string{"kafka:9092"}, config)

	if err != nil {
		panic(err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic:     "test",
		Partition: -1,
	}

	go func() {
		var last int64
		for {
			consumed := atomic.LoadInt64(&producedRecords)
			if consumed == 0 {
				continue
			}
			time.Sleep(1 * time.Second)
			fmt.Printf("produced %d records (%d in last second)\n", consumed, consumed-last)
			last = consumed
		}

	}()
	for record := range recordConfig.Records() {
		// go func(record utils.Codec) {
		data, _ := utils.ToKafkaBytes(record)
		msg.Value = sarama.ByteEncoder(data)
		_, _, err := producer.SendMessage(msg)

		if err != nil {
			fmt.Printf("%s error occured.", err.Error())
		} else {
			atomic.AddInt64(&producedRecords, 1)
			// fmt.Printf("Message was saved to partion: %d.\nMessage offset is: %d.\n", partition, offset)
		}
		// }(record)
	}
}
