package queue

import (
	"flag"
	"os"
	"strconv"
	"time"
)

type Option struct {
	// Not used in this version
	MinBatchSize int
	// Maximum batch size when dequeue
	MaxBatchSize int
	// Queue buffer size
	MaxQueueSize int
	// Return false in Enqueue() when the queue is full
	NonBlocking bool
	// Dequeue messages even the messages size is smaller than max batch size after the timeout.
	LingerTime time.Duration
}

func DefaultOption() *Option {
	return &Option{
		MinBatchSize: 1,
		MaxBatchSize: 10,
		MaxQueueSize: 1000,
		LingerTime:   50 * time.Microsecond,
	}
}

func (o *Option) SetMinBatchSize(size int) *Option {
	o.MinBatchSize = size
	return o
}
func (o *Option) SetMaxBatchSize(size int) *Option {
	o.MaxBatchSize = size
	return o
}
func (o *Option) SetMaxQueueSize(size int) *Option {
	o.MaxQueueSize = size
	return o
}
func (o *Option) SetLingerTime(d time.Duration) *Option {
	o.LingerTime = d
	return o
}

func (o *Option) FlagInit() {
	if val := os.Getenv("QUEUE_MIN_BATCH_SIZE"); val != "" {
		parsed, _ := strconv.ParseInt(val, 0, 0)
		o.MinBatchSize = int(parsed)
	}
	flag.IntVar(&o.MinBatchSize, "min.batch.size", o.MinBatchSize, "minimum batch size for queue")

	if val := os.Getenv("QUEUE_MAX_BATCH_SIZE"); val != "" {
		parsed, _ := strconv.ParseInt(val, 0, 0)
		o.MaxBatchSize = int(parsed)
	}
	flag.IntVar(&o.MaxBatchSize, "max.batch.size", o.MaxBatchSize, "maximum batch size for queue")

	if val := os.Getenv("QUEUE_MAX_QUEUE_SIZE"); val != "" {
		parsed, _ := strconv.ParseInt(val, 0, 0)
		o.MaxQueueSize = int(parsed)
	}
	flag.IntVar(&o.MaxQueueSize, "max.queue.size", o.MaxQueueSize, "maximum queue capacity")

	if val := os.Getenv("QUEUE_LINGER_TIME"); val != "" {
		o.LingerTime, _ = time.ParseDuration(val)
	}
	flag.DurationVar(&o.LingerTime, "linger.time", o.LingerTime, "linger time wait for execute a batch")

}
