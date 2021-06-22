package queue

import "time"

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
