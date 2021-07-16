package queue

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"
)

var ErrQueuePaused = errors.New("Queue is paused enqueue data")
var ErrQueueFull = errors.New("Queue is full")
var ErrQueueEmpty = errors.New("Queue is empty")
var ErrQueueTooSmall = errors.New("Queue size is smaller than minimum batch size")

// An batch queue implementation
type Queue struct {
	// Queue option
	option *Option

	// Status
	paused bool

	// Channel
	queue chan interface{}

	// Queue stats
	enqueued uint64
	dequeued uint64
	// Queue locks
	enqueueLocker *sync.Mutex
	dequeueLocker *sync.Mutex
	batchableLock *sync.Mutex
}

// Contructors

func New(size int) *Queue {
	option := DefaultOption().SetMaxQueueSize(size)
	return NewWithOption(option)
}
func NewWithOption(option *Option) *Queue {
	return &Queue{
		enqueueLocker: &sync.Mutex{},
		dequeueLocker: &sync.Mutex{},
		option:        option,
		queue:         make(chan interface{}, option.MaxQueueSize),
		batchableLock: &sync.Mutex{},

		enqueued: 0,
		dequeued: 0,
	}
}

func (q *Queue) MaxQueueSize() int {
	return q.option.MaxQueueSize
}
func (q *Queue) MaxBatchSize() int {
	return q.option.MaxBatchSize
}
func (q *Queue) MinBatchSize() int {
	return q.option.MinBatchSize
}
func (q *Queue) NonBlocking() bool {
	return q.option.NonBlocking
}

func (q *Queue) SetMinBatchSize(size int) *Queue {
	q.enqueueLocker.Lock()
	defer q.enqueueLocker.Unlock()
	q.option.MinBatchSize = size
	return q
}
func (q *Queue) SetMaxBatchSize(size int) *Queue {
	q.enqueueLocker.Lock()
	defer q.enqueueLocker.Unlock()
	q.option.MaxBatchSize = size
	return q
}

func (q *Queue) SetLingerTime(d time.Duration) *Queue {
	q.enqueueLocker.Lock()
	defer q.enqueueLocker.Unlock()
	q.option.LingerTime = d
	return q
}

// %s/%v format
func (q *Queue) String() string {
	return fmt.Sprintf("size: %v, batch size:%v, buffer size:%v, enqueued:%v, dequeued:%v",
		q.Size(), q.MaxBatchSize(), q.MaxQueueSize(), q.Enqueued(), q.Dequeued())
}

// Current queue size
func (q *Queue) Size() int {
	return len(q.queue)
}

// Check if current queue is empty (size is zero)
func (q *Queue) IsFull() bool {
	return q.Size() == q.MaxQueueSize()
}

// Check if current queue is empty (size is zero)
func (q *Queue) IsEmpty() bool {
	return q.Size() == 0
}

// Enqueued messages
func (q *Queue) Enqueued() uint64 {
	return q.enqueued
}

// Dequeued messages
func (q *Queue) Dequeued() uint64 {
	return q.dequeued
}

// Pause the queue means no more put data into channel.
func (q *Queue) Pause() {
	go func(q *Queue) {
		q.enqueueLocker.Lock()
		defer q.enqueueLocker.Unlock()
		q.paused = true
	}(q)
}

// Check if batchable or not
func (q *Queue) isBatchable() bool {
	return q.enqueued >= q.dequeued+uint64(q.MaxBatchSize()) && q.enqueued > 0
}

/// Increase message count enqueued
func (q *Queue) enqueuedWith(size uint64) uint64 {
	q.enqueued += size
	// q.poll_batchable()
	return q.enqueued
}

/// Incerase message count dequeued
func (q *Queue) dequeuedWith(size uint64) uint64 {
	q.dequeued += size
	// q.poll_batchable()
	return q.dequeued
}

/// Put one message into queue
func (q *Queue) EnqueueOnce(msg interface{}) (bool, error) {
	q.enqueueLocker.Lock()
	defer q.enqueueLocker.Unlock()
	if q.paused {
		return false, ErrQueuePaused
	}
	if q.NonBlocking() && q.IsFull() {
		return false, ErrQueueFull
	}
	q.queue <- msg
	q.enqueuedWith(1)
	return true, nil
}

/// Push messages into queue
func (q *Queue) Enqueue(msgs ...interface{}) (int, error) {
	q.enqueueLocker.Lock()
	defer q.enqueueLocker.Unlock()
	if q.paused {
		return 0, ErrQueuePaused
	}
	add := len(msgs)
	if q.Size()+add <= q.option.MaxQueueSize {
		for _, msg := range msgs {
			q.queue <- msg
		}
		q.enqueuedWith(uint64(add))
		return 0, nil
	} else {
		success := 0
		for _, msg := range msgs {
			if q.NonBlocking() && q.IsFull() {
				return success, ErrQueueFull
			}
			q.queue <- msg
			success++
			q.enqueuedWith(1)
		}
		// log.Printf("Queue size: %v, chan len: %v", q.Size(), len(q.queue))
		return add, nil
	}
}

func (q *Queue) takeOne() (interface{}, error) {
	q.dequeueLocker.Lock()
	defer q.dequeueLocker.Unlock()
	if q.IsEmpty() && q.NonBlocking() {
		return nil, ErrQueueEmpty
	}
	i := <-q.queue
	q.dequeuedWith(1)
	return i, nil
}

func (q *Queue) takeBatch(batchSize int, allowPartialDequeue bool) ([]interface{}, error) {
	q.dequeueLocker.Lock()
	defer q.dequeueLocker.Unlock()
	if q.IsEmpty() {
		return nil, ErrQueueEmpty
	}
	size := batchSize
	if q.Size() < size {
		if allowPartialDequeue {
			size = q.Size()
		} else {
			return nil, ErrQueueTooSmall
		}
	}
	msgs := make([]interface{}, size)
	for i := 0; i < size; i++ {
		item := <-q.queue
		msgs[i] = item
	}
	q.dequeuedWith(uint64(size))
	return msgs, nil
}

/// Dequeue one message
func (q *Queue) Take() (interface{}, error) {
	return q.takeOne()
}

/// Dequeue N messages
func (q *Queue) DequeueN(size int) ([]interface{}, error) {
	if size <= 0 {
		return q.Dequeue()
	} else {
		return q.takeBatch(size, true)
	}
}

/// Dequeue messages with preset options, default return array of messages with equal or less than MaxBatchSize
func (q *Queue) Dequeue() ([]interface{}, error) {
	batchSize := q.MaxBatchSize()
	msgs, err := q.takeBatch(batchSize, false)
	if err == nil {
		return msgs, nil
	}
	if q.NonBlocking() {
		return msgs, err
	}
	// log.Println(err)
	if err == ErrQueueEmpty {
		return nil, ErrQueueEmpty
	}
	if err == ErrQueueTooSmall {
		// Only one woker could wait batchable channel
		q.batchableLock.Lock()
		defer q.batchableLock.Unlock()
		batchable := make(chan bool)
		go func(ch chan bool) {
			for {
				runtime.Gosched()
				if q.isBatchable() && !q.IsEmpty() {
					// log.Printf("batchable now!")
					ch <- true
				}
			}
		}(batchable)
		select {
		case <-time.After(q.option.LingerTime):
			return q.takeBatch(batchSize, true)
		case <-batchable:
			// log.Printf("batchable branch!")
			return q.takeBatch(batchSize, true)
		}
	}
	return msgs, nil
}
