package queue

import (
	"log"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEnqueue(t *testing.T) {
	maxSize := 100
	q := New(maxSize)
	q.Enqueue(1)
	assert.Equal(t, q.Size(), 1, "add 1 message in batch queue")
	q.Enqueue(1, 2, 3)
	assert.Equal(t, q.Size(), 4, "add 3 message in batch queue")

	res, _ := q.DequeueN(2)
	assert.Equal(t, len(res), 2, "dequeue by batch of 2")
	v, err := q.Take()
	log.Println("read value ", v)
	assert.Equal(t, err, nil)

}

func TestDequeue(t *testing.T) {
	maxSize := 105
	q := New(maxSize).SetMaxBatchSize(10)
	for i := 0; i < maxSize; i++ {
		ret, err := q.Enqueue(i)
		log.Printf("goroutine %d -> %v -> %v, in queue %v\n", i, ret, err, q)
	}
	for i := 0; i < 10; i++ {
		r, err := q.Dequeue()
		log.Printf("goroutine %d -> %v -> %v, in queue %v\n", i, r, err, q)
		assert.True(t, err == nil, "dequeue one batch")
		assert.Equal(t, len(r), 10, "batch dequeue")
	}

	ret, _ := q.Dequeue()
	assert.Equal(t, len(ret), 5, "partial dequeue")
	_, err := q.Dequeue()
	assert.Equal(t, err, ErrQueueEmpty)
}
func TestDequeueGoroutines(t *testing.T) {
	maxSize := 10000
	batchSize := 170
	q := New(maxSize).SetMaxBatchSize(batchSize)
	var wg sync.WaitGroup

	for i := 0; i < maxSize*2; i++ {
		wg.Add(1)
		go func(i int, q *Queue, wg *sync.WaitGroup) {
			runtime.Gosched()
			_, err := q.Enqueue(i)
			if err != nil {
				log.Println(err)
				wg.Done()
			}
		}(i, q, &wg)
	}
	time.Sleep(3 * time.Second)
	for i := 0; i < (maxSize*2/batchSize + 100); i++ {
		go func(i int, q *Queue, wg *sync.WaitGroup) {
			runtime.Gosched()
			ret, err := q.Dequeue()
			if err != nil {
				log.Printf("error: goroutine %d -> %v -> %v, in queue %v\n", i, ret, err, q)
				return
			}
			// log.Printf("completed: enqueued %d, dequeued %d\n", q.enqueued, q.dequeued)
			for i := 0; i < len(ret); i++ {
				wg.Done()
			}
		}(i, q, &wg)
	}
	wg.Wait()
	time.Sleep(2 * time.Second)
	log.Printf("completed: enqueued %d, dequeued %d\n", q.enqueued, q.dequeued)
}

func TestPause(t *testing.T) {
	maxSize := 10000
	batchSize := 170
	q := New(maxSize).SetMaxBatchSize(batchSize)
	var wg sync.WaitGroup

	for i := 0; i < maxSize*3; i++ {
		wg.Add(1)
		go func(i int, q *Queue, wg *sync.WaitGroup) {
			runtime.Gosched()
			_, err := q.Enqueue(i)
			if err != nil {
				log.Println(err)
				wg.Done()
			}
		}(i, q, &wg)
	}
	q.Pause()
	time.Sleep(3 * time.Second)
	for i := 0; i < (maxSize*3/batchSize + 100); i++ {
		go func(i int, q *Queue, wg *sync.WaitGroup) {
			runtime.Gosched()
			ret, err := q.Dequeue()
			if err != nil {
				log.Printf("error: goroutine %d -> %v -> %v, in queue %v\n", i, ret, err, q)
				return
			}
			// log.Printf("completed: enqueued %d, dequeued %d\n", q.enqueued, q.dequeued)
			for i := 0; i < len(ret); i++ {
				wg.Done()
			}
		}(i, q, &wg)
	}
	wg.Wait()
	time.Sleep(2 * time.Second)
	log.Printf("completed: enqueued %d, dequeued %d\n", q.enqueued, q.dequeued)
	assert.True(t, q.dequeued < uint64(maxSize*20))
}
