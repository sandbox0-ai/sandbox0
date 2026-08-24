package service

import (
	"sync"
	"time"
)

const (
	retryQueueBaseDelay = 5 * time.Millisecond
	retryQueueMaxDelay  = 30 * time.Second
)

// retryQueue is a process-local wakeup optimization. PostgreSQL scans remain
// the durable recovery source for every controller using it.
type retryQueue[T comparable] struct {
	mu         sync.Mutex
	ready      *sync.Cond
	items      []T
	dirty      map[T]bool
	processing map[T]bool
	retries    map[T]int
	shutdown   bool
}

func newRetryQueue[T comparable]() *retryQueue[T] {
	q := &retryQueue[T]{
		dirty: make(map[T]bool), processing: make(map[T]bool), retries: make(map[T]int),
	}
	q.ready = sync.NewCond(&q.mu)
	return q
}

func (q *retryQueue[T]) Add(item T) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.shutdown || q.dirty[item] {
		return
	}
	q.dirty[item] = true
	if q.processing[item] {
		return
	}
	q.items = append(q.items, item)
	q.ready.Signal()
}

func (q *retryQueue[T]) Get() (T, bool) {
	var zero T
	if q == nil {
		return zero, true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.items) == 0 && !q.shutdown {
		q.ready.Wait()
	}
	if len(q.items) == 0 {
		return zero, true
	}
	item := q.items[0]
	q.items = q.items[1:]
	q.dirty[item] = false
	q.processing[item] = true
	return item, false
}

func (q *retryQueue[T]) Done(item T) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.processing, item)
	if q.shutdown || !q.dirty[item] {
		return
	}
	q.items = append(q.items, item)
	q.ready.Signal()
}

func (q *retryQueue[T]) Forget(item T) {
	if q == nil {
		return
	}
	q.mu.Lock()
	delete(q.retries, item)
	q.mu.Unlock()
}

func (q *retryQueue[T]) AddRateLimited(item T) {
	if q == nil {
		return
	}
	q.mu.Lock()
	retries := q.retries[item]
	q.retries[item] = retries + 1
	shutdown := q.shutdown
	q.mu.Unlock()
	if shutdown {
		return
	}
	delay := retryQueueBaseDelay << min(retries, 12)
	if delay > retryQueueMaxDelay {
		delay = retryQueueMaxDelay
	}
	time.AfterFunc(delay, func() { q.Add(item) })
}

func (q *retryQueue[T]) NumRequeues(item T) int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.retries[item]
}

func (q *retryQueue[T]) ShutDown() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.shutdown = true
	q.ready.Broadcast()
	q.mu.Unlock()
}
