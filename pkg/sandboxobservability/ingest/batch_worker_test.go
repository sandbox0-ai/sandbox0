package ingest

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBatchWorkerFlushesBatchBySize(t *testing.T) {
	inserted := make(chan int, 1)
	worker, err := NewBatchWorker(func(_ context.Context, values []int) error {
		inserted <- len(values)
		return nil
	}, Config{QueueSize: 4, BatchSize: 2, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewBatchWorker() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("batch worker did not stop")
		}
	})

	if !worker.TryEnqueue(1) || !worker.TryEnqueue(2) {
		t.Fatal("TryEnqueue() returned false")
	}
	select {
	case count := <-inserted:
		if count != 2 {
			t.Fatalf("inserted count = %d, want 2", count)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch flush")
	}
}

func TestBatchWorkerDropsWhenQueueIsFull(t *testing.T) {
	worker, err := NewBatchWorker(func(context.Context, []int) error {
		return nil
	}, Config{QueueSize: 1, BatchSize: 10})
	if err != nil {
		t.Fatalf("NewBatchWorker() error = %v", err)
	}

	if !worker.TryEnqueue(1) {
		t.Fatal("first TryEnqueue() returned false")
	}
	if worker.TryEnqueue(2) {
		t.Fatal("second TryEnqueue() returned true, want queue-full drop")
	}
	if got := worker.droppedCount.Load(); got != 1 {
		t.Fatalf("dropped count = %d, want 1", got)
	}
}

func TestBatchWorkerRetriesTransientInsertFailure(t *testing.T) {
	attempts := 0
	worker, err := NewBatchWorker(func(context.Context, []int) error {
		attempts++
		if attempts == 1 {
			return errors.New("transient failure")
		}
		return nil
	}, Config{MaxRetries: 1, RetryBackoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBatchWorker() error = %v", err)
	}

	worker.flushBatch(context.Background(), []int{1})

	if attempts != 2 {
		t.Fatalf("insert attempts = %d, want 2", attempts)
	}
	if got := worker.insertedCount.Load(); got != 1 {
		t.Fatalf("inserted count = %d, want 1", got)
	}
	if got := worker.droppedCount.Load(); got != 0 {
		t.Fatalf("dropped count = %d, want 0", got)
	}
	if got := worker.failedBatches.Load(); got != 0 {
		t.Fatalf("failed batches = %d, want 0", got)
	}
}

func TestBatchWorkerDropsBatchAfterConfiguredRetries(t *testing.T) {
	attempts := 0
	worker, err := NewBatchWorker(func(context.Context, []int) error {
		attempts++
		return errors.New("permanent failure")
	}, Config{MaxRetries: 1, RetryBackoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewBatchWorker() error = %v", err)
	}

	worker.flushBatch(context.Background(), []int{1, 2})

	if attempts != 2 {
		t.Fatalf("insert attempts = %d, want 2", attempts)
	}
	if got := worker.insertedCount.Load(); got != 0 {
		t.Fatalf("inserted count = %d, want 0", got)
	}
	if got := worker.droppedCount.Load(); got != 2 {
		t.Fatalf("dropped count = %d, want 2", got)
	}
	if got := worker.failedBatches.Load(); got != 1 {
		t.Fatalf("failed batches = %d, want 1", got)
	}
}

func TestBatchWorkerFlushesQueuedItemsWithLiveContextOnShutdown(t *testing.T) {
	inserted := make(chan int, 1)
	contextErrors := make(chan error, 1)
	worker, err := NewBatchWorker(func(ctx context.Context, values []int) error {
		contextErrors <- ctx.Err()
		inserted <- len(values)
		return nil
	}, Config{QueueSize: 4, BatchSize: 10, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewBatchWorker() error = %v", err)
	}
	if !worker.TryEnqueue(1) || !worker.TryEnqueue(2) {
		t.Fatal("TryEnqueue() returned false")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()
	cancel()

	select {
	case count := <-inserted:
		if count != 2 {
			t.Fatalf("inserted count = %d, want 2", count)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown flush")
	}
	select {
	case err := <-contextErrors:
		if err != nil {
			t.Fatalf("shutdown flush context error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for insert context")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch worker did not stop")
	}
}
