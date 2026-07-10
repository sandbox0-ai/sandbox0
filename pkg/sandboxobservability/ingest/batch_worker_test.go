package ingest

import (
	"context"
	"testing"
	"time"
)

func TestBatchWorkerFlushesQueuedItemsWithLiveContextOnShutdown(t *testing.T) {
	inserted := make(chan int, 1)
	contextErrors := make(chan error, 1)
	worker, err := newBatchWorker(func(ctx context.Context, values []int) error {
		contextErrors <- ctx.Err()
		inserted <- len(values)
		return nil
	}, Config{QueueSize: 4, BatchSize: 10, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("newBatchWorker() error = %v", err)
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
