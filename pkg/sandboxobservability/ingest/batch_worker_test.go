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

func TestGroupedBatchWorkerNeverMixesGroups(t *testing.T) {
	batches := make(chan []string, 3)
	worker, err := newGroupedBatchWorker(func(_ context.Context, values []string) error {
		batches <- append([]string(nil), values...)
		return nil
	}, func(value string) string {
		return value[:1]
	}, Config{QueueSize: 4, BatchSize: 10, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("newGroupedBatchWorker() error = %v", err)
	}
	for _, value := range []string{"a1", "a2", "b1", "a3"} {
		if !worker.TryEnqueue(value) {
			t.Fatalf("TryEnqueue(%q) returned false", value)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		worker.Run(ctx)
		close(done)
	}()
	cancel()

	want := [][]string{{"a1", "a2"}, {"b1"}, {"a3"}}
	for i := range want {
		select {
		case got := <-batches:
			if len(got) != len(want[i]) {
				t.Fatalf("batch %d = %#v, want %#v", i, got, want[i])
			}
			for j := range got {
				if got[j] != want[i][j] {
					t.Fatalf("batch %d = %#v, want %#v", i, got, want[i])
				}
			}
		case <-time.After(time.Second):
			t.Fatalf("timed out waiting for batch %d", i)
		}
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("batch worker did not stop")
	}
}
