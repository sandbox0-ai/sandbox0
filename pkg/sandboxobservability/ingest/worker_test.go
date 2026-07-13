package ingest

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type fakeWriter struct {
	mu        sync.Mutex
	errs      []error
	batches   [][]sandboxobservability.Event
	inserted  chan int
	callCount int
	ctxErrs   []error
}

func (f *fakeWriter) InsertEvents(ctx context.Context, events []sandboxobservability.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.callCount++
	f.ctxErrs = append(f.ctxErrs, ctx.Err())
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return err
		}
	}
	copied := append([]sandboxobservability.Event(nil), events...)
	f.batches = append(f.batches, copied)
	if f.inserted != nil {
		f.inserted <- len(copied)
	}
	return nil
}

func (f *fakeWriter) InsertLogs(context.Context, []sandboxobservability.LogEntry) error {
	return nil
}

func (f *fakeWriter) InsertRuntimeSamples(context.Context, []sandboxobservability.RuntimeSample) error {
	return nil
}

func TestWorkerFlushesBatchBySize(t *testing.T) {
	writer := &fakeWriter{inserted: make(chan int, 1)}
	worker, err := NewWorker(writer, Config{
		QueueSize:     4,
		BatchSize:     2,
		FlushInterval: time.Hour,
		MaxRetries:    1,
		RetryBackoff:  time.Millisecond,
	})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go worker.Run(ctx)

	if !worker.TryEnqueue(sandboxobservability.Event{EventID: "1"}) ||
		!worker.TryEnqueue(sandboxobservability.Event{EventID: "2"}) {
		t.Fatal("TryEnqueue() returned false")
	}

	select {
	case count := <-writer.inserted:
		if count != 2 {
			t.Fatalf("inserted count = %d, want 2", count)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for batch flush")
	}
	waitWorkerStats(t, worker, Stats{InsertedEvents: 2})
}

func TestWorkerDropsWhenQueueIsFull(t *testing.T) {
	worker, err := NewWorker(&fakeWriter{}, Config{QueueSize: 1, BatchSize: 10})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}

	if !worker.TryEnqueue(sandboxobservability.Event{EventID: "1"}) {
		t.Fatal("first TryEnqueue() returned false")
	}
	if worker.TryEnqueue(sandboxobservability.Event{EventID: "2"}) {
		t.Fatal("second TryEnqueue() returned true, want queue-full drop")
	}
	if stats := worker.Stats(); stats.DroppedEvents != 1 {
		t.Fatalf("stats = %+v, want one dropped event", stats)
	}
}

func TestWorkerRetriesTransientInsertFailure(t *testing.T) {
	writer := &fakeWriter{errs: []error{sandboxobservability.ErrBackendUnavailable, nil}}
	worker, err := NewWorker(writer, Config{MaxRetries: 1, RetryBackoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}

	worker.flushBatch(context.Background(), []sandboxobservability.Event{{EventID: "1"}})

	if writer.callCount != 2 {
		t.Fatalf("call count = %d, want 2", writer.callCount)
	}
	if stats := worker.Stats(); stats.InsertedEvents != 1 || stats.DroppedEvents != 0 || stats.FailedBatches != 0 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestWorkerDropsBatchAfterRetries(t *testing.T) {
	writer := &fakeWriter{errs: []error{errors.New("boom"), errors.New("boom")}}
	worker, err := NewWorker(writer, Config{MaxRetries: 1, RetryBackoff: time.Millisecond})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}

	worker.flushBatch(context.Background(), []sandboxobservability.Event{{EventID: "1"}, {EventID: "2"}})

	if writer.callCount != 2 {
		t.Fatalf("call count = %d, want 2", writer.callCount)
	}
	if stats := worker.Stats(); stats.InsertedEvents != 0 || stats.DroppedEvents != 2 || stats.FailedBatches != 1 {
		t.Fatalf("stats = %+v", stats)
	}
}

func TestWorkerFlushesQueuedItemsWithLiveContextOnShutdown(t *testing.T) {
	writer := &fakeWriter{inserted: make(chan int, 1)}
	worker, err := NewWorker(writer, Config{QueueSize: 4, BatchSize: 10, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("NewWorker() error = %v", err)
	}
	if !worker.TryEnqueue(sandboxobservability.Event{EventID: "shutdown"}) {
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
	case count := <-writer.inserted:
		if count != 1 {
			t.Fatalf("inserted count = %d, want 1", count)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shutdown flush")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop")
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.ctxErrs) != 1 || writer.ctxErrs[0] != nil {
		t.Fatalf("shutdown insert contexts = %v, want one live context", writer.ctxErrs)
	}
}

func waitWorkerStats(t *testing.T, worker *Worker, want Stats) {
	t.Helper()

	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()

	for {
		got := worker.Stats()
		if got == want {
			return
		}
		select {
		case <-ticker.C:
		case <-timeout.C:
			t.Fatalf("stats = %+v, want %+v", got, want)
		}
	}
}
