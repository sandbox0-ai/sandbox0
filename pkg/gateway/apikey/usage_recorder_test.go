package apikey

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestUsageRecorderBoundsPendingKeysAndDropsExcess(t *testing.T) {
	writer := &recordingUsageBatchWriter{}
	recorder := newUsageRecorder(UsageRecorderConfig{
		FlushInterval: time.Hour,
		FlushTimeout:  time.Second,
		CloseTimeout:  2 * time.Second,
		QueueSize:     4,
		MaxPending:    1,
	}, writer)

	if !recorder.enqueue("11111111-1111-4111-8111-111111111111", time.Now()) {
		t.Fatal("enqueue first key = false")
	}
	waitForUsageQueueDrain(t, recorder)
	if !recorder.enqueue("22222222-2222-4222-8222-222222222222", time.Now()) {
		t.Fatal("enqueue second key = false")
	}
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	writer.mu.Lock()
	defer writer.mu.Unlock()
	if len(writer.batches) != 1 || len(writer.batches[0]) != 1 {
		t.Fatalf("written batches = %#v, want one bounded key", writer.batches)
	}
	if writer.batches[0][0].KeyID != "11111111-1111-4111-8111-111111111111" {
		t.Fatalf("written key = %q, want first key", writer.batches[0][0].KeyID)
	}
}

func TestUsageRecorderCloseFlushesAndIsIdempotent(t *testing.T) {
	writer := &recordingUsageBatchWriter{}
	recorder := newUsageRecorder(UsageRecorderConfig{
		FlushInterval: time.Hour,
		FlushTimeout:  time.Second,
		CloseTimeout:  2 * time.Second,
		QueueSize:     4,
		MaxPending:    4,
	}, writer)
	at := time.Now().UTC()
	for range 3 {
		if !recorder.enqueue("11111111-1111-4111-8111-111111111111", at) {
			t.Fatal("enqueue = false")
		}
	}
	if err := recorder.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}
	if err := recorder.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	batches, total := writer.snapshot()
	if batches != 1 || total != 3 {
		t.Fatalf("batches, total = %d, %d, want 1, 3", batches, total)
	}
	if recorder.enqueue("22222222-2222-4222-8222-222222222222", at) {
		t.Fatal("enqueue after close = true")
	}
}

func TestUsageRecorderEnqueueDoesNotBlockWhenQueueIsFull(t *testing.T) {
	blockedWriter := &blockingUsageWriter{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	recorder := newUsageRecorder(UsageRecorderConfig{
		FlushInterval: time.Millisecond,
		FlushTimeout:  time.Second,
		CloseTimeout:  2 * time.Second,
		QueueSize:     1,
		MaxPending:    4,
	}, blockedWriter)
	if !recorder.enqueue("11111111-1111-4111-8111-111111111111", time.Now()) {
		t.Fatal("enqueue first key = false")
	}
	<-blockedWriter.started
	if !recorder.enqueue("22222222-2222-4222-8222-222222222222", time.Now()) {
		t.Fatal("enqueue buffered key = false")
	}
	started := time.Now()
	if recorder.enqueue("33333333-3333-4333-8333-333333333333", time.Now()) {
		t.Fatal("enqueue into full queue = true")
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("full-queue enqueue blocked for %s", elapsed)
	}
	close(blockedWriter.release)
	if err := recorder.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func waitForUsageQueueDrain(t *testing.T, recorder *usageRecorder) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(recorder.events) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("usage recorder did not drain event queue")
		}
		time.Sleep(time.Millisecond)
	}
}

type blockingUsageWriter struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *blockingUsageWriter) WriteAPIKeyUsageBatch(
	ctx context.Context,
	_ []APIKeyUsage,
) error {
	w.once.Do(func() { close(w.started) })
	select {
	case <-w.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
