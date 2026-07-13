package http

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

type auditDeliveryWriter struct {
	mu       sync.Mutex
	events   []sandboxobservability.Event
	batches  [][]sandboxobservability.Event
	err      error
	started  chan struct{}
	block    chan struct{}
	onInsert func()
}

func (w *auditDeliveryWriter) InsertEvents(_ context.Context, events []sandboxobservability.Event) error {
	if w.started != nil {
		select {
		case w.started <- struct{}{}:
		default:
		}
	}
	if w.block != nil {
		<-w.block
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}
	w.batches = append(w.batches, append([]sandboxobservability.Event(nil), events...))
	w.events = append(w.events, events...)
	if w.onInsert != nil {
		w.onInsert()
	}
	return nil
}

func (*auditDeliveryWriter) InsertLogs(context.Context, []sandboxobservability.LogEntry) error {
	return nil
}

func (*auditDeliveryWriter) InsertRuntimeSamples(context.Context, []sandboxobservability.RuntimeSample) error {
	return nil
}

func (w *auditDeliveryWriter) snapshotEvents() []sandboxobservability.Event {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]sandboxobservability.Event(nil), w.events...)
}

func (w *auditDeliveryWriter) snapshotBatchSizes() []int {
	w.mu.Lock()
	defer w.mu.Unlock()
	sizes := make([]int, 0, len(w.batches))
	for _, batch := range w.batches {
		sizes = append(sizes, len(batch))
	}
	return sizes
}

func TestAuditDeliveryEnqueueDurableReturnsWithClickHouseDown(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{err: errors.New("clickhouse unavailable")}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	event := sandboxobservability.Event{EventID: "00000000-0000-4000-8000-000000000001"}
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); err != nil {
		t.Fatalf("durable event is not in the spool: %v", err)
	}
	if got := writer.snapshotEvents(); len(got) != 0 {
		t.Fatalf("durable enqueue synchronously called ClickHouse: %#v", got)
	}
}

func TestAuditDeliveryEnqueueWakesBackgroundReplay(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	delivery.Start(ctx)
	// Let the worker finish its startup replay so this assertion exercises the
	// enqueue wake-up rather than the one-second periodic replay.
	time.Sleep(20 * time.Millisecond)

	event := sandboxobservability.Event{EventID: "00000000-0000-4000-8000-000000000002"}
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got := writer.snapshotEvents(); len(got) == 1 && got[0].EventID == event.EventID {
			if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); !os.IsNotExist(err) {
				t.Fatalf("replayed spool record still exists: %v", err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("background replay did not receive event promptly: %#v", writer.snapshotEvents())
}

func TestAuditDeliveryPersistsCanonicalBeforeClickHouseAndReplaysAfterRestart(t *testing.T) {
	dir := t.TempDir()
	event := sandboxobservability.Event{EventID: "11111111-1111-4111-8111-111111111111"}
	blocked := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{}), err: errors.New("unavailable")}
	delivery, err := newAuditDelivery(dir, blocked, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- delivery.PersistCanonical(context.Background(), event) }()
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("ClickHouse writer was not called")
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); err != nil {
		t.Fatalf("result was not fsynced before ClickHouse call: %v", err)
	}
	close(blocked.block)
	if err := <-done; err == nil || !errors.Is(err, errAuditDeliveryPending) {
		t.Fatalf("PersistCanonical() error = %v, want pending canonical event", err)
	}

	recovered := &auditDeliveryWriter{}
	restarted, err := newAuditDelivery(dir, recovered, zap.NewNop())
	if err != nil {
		t.Fatalf("restart delivery error = %v", err)
	}
	if err := restarted.replay(context.Background()); err != nil {
		t.Fatalf("replay() error = %v", err)
	}
	if len(recovered.events) != 1 || recovered.events[0].EventID != event.EventID {
		t.Fatalf("replayed events = %#v", recovered.events)
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); !os.IsNotExist(err) {
		t.Fatalf("acknowledged spool record still exists: %v", err)
	}
}

func TestAuditDeliveryReplayBatchesPendingEvents(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	for range auditReplayBatchSize + 1 {
		event := sandboxobservability.Event{EventID: uuid.NewString()}
		if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
			t.Fatalf("EnqueueDurable() error = %v", err)
		}
	}

	if err := delivery.replay(context.Background()); err != nil {
		t.Fatalf("first replay() error = %v", err)
	}
	if err := delivery.replay(context.Background()); err != nil {
		t.Fatalf("second replay() error = %v", err)
	}
	if got, want := writer.snapshotBatchSizes(), []int{auditReplayBatchSize, 1}; len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("canonical batch sizes = %v, want %v", got, want)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir() error = %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("acknowledged spool entries = %d, want 0", len(entries))
	}
}

func TestAuditDeliveryCanonicalWaitsForInFlightReplayWithoutDuplicate(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{})}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	event := sandboxobservability.Event{EventID: "77777777-7777-4777-8777-777777777777"}
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() error = %v", err)
	}
	replayDone := make(chan error, 1)
	go func() { replayDone <- delivery.replay(context.Background()) }()
	select {
	case <-writer.started:
	case <-time.After(time.Second):
		t.Fatal("background replay did not start")
	}

	canonicalDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		canonicalDone <- delivery.PersistCanonical(ctx, event)
	}()
	time.Sleep(20 * time.Millisecond)
	close(writer.block)
	if err := <-replayDone; err != nil {
		t.Fatalf("replay() error = %v", err)
	}
	if err := <-canonicalDone; err != nil {
		t.Fatalf("PersistCanonical() error = %v", err)
	}
	if got := writer.snapshotEvents(); len(got) != 1 || got[0].EventID != event.EventID {
		t.Fatalf("canonical events = %#v, want one copy", got)
	}
}

func TestAuditDeliveryFallsBackToCanonicalInsertWhenSpoolWriteFails(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	event := sandboxobservability.Event{EventID: "22222222-2222-4222-8222-222222222222"}
	if err := delivery.EnqueueDurable(context.Background(), event); err != nil {
		t.Fatalf("EnqueueDurable() fallback error = %v", err)
	}
	if len(writer.events) != 1 || writer.events[0].EventID != event.EventID {
		t.Fatalf("canonical fallback events = %#v", writer.events)
	}
}

func TestAuditDeliveryReportsUnrecordedWhenSpoolAndCanonicalInsertFail(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{err: errors.New("clickhouse unavailable")}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	err = delivery.EnqueueDurable(context.Background(), sandboxobservability.Event{EventID: "33333333-3333-4333-8333-333333333333"})
	if err == nil || !errors.Is(err, errAuditUnrecorded) {
		t.Fatalf("EnqueueDurable() error = %v, want unrecorded event", err)
	}
}

func TestAuditDeliveryDoesNotDowngradeCanonicalACKWhenSpoolCleanupFails(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	writer.onInsert = func() {
		replaceAuditSpoolDirectoryWithFile(t, dir)
	}
	event := sandboxobservability.Event{EventID: "44444444-4444-4444-8444-444444444444"}
	if err := delivery.PersistCanonical(context.Background(), event); err != nil {
		t.Fatalf("PersistCanonical() error after canonical ACK = %v", err)
	}
	if len(writer.events) != 1 || writer.events[0].EventID != event.EventID {
		t.Fatalf("canonical events = %#v", writer.events)
	}
}

func TestAuditDeliveryRejectsCorruptOrUnsafeIdentity(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditDelivery() error = %v", err)
	}
	if err := delivery.EnqueueDurable(context.Background(), sandboxobservability.Event{EventID: "../escape"}); err == nil {
		t.Fatal("EnqueueDurable() error = nil, want unsafe event ID rejection")
	}
	if len(writer.events) != 0 {
		t.Fatalf("unsafe event reached canonical fallback: %#v", writer.events)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := newAuditDelivery(dir, writer, zap.NewNop()); err == nil {
		t.Fatal("newAuditDelivery() error = nil, want corrupt spool startup failure")
	}
}

func replaceAuditSpoolDirectoryWithFile(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll(%q) error = %v", dir, err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", dir, err)
	}
}
