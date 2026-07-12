package http

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

type auditDeliveryWriter struct {
	mu       sync.Mutex
	events   []sandboxobservability.Event
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

func TestAuditResultDeliveryPersistsBeforeClickHouseAndReplaysAfterRestart(t *testing.T) {
	dir := t.TempDir()
	event := sandboxobservability.Event{EventID: "11111111-1111-4111-8111-111111111111"}
	blocked := &auditDeliveryWriter{started: make(chan struct{}, 1), block: make(chan struct{}), err: errors.New("unavailable")}
	delivery, err := newAuditResultDelivery(dir, blocked, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- delivery.Persist(context.Background(), event) }()
	select {
	case <-blocked.started:
	case <-time.After(time.Second):
		t.Fatal("ClickHouse writer was not called")
	}
	if _, err := os.Stat(filepath.Join(dir, event.EventID+".json")); err != nil {
		t.Fatalf("result was not fsynced before ClickHouse call: %v", err)
	}
	close(blocked.block)
	if err := <-done; err == nil || !errors.Is(err, errAuditResultPending) {
		t.Fatalf("Persist() error = %v, want pending canonical result", err)
	}

	recovered := &auditDeliveryWriter{}
	restarted, err := newAuditResultDelivery(dir, recovered, zap.NewNop())
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

func TestAuditResultDeliveryFallsBackToCanonicalInsertWhenSpoolWriteFails(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditResultDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	event := sandboxobservability.Event{EventID: "22222222-2222-4222-8222-222222222222"}
	if err := delivery.Persist(context.Background(), event); err != nil {
		t.Fatalf("Persist() fallback error = %v", err)
	}
	if len(writer.events) != 1 || writer.events[0].EventID != event.EventID {
		t.Fatalf("canonical fallback events = %#v", writer.events)
	}
}

func TestAuditResultDeliveryReportsUnrecordedWhenSpoolAndCanonicalInsertFail(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{err: errors.New("clickhouse unavailable")}
	delivery, err := newAuditResultDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	replaceAuditSpoolDirectoryWithFile(t, dir)
	err = delivery.Persist(context.Background(), sandboxobservability.Event{EventID: "33333333-3333-4333-8333-333333333333"})
	if err == nil || !errors.Is(err, errAuditResultUnrecorded) {
		t.Fatalf("Persist() error = %v, want unrecorded result", err)
	}
}

func TestAuditResultDeliveryDoesNotDowngradeCanonicalACKWhenSpoolCleanupFails(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditResultDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	writer.onInsert = func() {
		replaceAuditSpoolDirectoryWithFile(t, dir)
	}
	event := sandboxobservability.Event{EventID: "44444444-4444-4444-8444-444444444444"}
	if err := delivery.Persist(context.Background(), event); err != nil {
		t.Fatalf("Persist() error after canonical ACK = %v", err)
	}
	if len(writer.events) != 1 || writer.events[0].EventID != event.EventID {
		t.Fatalf("canonical events = %#v", writer.events)
	}
}

func TestAuditResultDeliveryRejectsCorruptOrUnsafeIdentity(t *testing.T) {
	dir := t.TempDir()
	writer := &auditDeliveryWriter{}
	delivery, err := newAuditResultDelivery(dir, writer, zap.NewNop())
	if err != nil {
		t.Fatalf("newAuditResultDelivery() error = %v", err)
	}
	if err := delivery.Persist(context.Background(), sandboxobservability.Event{EventID: "../escape"}); err == nil {
		t.Fatal("Persist() error = nil, want unsafe event ID rejection")
	}
	if len(writer.events) != 0 {
		t.Fatalf("unsafe event reached canonical fallback: %#v", writer.events)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := newAuditResultDelivery(dir, writer, zap.NewNop()); err == nil {
		t.Fatal("newAuditResultDelivery() error = nil, want corrupt spool startup failure")
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
