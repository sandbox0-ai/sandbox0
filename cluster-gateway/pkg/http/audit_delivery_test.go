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
	mu      sync.Mutex
	events  []sandboxobservability.Event
	err     error
	started chan struct{}
	block   chan struct{}
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
	if err := <-done; err == nil {
		t.Fatal("Persist() error = nil, want missing canonical ACK to reach the caller")
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
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	if _, err := newAuditResultDelivery(dir, writer, zap.NewNop()); err == nil {
		t.Fatal("newAuditResultDelivery() error = nil, want corrupt spool startup failure")
	}
}
