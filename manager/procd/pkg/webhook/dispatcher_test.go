package webhook

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestDispatcherBoundsDurablePendingRecordsAndRecordBytes(t *testing.T) {
	outboxDir := t.TempDir()
	dispatcher := NewDispatcher(Options{
		OutboxDir:         outboxDir,
		RequestTimeout:    10 * time.Millisecond,
		BaseBackoff:       time.Hour,
		MaxPendingRecords: 2,
		MaxRecordBytes:    2048,
	}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	dispatcher.SetConfig("http://127.0.0.1:1/webhook", "")

	for _, eventID := range []string{"evt-one", "evt-two"} {
		if _, err := dispatcher.Enqueue(Event{
			EventID:   eventID,
			EventType: EventTypeAgentEvent,
		}); err != nil {
			t.Fatalf("Enqueue(%s) error = %v", eventID, err)
		}
	}
	if _, err := dispatcher.Enqueue(Event{
		EventID:   "evt-three",
		EventType: EventTypeAgentEvent,
	}); !errors.Is(err, ErrOutboxFull) {
		t.Fatalf("third Enqueue() error = %v, want ErrOutboxFull", err)
	}

	large := NewDispatcher(Options{
		OutboxDir:      t.TempDir(),
		MaxRecordBytes: 256,
	}, zap.NewNop())
	t.Cleanup(func() {
		_ = large.Shutdown(context.Background())
	})
	large.SetConfig("http://127.0.0.1:1/webhook", "")
	if _, err := large.Enqueue(Event{
		EventID:   "evt-large",
		EventType: EventTypeAgentEvent,
		Payload:   map[string]any{"payload": strings.Repeat("x", 1024)},
	}); !errors.Is(err, ErrEventTooLarge) {
		t.Fatalf("large Enqueue() error = %v, want ErrEventTooLarge", err)
	}
}

func TestDispatcherBoundsAndExpiresDeadLetters(t *testing.T) {
	outboxDir := t.TempDir()
	dispatcher := &Dispatcher{
		options: Options{
			OutboxDir:            outboxDir,
			MaxDeadLetterRecords: 2,
			DeadLetterRetention:  time.Hour,
		},
		logger: zap.NewNop(),
	}

	for index, name := range []string{"one.json", "two.json", "three.json"} {
		path := filepath.Join(outboxDir, name)
		if err := os.WriteFile(path, []byte("invalid"), 0o600); err != nil {
			t.Fatalf("write pending dead letter: %v", err)
		}
		modTime := time.Now().Add(time.Duration(index) * time.Second)
		if err := os.Chtimes(path, modTime, modTime); err != nil {
			t.Fatalf("age pending dead letter: %v", err)
		}
		dispatcher.moveBadRecord(path, errors.New("invalid record"))
	}
	bad, err := filepath.Glob(filepath.Join(outboxDir, "bad", "*.json"))
	if err != nil {
		t.Fatalf("glob bad records: %v", err)
	}
	if len(bad) != 2 {
		t.Fatalf("bad record count = %d, want 2", len(bad))
	}

	old := bad[0]
	oldTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(old, oldTime, oldTime); err != nil {
		t.Fatalf("age bad record: %v", err)
	}
	if err := pruneDeadLetterDir(
		filepath.Dir(old),
		2,
		time.Hour,
	); err != nil {
		t.Fatalf("pruneDeadLetterDir() error = %v", err)
	}
	if _, err := os.Stat(old); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expired dead letter stat error = %v, want not exist", err)
	}
}

func TestDispatcherDurablyStoresBeforeDelivery(t *testing.T) {
	outboxDir := t.TempDir()
	dispatcher := NewDispatcher(Options{
		OutboxDir:      outboxDir,
		RequestTimeout: 10 * time.Millisecond,
		BaseBackoff:    10 * time.Millisecond,
	}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	dispatcher.SetIdentity("sandbox-1", "team-1")
	dispatcher.SetConfig("http://127.0.0.1:1/webhook", "secret")

	eventID, err := dispatcher.Enqueue(Event{
		EventID:   "evt-durable",
		EventType: EventTypeAgentEvent,
		Payload:   map[string]any{"ok": true},
	})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if eventID != "evt-durable" {
		t.Fatalf("eventID = %q, want evt-durable", eventID)
	}

	record := readTestRecord(t, filepath.Join(outboxDir, "evt-durable.json"))
	if record.TargetURL != "http://127.0.0.1:1/webhook" {
		t.Fatalf("target URL = %q", record.TargetURL)
	}
	if record.Signature == "" {
		t.Fatal("expected stored signed payload")
	}
	if record.Event.SandboxID != "sandbox-1" || record.Event.TeamID != "team-1" {
		t.Fatalf("identity not filled: %#v", record.Event)
	}
}

func TestDispatcherDurableOutboxReplaysAfterRestart(t *testing.T) {
	outboxDir := t.TempDir()
	first := NewDispatcher(Options{
		OutboxDir:      outboxDir,
		RequestTimeout: 10 * time.Millisecond,
		BaseBackoff:    10 * time.Millisecond,
	}, zap.NewNop())
	first.SetConfig("http://127.0.0.1:1/webhook", "")
	if _, err := first.Enqueue(Event{EventID: "evt-replay", EventType: EventTypeAgentEvent}); err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	_ = first.Shutdown(context.Background())

	var (
		mu       sync.Mutex
		received []Event
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var event Event
		if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
			t.Errorf("decode event: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		mu.Lock()
		received = append(received, event)
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	record := readTestRecord(t, filepath.Join(outboxDir, "evt-replay.json"))
	record.TargetURL = server.URL
	writeTestRecord(t, filepath.Join(outboxDir, "evt-replay.json"), record)

	second := NewDispatcher(Options{
		OutboxDir:      outboxDir,
		RequestTimeout: time.Second,
		BaseBackoff:    10 * time.Millisecond,
	}, zap.NewNop())
	defer second.Shutdown(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(received)
		mu.Unlock()
		if count == 1 {
			if _, err := os.Stat(filepath.Join(outboxDir, "evt-replay.json")); os.IsNotExist(err) {
				return
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("received %d events, want 1", len(received))
}

func TestDispatcherDurableOutboxDeduplicatesPendingEventID(t *testing.T) {
	outboxDir := t.TempDir()
	dispatcher := NewDispatcher(Options{
		OutboxDir:      outboxDir,
		RequestTimeout: 10 * time.Millisecond,
		BaseBackoff:    time.Hour,
	}, zap.NewNop())
	t.Cleanup(func() {
		_ = dispatcher.Shutdown(context.Background())
	})
	dispatcher.SetConfig("http://127.0.0.1:1/webhook", "")

	if _, err := dispatcher.Enqueue(Event{
		EventID:   "evt-same",
		EventType: EventTypeAgentEvent,
		Payload:   map[string]any{"value": "first"},
	}); err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	if _, err := dispatcher.Enqueue(Event{
		EventID:   "evt-same",
		EventType: EventTypeAgentEvent,
		Payload:   map[string]any{"value": "second"},
	}); err != nil {
		t.Fatalf("second Enqueue() error = %v", err)
	}

	files, err := filepath.Glob(filepath.Join(outboxDir, "*.json"))
	if err != nil {
		t.Fatalf("glob outbox: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("record count = %d, want 1 (%#v)", len(files), files)
	}
	record := readTestRecord(t, files[0])
	if string(record.Body) == "" {
		t.Fatal("expected stored body")
	}
	if got := record.Event.Payload.(map[string]any)["value"]; got != "first" {
		t.Fatalf("payload value = %v, want first", got)
	}
}

func readTestRecord(t *testing.T, path string) deliveryRecord {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		record, err := readRecord(path)
		if err == nil {
			return record
		}
		if !os.IsNotExist(err) || time.Now().After(deadline) {
			t.Fatalf("read record %s: %v", path, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func writeTestRecord(t *testing.T, path string, record deliveryRecord) {
	t.Helper()
	if err := writeRecordFile(path, record); err != nil {
		t.Fatalf("write record %s: %v", path, err)
	}
}
