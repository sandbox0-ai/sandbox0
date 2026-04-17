package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

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
