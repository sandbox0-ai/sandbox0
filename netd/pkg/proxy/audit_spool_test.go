package proxy

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestAuditSpoolPersistsLoadsAndRemoves(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := auditEvent{EventID: "11111111-1111-4111-8111-111111111111", TeamID: "team-1", SandboxID: "sb-1", Timestamp: time.Now().UTC()}
	if err := spool.Put(event); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	loaded, err := spool.Load(10)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded) != 1 || loaded[0].EventID != event.EventID {
		t.Fatalf("loaded = %#v", loaded)
	}
	if err := spool.Remove(event.EventID); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	loaded, err = spool.Load(10)
	if err != nil || len(loaded) != 0 {
		t.Fatalf("loaded after remove = %#v, %v", loaded, err)
	}
}

func TestHTTPAuditSinkReplaysSpoolAfterRestart(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "netd", PrivateKey: privateKey, TTL: time.Minute})

	unavailable := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	first := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint: unavailable.URL, Generator: generator, Spool: spool, QueueSize: 1,
		BatchSize: 1, FlushInterval: 10 * time.Millisecond, RequestTimeout: time.Second,
	})
	if err := first.WriteAuditEvent(auditEvent{
		EventID: "22222222-2222-4222-8222-222222222222", Timestamp: time.Now().UTC(),
		TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1", Transport: "tcp",
		Action: "pass-through", Outcome: "completed", Phase: string(sandboxobservability.EventPhaseResult),
	}); err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}
	waitForAuditSpoolCount(t, spool, 1)
	_ = first.Close()
	unavailable.Close()

	received := make(chan sandboxobservability.Event, 1)
	available := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Events []sandboxobservability.Event `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Events) != 1 {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- body.Events[0]
		w.WriteHeader(http.StatusAccepted)
	}))
	defer available.Close()
	second := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint: available.URL, Generator: generator, Spool: spool, QueueSize: 1,
		BatchSize: 1, FlushInterval: 10 * time.Millisecond, RequestTimeout: time.Second,
	})
	defer second.Close()
	select {
	case event := <-received:
		if event.EventID != "22222222-2222-4222-8222-222222222222" {
			t.Fatalf("EventID = %q", event.EventID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for spool replay")
	}
	waitForAuditSpoolCount(t, spool, 0)
}

func TestHTTPAuditSinkFailsClosedWhenAttemptCannotReachCanonicalStore(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey() error = %v", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "netd", PrivateKey: privateKey, TTL: time.Minute})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	sink := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint: server.URL, Generator: generator, Spool: spool, QueueSize: 1,
		BatchSize: 1, RequestTimeout: 100 * time.Millisecond, MaxRetries: 0,
	})
	defer sink.Close()
	err = sink.WriteAuditEvent(auditEvent{
		Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
		Transport: "tcp", Action: "pass-through", Outcome: "accepted", Phase: string(sandboxobservability.EventPhaseAttempt),
	})
	if err == nil {
		t.Fatal("WriteAuditEvent() error = nil, want fail-closed error")
	}
	waitForAuditSpoolCount(t, spool, 1)
}

func TestAuditSpoolFailsClosedOnCorruptRecord(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	events, err := spool.Load(10)
	if err == nil {
		t.Fatalf("Load() error = nil, events = %#v", events)
	}
	if _, err := os.Stat(filepath.Join(dir, "broken.json")); err != nil {
		t.Fatalf("corrupt evidence was not preserved: %v", err)
	}
}

func waitForAuditSpoolCount(t *testing.T, spool *auditSpool, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		events, err := spool.Load(100)
		if err == nil && len(events) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	events, err := spool.Load(100)
	t.Fatalf("spool count = %d, want %d: %v", len(events), want, err)
}
