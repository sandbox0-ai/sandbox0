package proxy

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
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

func TestHTTPAuditSinkFallsBackToCanonicalWhenSpoolPutFails(t *testing.T) {
	for _, phase := range []sandboxobservability.EventPhase{
		sandboxobservability.EventPhaseAttempt,
		sandboxobservability.EventPhaseResult,
	} {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()
			spool, err := newAuditSpool(dir)
			if err != nil {
				t.Fatalf("newAuditSpool() error = %v", err)
			}
			received := make(chan sandboxobservability.Event, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			defer server.Close()
			sink := directHTTPAuditSink(server.URL, server.Client(), spool, zap.NewNop())
			replaceNetdAuditSpoolDirectoryWithFile(t, dir)
			event := auditEvent{
				EventID:   "33333333-3333-4333-8333-333333333333",
				Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
				Transport: "tcp", Action: "pass-through", Outcome: "completed", Phase: string(phase),
			}
			if err := sink.WriteAuditEvent(event); err != nil {
				t.Fatalf("WriteAuditEvent() fallback error = %v", err)
			}
			select {
			case got := <-received:
				if got.EventID != event.EventID || got.Phase != phase || got.Producer.Sequence == 0 {
					t.Fatalf("canonical fallback event = %#v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for canonical fallback")
			}
		})
	}
}

func TestHTTPAuditSinkReportsUnrecordedWhenSpoolAndCanonicalFallbackFail(t *testing.T) {
	for _, phase := range []sandboxobservability.EventPhase{
		sandboxobservability.EventPhaseAttempt,
		sandboxobservability.EventPhaseResult,
	} {
		t.Run(string(phase), func(t *testing.T) {
			dir := t.TempDir()
			spool, err := newAuditSpool(dir)
			if err != nil {
				t.Fatalf("newAuditSpool() error = %v", err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			sink := directHTTPAuditSink(server.URL, server.Client(), spool, zap.NewNop())
			replaceNetdAuditSpoolDirectoryWithFile(t, dir)
			err = sink.WriteAuditEvent(auditEvent{
				EventID:   "44444444-4444-4444-8444-444444444444",
				Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
				Transport: "tcp", Action: "pass-through", Outcome: "completed", Phase: string(phase),
			})
			if err == nil || !strings.Contains(err.Error(), "audit "+string(phase)+" is unrecorded") || !strings.Contains(err.Error(), "canonical fallback failed") {
				t.Fatalf("WriteAuditEvent() error = %v, want explicit dual-delivery failure", err)
			}
		})
	}
}

func TestHTTPAuditSinkDoesNotFailAttemptAfterCanonicalACKWhenSpoolCleanupFails(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	core, observed := observer.New(zap.ErrorLevel)
	logger := zap.New(core)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := os.RemoveAll(dir); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	sink := directHTTPAuditSink(server.URL, server.Client(), spool, logger)
	if err := sink.WriteAuditEvent(auditEvent{
		EventID:   "55555555-5555-4555-8555-555555555555",
		Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
		Transport: "tcp", Action: "pass-through", Outcome: "accepted", Phase: string(sandboxobservability.EventPhaseAttempt),
	}); err != nil {
		t.Fatalf("WriteAuditEvent() error after canonical ACK = %v", err)
	}
	if observed.FilterMessage("Canonical netd audit attempt was acknowledged but spool cleanup failed").Len() != 1 {
		t.Fatalf("cleanup failure logs = %#v", observed.All())
	}
}

func TestHTTPAuditSinkLogsResultCleanupFailureAfterCanonicalACK(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := auditEvent{
		EventID: "66666666-6666-4666-8666-666666666666", ProducerSequence: 1,
		Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
		Transport: "tcp", Action: "pass-through", Outcome: "completed", Phase: string(sandboxobservability.EventPhaseResult),
	}
	if err := spool.Put(event); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	core, observed := observer.New(zap.ErrorLevel)
	sink := directHTTPAuditSink("http://unused", http.DefaultClient, spool, zap.New(core))
	replaceNetdAuditSpoolDirectoryWithFile(t, dir)
	sink.completeAuditGroup([]auditEvent{event}, true)
	if observed.FilterMessage("Canonical netd audit results were acknowledged but spool cleanup failed").Len() != 1 {
		t.Fatalf("cleanup failure logs = %#v", observed.All())
	}
}

func TestHTTPAuditSinkDoesNotFallbackOnSpoolIdentityCollision(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := auditEvent{
		EventID: "77777777-7777-4777-8777-777777777777", ProducerSequence: 1,
		Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
		Transport: "tcp", Action: "pass-through", Outcome: "completed", Phase: string(sandboxobservability.EventPhaseResult),
	}
	if err := spool.Put(event); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	canonicalCalled := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		canonicalCalled <- struct{}{}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()
	sink := directHTTPAuditSink(server.URL, server.Client(), spool, zap.NewNop())
	event.Outcome = "error"
	err = sink.WriteAuditEvent(event)
	if err == nil || !strings.Contains(err.Error(), "event_id collision") {
		t.Fatalf("WriteAuditEvent() error = %v, want spool identity collision", err)
	}
	select {
	case <-canonicalCalled:
		t.Fatal("identity collision must not reach canonical fallback")
	default:
	}
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

func directHTTPAuditSink(endpoint string, client *http.Client, spool *auditSpool, logger *zap.Logger) *httpAuditSink {
	return &httpAuditSink{
		endpoint: endpoint, client: client, spool: spool, logger: logger,
		requestTimeout: time.Second, maxRetries: 0, retryBackoff: time.Millisecond,
	}
}

func replaceNetdAuditSpoolDirectoryWithFile(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("RemoveAll(%q) error = %v", dir, err)
	}
	if err := os.WriteFile(dir, []byte("not a directory"), 0o600); err != nil {
		t.Fatalf("WriteFile(%q) error = %v", dir, err)
	}
}
