package proxy

import (
	"context"
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
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := newAuditDeliveryTestEvent("11111111-1111-4111-8111-111111111111", sandboxobservability.EventPhaseResult)
	if err := spool.Put(event); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".sequence")); !os.IsNotExist(err) {
		t.Fatalf("sequence metadata exists or cannot be inspected: %v", err)
	}
	loaded, err := spool.Load(10)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(loaded) != 1 ||
		loaded[0].EventID != event.EventID ||
		loaded[0].SchemaVersion != sandboxobservability.LegacyEventSchemaVersion {
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

func TestAuditSchemaVersionOnlyAdvancesForExecutionScope(t *testing.T) {
	unscoped := auditEvent{}
	if got := auditSchemaVersion(unscoped); got != sandboxobservability.LegacyEventSchemaVersion {
		t.Fatalf("unscoped schema version = %d, want %d", got, sandboxobservability.LegacyEventSchemaVersion)
	}
	scoped := auditEvent{ExecutionScope: &sandboxobservability.ExecutionScope{
		Namespace:   "codex",
		Kind:        "native_session",
		ID:          "thread-1",
		Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
	}}
	if got := auditSchemaVersion(scoped); got != sandboxobservability.CurrentEventSchemaVersion {
		t.Fatalf("scoped schema version = %d, want %d", got, sandboxobservability.CurrentEventSchemaVersion)
	}
}

func TestAuditSpoolLoadsPreV3RecordAsLegacySchema(t *testing.T) {
	dir := t.TempDir()
	event := newAuditDeliveryTestEvent(
		"19191919-1919-4919-8919-191919191919",
		sandboxobservability.EventPhaseResult,
	)
	event.SchemaVersion = 0
	payload, err := json.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	if err := os.WriteFile(
		filepath.Join(dir, event.EventID+".json"),
		payload,
		0o600,
	); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}

	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() legacy error = %v", err)
	}
	events, err := spool.Load(1)
	if err != nil {
		t.Fatalf("Load() legacy error = %v", err)
	}
	if len(events) != 1 ||
		events[0].SchemaVersion != sandboxobservability.LegacyEventSchemaVersion {
		t.Fatalf("legacy spool events = %#v", events)
	}
	projected, ok := (&httpAuditSink{}).toObservabilityEvent(events[0])
	if !ok || projected.SchemaVersion != sandboxobservability.LegacyEventSchemaVersion {
		t.Fatalf("legacy projection = %#v, ok = %v", projected, ok)
	}
}

func TestAuditSpoolConcurrentPutPreservesEventIDCollision(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	base := newAuditDeliveryTestEvent("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", sandboxobservability.EventPhaseResult)
	base.Timestamp = time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC)
	base.Outcome = "completed"
	conflicting := base
	conflicting.Outcome = "error"
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, event := range []auditEvent{base, conflicting} {
		event := event
		go func() {
			<-start
			results <- spool.Put(event)
		}()
	}
	close(start)

	successes := 0
	collisions := 0
	for range 2 {
		err := <-results
		switch {
		case err == nil:
			successes++
		case strings.Contains(err.Error(), "event_id collision"):
			collisions++
		default:
			t.Fatalf("Put() unexpected error = %v", err)
		}
	}
	if successes != 1 || collisions != 1 {
		t.Fatalf("concurrent Put results = %d success, %d collision; want one each", successes, collisions)
	}
	events, err := spool.Load(10)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(events) != 1 || (events[0].Outcome != base.Outcome && events[0].Outcome != conflicting.Outcome) {
		t.Fatalf("persisted events = %#v, want exactly one complete payload", events)
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
	event := newAuditDeliveryTestEvent("22222222-2222-4222-8222-222222222222", sandboxobservability.EventPhaseResult)
	event.Outcome = "completed"
	if err := first.WriteAuditEvent(event); err != nil {
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
		DeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
	})
	defer sink.Close()
	err = sink.WriteAuditEvent(newAuditDeliveryTestEvent("12121212-1212-4212-8212-121212121212", sandboxobservability.EventPhaseAttempt))
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
			event := newAuditDeliveryTestEvent("33333333-3333-4333-8333-333333333333", phase)
			event.Outcome = "completed"
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
			event := newAuditDeliveryTestEvent("44444444-4444-4444-8444-444444444444", phase)
			event.Outcome = "completed"
			err = sink.WriteAuditEvent(event)
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
	if err := sink.WriteAuditEvent(newAuditDeliveryTestEvent("55555555-5555-4555-8555-555555555555", sandboxobservability.EventPhaseAttempt)); err != nil {
		t.Fatalf("WriteAuditEvent() error after canonical ACK = %v", err)
	}
	if observed.FilterMessage("Canonical network audit attempt was acknowledged but spool cleanup failed").Len() != 1 {
		t.Fatalf("cleanup failure logs = %#v", observed.All())
	}
}

func TestHTTPAuditSinkLogsResultCleanupFailureAfterCanonicalACK(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := newAuditDeliveryTestEvent("66666666-6666-4666-8666-666666666666", sandboxobservability.EventPhaseResult)
	event.Outcome = "completed"
	if err := spool.Put(event); err != nil {
		t.Fatalf("Put() error = %v", err)
	}
	core, observed := observer.New(zap.ErrorLevel)
	sink := directHTTPAuditSink("http://unused", http.DefaultClient, spool, zap.New(core))
	replaceNetdAuditSpoolDirectoryWithFile(t, dir)
	sink.completeAuditGroup([]auditEvent{event}, true)
	if observed.FilterMessage("Canonical network audit results were acknowledged but spool cleanup failed").Len() != 1 {
		t.Fatalf("cleanup failure logs = %#v", observed.All())
	}
}

func TestHTTPAuditSinkDoesNotFallbackOnSpoolIdentityCollision(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := newAuditDeliveryTestEvent("77777777-7777-4777-8777-777777777777", sandboxobservability.EventPhaseResult)
	event.Outcome = "completed"
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

func TestAuditSpoolRejectsMalformedFactsOnEveryReadWritePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*auditEvent)
	}{
		{name: "missing team", mutate: func(event *auditEvent) { event.TeamID = "" }},
		{name: "missing sandbox", mutate: func(event *auditEvent) { event.SandboxID = "" }},
		{name: "non-canonical team", mutate: func(event *auditEvent) { event.TeamID = " team-1 " }},
		{name: "non-canonical sandbox", mutate: func(event *auditEvent) { event.SandboxID = " sb-1 " }},
		{name: "non-canonical producer", mutate: func(event *auditEvent) { event.ProducerInstance = " node-a:boot-a " }},
		{name: "negative producer sequence", mutate: func(event *auditEvent) { event.ProducerSequence = -1 }},
		{name: "missing timestamp", mutate: func(event *auditEvent) { event.Timestamp = time.Time{} }},
		{name: "timestamp outside storage range", mutate: func(event *auditEvent) {
			event.Timestamp = time.Date(1899, time.December, 31, 23, 59, 59, 0, time.UTC)
		}},
		{name: "invalid phase", mutate: func(event *auditEvent) { event.Phase = "finished" }},
		{name: "invalid outcome", mutate: func(event *auditEvent) { event.Outcome = "maybe" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			spool, err := newAuditSpool(dir)
			if err != nil {
				t.Fatalf("newAuditSpool() error = %v", err)
			}
			event := newAuditDeliveryTestEvent("abababab-abab-4bab-8bab-abababababab", sandboxobservability.EventPhaseResult)
			tt.mutate(&event)
			if err := spool.Put(event); err == nil {
				t.Fatal("Put() error = nil for malformed audit fact")
			}
			if _, ok := (&httpAuditSink{}).toObservabilityEvent(event); ok {
				t.Fatal("toObservabilityEvent() accepted malformed audit fact")
			}

			payload, err := json.Marshal(event)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			path := filepath.Join(dir, event.EventID+".json")
			if err := os.WriteFile(path, payload, 0o600); err != nil {
				t.Fatalf("WriteFile() error = %v", err)
			}
			if events, err := spool.Load(1); err == nil {
				t.Fatalf("Load() error = nil, events = %#v", events)
			}
			if _, err := newAuditSpool(dir); err == nil {
				t.Fatal("newAuditSpool() error = nil for malformed startup record")
			}
		})
	}
}

func TestAuditSpoolLoadIgnoresRecordThatVanishedAfterDirectoryListing(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	if err := os.Symlink(filepath.Join(dir, "already-removed.json"), filepath.Join(dir, "vanished.json")); err != nil {
		t.Fatalf("Symlink() error = %v", err)
	}

	events, err := spool.Load(10)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("Load() events = %#v, want none", events)
	}
}

func TestHTTPAuditSinkInvalidProjectionReleasesReservationAndStopsReplay(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	event := newAuditDeliveryTestEvent("89898989-8989-4989-8989-898989898989", sandboxobservability.EventPhaseResult)
	event.TeamID = ""
	sink := &httpAuditSink{
		spool:  spool,
		queue:  make(chan auditEvent, 1),
		queued: make(map[string]struct{}),
		logger: zap.NewNop(),
	}
	if !sink.reserveDelivery(event.EventID) {
		t.Fatal("reserveDelivery() = false, want true")
	}

	sink.flushBatch(context.Background(), []auditEvent{event})

	sink.queuedMu.Lock()
	_, stillReserved := sink.queued[event.EventID]
	sink.queuedMu.Unlock()
	if stillReserved {
		t.Fatal("invalid projected event remained reserved")
	}
	if !sink.spoolCorrupt.Load() {
		t.Fatal("invalid projected event did not fail the spool closed")
	}
	sink.enqueueSpool()
	if got := len(sink.queue); got != 0 {
		t.Fatalf("queue length after corrupt replay = %d, want 0", got)
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
		deliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
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
