package proxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestHTTPAuditSinkDefaultsAttemptsToDurableAsync(t *testing.T) {
	for _, failure := range []string{"status", "timeout"} {
		t.Run(failure, func(t *testing.T) {
			spool, err := newAuditSpool(t.TempDir())
			if err != nil {
				t.Fatalf("newAuditSpool() error = %v", err)
			}
			var available atomic.Bool
			var requests atomic.Int32
			firstRequest := make(chan struct{})
			var firstRequestOnce sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				firstRequestOnce.Do(func() { close(firstRequest) })
				if available.Load() {
					w.WriteHeader(http.StatusAccepted)
					return
				}
				if failure == "timeout" {
					timer := time.NewTimer(750 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-r.Context().Done():
					case <-timer.C:
					}
					return
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()

			sink := newHTTPAuditSink(httpAuditSinkOptions{
				Endpoint: server.URL, Spool: spool, QueueSize: 2, BatchSize: 1,
				FlushInterval: 10 * time.Millisecond, RequestTimeout: 500 * time.Millisecond,
			})
			defer sink.Close()

			started := time.Now()
			if err := sink.WriteAuditEvent(newAuditDeliveryTestEvent("11111111-1111-4111-8111-111111111111", sandboxobservability.EventPhaseAttempt)); err != nil {
				t.Fatalf("WriteAuditEvent() error = %v", err)
			}
			if elapsed := time.Since(started); elapsed >= 250*time.Millisecond {
				t.Fatalf("durable async admission took %s, want less than canonical timeout", elapsed)
			}
			if sink.deliveryMode != sandboxobservability.AuditDeliveryModeDurableAsync {
				t.Fatalf("delivery mode = %q, want durable_async", sink.deliveryMode)
			}
			select {
			case <-firstRequest:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for background canonical attempt")
			}
			waitForAuditSpoolCount(t, spool, 1)

			available.Store(true)
			waitForAuditSpoolCount(t, spool, 0)
			if requests.Load() < 2 {
				t.Fatalf("canonical requests = %d, want failure followed by recovery", requests.Load())
			}
		})
	}
}

func TestAuditLoggerCreatesIdentityBeforeFanout(t *testing.T) {
	var jsonl bytes.Buffer
	local := newJSONLAuditSink(nopWriteCloser{Writer: &jsonl})
	required := &auditSinkStub{}
	logger := newAuditLoggerWithSink(&multiAuditSink{bestEffort: []auditSink{local}, required: required}, "node-a")
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{TeamID: "team-1", SandboxID: "sb-1"},
		Audit:    newFlowAudit("tcp-1", time.Now()),
	}
	decision := trafficDecision{Transport: "tcp", Action: decisionActionPassThrough}
	if err := logger.RecordAttempt(req, decision, nil); err != nil {
		t.Fatalf("RecordAttempt() error = %v", err)
	}
	if err := logger.Record(req, decision, nil, time.Millisecond, nil); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	localEvents := make([]auditEvent, 2)
	decoder := json.NewDecoder(bytes.NewReader(jsonl.Bytes()))
	for i := range localEvents {
		if err := decoder.Decode(&localEvents[i]); err != nil {
			t.Fatalf("decode JSONL event %d: %v", i, err)
		}
	}
	if len(required.events) != 2 {
		t.Fatalf("required fanout events = %d, want 2", len(required.events))
	}
	for i := range localEvents {
		localEvent := localEvents[i]
		requiredEvent := required.events[i]
		if localEvent.EventID != requiredEvent.EventID ||
			localEvent.OperationID != requiredEvent.OperationID ||
			localEvent.ProducerInstance != requiredEvent.ProducerInstance ||
			localEvent.ProducerSequence != requiredEvent.ProducerSequence {
			t.Fatalf("fanout identity mismatch at %d: local=%+v required=%+v", i, localEvent, requiredEvent)
		}
		if _, err := uuid.Parse(localEvent.EventID); err != nil {
			t.Fatalf("event_id %q is not a UUID: %v", localEvent.EventID, err)
		}
		if _, err := uuid.Parse(localEvent.OperationID); err != nil {
			t.Fatalf("operation_id %q is not a UUID: %v", localEvent.OperationID, err)
		}
		if localEvent.OperationID != req.Audit.OperationID {
			t.Fatalf("operation_id = %q, want %q", localEvent.OperationID, req.Audit.OperationID)
		}
		if localEvent.ProducerSequence != int64(i+1) {
			t.Fatalf("producer sequence = %d, want %d", localEvent.ProducerSequence, i+1)
		}
	}
	if localEvents[0].EventID == localEvents[1].EventID {
		t.Fatal("attempt and result share an event_id")
	}
	if !strings.HasPrefix(localEvents[0].ProducerInstance, "node-a:") ||
		localEvents[0].ProducerInstance != localEvents[1].ProducerInstance {
		t.Fatalf("producer instances = %q and %q", localEvents[0].ProducerInstance, localEvents[1].ProducerInstance)
	}
	secondBoot := newAuditLoggerWithSink(&auditSinkStub{}, "node-a")
	if secondBoot.producerInstance == logger.producerInstance {
		t.Fatalf("producer instance was reused across boots: %q", logger.producerInstance)
	}
}

func TestHTTPAuditSinkAppliesFactDefaultsBeforeValidation(t *testing.T) {
	sink := &httpAuditSink{
		endpoint: "http://unused",
		queue:    make(chan auditEvent, 1),
		queued:   make(map[string]struct{}),
	}
	event := newAuditDeliveryTestEvent("67676767-6767-4767-8767-676767676767", sandboxobservability.EventPhaseResult)
	event.Timestamp = time.Time{}
	event.Phase = ""
	if err := sink.WriteAuditEvent(event); err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}
	queued := <-sink.queue
	if queued.Timestamp.IsZero() {
		t.Fatal("queued event timestamp was not defaulted")
	}
	if queued.Phase != string(sandboxobservability.EventPhaseResult) {
		t.Fatalf("queued event phase = %q, want result", queued.Phase)
	}
}

func TestMultiAuditSinkKeepsDiagnosticErrorsInJSONLOnly(t *testing.T) {
	dir := t.TempDir()
	spool, err := newAuditSpool(dir)
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	var jsonl bytes.Buffer
	local := newJSONLAuditSink(nopWriteCloser{Writer: &jsonl})
	httpSink := &httpAuditSink{
		endpoint: "http://unused",
		spool:    spool,
		queue:    make(chan auditEvent, 1),
		queued:   make(map[string]struct{}),
	}
	sink := &multiAuditSink{bestEffort: []auditSink{local}, required: httpSink}
	event := newAuditDeliveryTestEvent("78787878-7878-4787-8787-787878787878", sandboxobservability.EventPhaseResult)
	event.Error = "upstream diagnostic secret"
	event.AuthResolveError = "credential diagnostic secret"
	if err := sink.WriteAuditEvent(event); err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}

	var localEvent auditEvent
	if err := json.NewDecoder(bytes.NewReader(jsonl.Bytes())).Decode(&localEvent); err != nil {
		t.Fatalf("decode JSONL event: %v", err)
	}
	if localEvent.Error != event.Error || localEvent.AuthResolveError != event.AuthResolveError {
		t.Fatalf("JSONL diagnostics = error:%q auth:%q", localEvent.Error, localEvent.AuthResolveError)
	}
	spooled, err := spool.Load(1)
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if len(spooled) != 1 {
		t.Fatalf("spooled events = %d, want 1", len(spooled))
	}
	if spooled[0].Error != "" || spooled[0].AuthResolveError != "" {
		t.Fatalf("canonical spool retained diagnostics: %+v", spooled[0])
	}
}

func TestHTTPAuditSinkCanonicalSyncWaitsForAttemptACK(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sink := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint: server.URL, Spool: spool, QueueSize: 2, BatchSize: 100,
		FlushInterval: time.Hour, RequestTimeout: time.Second,
		DeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
	})
	defer sink.Close()

	if err := sink.WriteAuditEvent(newAuditDeliveryTestEvent("22222222-2222-4222-8222-222222222222", sandboxobservability.EventPhaseAttempt)); err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("canonical requests = %d, want 1", requests.Load())
	}
	waitForAuditSpoolCount(t, spool, 0)
}

func TestHTTPAuditSinkCanonicalSyncDoesNotRaceSpoolReplay(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	var requests atomic.Int32
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRequest) }) }
	defer release()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		startOnce.Do(func() { close(requestStarted) })
		<-releaseRequest
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sink := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint: server.URL, Spool: spool, QueueSize: 2, BatchSize: 1,
		FlushInterval: 5 * time.Millisecond, RequestTimeout: time.Second,
		DeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
	})
	defer sink.Close()

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- sink.WriteAuditEvent(newAuditDeliveryTestEvent("77777777-7777-4777-8777-777777777777", sandboxobservability.EventPhaseAttempt))
	}()
	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for strict canonical request")
	}
	time.Sleep(50 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		release()
		t.Fatalf("canonical requests before ACK = %d, want 1", got)
	}
	release()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatalf("WriteAuditEvent() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for strict audit admission")
	}
	waitForAuditSpoolCount(t, spool, 0)
	time.Sleep(20 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("canonical requests = %d, want exactly 1", got)
	}
}

func TestHTTPAuditSinkResultsRemainDurableAsyncInCanonicalSyncMode(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	sink := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint: server.URL, Spool: spool, QueueSize: 2, BatchSize: 100,
		FlushInterval: time.Hour, RequestTimeout: time.Second,
		DeliveryMode: sandboxobservability.AuditDeliveryModeCanonicalSync,
	})
	defer sink.Close()

	if err := sink.WriteAuditEvent(newAuditDeliveryTestEvent("33333333-3333-4333-8333-333333333333", sandboxobservability.EventPhaseResult)); err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}
	if requests.Load() != 0 {
		t.Fatalf("canonical requests = %d, want none on result path", requests.Load())
	}
	waitForAuditSpoolCount(t, spool, 1)
}

func TestHTTPAuditSinkQueueFullKeepsDurableEvents(t *testing.T) {
	spool, err := newAuditSpool(t.TempDir())
	if err != nil {
		t.Fatalf("newAuditSpool() error = %v", err)
	}
	sink := &httpAuditSink{
		endpoint:     "http://unused",
		spool:        spool,
		queue:        make(chan auditEvent, 1),
		queued:       make(map[string]struct{}),
		deliveryMode: sandboxobservability.AuditDeliveryModeDurableAsync,
	}

	for _, event := range []auditEvent{
		newAuditDeliveryTestEvent("44444444-4444-4444-8444-444444444444", sandboxobservability.EventPhaseAttempt),
		newAuditDeliveryTestEvent("55555555-5555-4555-8555-555555555555", sandboxobservability.EventPhaseAttempt),
	} {
		if err := sink.WriteAuditEvent(event); err != nil {
			t.Fatalf("WriteAuditEvent(%s) error = %v", event.EventID, err)
		}
	}
	waitForAuditSpoolCount(t, spool, 2)
	if len(sink.queue) != 1 {
		t.Fatalf("queue length = %d, want 1", len(sink.queue))
	}
}

func TestMultiAuditSinkUsesDurableHTTPResultForAdmission(t *testing.T) {
	legacyErr := errors.New("legacy JSONL unavailable")
	legacy := &auditSinkStub{writeErr: legacyErr}
	required := &auditSinkStub{}
	sink := &multiAuditSink{bestEffort: []auditSink{legacy}, required: required}

	if err := sink.WriteAuditEvent(auditEvent{EventID: "event-1"}); err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}
	if legacy.writes != 1 || required.writes != 1 {
		t.Fatalf("writes = legacy:%d required:%d, want 1 each", legacy.writes, required.writes)
	}
}

func TestMultiAuditSinkPreservesLocalOnlyFailure(t *testing.T) {
	wantErr := errors.New("local audit failed")
	local := &auditSinkStub{writeErr: wantErr}
	sink := &multiAuditSink{bestEffort: []auditSink{local}}

	if err := sink.WriteAuditEvent(auditEvent{}); !errors.Is(err, wantErr) {
		t.Fatalf("WriteAuditEvent() error = %v, want %v", err, wantErr)
	}
}

func TestMultiAuditSinkRequiredFailureBlocksAdmission(t *testing.T) {
	wantErr := errors.New("durable HTTP delivery failed")
	local := &auditSinkStub{}
	required := &auditSinkStub{writeErr: wantErr}
	sink := &multiAuditSink{bestEffort: []auditSink{local}, required: required}

	if err := sink.WriteAuditEvent(auditEvent{}); !errors.Is(err, wantErr) {
		t.Fatalf("WriteAuditEvent() error = %v, want %v", err, wantErr)
	}
}

func TestFlowAuditCapturesProtocolOperationsAcrossRequestCopies(t *testing.T) {
	flow := newFlowAudit("flow-1", time.Now())
	req := &adapterRequest{Audit: flow}
	requests := []*adapterRequest{}
	for range 8 {
		copy := *req
		requests = append(requests, &copy)
	}

	var wg sync.WaitGroup
	for i, request := range requests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request.appendProtocolAudit(protocolOperationAudit{Protocol: "http", Operation: "request", Object: string(rune('a' + i))})
		}()
	}
	wg.Wait()

	sink := &auditSinkStub{}
	logger := &auditLogger{sink: sink, now: func() time.Time { return time.Now().UTC() }}
	if err := logger.Record(req, trafficDecision{}, nil, time.Second, nil); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if len(sink.events) != 1 || len(sink.events[0].ProtocolOperations) != len(requests) {
		t.Fatalf("recorded protocol operations = %#v, want %d", sink.events, len(requests))
	}
}

func TestFlowAuditBoundsProtocolOperations(t *testing.T) {
	flow := newFlowAudit("flow-1", time.Now())
	entries := make([]protocolOperationAudit, maxFlowProtocolOperations+10)
	for i := range entries {
		entries[i] = protocolOperationAudit{Protocol: "http", Operation: "request"}
	}
	flow.appendProtocolOperations(entries...)
	flow.appendProtocolOperations(protocolOperationAudit{Protocol: "mcp", Operation: "tool_call"})

	operations, truncated := flow.protocolOperationsSnapshot()
	if got := len(operations); got != maxFlowProtocolOperations {
		t.Fatalf("protocol operations = %d, want %d", got, maxFlowProtocolOperations)
	}
	if !truncated {
		t.Fatal("protocol operations truncation was not recorded")
	}
	sink := &auditSinkStub{}
	logger := &auditLogger{sink: sink, now: func() time.Time { return time.Now().UTC() }}
	if err := logger.Record(&adapterRequest{Audit: flow}, trafficDecision{}, nil, time.Second, nil); err != nil {
		t.Fatalf("Record() error = %v", err)
	}
	if len(sink.events) != 1 || !sink.events[0].ProtocolOperationsTruncated {
		t.Fatalf("recorded event truncation = %#v, want true", sink.events)
	}
}

func TestFlowAuditBoundsProtocolOperationFields(t *testing.T) {
	flow := newFlowAudit("flow-1", time.Now())
	longPath := "GET /" + strings.Repeat("\x01<&/very-long-path", 1024)
	longReason := strings.Repeat("\x02<&/very-long-reason", 1024)
	flow.appendProtocolOperations(protocolOperationAudit{
		RuleName: longReason, Protocol: "http", Operation: "request",
		Object: longPath, Action: "allow", Reason: longReason,
	})

	operations, truncated := flow.protocolOperationsSnapshot()
	if len(operations) != 1 {
		t.Fatalf("protocol operations = %d, want 1", len(operations))
	}
	if !truncated {
		t.Fatal("protocol operation field truncation was not recorded")
	}
	for name, value := range map[string]string{
		"object": operations[0].Object,
		"reason": operations[0].Reason,
	} {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		if contentBytes := len(encoded) - 2; contentBytes > sandboxobservability.MaxNetworkAuditProtocolFieldEncodedBytes {
			t.Fatalf("encoded %s bytes = %d, want <= %d", name, contentBytes, sandboxobservability.MaxNetworkAuditProtocolFieldEncodedBytes)
		}
	}
}

type auditSinkStub struct {
	writeErr error
	writes   int
	events   []auditEvent
}

func (s *auditSinkStub) WriteAuditEvent(event auditEvent) error {
	s.writes++
	s.events = append(s.events, event)
	return s.writeErr
}

func (*auditSinkStub) Close() error { return nil }

func newAuditDeliveryTestEvent(id string, phase sandboxobservability.EventPhase) auditEvent {
	return auditEvent{
		EventID: id, SchemaVersion: sandboxobservability.LegacyEventSchemaVersion,
		OperationID:      "99999999-9999-4999-8999-999999999999",
		ProducerInstance: "node-a:boot-a", ProducerSequence: 1,
		Timestamp: time.Now().UTC(), TeamID: "team-1", SandboxID: "sb-1", FlowID: "tcp-1",
		Transport: "tcp", Action: "pass-through", Outcome: "accepted", Phase: string(phase),
	}
}
