package proxy

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type nopWriteCloser struct {
	io.Writer
}

func (n nopWriteCloser) Close() error { return nil }

func TestAuditLoggerRecord(t *testing.T) {
	var buf bytes.Buffer
	logger := newAuditLoggerFromWriter(nopWriteCloser{Writer: &buf})
	logger.now = func() time.Time {
		return time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)
	}

	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{
			SandboxID: "sb-1",
			TeamID:    "team-1",
		},
		Audit:    newFlowAudit("tcp-1", time.Date(2026, 3, 16, 0, 0, 0, 0, time.UTC)),
		SrcIP:    "10.0.0.2",
		DestIP:   net.IPv4(8, 8, 8, 8),
		DestPort: 443,
		Host:     "example.com",
		ExecutionScope: &sandboxobservability.ExecutionScope{
			Namespace:   "codex",
			Kind:        "native_session",
			ID:          "thread-1",
			Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
		},
	}
	req.Audit.RecordEgress(128)
	req.Audit.RecordIngress(64)
	decision := trafficDecision{
		Action:           decisionActionUseAdapter,
		Transport:        "tcp",
		Protocol:         "ssh",
		Reason:           "allowed",
		ClassifierResult: "known",
	}

	if err := logger.Record(req, decision, &sshAdapter{}, 250*time.Millisecond, nil); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}

	var event auditEvent
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("failed to decode audit event: %v", err)
	}
	if event.SandboxID != "sb-1" || event.TeamID != "team-1" {
		t.Fatalf("unexpected subject fields: %+v", event)
	}
	if event.Protocol != "ssh" || event.Adapter != "ssh" {
		t.Fatalf("unexpected protocol fields: %+v", event)
	}
	if event.AdapterCapability != string(adapterCapabilityTerminate) {
		t.Fatalf("unexpected adapter capability: %+v", event)
	}
	if event.FlowID != "tcp-1" || event.EgressBytes != 128 || event.IngressBytes != 64 {
		t.Fatalf("unexpected flow stats: %+v", event)
	}
	if event.Outcome != "completed" || event.DurationMS != 250 {
		t.Fatalf("unexpected outcome fields: %+v", event)
	}
	if event.ClassifierResult != "known" || event.Action != "use-adapter" {
		t.Fatalf("unexpected decision fields: %+v", event)
	}
	if event.ExecutionScope == nil || event.ExecutionScope.ID != "thread-1" {
		t.Fatalf("unexpected execution scope: %+v", event.ExecutionScope)
	}
}

func TestAuditLoggerRecordIncludesEgressAuthFields(t *testing.T) {
	var buf bytes.Buffer
	logger := newAuditLoggerFromWriter(nopWriteCloser{Writer: &buf})
	req := &adapterRequest{
		Compiled: &policy.CompiledPolicy{SandboxID: "sb-1", TeamID: "team-1"},
		Audit:    newFlowAudit("tcp-2", time.Now().UTC()),
		SrcIP:    "10.0.0.2",
		DestIP:   net.IPv4(1, 1, 1, 1),
		DestPort: 443,
		Host:     "api.example.com",
		EgressAuth: &egressAuthContext{
			Rule: &policy.CompiledEgressAuthRule{
				Name:          "example-https",
				AuthRef:       "example-api",
				FailurePolicy: v1alpha1.EgressAuthFailurePolicyFailOpen,
			},
			FailurePolicy:     string(v1alpha1.EgressAuthFailurePolicyFailOpen),
			BypassReason:      "cluster_disabled",
			EnforcementReason: "cache_hit",
		},
	}
	decision := trafficDecision{
		Action:           decisionActionUseAdapter,
		Transport:        "tcp",
		Protocol:         "tls",
		Reason:           "allowed",
		ClassifierResult: "known",
	}

	if err := logger.Record(req, decision, &tlsAdapter{}, 10*time.Millisecond, nil); err != nil {
		t.Fatalf("Record returned error: %v", err)
	}

	var event auditEvent
	if err := json.Unmarshal(buf.Bytes(), &event); err != nil {
		t.Fatalf("failed to decode audit event: %v", err)
	}
	if event.AuthFailurePolicy != "fail-open" || !event.AuthBypassed || event.AuthBypassReason != "cluster_disabled" {
		t.Fatalf("unexpected auth bypass fields: %+v", event)
	}
	if event.AuthEnforcement != "cache_hit" || event.AuthRef != "example-api" || event.AuthRuleName != "example-https" {
		t.Fatalf("unexpected auth enforcement fields: %+v", event)
	}
}

func TestHTTPAuditSinkPostsObservabilityEvents(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "netd",
		PrivateKey: privateKey,
		TTL:        time.Second,
	})

	received := make(chan []sandboxobservability.Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Error("missing internal token header")
		}
		var req struct {
			Events []sandboxobservability.Event `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		received <- req.Events
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	sink := newHTTPAuditSink(httpAuditSinkOptions{
		Endpoint:       server.URL,
		RegionID:       "aws-us-east-1",
		ClusterID:      "cluster-a",
		Generator:      generator,
		QueueSize:      2,
		BatchSize:      1,
		FlushInterval:  time.Hour,
		RequestTimeout: time.Second,
		MaxRetries:     0,
		RetryBackoff:   time.Millisecond,
	})
	defer sink.Close()

	event := newAuditDeliveryTestEvent("23232323-2323-4232-8232-232323232323", sandboxobservability.EventPhaseResult)
	event.Timestamp = time.Date(2026, 7, 1, 1, 2, 3, 0, time.UTC)
	event.DestIP = "8.8.8.8"
	event.DestPort = 443
	event.Protocol = "tls"
	event.Host = "example.com"
	event.Action = "use-adapter"
	event.Reason = "allowed"
	event.Outcome = "completed"
	event.EgressBytes = 128
	event.Error = "upstream response contained a diagnostic secret"
	event.AuthResolveError = "credential provider contained a diagnostic secret"
	event.ProtocolOperations = []protocolOperationAudit{{
		RuleName: "allow-tools", Protocol: "mcp", Operation: "tools/call",
		Object: "search", Action: "allow", Reason: "matched",
	}}
	err = sink.WriteAuditEvent(event)
	if err != nil {
		t.Fatalf("WriteAuditEvent() error = %v", err)
	}

	select {
	case events := <-received:
		if len(events) != 1 {
			t.Fatalf("events = %d, want 1", len(events))
		}
		event := events[0]
		if event.TeamID != "team-1" ||
			event.SandboxID != "sb-1" ||
			event.RegionID != "aws-us-east-1" ||
			event.ClusterID != "cluster-a" ||
			event.Source != sandboxobservability.SourceNetd ||
			event.EventType != sandboxobservability.EventTypeNetworkAudit ||
			event.Phase != sandboxobservability.EventPhaseResult ||
			event.Outcome != sandboxobservability.OutcomeCompleted ||
			event.EventID == "" ||
			event.OperationID != "99999999-9999-4999-8999-999999999999" ||
			event.Producer.Instance != "node-a:boot-a" ||
			event.Producer.Sequence != 1 {
			t.Fatalf("projected event = %+v", event)
		}
		if event.Attributes["host"] != "example.com" || event.Attributes["egress_bytes"].(float64) != 128 {
			t.Fatalf("attributes = %#v", event.Attributes)
		}
		if _, ok := event.Attributes["error"]; ok {
			t.Fatalf("raw proxy error entered canonical attributes: %#v", event.Attributes)
		}
		if _, ok := event.Attributes["auth_resolve_error"]; ok {
			t.Fatalf("raw auth resolution error entered canonical attributes: %#v", event.Attributes)
		}
		operations, ok := event.Attributes["protocol_operations"].([]any)
		if !ok || len(operations) != 1 {
			t.Fatalf("protocol_operations = %#v, want one typed operation", event.Attributes["protocol_operations"])
		}
		operation, ok := operations[0].(map[string]any)
		if !ok || operation["operation"] != "tools/call" || operation["object"] != "search" {
			t.Fatalf("protocol operation = %#v", operations[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ingest request")
	}
}
