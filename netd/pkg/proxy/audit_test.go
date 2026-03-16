package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"net"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/netd/pkg/policy"
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
	if event.AdapterCapability != string(adapterCapabilityPassThrough) {
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
}
