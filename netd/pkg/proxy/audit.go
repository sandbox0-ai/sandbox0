package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type auditLogger struct {
	mu      sync.Mutex
	writer  io.WriteCloser
	encoder *json.Encoder
	now     func() time.Time
}

type auditEvent struct {
	Timestamp         time.Time `json:"timestamp"`
	FlowID            string    `json:"flow_id,omitempty"`
	SandboxID         string    `json:"sandbox_id,omitempty"`
	TeamID            string    `json:"team_id,omitempty"`
	SrcIP             string    `json:"src_ip,omitempty"`
	DestIP            string    `json:"dest_ip,omitempty"`
	DestPort          int       `json:"dest_port,omitempty"`
	Transport         string    `json:"transport,omitempty"`
	Protocol          string    `json:"protocol,omitempty"`
	Host              string    `json:"host,omitempty"`
	ClassifierResult  string    `json:"classifier_result,omitempty"`
	Action            string    `json:"action,omitempty"`
	Reason            string    `json:"reason,omitempty"`
	Outcome           string    `json:"outcome,omitempty"`
	DurationMS        int64     `json:"duration_ms,omitempty"`
	EgressBytes       int64     `json:"egress_bytes,omitempty"`
	IngressBytes      int64     `json:"ingress_bytes,omitempty"`
	Adapter           string    `json:"adapter,omitempty"`
	AdapterCapability string    `json:"adapter_capability,omitempty"`
	Error             string    `json:"error,omitempty"`
}

type flowAudit struct {
	ID           string
	StartedAt    time.Time
	egressBytes  int64
	ingressBytes int64
}

func newAuditLogger(cfg *config.NetdConfig) (*auditLogger, error) {
	if cfg == nil || cfg.AuditLogPath == "" {
		return nil, nil
	}
	maxSizeMB := int(cfg.AuditLogMaxBytes / (1024 * 1024))
	if maxSizeMB <= 0 {
		maxSizeMB = 1
	}
	writer := &lumberjack.Logger{
		Filename:   cfg.AuditLogPath,
		MaxSize:    maxSizeMB,
		MaxBackups: cfg.AuditLogMaxBackups,
		Compress:   false,
	}
	return newAuditLoggerFromWriter(writer), nil
}

func newAuditLoggerFromWriter(writer io.WriteCloser) *auditLogger {
	if writer == nil {
		return nil
	}
	return &auditLogger{
		writer:  writer,
		encoder: json.NewEncoder(writer),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (l *auditLogger) Close() error {
	if l == nil || l.writer == nil {
		return nil
	}
	return l.writer.Close()
}

func (l *auditLogger) Record(req *adapterRequest, decision trafficDecision, adapter proxyAdapter, duration time.Duration, err error) error {
	if l == nil {
		return nil
	}
	event := auditEvent{
		Timestamp:         l.now(),
		SrcIP:             "",
		DestIP:            "",
		DestPort:          0,
		Transport:         decision.Transport,
		Protocol:          decision.Protocol,
		Host:              "",
		ClassifierResult:  decision.ClassifierResult,
		Action:            string(decision.Action),
		Reason:            decision.Reason,
		Outcome:           auditOutcome(decision, err),
		DurationMS:        duration.Milliseconds(),
		Adapter:           adapterName(adapter),
		AdapterCapability: string(adapterCapabilityOf(adapter)),
	}
	if req != nil {
		if req.Audit != nil {
			event.FlowID = req.Audit.ID
			event.EgressBytes = req.Audit.EgressBytes()
			event.IngressBytes = req.Audit.IngressBytes()
		}
		event.SrcIP = req.SrcIP
		event.DestIP = ipString(req.DestIP)
		event.DestPort = req.DestPort
		event.Host = req.Host
		if req.Compiled != nil {
			event.SandboxID = req.Compiled.SandboxID
			event.TeamID = req.Compiled.TeamID
		}
	}
	if err != nil {
		event.Error = err.Error()
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if encodeErr := l.encoder.Encode(event); encodeErr != nil {
		return fmt.Errorf("encode audit event: %w", encodeErr)
	}
	return nil
}

func newFlowAudit(id string, startedAt time.Time) *flowAudit {
	return &flowAudit{
		ID:        id,
		StartedAt: startedAt.UTC(),
	}
}

func (a *flowAudit) RecordEgress(bytes int64) {
	if a == nil || bytes <= 0 {
		return
	}
	atomic.AddInt64(&a.egressBytes, bytes)
}

func (a *flowAudit) RecordIngress(bytes int64) {
	if a == nil || bytes <= 0 {
		return
	}
	atomic.AddInt64(&a.ingressBytes, bytes)
}

func (a *flowAudit) EgressBytes() int64 {
	if a == nil {
		return 0
	}
	return atomic.LoadInt64(&a.egressBytes)
}

func (a *flowAudit) IngressBytes() int64 {
	if a == nil {
		return 0
	}
	return atomic.LoadInt64(&a.ingressBytes)
}

func auditOutcome(decision trafficDecision, err error) string {
	if err != nil {
		return "error"
	}
	if decision.Action == decisionActionDeny {
		return "denied"
	}
	return "completed"
}

func adapterName(adapter proxyAdapter) string {
	if adapter == nil {
		return ""
	}
	return adapter.Name()
}

func adapterCapabilityOf(adapter proxyAdapter) adapterCapability {
	if adapter == nil {
		return ""
	}
	return adapter.Capability()
}
