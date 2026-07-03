package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type auditLogger struct {
	sink auditSink
	now  func() time.Time
}

type auditSink interface {
	WriteAuditEvent(event auditEvent) error
	Close() error
}

type multiAuditSink []auditSink

type jsonlAuditSink struct {
	mu      sync.Mutex
	writer  io.WriteCloser
	encoder *json.Encoder
}

type auditEvent struct {
	Timestamp          time.Time                `json:"timestamp"`
	FlowID             string                   `json:"flow_id,omitempty"`
	SandboxID          string                   `json:"sandbox_id,omitempty"`
	TeamID             string                   `json:"team_id,omitempty"`
	SrcIP              string                   `json:"src_ip,omitempty"`
	DestIP             string                   `json:"dest_ip,omitempty"`
	DestPort           int                      `json:"dest_port,omitempty"`
	Transport          string                   `json:"transport,omitempty"`
	Protocol           string                   `json:"protocol,omitempty"`
	Host               string                   `json:"host,omitempty"`
	ClassifierResult   string                   `json:"classifier_result,omitempty"`
	Action             string                   `json:"action,omitempty"`
	Reason             string                   `json:"reason,omitempty"`
	Outcome            string                   `json:"outcome,omitempty"`
	DurationMS         int64                    `json:"duration_ms,omitempty"`
	EgressBytes        int64                    `json:"egress_bytes,omitempty"`
	IngressBytes       int64                    `json:"ingress_bytes,omitempty"`
	Adapter            string                   `json:"adapter,omitempty"`
	AdapterCapability  string                   `json:"adapter_capability,omitempty"`
	AuthRuleName       string                   `json:"auth_rule_name,omitempty"`
	AuthRef            string                   `json:"auth_ref,omitempty"`
	AuthFailurePolicy  string                   `json:"auth_failure_policy,omitempty"`
	AuthBypassed       bool                     `json:"auth_bypassed,omitempty"`
	AuthBypassReason   string                   `json:"auth_bypass_reason,omitempty"`
	AuthEnforcement    string                   `json:"auth_enforcement,omitempty"`
	AuthResolved       bool                     `json:"auth_resolved,omitempty"`
	AuthCacheHit       bool                     `json:"auth_cache_hit,omitempty"`
	AuthResolveError   string                   `json:"auth_resolve_error,omitempty"`
	ProtocolOperations []protocolOperationAudit `json:"protocol_operations,omitempty"`
	Error              string                   `json:"error,omitempty"`
}

type flowAudit struct {
	ID           string
	StartedAt    time.Time
	egressBytes  int64
	ingressBytes int64
}

func newAuditLogger(cfg *config.NetdConfig) (*auditLogger, error) {
	if cfg == nil {
		return nil, nil
	}
	var sinks []auditSink
	if cfg.AuditLogPath != "" {
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
		sinks = append(sinks, newJSONLAuditSink(writer))
	}
	httpSink, err := newHTTPAuditSinkFromConfig(cfg)
	if err != nil {
		for _, sink := range sinks {
			_ = sink.Close()
		}
		return nil, err
	}
	if httpSink != nil {
		sinks = append(sinks, httpSink)
	}
	if len(sinks) == 0 {
		return nil, nil
	}
	return &auditLogger{
		sink: multiAuditSink(sinks),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (s multiAuditSink) WriteAuditEvent(event auditEvent) error {
	var firstErr error
	for _, sink := range s {
		if sink == nil {
			continue
		}
		if err := sink.WriteAuditEvent(event); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s multiAuditSink) Close() error {
	var firstErr error
	for _, sink := range s {
		if sink == nil {
			continue
		}
		if err := sink.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func newAuditLoggerFromWriter(writer io.WriteCloser) *auditLogger {
	if writer == nil {
		return nil
	}
	return &auditLogger{
		sink: newJSONLAuditSink(writer),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func newJSONLAuditSink(writer io.WriteCloser) *jsonlAuditSink {
	if writer == nil {
		return nil
	}
	return &jsonlAuditSink{
		writer:  writer,
		encoder: json.NewEncoder(writer),
	}
}

func (l *auditLogger) Close() error {
	if l == nil || l.sink == nil {
		return nil
	}
	return l.sink.Close()
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
		if req.EgressAuth != nil {
			if req.EgressAuth.Rule != nil {
				event.AuthRuleName = req.EgressAuth.Rule.Name
				event.AuthRef = req.EgressAuth.Rule.AuthRef
			}
			event.AuthFailurePolicy = req.EgressAuth.FailurePolicy
			event.AuthBypassed = req.EgressAuth.ShouldBypass()
			event.AuthBypassReason = req.EgressAuth.BypassReason
			event.AuthEnforcement = req.EgressAuth.EnforcementReason
			event.AuthResolved = req.EgressAuth.Resolved != nil
			event.AuthCacheHit = req.EgressAuth.CacheHit
			if req.EgressAuth.ResolveError != nil {
				event.AuthResolveError = req.EgressAuth.ResolveError.Error()
			}
		}
		if len(req.ProtocolAudit) > 0 {
			event.ProtocolOperations = append(event.ProtocolOperations, req.ProtocolAudit...)
		}
	}
	if err != nil {
		event.Error = err.Error()
	}

	if l.sink == nil {
		return nil
	}
	if encodeErr := l.sink.WriteAuditEvent(event); encodeErr != nil {
		return fmt.Errorf("encode audit event: %w", encodeErr)
	}
	return nil
}

func (s *jsonlAuditSink) WriteAuditEvent(event auditEvent) error {
	if s == nil || s.encoder == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.encoder.Encode(event)
}

func (s *jsonlAuditSink) Close() error {
	if s == nil || s.writer == nil {
		return nil
	}
	return s.writer.Close()
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
		if errors.Is(err, errProtocolPolicyDenied) {
			return "denied"
		}
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
