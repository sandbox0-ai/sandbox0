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
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
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

const maxFlowProtocolOperations = sandboxobservability.MaxNetworkAuditProtocolOperations

type multiAuditSink struct {
	bestEffort []auditSink
	required   auditSink
	logger     *zap.Logger
}

type jsonlAuditSink struct {
	mu      sync.Mutex
	writer  io.WriteCloser
	encoder *json.Encoder
}

type auditEvent struct {
	EventID            string                   `json:"event_id,omitempty"`
	ProducerSequence   uint64                   `json:"producer_sequence,omitempty"`
	Phase              string                   `json:"phase,omitempty"`
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
	// ProtocolOperationsTruncated records that the bounded protocol-operation
	// snapshot omitted operations or field content.
	ProtocolOperationsTruncated bool   `json:"protocol_operations_truncated,omitempty"`
	Error                       string `json:"error,omitempty"`
}

type flowAudit struct {
	ID                 string
	StartedAt          time.Time
	egressBytes        int64
	ingressBytes       int64
	attempted          atomic.Bool
	protocolMu         sync.Mutex
	protocolOperations []protocolOperationAudit
	protocolTruncated  bool
}

func (a *flowAudit) startAttempt() bool {
	return a != nil && a.attempted.CompareAndSwap(false, true)
}

func (a *flowAudit) resetAttempt() {
	if a != nil {
		a.attempted.Store(false)
	}
}

func (a *flowAudit) appendProtocolOperations(entries ...protocolOperationAudit) {
	if a == nil || len(entries) == 0 {
		return
	}
	a.protocolMu.Lock()
	defer a.protocolMu.Unlock()
	remaining := maxFlowProtocolOperations - len(a.protocolOperations)
	if remaining <= 0 {
		a.protocolTruncated = true
		return
	}
	if len(entries) > remaining {
		a.protocolTruncated = true
		entries = entries[:remaining]
	}
	for _, entry := range entries {
		bounded, truncated := boundProtocolOperationAudit(entry)
		if truncated {
			a.protocolTruncated = true
		}
		a.protocolOperations = append(a.protocolOperations, bounded)
	}
}

func boundProtocolOperationAudit(entry protocolOperationAudit) (protocolOperationAudit, bool) {
	truncated := false
	bound := func(value string) string {
		value, changed := sandboxobservability.TruncateJSONStringContent(value, sandboxobservability.MaxNetworkAuditProtocolFieldEncodedBytes)
		truncated = truncated || changed
		return value
	}
	entry.RuleName = bound(entry.RuleName)
	entry.Protocol = bound(entry.Protocol)
	entry.Operation = bound(entry.Operation)
	entry.Object = bound(entry.Object)
	entry.Action = bound(entry.Action)
	entry.Reason = bound(entry.Reason)
	return entry, truncated
}

func (a *flowAudit) protocolOperationsSnapshot() ([]protocolOperationAudit, bool) {
	if a == nil {
		return nil, false
	}
	a.protocolMu.Lock()
	defer a.protocolMu.Unlock()
	return append([]protocolOperationAudit(nil), a.protocolOperations...), a.protocolTruncated
}

func newAuditLogger(cfg *config.NetdConfig, logger *zap.Logger) (*auditLogger, error) {
	if cfg == nil {
		return nil, nil
	}
	var localSinks []auditSink
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
		localSinks = append(localSinks, newJSONLAuditSink(writer))
	}
	httpSink, err := newHTTPAuditSinkFromConfig(cfg, logger)
	if err != nil {
		for _, sink := range localSinks {
			_ = sink.Close()
		}
		return nil, err
	}
	if len(localSinks) == 0 && httpSink == nil {
		return nil, nil
	}
	return &auditLogger{
		sink: &multiAuditSink{bestEffort: localSinks, required: httpSink, logger: logger},
		now: func() time.Time {
			return time.Now().UTC()
		},
	}, nil
}

func (s *multiAuditSink) WriteAuditEvent(event auditEvent) error {
	if s == nil {
		return nil
	}
	var localErr error
	for _, sink := range s.bestEffort {
		if sink == nil {
			continue
		}
		if err := sink.WriteAuditEvent(event); err != nil && localErr == nil {
			localErr = err
		}
	}
	if s.required == nil {
		return localErr
	}
	if err := s.required.WriteAuditEvent(event); err != nil {
		return err
	}
	if localErr != nil {
		logger := s.logger
		if logger == nil {
			logger = zap.NewNop()
		}
		logger.Warn("Legacy JSONL audit sink failed after durable HTTP delivery",
			zap.String("event_id", event.EventID),
			zap.String("phase", event.Phase),
			zap.Error(localErr),
		)
	}
	return nil
}

func (s *multiAuditSink) Close() error {
	if s == nil {
		return nil
	}
	var firstErr error
	for _, sink := range s.bestEffort {
		if sink == nil {
			continue
		}
		if err := sink.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if s.required != nil {
		if err := s.required.Close(); err != nil && firstErr == nil {
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
	return l.recordWithPhase(req, decision, adapter, duration, err, string(sandboxobservability.EventPhaseResult))
}

func (l *auditLogger) RecordAttempt(req *adapterRequest, decision trafficDecision, adapter proxyAdapter) error {
	return l.recordWithPhase(req, decision, adapter, 0, nil, string(sandboxobservability.EventPhaseAttempt))
}

func (l *auditLogger) recordWithPhase(req *adapterRequest, decision trafficDecision, adapter proxyAdapter, duration time.Duration, err error, phase string) error {
	if l == nil {
		return nil
	}
	event := auditEvent{
		Phase:             phase,
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
	if phase == string(sandboxobservability.EventPhaseAttempt) {
		event.Outcome = string(sandboxobservability.OutcomeAccepted)
	}
	if req != nil {
		if req.Audit != nil {
			event.FlowID = req.Audit.ID
			event.EgressBytes = req.Audit.EgressBytes()
			event.IngressBytes = req.Audit.IngressBytes()
			event.ProtocolOperations, event.ProtocolOperationsTruncated = req.Audit.protocolOperationsSnapshot()
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
