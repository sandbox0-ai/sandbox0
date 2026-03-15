package proxy

import (
	"encoding/json"
	"fmt"
	"io"
	"sync"
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
	Timestamp        time.Time `json:"timestamp"`
	SandboxID        string    `json:"sandbox_id,omitempty"`
	TeamID           string    `json:"team_id,omitempty"`
	SrcIP            string    `json:"src_ip,omitempty"`
	DestIP           string    `json:"dest_ip,omitempty"`
	DestPort         int       `json:"dest_port,omitempty"`
	Transport        string    `json:"transport,omitempty"`
	Protocol         string    `json:"protocol,omitempty"`
	Host             string    `json:"host,omitempty"`
	ClassifierResult string    `json:"classifier_result,omitempty"`
	Action           string    `json:"action,omitempty"`
	Reason           string    `json:"reason,omitempty"`
	Adapter          string    `json:"adapter,omitempty"`
	Error            string    `json:"error,omitempty"`
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

func (l *auditLogger) Record(req *adapterRequest, decision trafficDecision, adapterName string, err error) error {
	if l == nil {
		return nil
	}
	event := auditEvent{
		Timestamp:        l.now(),
		SrcIP:            "",
		DestIP:           "",
		DestPort:         0,
		Transport:        decision.Transport,
		Protocol:         decision.Protocol,
		Host:             "",
		ClassifierResult: decision.ClassifierResult,
		Action:           string(decision.Action),
		Reason:           decision.Reason,
		Adapter:          adapterName,
	}
	if req != nil {
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
