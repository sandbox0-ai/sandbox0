package audit

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// Event represents an audit event
type Event struct {
	Timestamp time.Time     `json:"timestamp"`
	VolumeID  string        `json:"volume_id"`
	SandboxID string        `json:"sandbox_id"`
	TeamID    string        `json:"team_id,omitempty"`
	Operation string        `json:"operation"`
	Inode     uint64        `json:"inode,omitempty"`
	Path      string        `json:"path,omitempty"`
	Size      int64         `json:"size,omitempty"`
	Latency   time.Duration `json:"latency_ms,omitempty"`
	Status    string        `json:"status"`
	Error     string        `json:"error,omitempty"`
}

// Logger handles audit logging
type Logger struct {
	mu     sync.Mutex
	file   *os.File
	logger *logrus.Logger
}

// NewLogger creates a new audit logger
func NewLogger(filename string, logger *logrus.Logger) (*Logger, error) {
	// Create audit log file if specified
	var file *os.File
	var err error

	if filename != "" {
		file, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return nil, err
		}
	}

	return &Logger{
		file:   file,
		logger: logger,
	}, nil
}

// Log logs an audit event
func (l *Logger) Log(ctx context.Context, event Event) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Set timestamp if not set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}

	// Write to audit file
	if l.file != nil {
		data, err := json.Marshal(event)
		if err == nil {
			l.file.Write(data)
			l.file.Write([]byte("\n"))
		}
	}

	// Also log to standard logger
	l.logger.WithFields(logrus.Fields{
		"volume_id":  event.VolumeID,
		"sandbox_id": event.SandboxID,
		"operation":  event.Operation,
		"inode":      event.Inode,
		"path":       event.Path,
		"size":       event.Size,
		"latency_ms": event.Latency.Milliseconds(),
		"status":     event.Status,
		"error":      event.Error,
	}).Info("Audit event")
}

// Close closes the audit logger
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.file != nil {
		return l.file.Close()
	}
	return nil
}
