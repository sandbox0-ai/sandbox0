// Package sandboxobservability contains manager-owned observability producers.
package sandboxobservability

import (
	"context"
	"fmt"

	shared "github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
)

// LogWriter persists manager-produced sandbox log entries.
type LogWriter interface {
	InsertLogs(context.Context, []shared.LogEntry) error
}

// LogWorker batches manager-produced sandbox log entries before ingestion.
type LogWorker struct {
	worker *ingest.BatchWorker[shared.LogEntry]
}

// NewLogWorker creates a manager-owned sandbox log worker.
func NewLogWorker(writer LogWriter, cfg ingest.Config) (*LogWorker, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	worker, err := ingest.NewBatchWorker(writer.InsertLogs, cfg)
	if err != nil {
		return nil, err
	}
	return &LogWorker{worker: worker}, nil
}

// TryEnqueue adds a log entry without blocking. It returns false when the
// bounded queue is full.
func (w *LogWorker) TryEnqueue(entry shared.LogEntry) bool {
	return w.worker.TryEnqueue(entry)
}

// Run flushes queued log entries until the context is cancelled.
func (w *LogWorker) Run(ctx context.Context) {
	w.worker.Run(ctx)
}
