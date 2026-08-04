package runtimemetrics

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability/ingest"
)

// RuntimeSampleWriter persists ctld-produced runtime samples.
type RuntimeSampleWriter interface {
	InsertRuntimeSamples(context.Context, []sandboxobservability.RuntimeSample) error
}

// RuntimeSampleWorker batches ctld-produced runtime samples before ingestion.
type RuntimeSampleWorker struct {
	worker *ingest.BatchWorker[sandboxobservability.RuntimeSample]
}

// NewRuntimeSampleWorker creates a ctld-owned runtime sample worker.
func NewRuntimeSampleWorker(writer RuntimeSampleWriter, cfg ingest.Config) (*RuntimeSampleWorker, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	worker, err := ingest.NewBatchWorker(writer.InsertRuntimeSamples, cfg)
	if err != nil {
		return nil, err
	}
	return &RuntimeSampleWorker{worker: worker}, nil
}

// TryEnqueue adds a runtime sample without blocking. It returns false when the
// bounded queue is full.
func (w *RuntimeSampleWorker) TryEnqueue(sample sandboxobservability.RuntimeSample) bool {
	return w.worker.TryEnqueue(sample)
}

// Run flushes queued runtime samples until the context is cancelled.
func (w *RuntimeSampleWorker) Run(ctx context.Context) {
	w.worker.Run(ctx)
}
