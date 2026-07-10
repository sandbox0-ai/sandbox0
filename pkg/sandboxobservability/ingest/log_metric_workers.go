package ingest

import (
	"context"
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

type LogWorker struct {
	worker *batchWorker[sandboxobservability.LogEntry]
}

func NewLogWorker(writer sandboxobservability.Writer, cfg Config) (*LogWorker, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	worker, err := newBatchWorker(func(ctx context.Context, logs []sandboxobservability.LogEntry) error {
		return writer.InsertLogs(ctx, logs)
	}, cfg)
	if err != nil {
		return nil, err
	}
	return &LogWorker{worker: worker}, nil
}

func (w *LogWorker) Enqueue(ctx context.Context, entry sandboxobservability.LogEntry) error {
	return w.worker.Enqueue(ctx, entry)
}

func (w *LogWorker) TryEnqueue(entry sandboxobservability.LogEntry) bool {
	return w.worker.TryEnqueue(entry)
}

func (w *LogWorker) Run(ctx context.Context) {
	w.worker.Run(ctx)
}

func (w *LogWorker) Stats() BatchStats {
	return w.worker.Stats()
}

type RuntimeSampleWorker struct {
	worker *batchWorker[sandboxobservability.RuntimeSample]
}

func NewRuntimeSampleWorker(writer sandboxobservability.Writer, cfg Config) (*RuntimeSampleWorker, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	worker, err := newBatchWorker(func(ctx context.Context, samples []sandboxobservability.RuntimeSample) error {
		return writer.InsertRuntimeSamples(ctx, samples)
	}, cfg)
	if err != nil {
		return nil, err
	}
	return &RuntimeSampleWorker{worker: worker}, nil
}

func (w *RuntimeSampleWorker) Enqueue(ctx context.Context, sample sandboxobservability.RuntimeSample) error {
	return w.worker.Enqueue(ctx, sample)
}

func (w *RuntimeSampleWorker) TryEnqueue(sample sandboxobservability.RuntimeSample) bool {
	return w.worker.TryEnqueue(sample)
}

func (w *RuntimeSampleWorker) Run(ctx context.Context) {
	w.worker.Run(ctx)
}

func (w *RuntimeSampleWorker) Stats() BatchStats {
	return w.worker.Stats()
}
