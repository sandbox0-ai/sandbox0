package ingest

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const (
	DefaultQueueSize     = 1024
	DefaultBatchSize     = 100
	DefaultFlushInterval = time.Second
	DefaultMaxRetries    = 3
	DefaultRetryBackoff  = 100 * time.Millisecond
)

type Config struct {
	QueueSize     int
	BatchSize     int
	FlushInterval time.Duration
	MaxRetries    int
	RetryBackoff  time.Duration
}

type Stats struct {
	InsertedEvents uint64
	DroppedEvents  uint64
	FailedBatches  uint64
}

// Worker batches sandbox observability events into a bounded asynchronous writer.
type Worker struct {
	writer        sandboxobservability.Writer
	cfg           Config
	queue         chan sandboxobservability.Event
	insertedCount atomic.Uint64
	droppedCount  atomic.Uint64
	failedBatches atomic.Uint64
}

func NewWorker(writer sandboxobservability.Writer, cfg Config) (*Worker, error) {
	if writer == nil {
		return nil, fmt.Errorf("writer is nil")
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &Worker{
		writer: writer,
		cfg:    normalized,
		queue:  make(chan sandboxobservability.Event, normalized.QueueSize),
	}, nil
}

func (w *Worker) Enqueue(ctx context.Context, event sandboxobservability.Event) error {
	select {
	case w.queue <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Worker) TryEnqueue(event sandboxobservability.Event) bool {
	select {
	case w.queue <- event:
		return true
	default:
		w.droppedCount.Add(1)
		return false
	}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]sandboxobservability.Event, 0, w.cfg.BatchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		w.flushBatch(ctx, batch)
		batch = make([]sandboxobservability.Event, 0, w.cfg.BatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			flush()
			return
		case event := <-w.queue:
			batch = append(batch, event)
			if len(batch) >= w.cfg.BatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (w *Worker) Stats() Stats {
	return Stats{
		InsertedEvents: w.insertedCount.Load(),
		DroppedEvents:  w.droppedCount.Load(),
		FailedBatches:  w.failedBatches.Load(),
	}
}

func (w *Worker) flushBatch(ctx context.Context, batch []sandboxobservability.Event) {
	for attempt := 0; attempt <= w.cfg.MaxRetries; attempt++ {
		err := w.writer.InsertEvents(ctx, batch)
		if err == nil {
			w.insertedCount.Add(uint64(len(batch)))
			return
		}
		if attempt == w.cfg.MaxRetries {
			w.failedBatches.Add(1)
			w.droppedCount.Add(uint64(len(batch)))
			return
		}
		timer := time.NewTimer(w.cfg.RetryBackoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			w.failedBatches.Add(1)
			w.droppedCount.Add(uint64(len(batch)))
			return
		case <-timer.C:
		}
	}
}

func normalizeConfig(cfg Config) (Config, error) {
	if cfg.QueueSize == 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = DefaultMaxRetries
	}
	if cfg.RetryBackoff == 0 {
		cfg.RetryBackoff = DefaultRetryBackoff
	}
	if cfg.QueueSize < 0 {
		return Config{}, fmt.Errorf("queue_size must be non-negative")
	}
	if cfg.BatchSize < 0 {
		return Config{}, fmt.Errorf("batch_size must be non-negative")
	}
	if cfg.FlushInterval < 0 {
		return Config{}, fmt.Errorf("flush_interval must be non-negative")
	}
	if cfg.MaxRetries < 0 {
		return Config{}, fmt.Errorf("max_retries must be non-negative")
	}
	if cfg.RetryBackoff < 0 {
		return Config{}, fmt.Errorf("retry_backoff must be non-negative")
	}
	return cfg, nil
}
