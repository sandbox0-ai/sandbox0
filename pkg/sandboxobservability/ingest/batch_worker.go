package ingest

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

const shutdownFlushTimeout = 5 * time.Second

// BatchWorker batches one service-owned record type for asynchronous ingest.
// It deliberately accepts a callback instead of a domain writer so this shared
// package does not couple itself to a particular producer or record model.
type BatchWorker[T any] struct {
	insertBatch   func(context.Context, []T) error
	cfg           Config
	queue         chan T
	insertedCount atomic.Uint64
	droppedCount  atomic.Uint64
	failedBatches atomic.Uint64
}

// NewBatchWorker creates a bounded, retrying batch worker for values of T.
func NewBatchWorker[T any](insertBatch func(context.Context, []T) error, cfg Config) (*BatchWorker[T], error) {
	if insertBatch == nil {
		return nil, fmt.Errorf("insert batch function is nil")
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &BatchWorker[T]{
		insertBatch: insertBatch,
		cfg:         normalized,
		queue:       make(chan T, normalized.QueueSize),
	}, nil
}

func (w *BatchWorker[T]) TryEnqueue(item T) bool {
	select {
	case w.queue <- item:
		return true
	default:
		w.droppedCount.Add(1)
		return false
	}
}

func (w *BatchWorker[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]T, 0, w.cfg.BatchSize)
	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		w.flushBatch(flushCtx, batch)
		batch = make([]T, 0, w.cfg.BatchSize)
	}

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushTimeout)
			for {
				select {
				case item := <-w.queue:
					batch = append(batch, item)
					if len(batch) >= w.cfg.BatchSize {
						flush(shutdownCtx)
					}
				default:
					flush(shutdownCtx)
					cancel()
					return
				}
			}
		case item := <-w.queue:
			batch = append(batch, item)
			if len(batch) >= w.cfg.BatchSize {
				flush(ctx)
			}
		case <-ticker.C:
			flush(ctx)
		}
	}
}

func (w *BatchWorker[T]) flushBatch(ctx context.Context, batch []T) {
	for attempt := 0; attempt <= w.cfg.MaxRetries; attempt++ {
		err := w.insertBatch(ctx, batch)
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
