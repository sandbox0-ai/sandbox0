package ingest

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type BatchStats struct {
	InsertedItems uint64
	DroppedItems  uint64
	FailedBatches uint64
}

const shutdownFlushTimeout = 5 * time.Second

type batchWorker[T any] struct {
	insertBatch   func(context.Context, []T) error
	groupKey      func(T) string
	cfg           Config
	queue         chan T
	insertedCount atomic.Uint64
	droppedCount  atomic.Uint64
	failedBatches atomic.Uint64
}

func newBatchWorker[T any](insertBatch func(context.Context, []T) error, cfg Config) (*batchWorker[T], error) {
	return newGroupedBatchWorker(insertBatch, nil, cfg)
}

func newGroupedBatchWorker[T any](
	insertBatch func(context.Context, []T) error,
	groupKey func(T) string,
	cfg Config,
) (*batchWorker[T], error) {
	if insertBatch == nil {
		return nil, fmt.Errorf("insert batch function is nil")
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	return &batchWorker[T]{
		insertBatch: insertBatch,
		groupKey:    groupKey,
		cfg:         normalized,
		queue:       make(chan T, normalized.QueueSize),
	}, nil
}

func (w *batchWorker[T]) Enqueue(ctx context.Context, item T) error {
	select {
	case w.queue <- item:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *batchWorker[T]) TryEnqueue(item T) bool {
	select {
	case w.queue <- item:
		return true
	default:
		w.droppedCount.Add(1)
		return false
	}
}

func (w *batchWorker[T]) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]T, 0, w.cfg.BatchSize)
	batchGroup := ""
	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		w.flushBatch(flushCtx, batch)
		batch = make([]T, 0, w.cfg.BatchSize)
		batchGroup = ""
	}
	appendItem := func(flushCtx context.Context, item T) {
		if w.groupKey != nil {
			itemGroup := w.groupKey(item)
			if len(batch) > 0 && itemGroup != batchGroup {
				flush(flushCtx)
			}
			if len(batch) == 0 {
				batchGroup = itemGroup
			}
		}
		batch = append(batch, item)
		if len(batch) >= w.cfg.BatchSize {
			flush(flushCtx)
		}
	}

	for {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownFlushTimeout)
			for {
				select {
				case item := <-w.queue:
					appendItem(shutdownCtx, item)
				default:
					flush(shutdownCtx)
					cancel()
					return
				}
			}
		case item := <-w.queue:
			appendItem(ctx, item)
		case <-ticker.C:
			flush(ctx)
		}
	}
}

func (w *batchWorker[T]) Stats() BatchStats {
	return BatchStats{
		InsertedItems: w.insertedCount.Load(),
		DroppedItems:  w.droppedCount.Load(),
		FailedBatches: w.failedBatches.Load(),
	}
}

func (w *batchWorker[T]) flushBatch(ctx context.Context, batch []T) {
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
