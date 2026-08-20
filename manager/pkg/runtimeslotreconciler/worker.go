package runtimeslotreconciler

import (
	"context"
	"errors"
	"fmt"
	"time"
)

const (
	// DefaultWorkerInterval is the delay between completed terminal passes.
	DefaultWorkerInterval = time.Second
	// DefaultWorkerPassTimeout bounds one candidate batch, including all
	// regional, node, and Nomad operations.
	DefaultWorkerPassTimeout = 2 * time.Minute
	minWorkerInterval        = 100 * time.Millisecond
	maxWorkerInterval        = time.Hour
	minWorkerPassTimeout     = time.Second
	maxWorkerPassTimeout     = time.Hour
)

// Runner executes one bounded terminal reconciliation pass.
type Runner interface {
	RunOnce(context.Context) (Result, error)
}

// WorkerConfig configures a non-overlapping terminal reconciliation loop.
type WorkerConfig struct {
	Runner      Runner
	Interval    time.Duration
	PassTimeout time.Duration
}

// WorkerReport describes one completed pass without exposing slot IDs as
// unbounded labels.
type WorkerReport struct {
	Result   Result
	Duration time.Duration
	Error    error
}

// Worker runs terminal reconciliation immediately and then waits a fixed
// delay after every pass. It never overlaps passes in one process.
type Worker struct {
	runner      Runner
	interval    time.Duration
	passTimeout time.Duration
}

// NewWorker validates a bounded reconciliation loop.
func NewWorker(config WorkerConfig) (*Worker, error) {
	if config.Runner == nil {
		return nil, errors.New("runtime slot terminal reconcile runner is required")
	}
	if config.Interval == 0 {
		config.Interval = DefaultWorkerInterval
	}
	if config.PassTimeout == 0 {
		config.PassTimeout = DefaultWorkerPassTimeout
	}
	if config.Interval < minWorkerInterval || config.Interval > maxWorkerInterval {
		return nil, fmt.Errorf("runtime slot terminal interval must be between %s and %s", minWorkerInterval, maxWorkerInterval)
	}
	if config.PassTimeout < minWorkerPassTimeout || config.PassTimeout > maxWorkerPassTimeout {
		return nil, fmt.Errorf("runtime slot terminal pass timeout must be between %s and %s", minWorkerPassTimeout, maxWorkerPassTimeout)
	}
	return &Worker{runner: config.Runner, interval: config.Interval, passTimeout: config.PassTimeout}, nil
}

// Run executes non-overlapping passes until context cancellation. Report is
// optional and is called synchronously after every completed pass.
func (w *Worker) Run(ctx context.Context, report func(WorkerReport)) error {
	if w == nil || w.runner == nil {
		return errors.New("runtime slot terminal worker is not initialized")
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		startedAt := time.Now()
		passCtx, cancel := context.WithTimeout(ctx, w.passTimeout)
		result, err := w.runner.RunOnce(passCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if report != nil {
			report(WorkerReport{Result: result, Duration: time.Since(startedAt), Error: err})
		}
		timer := time.NewTimer(w.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
