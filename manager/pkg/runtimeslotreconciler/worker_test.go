package runtimeslotreconciler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type workerTestRunner struct {
	mu          sync.Mutex
	calls       int
	inFlight    int
	maxInFlight int
	started     chan<- struct{}
	release     <-chan struct{}
	result      Result
	err         error
}

func (r *workerTestRunner) RunOnce(ctx context.Context) (Result, error) {
	r.mu.Lock()
	r.calls++
	r.inFlight++
	if r.inFlight > r.maxInFlight {
		r.maxInFlight = r.inFlight
	}
	started := r.started
	release := r.release
	result := r.result
	err := r.err
	r.mu.Unlock()
	if started != nil {
		started <- struct{}{}
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			err = ctx.Err()
		}
	}
	r.mu.Lock()
	r.inFlight--
	r.mu.Unlock()
	return result, err
}

func TestWorkerRunsImmediatelyWithoutOverlappingPasses(t *testing.T) {
	started := make(chan struct{}, 2)
	release := make(chan struct{}, 2)
	runner := &workerTestRunner{
		started: started, release: release,
		result: Result{Candidates: 2, Completed: 1, Failed: 1}, err: errors.New("slot failed"),
	}
	worker, err := NewWorker(WorkerConfig{
		Runner: runner, Interval: 100 * time.Millisecond, PassTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	reports := make(chan WorkerReport, 2)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, func(report WorkerReport) { reports <- report }) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first reconcile pass did not start immediately")
	}
	select {
	case <-started:
		t.Fatal("worker overlapped a blocked reconcile pass")
	case <-time.After(30 * time.Millisecond):
	}
	release <- struct{}{}
	select {
	case report := <-reports:
		if report.Result != runner.result || report.Error == nil || report.Duration <= 0 {
			t.Fatalf("worker report = %+v", report)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not report the completed pass")
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second reconcile pass did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error = %v", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.maxInFlight != 1 {
		t.Fatalf("max in-flight passes = %d", runner.maxInFlight)
	}
}

func TestWorkerBoundsConfiguration(t *testing.T) {
	runner := &workerTestRunner{}
	for name, config := range map[string]WorkerConfig{
		"nil runner":     {},
		"short interval": {Runner: runner, Interval: time.Millisecond, PassTimeout: time.Second},
		"long interval":  {Runner: runner, Interval: 2 * time.Hour, PassTimeout: time.Second},
		"short timeout":  {Runner: runner, Interval: time.Second, PassTimeout: time.Millisecond},
		"long timeout":   {Runner: runner, Interval: time.Second, PassTimeout: 2 * time.Hour},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewWorker(config); err == nil {
				t.Fatal("invalid worker config unexpectedly succeeded")
			}
		})
	}
	worker, err := NewWorker(WorkerConfig{Runner: runner})
	if err != nil {
		t.Fatal(err)
	}
	if worker.interval != DefaultWorkerInterval || worker.passTimeout != DefaultWorkerPassTimeout {
		t.Fatalf("worker defaults = %s, %s", worker.interval, worker.passTimeout)
	}
}
