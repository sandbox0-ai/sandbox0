package session

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/stretchr/testify/require"
)

func TestSupervisorReconcilesAllRequestsWithinConcurrencyLimit(t *testing.T) {
	requests := make([]rootfshandoff.StageRequest, 8)
	for index := range requests {
		requests[index].Parent = fmt.Sprintf("parent-%02d", index)
	}
	source := &supervisorSource{requests: requests}
	ensurer := &supervisorEnsurer{delay: 5 * time.Millisecond}
	var publishedMu sync.Mutex
	published := make(map[string]Mount)
	value, err := NewSupervisor(SupervisorConfig{
		Source: source, Sessions: ensurer, Concurrency: 3, Interval: time.Hour,
		Publish: func(request rootfshandoff.StageRequest, mount Mount) error {
			publishedMu.Lock()
			defer publishedMu.Unlock()
			published[request.Parent] = mount
			return nil
		},
	})
	require.NoError(t, err)

	require.NoError(t, value.Reconcile(t.Context()))

	publishedMu.Lock()
	require.Len(t, published, len(requests))
	publishedMu.Unlock()
	require.LessOrEqual(t, ensurer.maxActive, 3)
	require.GreaterOrEqual(t, ensurer.maxActive, 2)
}

func TestSupervisorTriggerFindsNewStageWithoutWaitingForInterval(t *testing.T) {
	source := &supervisorSource{}
	published := make(chan string, 1)
	value, err := NewSupervisor(SupervisorConfig{
		Source: source, Sessions: &supervisorEnsurer{}, Concurrency: 1, Interval: time.Hour,
		Publish: func(request rootfshandoff.StageRequest, _ Mount) error {
			published <- request.Parent
			return nil
		},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- value.Run(ctx) }()

	source.set([]rootfshandoff.StageRequest{{Parent: "parent-new"}})
	value.Trigger()
	require.Equal(t, "parent-new", <-published)
	cancel()
	require.NoError(t, <-done)
}

func TestSupervisorDispatchesNewStageWhileEarlierAttachIsRunning(t *testing.T) {
	source := &supervisorSource{requests: []rootfshandoff.StageRequest{{Parent: "first"}}}
	ensurer := &overlappingSupervisorEnsurer{
		started:      make(chan string, 2),
		releaseFirst: make(chan struct{}),
	}
	value, err := NewSupervisor(SupervisorConfig{
		Source: source, Sessions: ensurer, Concurrency: 2, Interval: time.Hour,
		Publish: func(rootfshandoff.StageRequest, Mount) error { return nil },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- value.Run(ctx) }()

	require.Equal(t, "first", <-ensurer.started)
	source.set([]rootfshandoff.StageRequest{{Parent: "first"}, {Parent: "second"}})
	value.Trigger()
	select {
	case parent := <-ensurer.started:
		require.Equal(t, "second", parent)
	case <-time.After(time.Second):
		t.Fatal("new Stage waited for the earlier attach to finish")
	}

	close(ensurer.releaseFirst)
	cancel()
	require.NoError(t, <-done)
}

func TestSupervisorReportsOneFailureWithoutBlockingOtherParents(t *testing.T) {
	source := &supervisorSource{requests: []rootfshandoff.StageRequest{{Parent: "bad"}, {Parent: "good"}}}
	ensurer := &supervisorEnsurer{failParent: "bad"}
	var published []string
	value, err := NewSupervisor(SupervisorConfig{
		Source: source, Sessions: ensurer, Concurrency: 2, Interval: time.Hour,
		Publish: func(request rootfshandoff.StageRequest, _ Mount) error {
			published = append(published, request.Parent)
			return nil
		},
	})
	require.NoError(t, err)

	reconcileErr := value.Reconcile(t.Context())

	require.Equal(t, []string{"good"}, published)
	require.ErrorContains(t, reconcileErr, `parent "bad"`)
}

func TestSupervisorStartupIsolatesFailedHandoff(t *testing.T) {
	source := &supervisorSource{requests: []rootfshandoff.StageRequest{{Parent: "bad"}, {Parent: "good"}}}
	reported := make(chan error, 1)
	value, err := NewSupervisor(SupervisorConfig{
		Source: source, Sessions: &supervisorEnsurer{failParent: "bad"}, Concurrency: 2, Interval: time.Hour,
		Publish: func(rootfshandoff.StageRequest, Mount) error { return nil },
		OnError: func(err error) { reported <- err },
	})
	require.NoError(t, err)

	require.NoError(t, value.ReconcileForStartup(t.Context()))
	require.ErrorContains(t, <-reported, `parent "bad"`)
}

func TestSupervisorStartupRejectsJournalScanFailure(t *testing.T) {
	sourceErr := fmt.Errorf("journal unavailable")
	value, err := NewSupervisor(SupervisorConfig{
		Source: &supervisorSource{err: sourceErr}, Sessions: &supervisorEnsurer{}, Concurrency: 1, Interval: time.Hour,
		Publish: func(rootfshandoff.StageRequest, Mount) error { return nil },
	})
	require.NoError(t, err)

	err = value.ReconcileForStartup(t.Context())
	require.ErrorIs(t, err, sourceErr)
}

func TestSupervisorCancellationWaitsForSubmittedWorker(t *testing.T) {
	source := &supervisorSource{requests: []rootfshandoff.StageRequest{{Parent: "first"}, {Parent: "second"}}}
	ensurer := &cancelAwareEnsurer{started: make(chan struct{}), stopped: make(chan struct{})}
	value, err := NewSupervisor(SupervisorConfig{
		Source: source, Sessions: ensurer, Concurrency: 1, Interval: time.Hour,
		Publish: func(rootfshandoff.StageRequest, Mount) error { return nil },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- value.Reconcile(ctx) }()
	<-ensurer.started
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	select {
	case <-ensurer.stopped:
	default:
		t.Fatal("reconcile returned before the submitted worker stopped")
	}
}

type supervisorSource struct {
	mu       sync.Mutex
	requests []rootfshandoff.StageRequest
	err      error
}

func (s *supervisorSource) AttachableRequests(context.Context) ([]rootfshandoff.StageRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]rootfshandoff.StageRequest(nil), s.requests...), s.err
}

func (s *supervisorSource) set(requests []rootfshandoff.StageRequest) {
	s.mu.Lock()
	s.requests = append([]rootfshandoff.StageRequest(nil), requests...)
	s.mu.Unlock()
}

type supervisorEnsurer struct {
	mu         sync.Mutex
	active     int
	maxActive  int
	delay      time.Duration
	failParent string
}

type cancelAwareEnsurer struct {
	started chan struct{}
	stopped chan struct{}
}

type overlappingSupervisorEnsurer struct {
	started      chan string
	releaseFirst chan struct{}
}

func (e *overlappingSupervisorEnsurer) Ensure(ctx context.Context, request rootfshandoff.StageRequest) (Mount, error) {
	e.started <- request.Parent
	if request.Parent == "first" {
		select {
		case <-e.releaseFirst:
		case <-ctx.Done():
			return Mount{}, ctx.Err()
		}
	}
	return Mount{Source: "/ready/" + request.Parent, Type: "bind"}, nil
}

func (e *cancelAwareEnsurer) Ensure(ctx context.Context, _ rootfshandoff.StageRequest) (Mount, error) {
	close(e.started)
	<-ctx.Done()
	close(e.stopped)
	return Mount{}, ctx.Err()
}

func (e *supervisorEnsurer) Ensure(ctx context.Context, request rootfshandoff.StageRequest) (Mount, error) {
	e.mu.Lock()
	e.active++
	e.maxActive = max(e.maxActive, e.active)
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		e.active--
		e.mu.Unlock()
	}()
	if e.delay > 0 {
		select {
		case <-time.After(e.delay):
		case <-ctx.Done():
			return Mount{}, ctx.Err()
		}
	}
	if request.Parent == e.failParent {
		return Mount{}, fmt.Errorf("attach failed")
	}
	return Mount{Source: "/ready/" + request.Parent, Type: "bind"}, nil
}
