package session

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
)

// RequestSource exposes tokenless durable Stage bindings from the rootfs runtime
// journal after regional writer lease recovery has completed.
type RequestSource interface {
	AttachableRequests(context.Context) ([]rootfshandoff.StageRequest, error)
}

// SessionEnsurer creates one physical D session for an exact Stage binding.
type SessionEnsurer interface {
	Ensure(context.Context, rootfshandoff.StageRequest) (Mount, error)
}

type PublishReadyFunc func(rootfshandoff.StageRequest, Mount) error

type SupervisorConfig struct {
	Source      RequestSource
	Sessions    SessionEnsurer
	Publish     PublishReadyFunc
	Interval    time.Duration
	Concurrency int
	OnError     func(error)
}

// Supervisor coalesces Stage notifications and periodically reconciles the
// rootfs journal. It owns no lifecycle truth: Stage remains authoritative
// in the rootfs runtime, while Manager owns the physical device and mounts.
type Supervisor struct {
	config  SupervisorConfig
	trigger chan struct{}
}

type reconcileResult struct {
	parent string
	err    error
}

func NewSupervisor(config SupervisorConfig) (*Supervisor, error) {
	if config.Source == nil || config.Sessions == nil || config.Publish == nil {
		return nil, fmt.Errorf("request source, session manager, and ready publisher are required")
	}
	if config.Interval <= 0 {
		config.Interval = time.Second
	}
	if config.Concurrency <= 0 {
		return nil, fmt.Errorf("attach concurrency must be positive")
	}
	return &Supervisor{config: config, trigger: make(chan struct{}, 1)}, nil
}

// Trigger requests a new journal scan without blocking the Stage response.
func (s *Supervisor) Trigger() {
	if s == nil {
		return
	}
	select {
	case s.trigger <- struct{}{}:
	default:
	}
}

// Run continuously dispatches newly staged parents into the bounded worker
// pool. A Stage that arrives while another attach is running must not wait for
// the entire earlier scan to finish before it can use an idle transition slot.
// Per-parent failures are reported and retried; they do not terminate other
// independent handoffs or make a failed writer available for reuse.
func (s *Supervisor) Run(ctx context.Context) error {
	if s == nil {
		return fmt.Errorf("physical session supervisor is nil: %w", errdefs.ErrUnavailable)
	}
	jobs := make(chan rootfshandoff.StageRequest)
	results := make(chan reconcileResult, s.config.Concurrency)
	var workers sync.WaitGroup
	for range s.config.Concurrency {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for request := range jobs {
				mount, err := s.config.Sessions.Ensure(ctx, request)
				if err == nil {
					err = s.config.Publish(request, mount)
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					err = fmt.Errorf("attach RootFS parent %q: %w", request.Parent, err)
				} else {
					err = nil
				}
				results <- reconcileResult{parent: request.Parent, err: err}
			}
		}()
	}

	pending := make([]rootfshandoff.StageRequest, 0)
	queued := make(map[string]bool)
	inflight := make(map[string]bool)
	scan := func() {
		requests, err := s.config.Source.AttachableRequests(ctx)
		if err != nil {
			s.report(fmt.Errorf("list attachable RootFS sessions: %w", err))
			return
		}
		sort.Slice(requests, func(i, j int) bool { return requests[i].Parent < requests[j].Parent })
		for _, request := range requests {
			if queued[request.Parent] || inflight[request.Parent] {
				continue
			}
			queued[request.Parent] = true
			pending = append(pending, request)
		}
	}
	scan()
	ticker := time.NewTicker(s.config.Interval)
	defer ticker.Stop()
	for {
		var dispatch chan rootfshandoff.StageRequest
		var next rootfshandoff.StageRequest
		if len(pending) > 0 {
			dispatch = jobs
			next = pending[0]
		}
		select {
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil
		case <-s.trigger:
			scan()
		case <-ticker.C:
			scan()
		case dispatch <- next:
			pending = pending[1:]
			delete(queued, next.Parent)
			inflight[next.Parent] = true
		case result := <-results:
			delete(inflight, result.parent)
			s.report(result.err)
		}
	}
}

// ReconcileForStartup requires the authoritative journal scan to succeed but
// isolates failures of individual handoffs. Each failed handoff retains its
// durable writer and session fences and is reported for later retry; it must
// not prevent unrelated one-shot slots on the node from attaching.
func (s *Supervisor) ReconcileForStartup(ctx context.Context) error {
	globalErr, handoffErr := s.reconcile(ctx)
	s.report(handoffErr)
	return globalErr
}

// Reconcile attaches every currently eligible binding once and waits for all
// bounded workers. Startup calls this before exposing the handoff control API.
func (s *Supervisor) Reconcile(ctx context.Context) error {
	globalErr, handoffErr := s.reconcile(ctx)
	return errors.Join(globalErr, handoffErr)
}

func (s *Supervisor) reconcile(ctx context.Context) (error, error) {
	requests, err := s.config.Source.AttachableRequests(ctx)
	if err != nil {
		return fmt.Errorf("list attachable RootFS sessions: %w", err), nil
	}
	sort.Slice(requests, func(i, j int) bool { return requests[i].Parent < requests[j].Parent })
	jobs := make(chan rootfshandoff.StageRequest)
	results := make(chan error, len(requests))
	workers := min(s.config.Concurrency, len(requests))
	for range workers {
		go func() {
			for request := range jobs {
				if ctx.Err() != nil {
					results <- nil
					continue
				}
				mount, err := s.config.Sessions.Ensure(ctx, request)
				if err == nil {
					err = s.config.Publish(request, mount)
				}
				if err != nil && !errors.Is(err, context.Canceled) {
					results <- fmt.Errorf("attach RootFS parent %q: %w", request.Parent, err)
				} else {
					results <- nil
				}
			}
		}()
	}
	submitted := 0
submit:
	for _, request := range requests {
		select {
		case jobs <- request:
			submitted++
		case <-ctx.Done():
			break submit
		}
	}
	close(jobs)
	var handoffErr error
	for range submitted {
		handoffErr = errors.Join(handoffErr, <-results)
	}
	return ctx.Err(), handoffErr
}

func (s *Supervisor) report(err error) {
	if err != nil && s.config.OnError != nil {
		s.config.OnError(err)
	}
}
