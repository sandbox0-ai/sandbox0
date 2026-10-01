package nomadruntime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

var ErrLiveUpdateTransferred = errors.New("live node runtime ownership transferred")

// LiveRuntimeHandoff contains short-lived writer tokens. Only transmit it to an
// authenticated root peer. Never persist or log it; Sessions is tokenless.
type LiveRuntimeHandoff struct {
	Sessions rootfssession.LiveHandoff    `json:"sessions"`
	Requests []rootfshandoff.StageRequest `json:"requests"`
	Files    []*os.File                   `json:"-"`
	Guard    *LiveWriterGuard             `json:"-"`
}

type PreparedLiveUpdate struct {
	Handoff  LiveRuntimeHandoff
	sessions *rootfssession.PreparedLiveHandoff
	service  *Service
	mu       sync.Mutex
	finished bool
}

func (s *Service) ImportLiveUpdate(handoff *LiveRuntimeHandoff) error {
	s.liveMu.Lock()
	defer s.liveMu.Unlock()
	if s.liveCancel != nil || s.liveImport != nil {
		return fmt.Errorf("runtime already started or imported")
	}
	s.liveImport = handoff
	s.liveGuard = handoff.Guard
	return nil
}

func (s *Service) Run(ctx context.Context) error {
	defer s.liveGuard.Close()
	for {
		err := s.runOnce(ctx)
		if !s.keepRuntime.Load() {
			return err
		}
		s.liveMu.Lock()
		decision, runtime := s.liveDecision, s.liveRuntime
		s.liveMu.Unlock()
		select {
		case committed := <-decision:
			if committed {
				return ErrLiveUpdateTransferred
			}
			s.keepRuntime.Store(false)
			if ctx.Err() != nil {
				return runtime.Close()
			}
		case <-ctx.Done():
			s.keepRuntime.Store(false)
			return runtime.Close()
		}
	}
}

func (s *Service) PrepareLiveUpdate(ctx context.Context) (*PreparedLiveUpdate, error) {
	s.liveMu.Lock()
	if !s.Ready() || s.liveCancel == nil || s.keepRuntime.Load() {
		s.liveMu.Unlock()
		return nil, fmt.Errorf("runtime is not ready for live update")
	}
	s.keepRuntime.Store(true)
	s.liveDecision = make(chan bool, 1)
	cancel, stopped := s.liveCancel, s.liveStopped
	s.liveMu.Unlock()
	s.ready.Store(false)
	cancel() // Joins RPCs, node channel, reconciliation and journal ownership.
	select {
	case <-stopped:
	case <-ctx.Done():
		s.liveDecision <- false
		return nil, ctx.Err()
	}
	s.liveMu.Lock()
	runtime := s.liveRuntime
	s.liveMu.Unlock()
	prepared, err := runtime.sessions.PrepareLiveHandoff(ctx)
	if err != nil {
		s.liveDecision <- false
		return nil, err
	}
	result := &PreparedLiveUpdate{service: s, sessions: prepared, Handoff: LiveRuntimeHandoff{Sessions: prepared.Manifest, Files: prepared.Files}}
	requests, err := runtime.liveWriterRequests(ctx, prepared.Manifest, nil)
	if err != nil {
		result.Abort()
		return nil, err
	}
	result.Handoff.Requests = requests
	return result, nil
}

func (p *PreparedLiveUpdate) Abort() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return
	}
	p.finished = true
	p.sessions.Abort()
	p.service.liveDecision <- false
}

func (p *PreparedLiveUpdate) ValidateCommit(ctx context.Context) error {
	_, err := p.service.liveRuntime.liveWriterRequests(ctx, p.Handoff.Sessions, nil)
	return err
}

func (p *PreparedLiveUpdate) Commit() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return fmt.Errorf("live update decision was already made")
	}
	p.finished = true
	if err := p.sessions.Commit(); err != nil {
		return err
	}
	p.service.liveRuntime.stopAllRenewals()
	p.service.liveDecision <- true
	return nil
}

func (r *rootfsRuntime) liveWriterRequests(ctx context.Context, sessions rootfssession.LiveHandoff, supplied []rootfshandoff.StageRequest) ([]rootfshandoff.StageRequest, error) {
	requests := make(map[string]rootfshandoff.StageRequest)
	if supplied != nil {
		for _, request := range supplied {
			if _, exists := requests[request.Parent]; exists {
				return nil, fmt.Errorf("duplicate writer in live update")
			}
			requests[request.Parent] = request
		}
	} else {
		r.renewalMu.Lock()
		for parent, renewal := range r.renewals {
			requests[parent] = renewal.request
		}
		r.renewalMu.Unlock()
	}
	result := make([]rootfshandoff.StageRequest, 0, len(sessions.Sessions))
	for _, session := range sessions.Sessions {
		request, ok := requests[session.Parent]
		if !ok || request.Validate() != nil {
			return nil, fmt.Errorf("live RootFS session has no writer renewal")
		}
		current, err := r.sessions.RecoverySession(session.Parent)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(current.Stage, request.WithoutWriterGrantToken()) {
			return nil, fmt.Errorf("live writer differs from durable session")
		}
		result = append(result, request)
	}
	if supplied != nil && len(result) != len(supplied) {
		return nil, fmt.Errorf("live update includes extra writers")
	}
	if _, err := proveLiveWriters(ctx, r.authority, result); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Service) rebindWriterRenewals(ctx context.Context, runtime *rootfsRuntime, lost func(rootfshandoff.StageRequest, error)) error {
	s.liveMu.Lock()
	imported := s.liveImport
	s.liveImport = nil
	s.liveMu.Unlock()
	var requests []rootfshandoff.StageRequest
	if imported != nil {
		if err := imported.Guard.Err(); err != nil {
			return err
		}
		var err error
		requests, err = runtime.liveWriterRequests(ctx, imported.Sessions, imported.Requests)
		if err != nil {
			return err
		}
	} else {
		runtime.renewalMu.Lock()
		for _, renewal := range runtime.renewals {
			requests = append(requests, renewal.request)
		}
		runtime.renewalMu.Unlock()
	}
	proofs, err := proveLiveWriters(ctx, runtime.authority, requests)
	if err != nil {
		return err
	}
	for _, proof := range proofs {
		request := proof.request
		runtime.startRenewal(request, proof.observation, proof.started, func(err error) { lost(request, err) })
	}
	if imported != nil {
		imported.Guard.Close()
	}
	return nil
}
