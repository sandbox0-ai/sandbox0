package http

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
)

const defaultLifecycleBarrierWaitTimeout = 30 * time.Second

type lifecycleBarrier struct {
	mu                sync.Mutex
	cond              *sync.Cond
	active            bool
	epoch             int64
	runtimeGeneration int64
	inFlight          int
}

type lifecycleBarrierRequest struct {
	Active            bool  `json:"active"`
	Epoch             int64 `json:"epoch,omitempty"`
	RuntimeGeneration int64 `json:"runtime_generation,omitempty"`
	WaitTimeoutMS     int64 `json:"wait_timeout_ms,omitempty"`
}

type lifecycleBarrierResponse struct {
	Active            bool  `json:"active"`
	Epoch             int64 `json:"epoch,omitempty"`
	RuntimeGeneration int64 `json:"runtime_generation,omitempty"`
	InFlight          int   `json:"in_flight"`
}

func newLifecycleBarrier() *lifecycleBarrier {
	b := &lifecycleBarrier{}
	b.cond = sync.NewCond(&b.mu)
	return b
}

func (b *lifecycleBarrier) middleware(next http.Handler) http.Handler {
	if b == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !procdRequestMutatesRuntime(r) {
			next.ServeHTTP(w, r)
			return
		}
		release, ok := b.enter()
		if !ok {
			_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox lifecycle barrier is active")
			return
		}
		defer release()
		next.ServeHTTP(w, r)
	})
}

func (b *lifecycleBarrier) enter() (func(), bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active {
		return nil, false
	}
	b.inFlight++
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.inFlight > 0 {
			b.inFlight--
		}
		if b.inFlight == 0 {
			b.cond.Broadcast()
		}
	}, true
}

func (b *lifecycleBarrier) setActive(r *http.Request, req lifecycleBarrierRequest) (lifecycleBarrierResponse, error) {
	timeout := time.Duration(req.WaitTimeoutMS) * time.Millisecond
	if timeout <= 0 {
		timeout = defaultLifecycleBarrierWaitTimeout
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if !req.Active {
		b.active = false
		b.epoch = 0
		b.runtimeGeneration = 0
		b.cond.Broadcast()
		return b.snapshotLocked(), nil
	}

	b.active = true
	b.epoch = req.Epoch
	b.runtimeGeneration = req.RuntimeGeneration

	deadline := time.Now().Add(timeout)
	var timer *time.Timer
	if timeout > 0 {
		timer = time.AfterFunc(timeout, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			b.cond.Broadcast()
		})
		defer timer.Stop()
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-r.Context().Done():
			b.mu.Lock()
			b.cond.Broadcast()
			b.mu.Unlock()
		case <-done:
		}
	}()
	defer close(done)

	for b.inFlight > 0 {
		if err := r.Context().Err(); err != nil {
			b.active = false
			b.epoch = 0
			b.runtimeGeneration = 0
			b.cond.Broadcast()
			return b.snapshotLocked(), err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			b.active = false
			b.epoch = 0
			b.runtimeGeneration = 0
			b.cond.Broadcast()
			return b.snapshotLocked(), http.ErrHandlerTimeout
		}
		b.cond.Wait()
	}
	return b.snapshotLocked(), nil
}

func (b *lifecycleBarrier) snapshotLocked() lifecycleBarrierResponse {
	return lifecycleBarrierResponse{
		Active:            b.active,
		Epoch:             b.epoch,
		RuntimeGeneration: b.runtimeGeneration,
		InFlight:          b.inFlight,
	}
}

func (s *Server) lifecycleBarrierHandler(w http.ResponseWriter, r *http.Request) {
	var req lifecycleBarrierRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	resp, err := s.barrier.setActive(r, req)
	if err != nil {
		_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, err.Error())
		return
	}
	_ = spec.WriteSuccess(w, http.StatusOK, resp)
}

func procdRequestMutatesRuntime(r *http.Request) bool {
	if r == nil || r.URL == nil {
		return false
	}
	path := strings.TrimRight(r.URL.Path, "/")
	switch path {
	case "/api/v1/lifecycle/barrier", "/api/v1/sandbox/pause", "/api/v1/sandbox/resume":
		return false
	}
	if !strings.HasPrefix(path, "/api/v1/") {
		return false
	}
	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	case http.MethodGet:
		return strings.HasSuffix(path, "/ws") || path == "/api/v1/functions/ws"
	default:
		return true
	}
}
