package http

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

const defaultLifecycleBarrierWaitTimeout = 30 * time.Second

type lifecycleBarrier struct {
	mu                sync.Mutex
	cond              *sync.Cond
	active            bool
	epoch             int64
	runtimeGeneration int64
	nextOperationID   uint64
	inFlight          map[uint64]lifecycleBarrierOperation
}

type lifecycleBarrierOperation struct {
	ID        uint64    `json:"id"`
	Method    string    `json:"method"`
	Path      string    `json:"path"`
	StartedAt time.Time `json:"started_at"`
}

type lifecycleBarrierRequest struct {
	Active            bool  `json:"active"`
	Epoch             int64 `json:"epoch,omitempty"`
	RuntimeGeneration int64 `json:"runtime_generation,omitempty"`
	WaitTimeoutMS     int64 `json:"wait_timeout_ms,omitempty"`
}

type lifecycleBarrierResponse struct {
	Active            bool                        `json:"active"`
	Epoch             int64                       `json:"epoch,omitempty"`
	RuntimeGeneration int64                       `json:"runtime_generation,omitempty"`
	InFlight          int                         `json:"in_flight"`
	Operations        []lifecycleBarrierOperation `json:"operations,omitempty"`
}

func newLifecycleBarrier() *lifecycleBarrier {
	b := &lifecycleBarrier{inFlight: make(map[uint64]lifecycleBarrierOperation)}
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
		release, ok := b.enter(r)
		if !ok {
			_ = spec.WriteError(w, http.StatusServiceUnavailable, spec.CodeUnavailable, "sandbox lifecycle barrier is active")
			return
		}
		defer release()
		next.ServeHTTP(w, r)
	})
}

func (b *lifecycleBarrier) enter(r *http.Request) (func(), bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.active {
		return nil, false
	}
	if b.inFlight == nil {
		b.inFlight = make(map[uint64]lifecycleBarrierOperation)
	}
	b.nextOperationID++
	operationID := b.nextOperationID
	operation := lifecycleBarrierOperation{ID: operationID, StartedAt: time.Now().UTC()}
	if r != nil {
		operation.Method = r.Method
		if r.URL != nil {
			operation.Path = r.URL.Path
		}
	}
	b.inFlight[operationID] = operation
	return func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		delete(b.inFlight, operationID)
		if len(b.inFlight) == 0 {
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

	for len(b.inFlight) > 0 {
		if err := r.Context().Err(); err != nil {
			waitErr := b.waitErrorLocked(err)
			b.active = false
			b.epoch = 0
			b.runtimeGeneration = 0
			b.cond.Broadcast()
			return b.snapshotLocked(), waitErr
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			waitErr := b.waitErrorLocked(http.ErrHandlerTimeout)
			b.active = false
			b.epoch = 0
			b.runtimeGeneration = 0
			b.cond.Broadcast()
			return b.snapshotLocked(), waitErr
		}
		b.cond.Wait()
	}
	return b.snapshotLocked(), nil
}

func (b *lifecycleBarrier) snapshotLocked() lifecycleBarrierResponse {
	operations := make([]lifecycleBarrierOperation, 0, len(b.inFlight))
	for _, operation := range b.inFlight {
		operations = append(operations, operation)
	}
	sort.Slice(operations, func(i, j int) bool { return operations[i].ID < operations[j].ID })
	return lifecycleBarrierResponse{
		Active:            b.active,
		Epoch:             b.epoch,
		RuntimeGeneration: b.runtimeGeneration,
		InFlight:          len(operations),
		Operations:        operations,
	}
}

func (b *lifecycleBarrier) waitErrorLocked(cause error) error {
	operations := b.snapshotLocked().Operations
	details := make([]string, 0, len(operations))
	now := time.Now()
	for _, operation := range operations {
		details = append(details, fmt.Sprintf("%s %s active for %s", operation.Method, operation.Path, now.Sub(operation.StartedAt).Round(time.Millisecond)))
	}
	return fmt.Errorf("%w waiting for %d runtime operation(s): %s", cause, len(operations), strings.Join(details, ", "))
}

func (s *Server) lifecycleBarrierHandler(w http.ResponseWriter, r *http.Request) {
	var req lifecycleBarrierRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = spec.WriteError(w, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}
	resp, err := s.barrier.setActive(r, req)
	if err != nil {
		if s.logger != nil {
			s.logger.Warn("Lifecycle barrier wait failed",
				zap.Error(err),
				zap.Int("in_flight", resp.InFlight),
				zap.Any("operations", resp.Operations),
			)
		}
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
