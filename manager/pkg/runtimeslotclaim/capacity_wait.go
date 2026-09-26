package runtimeslotclaim

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
)

// CapacityWaitConfig bounds HTTP waiters and database acquisition work per
// manager. PostgreSQL remains the authority for quota, operation identity and
// resource leases; this queue only shares retry opportunities between teams.
type CapacityWaitConfig struct {
	Timeout           time.Duration
	MaxPending        int
	MaxPendingPerTeam int
	Concurrency       int
	RetryInterval     time.Duration
}

type capacityAttempt func(context.Context) (*sandboxstore.RuntimeSlot, error)
type capacityResult struct {
	slot *sandboxstore.RuntimeSlot
	err  error
}
type capacityRequest struct {
	ctx     context.Context
	team    string
	attempt capacityAttempt
	result  chan capacityResult
	next    time.Time
}

type capacityQueue struct {
	mu               sync.Mutex
	config           CapacityWaitConfig
	teams            []string
	queues           map[string][]*capacityRequest
	counts           map[string]int
	pending, workers int
}

func newCapacityQueue(c CapacityWaitConfig) (*capacityQueue, error) {
	if c.Timeout < 0 || c.Timeout > 2*time.Minute {
		return nil, errors.New("capacity wait timeout must be between zero and two minutes")
	}
	if c.Timeout == 0 {
		return nil, nil
	}
	if c.MaxPending == 0 {
		c.MaxPending = 1024
	}
	if c.MaxPendingPerTeam == 0 {
		c.MaxPendingPerTeam = 256
	}
	if c.Concurrency == 0 {
		c.Concurrency = 8
	}
	if c.RetryInterval == 0 {
		c.RetryInterval = 100 * time.Millisecond
	}
	if c.MaxPending < 1 || c.MaxPending > 4096 || c.MaxPendingPerTeam < 1 || c.MaxPendingPerTeam > c.MaxPending || c.Concurrency < 1 || c.Concurrency > 32 || c.RetryInterval < 10*time.Millisecond || c.RetryInterval > time.Second {
		return nil, errors.New("invalid bounded capacity wait configuration")
	}
	return &capacityQueue{config: c, queues: map[string][]*capacityRequest{}, counts: map[string]int{}}, nil
}

func (q *capacityQueue) waiting() bool {
	if q == nil {
		return false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pending > 0
}

func (q *capacityQueue) acquire(ctx context.Context, team string, attempt capacityAttempt, admitted ...func()) (*sandboxstore.RuntimeSlot, error) {
	r := &capacityRequest{ctx: ctx, team: team, attempt: attempt, result: make(chan capacityResult, 1)}
	q.mu.Lock()
	if q.pending >= q.config.MaxPending || q.counts[team] >= q.config.MaxPendingPerTeam {
		q.mu.Unlock()
		return nil, fmt.Errorf("capacity wait queue is full: %w", sandboxstore.ErrRuntimeSlotUnavailable)
	}
	q.pending++
	q.counts[team]++
	q.pushLocked(r)
	for q.workers < min(q.config.Concurrency, q.pending) {
		q.workers++
		go q.run()
	}
	q.mu.Unlock()
	for _, f := range admitted {
		f()
	}
	select {
	case result := <-r.result:
		return result.slot, result.err
	case <-ctx.Done():
		return nil, errors.Join(sandboxstore.ErrRuntimeSlotUnavailable, ctx.Err())
	}
}

func (q *capacityQueue) pushLocked(r *capacityRequest) {
	if len(q.queues[r.team]) == 0 {
		q.teams = append(q.teams, r.team)
	}
	q.queues[r.team] = append(q.queues[r.team], r)
}

func (q *capacityQueue) finishLocked(r *capacityRequest, result capacityResult) {
	q.pending--
	q.counts[r.team]--
	if q.counts[r.team] == 0 {
		delete(q.counts, r.team)
	}
	r.result <- result
}

// popLocked rotates teams, including when a team's next request is cooling
// down. A burst from one team cannot consume every retry opportunity.
func (q *capacityQueue) popLocked() *capacityRequest {
	for n := len(q.teams); n > 0; n-- {
		team := q.teams[0]
		q.teams = q.teams[1:]
		queue := q.queues[team]
		for len(queue) > 0 && queue[0].ctx.Err() != nil {
			r := queue[0]
			queue = queue[1:]
			q.finishLocked(r, capacityResult{err: errors.Join(sandboxstore.ErrRuntimeSlotUnavailable, r.ctx.Err())})
		}
		var selected *capacityRequest
		if len(queue) > 0 && !time.Now().Before(queue[0].next) {
			selected = queue[0]
			queue = queue[1:]
		}
		if len(queue) > 0 {
			q.queues[team] = queue
			q.teams = append(q.teams, team)
		} else {
			delete(q.queues, team)
		}
		if selected != nil {
			return selected
		}
	}
	return nil
}

func (q *capacityQueue) run() {
	for {
		q.mu.Lock()
		r := q.popLocked()
		if r == nil && q.pending == 0 {
			q.workers--
			q.mu.Unlock()
			return
		}
		q.mu.Unlock()
		if r == nil {
			time.Sleep(q.config.RetryInterval)
			continue
		}
		slot, err := r.attempt(r.ctx)
		if slot == nil && r.ctx.Err() != nil {
			err = errors.Join(sandboxstore.ErrRuntimeSlotUnavailable, r.ctx.Err(), err)
		}
		q.mu.Lock()
		if errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable) && r.ctx.Err() == nil {
			r.next = time.Now().Add(q.config.RetryInterval)
			q.pushLocked(r)
		} else {
			q.finishLocked(r, capacityResult{slot: slot, err: err})
		}
		q.mu.Unlock()
		// Failed acquisitions must not create a tight database retry loop even
		// when thousands of different requests are waiting for the same supply.
		if errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable) {
			time.Sleep(q.config.RetryInterval)
		}
	}
}

func (p *Planner) acquireCapacity(ctx context.Context, team string, request *sandboxstore.AcquireRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	if p.capacityQueue == nil || request.TargetNodeID != "" {
		return p.store.AcquireRuntimeSlot(ctx, request)
	}
	waitCtx, cancel := context.WithTimeout(ctx, p.capacityQueue.config.Timeout)
	defer cancel()
	attempt := func(ctx context.Context) (*sandboxstore.RuntimeSlot, error) {
		return p.store.AcquireRuntimeSlot(ctx, request)
	}
	if !p.capacityQueue.waiting() {
		slot, err := attempt(waitCtx)
		if !errors.Is(err, sandboxstore.ErrRuntimeSlotUnavailable) {
			return slot, err
		}
	}
	return p.capacityQueue.acquire(waitCtx, team, attempt, func() {
		if p.demandRecorder != nil {
			// A canceled request can leave only bounded pressure, never a resource
			// reservation. Acquired operations are excluded by the demand queries.
			_ = p.demandRecorder.RecordRuntimeNodePoolDemand(waitCtx, &sandboxstore.RuntimeNodePoolDemandRequest{
				PoolID: p.demandPoolID, OperationID: request.OperationID, ClusterID: request.ClusterID,
				CPUMillicores: request.Resources.CPUMillicores, MemoryBytes: request.Resources.MemoryBytes,
				Slots: 1, TTL: max(time.Second, min(p.demandTTL, p.capacityQueue.config.Timeout)), CompatibilityDigest: request.CompatibilityDigest,
			})
		}
		if p.capacityWake != nil {
			p.capacityWake()
		}
	})
}
