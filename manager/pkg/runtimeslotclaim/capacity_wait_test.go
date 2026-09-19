package runtimeslotclaim

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func testCapacityQueue(t *testing.T, concurrency int) *capacityQueue {
	t.Helper()
	q, err := newCapacityQueue(CapacityWaitConfig{Timeout: time.Second, MaxPending: 4, MaxPendingPerTeam: 2, Concurrency: concurrency, RetryInterval: 10 * time.Millisecond})
	require.NoError(t, err)
	return q
}

func TestCapacityQueueRetriesOnlyCapacityErrors(t *testing.T) {
	q := testCapacityQueue(t, 1)
	var calls atomic.Int32
	expected := &sandboxstore.RuntimeSlot{ID: "same-operation-slot"}
	got, err := q.acquire(t.Context(), "team", func(context.Context) (*sandboxstore.RuntimeSlot, error) {
		if calls.Add(1) < 3 {
			return nil, sandboxstore.ErrRuntimeSlotUnavailable
		}
		return expected, nil
	})
	require.NoError(t, err)
	require.Same(t, expected, got)
	require.EqualValues(t, 3, calls.Load())
	denied := errors.New("quota or authority rejection")
	calls.Store(0)
	_, err = q.acquire(t.Context(), "team", func(context.Context) (*sandboxstore.RuntimeSlot, error) { calls.Add(1); return nil, denied })
	require.ErrorIs(t, err, denied)
	require.EqualValues(t, 1, calls.Load())
}

func TestCapacityQueueBoundsWaitAndStopsCanceledWork(t *testing.T) {
	q := testCapacityQueue(t, 2)
	var calls atomic.Int32
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	_, err := q.acquire(ctx, "team", func(ctx context.Context) (*sandboxstore.RuntimeSlot, error) {
		calls.Add(1)
		return nil, sandboxstore.ErrRuntimeSlotUnavailable
	})
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, err, sandboxstore.ErrRuntimeSlotUnavailable)
	require.Eventually(t, func() bool { return !q.waiting() }, time.Second, time.Millisecond)
	before := calls.Load()
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, before, calls.Load())
}

func TestCapacityQueueBoundsTeamsAndConcurrentAttempts(t *testing.T) {
	q := testCapacityQueue(t, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	entered := make(chan struct{}, 4)
	attempt := func(ctx context.Context) (*sandboxstore.RuntimeSlot, error) {
		entered <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	done := make(chan error, 4)
	for _, team := range []string{"a", "a", "b", "b"} {
		go func() { _, err := q.acquire(ctx, team, attempt); done <- err }()
	}
	<-entered
	require.Eventually(t, func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.pending == 4 }, time.Second, time.Millisecond)
	_, err := q.acquire(ctx, "c", attempt)
	require.ErrorIs(t, err, sandboxstore.ErrRuntimeSlotUnavailable)
	require.Empty(t, entered, "only one acquisition may be in flight")
	cancel()
	for range 4 {
		require.ErrorIs(t, <-done, context.Canceled)
	}
	require.Eventually(t, func() bool { return !q.waiting() }, time.Second, time.Millisecond)
	// Team bounds apply even when total capacity remains.
	ctx2, cancel2 := context.WithCancel(t.Context())
	defer cancel2()
	for range 2 {
		go func() { _, err := q.acquire(ctx2, "a", attempt); done <- err }()
	}
	<-entered
	require.Eventually(t, func() bool { q.mu.Lock(); defer q.mu.Unlock(); return q.pending == 2 }, time.Second, time.Millisecond)
	_, err = q.acquire(ctx2, "a", attempt)
	require.ErrorIs(t, err, sandboxstore.ErrRuntimeSlotUnavailable)
	cancel2()
	for range 2 {
		<-done
	}
}

func TestCapacityQueueRotatesTeamsAndSkipsCanceledWaiters(t *testing.T) {
	q := testCapacityQueue(t, 1)
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	requests := []*capacityRequest{
		{ctx: canceled, team: "a", result: make(chan capacityResult, 1)},
		{ctx: t.Context(), team: "a"}, {ctx: t.Context(), team: "a"}, {ctx: t.Context(), team: "b"},
	}
	for _, r := range requests {
		q.pending++
		q.counts[r.team]++
		q.pushLocked(r)
	}
	require.Same(t, requests[1], q.popLocked())
	require.Same(t, requests[3], q.popLocked(), "a second team gets the next opportunity despite a burst")
	require.Same(t, requests[2], q.popLocked())
	require.ErrorIs(t, (<-requests[0].result).err, context.Canceled)
}

type delayedCapacityStore struct {
	Store
	requests []sandboxstore.AcquireRuntimeSlotRequest
}

func (s *delayedCapacityStore) AcquireRuntimeSlot(ctx context.Context, r *sandboxstore.AcquireRuntimeSlotRequest) (*sandboxstore.RuntimeSlot, error) {
	s.requests = append(s.requests, *r)
	if len(s.requests) < 4 {
		return nil, sandboxstore.ErrRuntimeSlotUnavailable
	}
	return s.Store.AcquireRuntimeSlot(ctx, r)
}
func TestPlannerWaitsForRefillWithSameOperationAndResourceRequest(t *testing.T) {
	f := newPlannerFixture(t)
	store := &delayedCapacityStore{Store: f.store}
	f.planner.store = store
	f.planner.capacityQueue = testCapacityQueue(t, 1)
	var wakes atomic.Int32
	f.planner.capacityWake = func() { wakes.Add(1) }
	result, err := f.planner.Claim(t.Context(), f.request)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Len(t, store.requests, 4)
	for _, request := range store.requests {
		require.Equal(t, store.requests[0], request)
	}
	require.EqualValues(t, 1, wakes.Load())
}
