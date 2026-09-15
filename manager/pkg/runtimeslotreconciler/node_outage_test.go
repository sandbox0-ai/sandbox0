package runtimeslotreconciler

import (
	"context"
	"errors"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

type outageStore struct {
	Store
	stale []sandboxstore.RuntimeSlot
}

func (s *outageStore) ListRuntimeSlotsForReconcileAfter(ctx context.Context, limit int, after *sandboxstore.RuntimeSlot) ([]sandboxstore.RuntimeSlot, error) {
	healthy, err := s.Store.ListRuntimeSlotsForReconcileAfter(ctx, limit, after)
	return append(append([]sandboxstore.RuntimeSlot{}, s.stale...), healthy...), err
}

func (s *outageStore) GetRuntimeSlot(ctx context.Context, id string) (*sandboxstore.RuntimeSlot, error) {
	for _, slot := range s.stale {
		if slot.ID == id {
			return cloneSlot(&slot), nil
		}
	}
	return s.Store.GetRuntimeSlot(ctx, id)
}

type outageAllocation struct {
	AllocationController
	calls int
}

func (a *outageAllocation) Observe(ctx context.Context, target AllocationTarget) (AllocationObservation, error) {
	if target.NodeID == "unreachable-node" {
		a.calls++
		return AllocationObservation{}, ErrAllocationNodeUnavailable
	}
	return a.AllocationController.Observe(ctx, target)
}

func TestReconcilerDefersRepeatedUnreachableNodeWithoutBlockingHealthyCleanup(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	attachResourceLease(t, fixture)
	stale := newReconcileFixture(t, false).store.slot
	stale.NodeID = "unreachable-node"
	store := &outageStore{Store: fixture.store}
	for _, id := range []string{"stale-1", "stale-2", "stale-3"} {
		slot := *cloneSlot(stale)
		slot.ID = id
		store.stale = append(store.stale, slot)
	}
	allocation := &outageAllocation{AllocationController: fixture.allocation}
	fixture.reconciler.store, fixture.reconciler.allocation = store, allocation

	result, err := fixture.reconciler.RunOnce(t.Context())
	require.ErrorIs(t, err, ErrAllocationNodeUnavailable)
	require.Equal(t, Result{Candidates: 4, Completed: 1, Skipped: 2, Failed: 1}, result)
	require.Equal(t, 1, allocation.calls)
	require.Equal(t, sandboxstore.RuntimeResourceLeaseReleased, fixture.store.slot.ResourceLeaseState)
	require.Len(t, fixture.node.requests, 1, "only the healthy claimed slot may be physically cleaned")
	for _, slot := range store.stale {
		require.Equal(t, sandboxstore.RuntimeSlotStateFastpathReady, slot.State)
		require.Empty(t, slot.TerminalProofDigest)
	}

	// An outage is retried on the next pass and cannot suppress a new boot.
	store.stale[2].NodeBootID = "successor-boot"
	result, err = fixture.reconciler.RunOnce(t.Context())
	require.ErrorIs(t, err, ErrAllocationNodeUnavailable)
	require.Equal(t, Result{Candidates: 3, Skipped: 1, Failed: 2}, result)
	require.Equal(t, 3, allocation.calls)
}

func TestReconcilerStopsDispatchAfterPassCancellation(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result, err := fixture.reconciler.RunOnce(ctx)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, Result{Candidates: 1, Skipped: 1}, result)
	require.Empty(t, fixture.allocation.observations)
	require.Empty(t, fixture.node.requests)
}

type pagedOutageStore struct {
	*outageStore
}

func (s *pagedOutageStore) ListRuntimeSlotsForReconcileAfter(ctx context.Context, limit int, after *sandboxstore.RuntimeSlot) ([]sandboxstore.RuntimeSlot, error) {
	rows, err := s.outageStore.ListRuntimeSlotsForReconcileAfter(ctx, limit, nil)
	if err != nil {
		return nil, err
	}
	start := 0
	if after != nil {
		start = len(rows)
		for index, row := range rows {
			if row.ID == after.ID {
				start = index + 1
				break
			}
		}
	}
	return rows[start:min(start+limit, len(rows))], nil
}

func TestReconcilerTraversesUnresolvedFullBatchAndWrapsForRetry(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	attachResourceLease(t, fixture)
	stale := newReconcileFixture(t, false).store.slot
	stale.NodeID = "unreachable-node"
	store := &pagedOutageStore{&outageStore{Store: fixture.store}}
	for _, id := range []string{"stale-1", "stale-2", "stale-3"} {
		slot := *cloneSlot(stale)
		slot.ID = id
		store.stale = append(store.stale, slot)
	}
	allocation := &outageAllocation{AllocationController: fixture.allocation}
	fixture.reconciler.store, fixture.reconciler.allocation, fixture.reconciler.limit = store, allocation, 2
	first, err := fixture.reconciler.RunOnce(t.Context())
	require.ErrorIs(t, err, ErrAllocationNodeUnavailable)
	require.Equal(t, Result{Candidates: 2, Failed: 1, Skipped: 1}, first)
	second, err := fixture.reconciler.RunOnce(t.Context())
	require.ErrorIs(t, err, ErrAllocationNodeUnavailable)
	require.Equal(t, Result{Candidates: 2, Failed: 1, Completed: 1}, second)
	require.Equal(t, sandboxstore.RuntimeResourceLeaseReleased, fixture.store.slot.ResourceLeaseState)
	require.Len(t, fixture.node.requests, 1)

	// The completed final slot no longer appears; the next scan reaches the end.
	end, err := fixture.reconciler.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{}, end)
	require.Nil(t, fixture.reconciler.after)
	retry, err := fixture.reconciler.RunOnce(t.Context())
	require.ErrorIs(t, err, ErrAllocationNodeUnavailable)
	require.Equal(t, first, retry)
	require.Equal(t, 3, allocation.calls)
}

type cancelingOutageAllocation struct {
	*outageAllocation
	cancel context.CancelFunc
}

func (a *cancelingOutageAllocation) Observe(ctx context.Context, target AllocationTarget) (AllocationObservation, error) {
	observation, err := a.outageAllocation.Observe(ctx, target)
	if target.NodeID == "unreachable-node" {
		a.cancel()
	}
	return observation, err
}

func TestReconcilerCancellationRetainsUnprocessedPageSuffix(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	stale := *cloneSlot(newReconcileFixture(t, false).store.slot)
	stale.ID, stale.NodeID = "stale-1", "unreachable-node"
	store := &pagedOutageStore{&outageStore{Store: fixture.store, stale: []sandboxstore.RuntimeSlot{stale}}}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	allocation := &cancelingOutageAllocation{&outageAllocation{AllocationController: fixture.allocation}, cancel}
	fixture.reconciler.store, fixture.reconciler.allocation, fixture.reconciler.limit = store, allocation, 2
	first, err := fixture.reconciler.RunOnce(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, Result{Candidates: 2, Failed: 1, Skipped: 1}, first)
	require.Equal(t, stale.ID, fixture.reconciler.after.ID)
	require.Empty(t, fixture.node.requests)
	second, err := fixture.reconciler.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, Result{Candidates: 1, Completed: 1}, second)
	require.Nil(t, fixture.reconciler.after)
}
