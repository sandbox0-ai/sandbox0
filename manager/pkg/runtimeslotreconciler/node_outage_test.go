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

func (s *outageStore) ListRuntimeSlotsForReconcile(ctx context.Context, limit int) ([]sandboxstore.RuntimeSlot, error) {
	healthy, err := s.Store.ListRuntimeSlotsForReconcile(ctx, limit)
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
