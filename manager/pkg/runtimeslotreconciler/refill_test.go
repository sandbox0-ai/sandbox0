package runtimeslotreconciler

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func TestReconcilerDoesNotSkipNomadConvergenceAfterPhysicalAbsence(t *testing.T) {
	fixture := newReconcileFixture(t, true)
	lease := attachResourceLease(t, fixture)
	fixture.allocation.present = false
	fixture.allocation.purgeLost = true
	first, err := fixture.reconciler.RunOnce(t.Context())
	require.Error(t, err)
	require.Equal(t, 1, first.Failed)
	require.Zero(t, fixture.store.finalizeCalls, "physical absence does not acknowledge post-stop scheduling")
	require.Equal(t, sandboxstore.RuntimeResourceLeaseActive, fixture.store.slot.ResourceLeaseState)
	require.Zero(t, fixture.store.slot.ResourceLeaseReleasedAt)
	require.Equal(t, lease, fixture.node.requests[0].Resources)
	require.Len(t, fixture.allocation.purges, 1)

	second, err := fixture.reconciler.RunOnce(t.Context())
	require.NoError(t, err)
	require.Equal(t, 1, second.Completed)
	require.Equal(t, sandboxstore.RuntimeSlotStateTerminal, fixture.store.slot.State)
	require.Equal(t, sandboxstore.RuntimeResourceLeaseReleased, fixture.store.slot.ResourceLeaseState)
	require.False(t, fixture.store.slot.ResourceLeaseReleasedAt.IsZero())
	require.Len(t, fixture.allocation.purges, 2)
	require.Equal(t, fixture.allocation.purges[0], fixture.allocation.purges[1])
	require.Greater(t, indexOf(*fixture.order, "purge-allocation"), indexOf(*fixture.order, "cleanup-node"))
}
