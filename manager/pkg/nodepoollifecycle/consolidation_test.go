package nodepoollifecycle

import (
	"context"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/stretchr/testify/require"
)

func TestConsolidationProtectsAllOtherElasticInstances(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"source": {ProviderInstanceID: "source", PoolKind: "elastic", State: "draining",
			DrainReason: sandboxstore.RuntimeNodeConsolidationReason, ProviderReady: true},
		"other": {ProviderInstanceID: "other", PoolKind: "elastic", State: "active", ProviderReady: true},
	}}
	cloud := &fakeCloud{}
	worker, _ := testWorker(t, store, cloud)
	require.NoError(t, worker.reconcileProtection(context.Background()))
	require.False(t, cloud.protected["source"])
	require.True(t, cloud.protected["other"])
}

func TestConsolidationRollsBackWrongProviderSelection(t *testing.T) {
	store := &fakeStore{nodes: map[string]sandboxstore.RuntimeNodePoolNodeUsage{
		"source": {ProviderInstanceID: "source", PoolKind: "elastic", State: "draining",
			DrainReason: sandboxstore.RuntimeNodeConsolidationReason, ProviderReady: true},
		"other": {ProviderInstanceID: "other", PoolKind: "elastic", State: "active", ProviderReady: true},
	}}
	cloud := &fakeCloud{}
	worker, nomad := testWorker(t, store, cloud)
	completed, rolledBack, err := worker.reconcileScaleIn(context.Background(), Action{
		Token: "wrong", HookID: "in", Transition: TransitionScaleIn, InstanceIDs: []string{"other"},
	})
	require.NoError(t, err)
	require.True(t, completed)
	require.True(t, rolledBack)
	require.Equal(t, LifecycleRollback, cloud.completed["wrong"])
	require.Empty(t, nomad.fenced)
}
