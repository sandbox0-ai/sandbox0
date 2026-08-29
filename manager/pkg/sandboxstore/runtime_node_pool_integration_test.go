package sandboxstore

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeNodePoolWarmingFenceAndSnapshotIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	_, err := store.EnsureRuntimeNodePoolState(ctx, "elastic", "nomad")
	require.NoError(t, err)
	reservation, err := store.ReserveRuntimeNode(ctx, &ReserveRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-1", PoolKind: RuntimeNodePoolKindElastic,
		ClusterID: "nomad", NodeName: "s0-i-1", NodeUID: "ecs/us-east-1/i-1",
		PrivateIP: "10.0.1.10", AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26,
	})
	require.NoError(t, err)
	require.Equal(t, "172.27.0.0/26", reservation.AllocationCIDR)
	require.NoError(t, store.ActivateRuntimeNode(ctx, &ActivateRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-1",
		NomadNodeID:         "11111111-1111-1111-1111-111111111111",
		AuthorityCommonName: "ctld-i-1", AgentUID: "ctld/ali-ue1/i-1",
	}))
	actionRequest := &ObserveRuntimeNodeLifecycleActionRequest{
		Token: "ready-recovery-token", PoolID: "elastic", LifecycleHookID: "hook-out",
		ProviderInstanceIDs: []string{"i-1"}, Transition: "scale_out",
	}
	_, err = store.ObserveRuntimeNodeLifecycleAction(ctx, actionRequest)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
			INSERT INTO manager.runtime_node_capacities (
				cluster_id, node_id, node_uid, node_boot_id,
				cpu_millicores, memory_bytes, cpuset_cpus, cpuset_mems,
				heartbeat_expires_at
			) VALUES ('nomad', 'fixed-node', 'fixed-uid', 'fixed-boot',
				14000, 60129542144, '0-13', '0', NOW() + INTERVAL '1 minute');
			INSERT INTO manager.runtime_slots (
				slot_id, cluster_id, allocation_id, allocation_namespace,
				node_id, node_uid, node_boot_id, netns_identity,
				control_endpoint, compatibility_digest, state,
				runtime_ready_digest, network_ready_digest, storage_ready_digest,
				heartbeat_expires_at, fastpath_ready_at
			)
			SELECT 'fixed-slot-' || value, 'nomad', 'fixed-allocation-' || value, 'default',
				'fixed-node', 'fixed-uid', 'fixed-boot', 'netns-' || value,
				'https://10.0.1.9:4646', 'sha256:' || repeat('a', 64), 'fastpath_ready',
				decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex'),
				decode(repeat('03', 32), 'hex'), NOW() + INTERVAL '1 minute', NOW()
			FROM generate_series(1, 8) AS value;
	`)
	require.NoError(t, err)

	snapshot, err := store.GetRuntimeNodePoolSnapshot(ctx, "elastic")
	require.NoError(t, err)
	require.Len(t, snapshot.Nodes, 1)
	require.False(t, snapshot.Nodes[0].ProviderReady)
	require.False(t, snapshot.Nodes[0].CapacityLive)
	require.Equal(t, 8, snapshot.ClusterFixedUsableSlots)

	_, err = pool.Exec(ctx, `
		INSERT INTO manager.runtime_node_capacities (
			cluster_id, node_id, node_uid, node_boot_id,
			cpu_millicores, memory_bytes, cpuset_cpus, cpuset_mems,
			heartbeat_expires_at
		) VALUES ('nomad', '11111111-1111-1111-1111-111111111111',
			'ecs/us-east-1/i-1', 'elastic-boot', 14000, 60129542144,
			'0-13', '0', NOW() + INTERVAL '1 minute');
		INSERT INTO manager.runtime_slots (
			slot_id, cluster_id, allocation_id, allocation_namespace,
			node_id, node_uid, node_boot_id, netns_identity,
			control_endpoint, compatibility_digest, state,
			runtime_ready_digest, network_ready_digest, storage_ready_digest,
			heartbeat_expires_at, fastpath_ready_at
		)
		SELECT 'elastic-slot-' || value, 'nomad', 'elastic-allocation-' || value, 'default',
			'11111111-1111-1111-1111-111111111111', 'ecs/us-east-1/i-1',
			'elastic-boot', 'elastic-netns-' || value, 'https://10.0.1.10:4646',
			'sha256:' || repeat('a', 64), 'fastpath_ready',
			decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex'),
			decode(repeat('03', 32), 'hex'), NOW() + INTERVAL '1 minute', NOW()
		FROM generate_series(1, 8) AS value;
	`)
	require.NoError(t, err)
	require.NoError(t, store.MarkRuntimeNodeProviderReady(ctx, "elastic", "i-1", 8))
	require.NoError(t, store.MarkRuntimeNodeProviderReady(ctx, "elastic", "i-1", 8))
	require.NoError(t, store.CompleteReadyRuntimeNodeScaleOutActions(ctx, "elastic"))
	action, err := store.ObserveRuntimeNodeLifecycleAction(ctx, actionRequest)
	require.NoError(t, err)
	require.Equal(t, "completed", action.State)
	snapshot, err = store.GetRuntimeNodePoolSnapshot(ctx, "elastic")
	require.NoError(t, err)
	require.True(t, snapshot.Nodes[0].ProviderReady)
	require.Equal(t, 8, snapshot.ClusterFixedUsableSlots)

	second, err := store.ReserveRuntimeNode(ctx, &ReserveRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-2", PoolKind: RuntimeNodePoolKindElastic,
		ClusterID: "nomad", NodeName: "s0-i-2", NodeUID: "ecs/us-east-1/i-2",
		PrivateIP: "10.0.1.11", AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26,
	})
	require.NoError(t, err)
	require.NotEmpty(t, second.AllocationCIDR)
	require.NoError(t, store.ActivateRuntimeNode(ctx, &ActivateRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-2",
		NomadNodeID:         "22222222-2222-2222-2222-222222222222",
		AuthorityCommonName: "ctld-i-2", AgentUID: "ctld/ali-ue1/i-2",
	}))
	require.NoError(t, store.BeginRuntimeNodeDrain(ctx, "elastic", "i-2", "timed out"))
	draining, err := store.GetRuntimeNodeDrainStatus(ctx, "elastic", "i-2")
	require.NoError(t, err)
	require.False(t, draining.Instance.ProviderReady)
}

func TestAbandonedScaleOutBlocksLateEnrollmentAndReleasesCIDRIntegration(t *testing.T) {
	ctx := context.Background()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	_, err := store.EnsureRuntimeNodePoolState(ctx, "elastic", "nomad")
	require.NoError(t, err)
	request := &ReserveRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-stale", PoolKind: RuntimeNodePoolKindElastic,
		ClusterID: "nomad", NodeName: "s0-i-stale", NodeUID: "ecs/us-east-1/i-stale",
		PrivateIP: "10.0.1.11", AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26,
	}
	reservation, err := store.ReserveRuntimeNode(ctx, request)
	require.NoError(t, err)
	action, err := store.ObserveRuntimeNodeLifecycleAction(ctx,
		&ObserveRuntimeNodeLifecycleActionRequest{
			Token: "scale-out-token", PoolID: "elastic", LifecycleHookID: "hook-out",
			ProviderInstanceIDs: []string{"i-stale"}, Transition: "scale_out",
		})
	require.NoError(t, err)
	require.Equal(t, "pending", action.State)
	require.NoError(t, store.BeginRuntimeNodeLifecycleActionCleanup(ctx, action.Token))
	require.NoError(t, store.AbandonRuntimeNodeEnrollment(ctx, "elastic", "i-stale"))

	_, err = store.ReserveRuntimeNode(ctx, request)
	require.ErrorContains(t, err, "abandoned by its scale-out lifecycle action")
	replacement := *request
	replacement.ProviderInstanceID = "i-replacement"
	replacement.NodeName = "s0-i-replacement"
	replacement.NodeUID = "ecs/us-east-1/i-replacement"
	replacement.PrivateIP = "10.0.1.12"
	replacementReservation, err := store.ReserveRuntimeNode(ctx, &replacement)
	require.NoError(t, err)
	require.Equal(t, reservation.AllocationCIDR, replacementReservation.AllocationCIDR)
}
