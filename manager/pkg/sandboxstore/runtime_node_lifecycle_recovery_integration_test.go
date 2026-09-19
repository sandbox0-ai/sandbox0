package sandboxstore

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDurableLifecycleRecoveryFencingAndProofIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	_, err := store.EnsureRuntimeNodePoolState(ctx, "elastic", "nomad")
	require.NoError(t, err)
	_, err = store.ReserveRuntimeNode(ctx, &ReserveRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-absent", PoolKind: RuntimeNodePoolKindElastic,
		ClusterID: "nomad", NodeName: "s0-i-absent", NodeUID: "ecs/us-east-1/i-absent",
		PrivateIP: "10.0.1.10", AllocationSupernet: "172.27.0.0/17", AllocationPrefix: 26,
	})
	require.NoError(t, err)
	require.NoError(t, store.ActivateRuntimeNode(ctx, &ActivateRuntimeNodeRequest{
		PoolID: "elastic", ProviderInstanceID: "i-absent",
		NomadNodeID:         "21111111-1111-1111-1111-111111111111",
		AuthorityCommonName: "ctld-i-absent", AgentUID: "ctld/ali-ue1/i-absent",
	}))
	_, err = store.ObserveRuntimeNodeLifecycleAction(ctx, &ObserveRuntimeNodeLifecycleActionRequest{
		Token: "absent-token", PoolID: "elastic", LifecycleHookID: "hook-out",
		ProviderInstanceIDs: []string{"i-absent"}, Transition: "scale_out",
		RecoveryDeadline: time.Now().Add(-time.Minute),
	})
	require.NoError(t, err)

	claimed, err := store.AcquireRuntimeNodeLifecycleAction(ctx, "elastic", "absent-token", "manager-1", time.Minute)
	require.NoError(t, err)
	require.NotNil(t, claimed)
	require.Equal(t, "manager-1", claimed.RecoveryOwnerID)
	require.EqualValues(t, 1, claimed.RecoveryEpoch)
	require.False(t, claimed.RecoveryDeadlineAt.After(time.Now().UTC()))

	_, err = store.ObserveRuntimeNodeLifecycleProviderAction(
		ctx, "elastic", "absent-token", "manager-1", claimed.RecoveryEpoch, false)
	require.NoError(t, err)
	recovered, err := store.ObserveRuntimeNodeLifecycleProviderInstances(
		ctx, "elastic", "absent-token", "manager-1", claimed.RecoveryEpoch, true)
	require.NoError(t, err)
	require.NotNil(t, recovered.ProviderActionAbsentSince)
	require.NotNil(t, recovered.ProviderInstanceAbsentSince)

	_, err = pool.Exec(ctx, `
		INSERT INTO manager.runtime_slots (
			slot_id, cluster_id, allocation_id, allocation_namespace, node_id, node_uid,
			node_boot_id, netns_identity, control_endpoint, compatibility_digest, state,
			runtime_ready_digest, network_ready_digest, storage_ready_digest,
			heartbeat_expires_at, fastpath_ready_at
		) VALUES (
			'absent-warm-slot', 'nomad', 'absent-allocation', 'default',
			'21111111-1111-1111-1111-111111111111', 'ecs/us-east-1/i-absent', 'absent-boot',
			'absent-netns', 'https://10.0.1.10:4646', 'sha256:' || repeat('a', 64),
			'fastpath_ready', decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex'),
			decode(repeat('03', 32), 'hex'), NOW() - INTERVAL '1 minute', NOW() - INTERVAL '2 minutes'
		)
	`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.runtime_slots (
			slot_id, cluster_id, allocation_id, allocation_namespace, node_id, node_uid,
			node_boot_id, netns_identity, control_endpoint, compatibility_digest, state,
			runtime_ready_digest, network_ready_digest, storage_ready_digest,
			heartbeat_expires_at, fastpath_ready_at
		) VALUES (
			'absent-live-slot', 'nomad', 'absent-live-allocation', 'default',
			'21111111-1111-1111-1111-111111111111', 'ecs/us-east-1/i-absent', 'absent-boot',
			'absent-live-netns', 'https://10.0.1.10:4646', 'sha256:' || repeat('a', 64),
			'fastpath_ready', decode(repeat('01', 32), 'hex'), decode(repeat('02', 32), 'hex'),
			decode(repeat('03', 32), 'hex'), NOW() + INTERVAL '1 minute', NOW()
		)
	`)
	require.NoError(t, err)
	proofDigest := sha256.Sum256([]byte("absent provider proof"))
	count, err := store.TerminalizeRuntimeSlotsForProviderAbsentInstance(
		ctx, "elastic", "i-absent", "absent-token", "manager-1", claimed.RecoveryEpoch, proofDigest[:])
	require.NoError(t, err)
	require.Equal(t, 1, count)

	var terminal string
	var retired bool
	var reason string
	var storedProof []byte
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, carrier_retired, terminal_reason, terminal_proof_digest
		FROM manager.runtime_slots WHERE slot_id='absent-warm-slot'
	`).Scan(&terminal, &retired, &reason, &storedProof))
	require.Equal(t, RuntimeSlotStateTerminal, terminal)
	require.True(t, retired)
	require.Equal(t, "provider_instance_absent", reason)
	require.Equal(t, proofDigest[:], storedProof)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state FROM manager.runtime_slots WHERE slot_id='absent-live-slot'
	`).Scan(&terminal))
	require.Equal(t, RuntimeSlotStateFastpathReady, terminal)

	_, err = pool.Exec(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions
		SET recovery_lease_expires_at = NOW() - INTERVAL '1 second'
		WHERE lifecycle_action_token='absent-token'
	`)
	require.NoError(t, err)
	_, err = store.ObserveRuntimeNodeLifecycleProviderAction(
		ctx, "elastic", "absent-token", "manager-1", claimed.RecoveryEpoch, false)
	require.ErrorContains(t, err, "recovery lease is not current")
	claimed, err = store.AcquireRuntimeNodeLifecycleAction(ctx, "elastic", "absent-token", "manager-2", time.Minute)
	require.NoError(t, err)
	require.Equal(t, "manager-2", claimed.RecoveryOwnerID)
	require.EqualValues(t, 2, claimed.RecoveryEpoch)

	require.NoError(t, store.BeginRuntimeNodeLifecycleActionCleanupForOwner(
		ctx, "elastic", "absent-token", "manager-2", claimed.RecoveryEpoch,
	))
	require.NoError(t, store.CompleteRuntimeNodeLifecycleActionRecovery(
		ctx, "elastic", "absent-token", "manager-2", claimed.RecoveryEpoch,
		"abandoned", map[string]string{"source": "provider_absent"},
	))
	var state string
	var convergence []byte
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT state, convergence_proof
		FROM manager.runtime_node_lifecycle_actions WHERE lifecycle_action_token='absent-token'
	`).Scan(&state, &convergence))
	require.Equal(t, "abandoned", state)
	require.JSONEq(t, `{"source":"provider_absent"}`, string(convergence))
}
