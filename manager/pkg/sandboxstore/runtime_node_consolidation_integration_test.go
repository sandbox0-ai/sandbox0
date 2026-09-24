package sandboxstore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func beginConsolidationForTest(ctx context.Context, store *PGSandboxStore, poolID, instanceID string, cpu, memory int64, slots int) (bool, error) {
	plan, err := store.GetRuntimeNodeConsolidationCPUPlan(ctx, poolID, instanceID)
	if err != nil {
		return false, err
	}
	if plan == nil {
		return false, nil
	}
	return store.BeginRuntimeNodeConsolidation(ctx, poolID, instanceID, cpu, memory, slots, plan)
}

func registerConsolidationElastic(t *testing.T, f *nomadPauseStoreFixture, instanceID, nodeID, nodeUID, privateIP, allocationCIDR string) {
	t.Helper()
	_, err := f.store.EnsureRuntimeNodePoolState(f.ctx, "elastic", "cluster-a")
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `INSERT INTO manager.runtime_node_instances
		(pool_id,provider,provider_instance_id,pool_kind,cluster_id,node_name,node_uid,
		 private_ip,allocation_cidr,state,nomad_node_id,authority_common_name,agent_uid,
		 admitted_at,provider_ready_at)
		VALUES('elastic','aliyun',$1,'elastic','cluster-a',$2,$3,
		 $4,$5,'active',$2,'ctld-' || $1,'ctld/' || $1,NOW(),NOW())`,
		instanceID, nodeID, nodeUID, privateIP, allocationCIDR)
	require.NoError(t, err)
}

func TestNodeConsolidationFencesOnlyWhenFixedDestinationFitsIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-fixed", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	fixed := migrationReadyTarget(t, f, "consolidation-fixed", "b")
	elastic := migrationReadyTarget(t, f, "consolidation-elastic", "c")
	registerConsolidationElastic(t, f, "i-other", elastic.NodeID, elastic.NodeUID, "10.0.0.11", "172.27.0.64/26")

	_, err := f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities
		SET heartbeat_expires_at=NOW()-INTERVAL '1 minute'
		WHERE cluster_id='cluster-a' AND node_uid='node-a'`)
	require.NoError(t, err)
	started, err := beginConsolidationForTest(f.ctx, f.store, "elastic", "i-source", 0, 0, 0)
	require.NoError(t, err)
	require.False(t, started, "an expired source cannot be reopened through a consolidation drain")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities
		SET heartbeat_expires_at=NOW()+INTERVAL '1 hour'
		WHERE cluster_id='cluster-a' AND node_uid='node-a'`)
	require.NoError(t, err)

	started, err = beginConsolidationForTest(f.ctx, f.store, "elastic", "i-source",
		8_000, 0, 0)
	require.NoError(t, err)
	require.False(t, started, "fixed worker cannot fit the sandbox and headroom")

	started, err = beginConsolidationForTest(f.ctx, f.store, "elastic", "i-source", 0, 0, 0)
	require.NoError(t, err)
	require.True(t, started)
	status, err := f.store.GetRuntimeNodeDrainStatus(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceDraining, status.Instance.State)
	require.Equal(t, RuntimeNodeConsolidationReason, status.Instance.DrainReason)

	reserved, err := f.store.ReserveNomadMigrationEvacuation(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.True(t, reserved)
	var targetNode string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT slot.node_id FROM manager.sandbox_runtime_migrations migration
		JOIN manager.runtime_slots slot ON slot.slot_id=migration.target_slot_id`).Scan(&targetNode))
	require.Equal(t, fixed.NodeID, targetNode)
	snapshot, err := f.store.GetRuntimeNodePoolSnapshot(f.ctx, "elastic")
	require.NoError(t, err)
	require.EqualValues(t, 2_000, snapshot.ClusterUsedCPU, "both physical leases remain reserved")
	require.EqualValues(t, 1_000, snapshot.ClusterWorkloadCPU, "one migration is one workload")
	require.Equal(t, 1, snapshot.ClusterWorkloadSlots)
}

func TestNodeConsolidationRejectsChangedSourceAfterCPUPlanningIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-stale-cpu", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-stale-cpu", "b")
	plan, err := f.store.GetRuntimeNodeConsolidationCPUPlan(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.NotNil(t, plan)
	require.Len(t, plan.Sources, 1)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET lifecycle_epoch=lifecycle_epoch+1 WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	started, err := f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 0, 0, 0, plan)
	require.NoError(t, err)
	require.False(t, started)
	status, err := f.store.GetRuntimeNodeDrainStatus(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceActive, status.Instance.State)
	require.Empty(t, status.Instance.DrainReason)
}

func TestNodeConsolidationRejectsChangedFixedCPUSetAfterPlanningIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-stale-fixed-cpu", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-stale-fixed-cpu", "b")
	plan, err := f.store.GetRuntimeNodeConsolidationCPUPlan(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.NotNil(t, plan)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities
		SET cpuset_cpus=CASE WHEN cpuset_cpus='0' THEN '0-1' ELSE '0' END
		WHERE cluster_id='cluster-a' AND node_uid=$1`, plan.FixedNodeUID)
	require.NoError(t, err)
	started, err := f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 0, 0, 0, plan)
	require.NoError(t, err)
	require.False(t, started)
	status, err := f.store.GetRuntimeNodeDrainStatus(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceActive, status.Instance.State)
}

func TestNodeConsolidationRejectsChangedSourceRuntimeAfterPlanningIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-stale-source-runtime", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-stale-source-runtime", "b")
	plan, err := f.store.GetRuntimeNodeConsolidationCPUPlan(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.NotNil(t, plan)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET procd_instance_id='new-procd-instance'
		WHERE slot_id=$1`, plan.Sources[0].SourceSlotID)
	require.NoError(t, err)
	started, err := f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 0, 0, 0, plan)
	require.NoError(t, err)
	require.False(t, started)
	status, err := f.store.GetRuntimeNodeDrainStatus(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceActive, status.Instance.State)
}

func TestNodeConsolidationMovesMultipleSandboxesOneAtATimeIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	first, _ := migrationExecutionSource(t, pool, "consolidation-multi-first", "a")
	second, _ := migrationExecutionSource(t, pool, "consolidation-multi-second", "a")
	registerConsolidationElastic(t, first, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, first, "consolidation-multi-first", "b")
	migrationReadyTarget(t, second, "consolidation-multi-second", "b")

	started, err := beginConsolidationForTest(first.ctx, first.store, "elastic", "i-source", 0, 0, 0)
	require.NoError(t, err)
	require.True(t, started, "two eligible source leases must not be treated as a concurrency limit")

	reserved, err := first.store.ReserveNomadMigrationEvacuation(first.ctx, first.sandboxID)
	require.NoError(t, err)
	require.True(t, reserved)
	reserved, err = second.store.ReserveNomadMigrationEvacuation(second.ctx, second.sandboxID)
	require.NoError(t, err)
	require.False(t, reserved, "the shared source and fixed destination can each own only one in-flight migration")
}

func TestStalledNodeConsolidationReopensSourceIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-cancel", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-cancel", "b")
	started, err := beginConsolidationForTest(f.ctx, f.store, "elastic", "i-source", 0, 0, 0)
	require.NoError(t, err)
	require.True(t, started)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_instances
		SET drain_started_at=NOW()-INTERVAL '31 minutes'
		WHERE pool_id='elastic' AND provider_instance_id='i-source'`)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities
		SET heartbeat_expires_at=NOW()-INTERVAL '1 minute'
		WHERE cluster_id='cluster-a' AND node_uid='node-a'`)
	require.NoError(t, err)
	cancelled, err := f.store.CancelRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 30*time.Minute)
	require.NoError(t, err)
	require.False(t, cancelled, "a source without a live capacity record must remain fenced")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities
		SET heartbeat_expires_at=NOW()+INTERVAL '1 hour'
		WHERE cluster_id='cluster-a' AND node_uid='node-a'`)
	require.NoError(t, err)
	cancelled, err = f.store.CancelRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 30*time.Minute)
	require.NoError(t, err)
	require.True(t, cancelled)
	status, err := f.store.GetRuntimeNodeDrainStatus(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceActive, status.Instance.State)
	require.Empty(t, status.Instance.DrainReason)
}

func TestStalledNodeConsolidationCancelsAfterSafeAbortIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-abort", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-abort", "b")
	started, err := beginConsolidationForTest(f.ctx, f.store, "elastic", "i-source", 0, 0, 0)
	require.NoError(t, err)
	require.True(t, started)
	reserved, err := f.store.ReserveNomadMigrationEvacuation(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.True(t, reserved)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_instances
		SET drain_started_at=NOW()-INTERVAL '31 minutes'
		WHERE pool_id='elastic' AND provider_instance_id='i-source'`)
	require.NoError(t, err)
	cancelled, err := f.store.CancelRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 30*time.Minute)
	require.NoError(t, err)
	require.False(t, cancelled, "an in-flight migration still owns the drain fence")
	var operationID string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT migration.operation_id
		FROM manager.sandbox_runtime_migrations migration
		JOIN manager.sandbox_lifecycle_txns lifecycle ON lifecycle.txn_id=migration.operation_id
		WHERE lifecycle.sandbox_id=$1`, f.sandboxID).Scan(&operationID))
	require.NoError(t, f.store.AbortNomadSandboxMigrationReservation(f.ctx, f.sandboxID, operationID, "CPU profiles differ"))
	cancelled, err = f.store.CancelRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 30*time.Minute)
	require.NoError(t, err)
	require.True(t, cancelled, "a safe abort must not extend a timed-out drain by its retry cooldown")
	status, err := f.store.GetRuntimeNodeDrainStatus(f.ctx, "elastic", "i-source")
	require.NoError(t, err)
	require.Equal(t, RuntimeNodeInstanceActive, status.Instance.State)
}

func TestNodeConsolidationTimeoutCountsFromLastCompletedMoveIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-progress", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-progress", "b")
	started, err := beginConsolidationForTest(f.ctx, f.store, "elastic", "i-source", 0, 0, 0)
	require.NoError(t, err)
	require.True(t, started)
	reserved, err := f.store.ReserveNomadMigrationEvacuation(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.True(t, reserved)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_instances
		SET drain_started_at=NOW()-INTERVAL '31 minutes'
		WHERE pool_id='elastic' AND provider_instance_id='i-source'`)
	require.NoError(t, err)
	// Isolate the cancellation clock from the physical migration protocol in
	// this per-test database. Production completion passes the physical guard
	// and writes committed_at only after the source lease is released.
	_, err = f.pool.Exec(f.ctx, `ALTER TABLE manager.sandbox_lifecycle_txns
		DISABLE TRIGGER runtime_migration_completion_guard`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = f.pool.Exec(f.ctx, `ALTER TABLE manager.sandbox_lifecycle_txns
			ENABLE TRIGGER runtime_migration_completion_guard`)
	})
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns
		SET phase='committed',committed_at=NOW()
		WHERE sandbox_id=$1 AND kind='migrate'`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `ALTER TABLE manager.sandbox_lifecycle_txns
		ENABLE TRIGGER runtime_migration_completion_guard`)
	require.NoError(t, err)
	require.NoError(t, err)
	cancelled, err := f.store.CancelRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 30*time.Minute)
	require.NoError(t, err)
	require.False(t, cancelled, "recently completed migration keeps the source fenced for the next move")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_lifecycle_txns
		SET committed_at=NOW()-INTERVAL '31 minutes'
		WHERE sandbox_id=$1 AND kind='migrate'`, f.sandboxID)
	require.NoError(t, err)
	cancelled, err = f.store.CancelRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 30*time.Minute)
	require.NoError(t, err)
	require.True(t, cancelled, "an idle source still reopens after the timeout")
}
