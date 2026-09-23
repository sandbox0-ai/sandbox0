package sandboxstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	started, err := f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 1, 0, 0, 0)
	require.NoError(t, err)
	require.False(t, started, "an expired source cannot be reopened through a consolidation drain")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities
		SET heartbeat_expires_at=NOW()+INTERVAL '1 hour'
		WHERE cluster_id='cluster-a' AND node_uid='node-a'`)
	require.NoError(t, err)

	started, err = f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 1,
		8_000, 0, 0)
	require.NoError(t, err)
	require.False(t, started, "fixed worker cannot fit the sandbox and headroom")

	started, err = f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 1, 0, 0, 0)
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

func TestStalledNodeConsolidationReopensSourceIntegration(t *testing.T) {
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), "consolidation-cancel", "a")
	registerConsolidationElastic(t, f, "i-source", "nomad-node-a", "node-a", "10.0.0.10", "172.27.0.0/26")
	migrationReadyTarget(t, f, "consolidation-cancel", "b")
	started, err := f.store.BeginRuntimeNodeConsolidation(f.ctx, "elastic", "i-source", 1, 0, 0, 0)
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
