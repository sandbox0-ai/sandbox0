package sandboxstore

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAutoscalingRetainsLeasesWhenCapacityHeartbeatExpiresIntegration(t *testing.T) {
	ctx := t.Context()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	fs, generation := runtimeSlotTestGeneration(t, store, "sandbox-scaling", "claim-scaling")
	registration := runtimeSlotTestRegistration("slot-scaling", "allocation-scaling")
	_, err := store.EnsureRuntimeNodePoolState(ctx, "elastic", registration.ClusterID)
	require.NoError(t, err)
	_, err = registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
	require.NoError(t, err)
	_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: bytes.Repeat([]byte{1}, 32), NetworkReadyDigest: bytes.Repeat([]byte{2}, 32),
		StorageReadyDigest: bytes.Repeat([]byte{3}, 32), HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	// A live carrier on a fenced fixed node must not suppress replacement capacity.
	_, err = pool.Exec(ctx, `INSERT INTO manager.runtime_node_fences
		(cluster_id,node_id,node_uid,state,reason) VALUES ($1,$2,$3,'draining','test')`,
		registration.ClusterID, registration.NodeID, registration.NodeUID)
	require.NoError(t, err)
	snapshot, err := store.GetRuntimeNodePoolSnapshot(ctx, "elastic")
	require.NoError(t, err)
	require.Zero(t, snapshot.ClusterFixedUsableSlots)
	require.Empty(t, snapshot.PlacementNodes)
	require.Zero(t, snapshot.ClusterFixedCPU)
	require.Zero(t, snapshot.ClusterFixedMemory)
	_, err = pool.Exec(ctx, `DELETE FROM manager.runtime_node_fences WHERE node_uid=$1`, registration.NodeUID)
	require.NoError(t, err)
	_, err = store.AcquireRuntimeSlot(ctx, &AcquireRuntimeSlotRequest{
		OperationID: "claim-scaling", ClaimID: "claim-scaling", SandboxID: "sandbox-scaling",
		FilesystemID: fs.ID, SourceGenerationID: generation.ID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32),
		ClaimTTL: time.Minute, Resources: runtimeSlotTestResources(),
	})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO manager.runtime_node_instances
		(pool_id,provider,provider_instance_id,pool_kind,cluster_id,node_name,node_uid,private_ip,allocation_cidr,state,nomad_node_id,authority_common_name,agent_uid,admitted_at,provider_ready_at)
		VALUES ('elastic','aliyun','i-scaling','elastic',$1,'scaling',$2,'10.0.1.10','172.27.0.0/26','active',$3,'ctld-scaling','ctld-scaling',NOW(),NOW())`,
		registration.ClusterID, registration.NodeUID, registration.NodeID)
	require.NoError(t, err)
	for _, change := range []string{
		`UPDATE manager.runtime_node_capacities SET heartbeat_expires_at=NOW()-INTERVAL '1 minute'`,
		`INSERT INTO manager.runtime_node_capacities
			(cluster_id,node_id,node_uid,node_boot_id,cpu_millicores,memory_bytes,cpuset_cpus,cpuset_mems,heartbeat_expires_at)
			SELECT cluster_id,node_id,node_uid,'successor-boot',cpu_millicores,memory_bytes,cpuset_cpus,cpuset_mems,NOW()+INTERVAL '1 minute'
			FROM manager.runtime_node_capacities`,
	} {
		_, err = pool.Exec(ctx, change)
		require.NoError(t, err)
		snapshot, err = store.GetRuntimeNodePoolSnapshot(ctx, "elastic")
		require.NoError(t, err)
		require.Len(t, snapshot.Nodes, 1)
		require.Equal(t, 1, snapshot.Nodes[0].ActiveLeases)
		require.Equal(t, runtimeSlotTestResources().CPUMillicores, snapshot.Nodes[0].UsedCPUMillicores)
		status, err := store.GetRuntimeNodeDrainStatus(ctx, "elastic", "i-scaling")
		require.NoError(t, err)
		require.False(t, status.SafeToStop())
	}
}
