package sandboxstore

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuntimeAdmissionMigrationPreservesLegacyWritersIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPoolAt(t, 55)
	ctx := t.Context()
	insertLegacy := func(node string) {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO manager.runtime_node_capacities (
			cluster_id, node_id, node_uid, node_boot_id, cpu_millicores, memory_bytes,
			cpuset_cpus, cpuset_mems, heartbeat_expires_at
		) VALUES ('cluster-1', $1, 'node-uid-1', 'boot-1', 2000, 2147483648, '0-1', '0', NOW() + INTERVAL '1 minute')`, node)
		require.NoError(t, err)
	}
	insertLegacy("before-upgrade")
	applySandboxStoreMigrationsThrough(t, pool, 56)
	insertLegacy("after-upgrade")
	store := NewPGSandboxStore(pool)
	for _, node := range []string{"before-upgrade", "after-upgrade"} {
		capacity, err := store.RegisterRuntimeNodeCapacity(ctx, &RegisterRuntimeNodeCapacityRequest{
			ClusterID: "cluster-1", NodeID: node, NodeUID: "node-uid-1", NodeBootID: "boot-1",
			CPUMillicores: 2000, MemoryBytes: 2 << 30, CPUSetCPUs: "0-1", CPUSetMems: "0", TTL: time.Minute,
		})
		require.NoError(t, err, "new code must refresh legacy rows without changing their budget")
		require.Equal(t, int64(2000), capacity.AdmissionCPUMillicores)
		require.Equal(t, int64(2<<30), capacity.AdmissionMemoryBytes)
	}
}
