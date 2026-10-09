package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

// Both claims select a different ready carrier from the same free-capacity
// snapshot. The later capacity-lock holder must see the first committed lease.
func TestRuntimeCapacitySelectionHintRechecksAfterLockWaitIntegration(t *testing.T) {
	authority := newSandboxStoreIntegrationPool(t)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	config := authority.Config().Copy()
	config.MaxConns, config.MinConns = 8, 0
	config.ConnConfig.RuntimeParams["application_name"] = "capacity-snapshot-regression"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	store := NewPGSandboxStore(pool)
	registration := runtimeSlotTestRegistration("unused", "unused")
	_, err = store.RegisterRuntimeNodeCapacity(ctx, &RegisterRuntimeNodeCapacityRequest{
		ClusterID: registration.ClusterID, NodeID: registration.NodeID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		CPUMillicores: 1000, MemoryBytes: 1 << 30, CPUSetCPUs: "0", CPUSetMems: "0", TTL: time.Minute,
	})
	require.NoError(t, err)
	requests := make([]*AcquireRuntimeSlotRequest, 2)
	for i := range requests {
		suffix := fmt.Sprintf("snapshot-%d", i)
		reg := runtimeSlotTestRegistration("slot-"+suffix, "allocation-"+suffix)
		_, err = store.RegisterRuntimeSlot(ctx, reg)
		require.NoError(t, err)
		proof := bytes.Repeat([]byte{1}, 32)
		_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
			SlotID: reg.SlotID, AllocationID: reg.AllocationID, NodeUID: reg.NodeUID, NodeBootID: reg.NodeBootID,
			RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof, HeartbeatTTL: time.Minute,
		})
		require.NoError(t, err)
		filesystem, generation := runtimeSlotTestGeneration(t, store, "sandbox-"+suffix, "operation-"+suffix)
		requests[i] = &AcquireRuntimeSlotRequest{
			OperationID: "operation-" + suffix, ClaimID: "claim-" + suffix, SandboxID: "sandbox-" + suffix,
			FilesystemID: filesystem.ID, SourceGenerationID: generation.ID, CompatibilityDigest: reg.CompatibilityDigest,
			ClusterID: reg.ClusterID, RuntimeAssignmentRevision: strings.Repeat("ab", 32),
			NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources(),
		}
	}
	blocker, err := authority.BeginTx(ctx, pgx.TxOptions{})
	require.NoError(t, err)
	_, err = blocker.Exec(ctx, `SELECT 1 FROM manager.runtime_node_capacities
        WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 FOR UPDATE`,
		registration.ClusterID, registration.NodeID, registration.NodeUID, registration.NodeBootID)
	require.NoError(t, err)
	var workers sync.WaitGroup
	defer func() { cancel(); _ = blocker.Rollback(context.Background()); workers.Wait() }()
	results := make(chan error, len(requests))
	for _, request := range requests {
		workers.Add(1)
		go func() { defer workers.Done(); _, err := store.AcquireRuntimeSlot(ctx, request); results <- err }()
	}
	require.Eventually(t, func() bool {
		var waiting int
		err := authority.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
            WHERE application_name='capacity-snapshot-regression' AND wait_event_type='Lock'
                AND query LIKE '%runtime_node_capacities%'`).Scan(&waiting)
		return err == nil && waiting == len(requests)
	}, 3*time.Second, 5*time.Millisecond, "both claims must select before either capacity holder can commit")
	require.NoError(t, blocker.Rollback(ctx))
	var admitted, unavailable int
	for range requests {
		err := <-results
		if err == nil {
			admitted++
		} else if errors.Is(err, ErrRuntimeSlotUnavailable) {
			unavailable++
		} else {
			t.Fatalf("unexpected admission result: %v", err)
		}
	}
	require.Equal(t, 1, admitted)
	require.Equal(t, 1, unavailable)
	var cpu, memory int64
	require.NoError(t, authority.QueryRow(ctx, `SELECT SUM(cpu_millicores), SUM(memory_bytes)
        FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&cpu, &memory))
	require.Equal(t, int64(1000), cpu)
	require.Equal(t, int64(1<<30), memory)
}
