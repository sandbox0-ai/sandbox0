package sandboxstore

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSlotBusyCapacityUsesAvailableNodeAndWaitsWhenAllBusyIntegration(t *testing.T) {
	for _, allBusy := range []bool{false, true} {
		name := "available"
		if allBusy {
			name = "all-busy"
		}
		t.Run(name, func(t *testing.T) {
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			const operation = "capacity-skip-operation"
			filesystem, generation := runtimeSlotTestGeneration(t, store, "capacity-skip-sandbox", operation)
			var registrations []*RegisterRuntimeSlotRequest
			for i := range 2 {
				r := runtimeSlotTestRegistration(fmt.Sprintf("capacity-skip-slot-%d", i), fmt.Sprintf("capacity-skip-allocation-%d", i))
				r.NodeID, r.NodeUID, r.NodeBootID = fmt.Sprintf("capacity-node-%d", i), fmt.Sprintf("capacity-uid-%d", i), fmt.Sprintf("capacity-boot-%d", i)
				_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, r)
				require.NoError(t, err)
				proof := bytes.Repeat([]byte{1}, 32)
				_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
					SlotID: r.SlotID, AllocationID: r.AllocationID, NodeUID: r.NodeUID, NodeBootID: r.NodeBootID,
					RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof, HeartbeatTTL: time.Minute,
				})
				require.NoError(t, err)
				registrations = append(registrations, r)
			}
			var firstNode string
			require.NoError(t, pool.QueryRow(ctx, `SELECT node_uid FROM manager.runtime_node_capacities ORDER BY hashtextextended($1 || node_uid, 0) LIMIT 1`, operation).Scan(&firstNode))
			blocker, err := pool.BeginTx(ctx, pgx.TxOptions{})
			require.NoError(t, err)
			defer func() { _ = blocker.Rollback(context.Background()) }()
			_, err = blocker.Exec(ctx, `SELECT node_uid FROM manager.runtime_node_capacities WHERE $2::boolean OR node_uid=$1 FOR UPDATE`, firstNode, allBusy)
			require.NoError(t, err)
			request := &AcquireRuntimeSlotRequest{
				OperationID: operation, ClaimID: "capacity-skip-claim", SandboxID: "capacity-skip-sandbox",
				FilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
				CompatibilityDigest: registrations[0].CompatibilityDigest, ClusterID: registrations[0].ClusterID,
				RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32),
				ClaimTTL: time.Minute, Resources: runtimeSlotTestResources(),
			}
			type outcome struct {
				slot *RuntimeSlot
				err  error
			}
			done := make(chan outcome, 1)
			go func() { slot, err := store.AcquireRuntimeSlot(ctx, request); done <- outcome{slot, err} }()
			if allBusy {
				require.Eventually(t, func() bool {
					var waiting int
					err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND query LIKE '%runtime_node_capacities%'`).Scan(&waiting)
					return err == nil && waiting > 0
				}, time.Second, 5*time.Millisecond)
				require.Never(t, func() bool { return len(done) > 0 }, 100*time.Millisecond, 5*time.Millisecond)
				require.NoError(t, blocker.Rollback(ctx))
			}
			select {
			case result := <-done:
				require.NoError(t, result.err)
				if !allBusy {
					require.NotEqual(t, firstNode, result.slot.NodeUID, "free capacity must be acquired before the busy node is unlocked")
				}
				// Exact retries return the durable incarnation even while the
				// originally preferred capacity row remains locked elsewhere.
				retried, err := store.AcquireRuntimeSlot(ctx, request)
				require.NoError(t, err)
				require.Equal(t, result.slot.ID, retried.ID)
			case <-time.After(time.Second):
				t.Fatal("claim waited behind busy capacity despite an available node")
			}
		})
	}
}
