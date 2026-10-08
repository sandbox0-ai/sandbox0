package sandboxstore

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A node's oldest carriers must not absorb the whole burst while all other
// capacity locks sit idle. Capacity is sufficient on every node, so this test
// checks placement before accounting forces a fallback to another node.
func TestRuntimeSlotSpreadsFreshClaimsAcrossIndependentNodesIntegration(t *testing.T) {
	ctx := t.Context()
	store := NewPGSandboxStore(newSandboxStoreIntegrationPool(t))
	compatibility := runtimeSlotTestRegistration("unused", "unused").CompatibilityDigest
	for node := 0; node < 4; node++ {
		for slot := 0; slot < 8; slot++ {
			id := fmt.Sprintf("spread-slot-%d-%d", node, slot)
			registration := runtimeSlotTestRegistration(id, "allocation-"+id)
			registration.NodeID = fmt.Sprintf("nomad-node-%d", node)
			registration.NodeUID = fmt.Sprintf("spread-node-%d", node)
			registration.NodeBootID = fmt.Sprintf("spread-boot-%d", node)
			_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
			require.NoError(t, err)
			_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
				SlotID: registration.SlotID, AllocationID: registration.AllocationID, NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
				RuntimeReadyDigest: bytes.Repeat([]byte{1}, 32), NetworkReadyDigest: bytes.Repeat([]byte{2}, 32), StorageReadyDigest: bytes.Repeat([]byte{3}, 32), HeartbeatTTL: time.Minute,
			})
			require.NoError(t, err)
		}
	}
	nodes := map[string]int{}
	for i := 0; i < 8; i++ {
		operation := fmt.Sprintf("spread-operation-%d", i)
		sandbox := fmt.Sprintf("spread-sandbox-%d", i)
		fs, generation := runtimeSlotTestGeneration(t, store, sandbox, operation)
		request := &AcquireRuntimeSlotRequest{
			OperationID: operation, ClaimID: "claim-" + operation, SandboxID: sandbox, FilesystemID: fs.ID, SourceGenerationID: generation.ID,
			CompatibilityDigest: compatibility, ClusterID: "cluster-a", RuntimeAssignmentRevision: strings.Repeat("ab", 32), NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute, Resources: runtimeSlotTestResources(),
		}
		claimed, err := store.AcquireRuntimeSlot(ctx, request)
		require.NoError(t, err)
		nodes[claimed.NodeUID]++
		// Replaying the same request must keep the first durable decision.
		retried, err := store.AcquireRuntimeSlot(ctx, request)
		require.NoError(t, err)
		require.Equal(t, claimed.ID, retried.ID)
	}
	require.GreaterOrEqual(t, len(nodes), 3, "fresh operations should use independent node capacity locks before any node becomes full")
	var leases int
	require.NoError(t, store.pool.QueryRow(ctx, "SELECT count(*) FROM manager.runtime_resource_leases WHERE lease_state='active'").Scan(&leases))
	require.Equal(t, 8, leases, "retry must not allocate extra resources")
}
