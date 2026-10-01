package sandboxstore

import (
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRestorePrefersCaptureNodeWithoutBlockingFallbackIntegration(t *testing.T) {
	for _, condition := range []string{"available", "draining", "capacity", "locked"} {
		t.Run(condition, func(t *testing.T) {
			f := completedCheckpointStoreFixture(t, "cache-locality-"+condition)
			candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
				SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
			require.NoError(t, err)
			// The other node's slot is older: locality must take priority over
			// ordinary FIFO, while retaining every existing admission predicate.
			other := migrationReadyTarget(t, f, "cache-other-"+condition, "b")
			local := migrationReadyTarget(t, f, "cache-local-"+condition, "a")
			switch condition {
			case "draining":
				_, err = f.pool.Exec(f.ctx, `INSERT INTO manager.runtime_node_fences(cluster_id,node_id,node_uid,state,reason)
                    VALUES($1,$2,$3,'draining','locality test')`, local.ClusterID, local.NodeID, local.NodeUID)
				require.NoError(t, err)
			case "capacity":
				_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_node_capacities SET admission_cpu_millicores=1
                    WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3`, local.ClusterID, local.NodeID, local.NodeUID)
				require.NoError(t, err)
			case "locked":
				tx, err := f.pool.Begin(f.ctx)
				require.NoError(t, err)
				defer tx.Rollback(f.ctx)
				_, err = tx.Exec(f.ctx, `SELECT slot_id FROM manager.runtime_slots WHERE slot_id=$1 FOR UPDATE`, local.SlotID)
				require.NoError(t, err)
			}
			revision, err := candidate.Checkpoint.Assignment.Target.Revision()
			require.NoError(t, err)
			request := &AcquireRuntimeSlotRequest{OperationID: candidate.OperationID, ClaimID: "cache-claim-" + condition,
				SandboxID: candidate.SandboxID, FilesystemID: candidate.FilesystemID, SourceGenerationID: candidate.SourceGenerationID,
				CompatibilityDigest: candidate.CheckpointCompatibilityDigest, ClusterID: local.ClusterID,
				RuntimeAssignmentRevision: revision, NetworkPolicyDigest: protocol.NetworkPolicyDigest(migrationSourcePolicy(candidate.SandboxID, candidate.Record.TeamID)),
				MemoryRestore: true, Resources: runtimeSlotTestResources()}
			target, err := f.store.AcquireRuntimeSlot(f.ctx, request)
			require.NoError(t, err)
			expected := other.SlotID
			if condition == "available" {
				expected = local.SlotID
			}
			require.Equal(t, expected, target.ID)
			// Retry keeps the original binding even if node/cache availability changes.
			again, err := NewPGSandboxStore(f.pool).AcquireRuntimeSlot(f.ctx, request)
			require.NoError(t, err)
			require.Equal(t, target.ID, again.ID)
		})
	}
}
