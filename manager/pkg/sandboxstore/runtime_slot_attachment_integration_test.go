package sandboxstore

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRuntimeSlotAttachmentFailureDoesNotLeakCapacityIntegration(t *testing.T) {
	for _, phase := range []string{"attachment", "no_update", "commit"} {
		t.Run(phase, func(t *testing.T) {
			ctx := context.Background()
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)
			fs, generation := runtimeSlotTestGeneration(t, store, "sandbox-attachment", "operation-attachment")
			registration := runtimeSlotTestRegistration("slot-attachment", "allocation-attachment")
			_, err := registerRuntimeSlotWithTestCapacity(t, ctx, store, registration)
			require.NoError(t, err)
			proof := bytes.Repeat([]byte{0x91}, 32)
			_, err = store.ReportRuntimeSlotReady(ctx, &ReportRuntimeSlotReadyRequest{
				SlotID: registration.SlotID, AllocationID: registration.AllocationID,
				NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
				RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof,
				HeartbeatTTL: time.Minute,
			})
			require.NoError(t, err)
			request := &AcquireRuntimeSlotRequest{
				OperationID: "operation-attachment", ClaimID: "claim-attachment", SandboxID: "sandbox-attachment",
				FilesystemID: fs.ID, SourceGenerationID: generation.ID, ClusterID: registration.ClusterID,
				CompatibilityDigest: registration.CompatibilityDigest, RuntimeAssignmentRevision: strings.Repeat("ab", 32),
				NetworkPolicyDigest: "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
				Resources: runtimeSlotTestResources(),
			}
			var install, remove string
			switch phase {
			case "attachment":
				install = `ALTER TABLE manager.runtime_slots ADD CONSTRAINT tti_reject_attachment CHECK (claim_operation_id IS DISTINCT FROM 'operation-attachment')`
				remove = `ALTER TABLE manager.runtime_slots DROP CONSTRAINT tti_reject_attachment`
			case "no_update":
				install = `CREATE FUNCTION manager.tti_skip_attachment() RETURNS trigger LANGUAGE plpgsql AS $$
     BEGIN RETURN NULL; END $$;
     CREATE TRIGGER tti_skip_attachment BEFORE UPDATE ON manager.runtime_slots
     FOR EACH ROW EXECUTE FUNCTION manager.tti_skip_attachment();`
				remove = `DROP TRIGGER tti_skip_attachment ON manager.runtime_slots; DROP FUNCTION manager.tti_skip_attachment();`
			default:
				install = `CREATE FUNCTION manager.tti_reject_lease_commit() RETURNS trigger LANGUAGE plpgsql AS $$
     BEGIN RAISE EXCEPTION 'test rejects lease commit' USING ERRCODE='23514'; END $$;
     CREATE CONSTRAINT TRIGGER tti_reject_lease_commit AFTER INSERT ON manager.runtime_resource_leases
     DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION manager.tti_reject_lease_commit();`
				remove = `DROP TRIGGER tti_reject_lease_commit ON manager.runtime_resource_leases; DROP FUNCTION manager.tti_reject_lease_commit();`
			}
			_, err = pool.Exec(ctx, install)
			require.NoError(t, err)
			// A rejected or skipped attachment, or failed deferred commit, must not succeed. Neither
			// the reserved capacity nor the claim may escape that aborted transaction.
			claimed, err := store.AcquireRuntimeSlot(ctx, request)
			require.Error(t, err)
			require.Nil(t, claimed)
			var leases int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM manager.runtime_resource_leases`).Scan(&leases))
			require.Zero(t, leases)
			ready, err := store.GetRuntimeSlot(ctx, registration.SlotID)
			require.NoError(t, err)
			require.Equal(t, RuntimeSlotStateFastpathReady, ready.State)
			require.Empty(t, ready.ResourceLease.LeaseID)
			require.Empty(t, ready.ClaimOperationID)
			_, err = pool.Exec(ctx, remove)
			require.NoError(t, err)
			claimed, err = store.AcquireRuntimeSlot(ctx, request)
			require.NoError(t, err)
			require.NotEmpty(t, claimed.ResourceLease.LeaseID)
			durable, err := NewPGSandboxStore(pool).GetRuntimeSlot(ctx, claimed.ID)
			require.NoError(t, err)
			require.False(t, durable.AuthorityObservedAt.Before(claimed.AuthorityObservedAt))
			assertSameCommittedSlot := func(expected, actual *RuntimeSlot) {
				t.Helper()
				before, after := *expected, *actual
				// This field is observation-time NOW(), not persisted slot state.
				before.AuthorityObservedAt, after.AuthorityObservedAt = time.Time{}, time.Time{}
				require.Equal(t, before, after, "return the committed lease and full slot transition")
			}
			assertSameCommittedSlot(claimed, durable)
			retry, err := NewPGSandboxStore(pool).AcquireRuntimeSlot(ctx, request)
			require.NoError(t, err)
			assertSameCommittedSlot(durable, retry)
		})
	}
}
