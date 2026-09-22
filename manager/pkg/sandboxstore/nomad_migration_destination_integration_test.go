package sandboxstore

import (
	"bytes"
	"strconv"
	"sync"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestNomadMigrationDestinationRequiresFenceAndKeepsCommittedRoutingIntegration(t *testing.T) {
	f, reservation, request, proof := migrationFenceStoreFixture(t, "destination-fence")
	assignment := request.PublicationRequest.Assignment
	_, err := f.store.AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET state='quiescing',migration_source_operation_id=$2 WHERE slot_id=$1`, f.slotID, assignment.OperationID)
	require.Error(t, err, "the unique-claim exception cannot precede source fencing")
	_, err = f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	_, err = f.store.AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict, "intent is not physical proof")
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof))
	const workers = 8
	results := make([]*RuntimeSlot, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			results[i], errs[i] = NewPGSandboxStore(f.pool).AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
		})
	}
	wg.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		require.Equal(t, reservation.TargetSlot.ID, results[i].ID)
		require.Equal(t, results[0].ClaimLeaseExpiresAt, results[i].ClaimLeaseExpiresAt, "retries cannot extend the claim deadline")
		require.Equal(t, reservation.TargetResourceLease, results[i].ResourceLease)
		require.Empty(t, results[i].WriterGrantID)
	}
	inputs, err := f.store.GetRuntimeSlotClaimInputs(f.ctx, results[0].ID)
	require.NoError(t, err)
	require.NotNil(t, inputs)
	require.Equal(t, assignment.Target, inputs.Runtime, "a restored destination retains input for its next migration")
	require.Equal(t, migrationSourcePolicy(f.sandboxID, assignment.Target.TeamID), inputs.NetworkPolicy)
	visible, err := f.store.GetRuntimeSlotBySandboxID(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, f.slotID, visible.ID, "uncommitted destination must not become the routed runtime")
	require.Equal(t, RuntimeSlotStateQuiescing, visible.State)
	var count int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases WHERE lease_state='active'`).Scan(&count))
	require.Equal(t, 2, count, "source custody remains charged to physical capacity")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET migration_source_operation_id=NULL WHERE slot_id=$1`, f.slotID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET state='active' WHERE slot_id=$1`, f.slotID)
	require.Error(t, err, "a predecessor cannot regain execution authority")
	// Projection follows the canonical allocation once a later generation
	// commit updates it, even while predecessor cleanup is still pending.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET runtime_id=$2,runtime_namespace=$3 WHERE sandbox_id=$1`, f.sandboxID, results[0].AllocationID, results[0].AllocationNamespace)
	require.NoError(t, err)
	visible, err = f.store.GetRuntimeSlotBySandboxID(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, results[0].ID, visible.ID)
}

func TestNomadMigrationDestinationWriterIsExactAndCannotStartFreshIntegration(t *testing.T) {
	f, _, request, proof := migrationFenceStoreFixture(t, "destination-writer")
	assignment := request.PublicationRequest.Assignment
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof))
	target, err := f.store.AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.NoError(t, err)
	issue := rootFSWriterGrantTestIssueRequest(f.sandboxID, "migration-target-writer", target.ClaimID, target.ID, bytes.Repeat([]byte{0x79}, 32))
	issue.OperationID = assignment.OperationID
	issue.ExpectedFilesystemID = target.FilesystemID
	issue.InitialGenerationID = target.SourceGenerationID
	issue.ExpectedWriterEpoch = f.writerEpoch
	issue.NodeUID, issue.NodeBootID = target.NodeUID, target.NodeBootID
	issue.RuntimeNamespace, issue.RuntimeIncarnationID = target.AllocationNamespace, target.AllocationID
	issue.NodeName, issue.RuntimeID = target.NodeID, protocol.NomadTaskName
	issue.RuntimeGeneration = strconv.FormatInt(assignment.Target.RuntimeGeneration, 10)
	issue.ConsumeExpiresAt = target.ClaimLeaseExpiresAt
	wrong := *issue
	wrong.RuntimeIncarnationID = f.slotID
	_, err = f.store.IssueNomadSandboxMigrationTargetWriter(f.ctx, assignment, &wrong)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch, "failed attachment must not advance the writer epoch")
	const workers = 8
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() {
			_, errs[i] = NewPGSandboxStore(f.pool).IssueNomadSandboxMigrationTargetWriter(f.ctx, assignment, issue)
		})
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	filesystem, err = f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.writerEpoch+1, filesystem.WriterEpoch)
	_, err = f.store.ConsumeRootFSWriterGrant(f.ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: filesystem.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: issue.BindingVersion, BindingDigest: issue.BindingDigest,
		ConsumerNodeUID: target.NodeUID, ConsumerAgentUID: "target-ctld", LeaseTTL: time.Minute})
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, &StartRuntimeSlotRequest{
		SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: target.ClaimOperationID, ClaimID: target.ClaimID, LaunchAttempt: "restore-attempt",
		RunscContainerID: protocol.NomadRunscContainerID(target.ID), RootFSBindingDigest: issue.BindingDigest,
		ClaimNetworkDigest: bytes.Repeat([]byte{0x71}, 32), ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest})
	require.ErrorIs(t, err, ErrRuntimeSlotInvalid, "a restored workload cannot be substituted with a fresh entrypoint")
	wrong = *issue
	wrong.RawToken += "changed"
	_, err = f.store.IssueNomadSandboxMigrationTargetWriter(f.ctx, assignment, &wrong)
	require.ErrorIs(t, err, ErrRootFSWriterGrantConflict)
	visible, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, assignment.SourceGeneration, visible.RuntimeGeneration)
}

func TestNomadMigrationDestinationAdmissionRollbackRetainsSourceCustodyIntegration(t *testing.T) {
	f, reservation, request, proof := migrationFenceStoreFixture(t, "destination-unavailable")
	assignment := request.PublicationRequest.Assignment
	_, err := f.store.AuthorizeNomadSandboxMigrationSourceFence(f.ctx, request)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationSourceFence(f.ctx, request, proof))
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()-INTERVAL '1 second' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	_, err = f.store.AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.ErrorIs(t, err, ErrNomadSandboxMigrationConflict)
	var predecessor *string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT migration_source_operation_id FROM manager.runtime_slots WHERE slot_id=$1`, f.slotID).Scan(&predecessor))
	require.Nil(t, predecessor)
	target, err := f.store.GetRuntimeSlot(f.ctx, reservation.TargetSlot.ID)
	require.NoError(t, err)
	require.Empty(t, target.SandboxID)
	require.Empty(t, target.WriterGrantID)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	require.Equal(t, f.writerEpoch, filesystem.WriterEpoch)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET heartbeat_expires_at=NOW()+INTERVAL '1 minute' WHERE slot_id=$1`, reservation.TargetSlot.ID)
	require.NoError(t, err)
	// Fail after the predecessor mutation, while attaching the destination.
	// Recovery must see neither half of the interrupted transaction.
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_migration_attachment() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN
			IF OLD.state='fastpath_ready' AND NEW.state='claiming' THEN
				RAISE EXCEPTION 'injected destination attachment failure';
			END IF;
			RETURN NEW;
		END; $$;
		CREATE TRIGGER reject_test_migration_attachment BEFORE UPDATE ON manager.runtime_slots
		FOR EACH ROW EXECUTE FUNCTION manager.reject_test_migration_attachment()`)
	require.NoError(t, err)
	_, err = f.store.AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.ErrorContains(t, err, "injected destination attachment failure")
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT migration_source_operation_id FROM manager.runtime_slots WHERE slot_id=$1`, f.slotID).Scan(&predecessor))
	require.Nil(t, predecessor)
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_migration_attachment ON manager.runtime_slots; DROP FUNCTION manager.reject_test_migration_attachment()`)
	require.NoError(t, err)
	target, err = NewPGSandboxStore(f.pool).AcquireNomadSandboxMigrationTarget(f.ctx, assignment)
	require.NoError(t, err)
	require.Equal(t, reservation.TargetResourceLease, target.ResourceLease)
}
