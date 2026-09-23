package sandboxstore

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointRestoreTargetFixture(t *testing.T, name string) (*nomadPauseStoreFixture, *NomadSandboxResumeCandidate, *RuntimeSlot) {
	t.Helper()
	f := completedCheckpointStoreFixture(t, name)
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.NoError(t, err)
	return f, candidate, acquireCheckpointRestoreTarget(t, f, candidate, name)
}

func acquireCheckpointRestoreTarget(t *testing.T, f *nomadPauseStoreFixture, candidate *NomadSandboxResumeCandidate, name string, memoryBudget ...bool) *RuntimeSlot {
	t.Helper()
	r := migrationReadyTarget(t, f, name, "a")
	revision, err := candidate.Checkpoint.Assignment.Target.Revision()
	require.NoError(t, err)
	request := &AcquireRuntimeSlotRequest{
		OperationID: candidate.OperationID, ClaimID: "claim-" + name, SandboxID: candidate.SandboxID,
		FilesystemID: candidate.FilesystemID, SourceGenerationID: candidate.SourceGenerationID,
		CompatibilityDigest: r.CompatibilityDigest, ClusterID: r.ClusterID, RuntimeAssignmentRevision: revision,
		NetworkPolicyDigest: protocol.NetworkPolicyDigest(migrationSourcePolicy(candidate.SandboxID, candidate.Record.TeamID)),
		ClaimTTL:            time.Minute, Resources: runtimeSlotTestResources()}
	if len(memoryBudget) == 0 || memoryBudget[0] {
		request.MemoryRestore, request.ClaimTTL = true, MemoryRuntimeSlotClaimTTL
	}
	target, err := f.store.AcquireRuntimeSlot(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, r.SlotID, target.ID)
	return target
}

func TestNomadCheckpointRestoreImageRequiresExactCPUAndTargetIntegration(t *testing.T) {
	f, candidate, target := checkpointRestoreTargetFixture(t, "checkpoint-image")
	a := *candidate.Checkpoint
	_, err := f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, a, target.ID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	cpu, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.False(t, cpu.IsSource())
	require.False(t, cpu.CaptureOnly)
	require.Equal(t, a.Assignment, *cpu.Checkpoint)
	require.Equal(t, cpu.Source.Target.NodeUID, cpu.Target.NodeUID, "same-node fresh carrier is supported")
	require.NotEqual(t, cpu.Source.Target.SlotID, cpu.Target.SlotID)
	result := migrationCPUStoreResult(t, f, *cpu)
	wrong := *cpu
	wrong.Target.ControlEndpoint = "unix:///changed.sock"
	wrong.Destination = wrong.Target
	require.NoError(t, wrong.Validate())
	require.ErrorIs(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, wrong, a, migrationCPUStoreResult(t, f, wrong)), ErrNomadCheckpointConflict)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, *cpu, a, result))
	image, err := f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.Equal(t, target.ResourceLease, image.Resources)
	require.Equal(t, a.Retained.Reference, image.Receipt.Reference)
	var originalTime time.Time
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT (evidence->>'cpu_requested_at')::timestamptz
		FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, candidate.OperationID).Scan(&originalTime))
	restarted := NewPGSandboxStore(f.pool)
	again, err := restarted.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.Equal(t, cpu, again)
	var after time.Time
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT (evidence->>'cpu_requested_at')::timestamptz
		FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, candidate.OperationID).Scan(&after))
	require.Equal(t, originalTime, after)
	issue := rootFSWriterGrantTestIssueRequest(f.sandboxID, "grant-checkpoint-image", target.ClaimID, target.ID, bytes.Repeat([]byte{0xa2}, 32))
	issue.ExpectedFilesystemID, issue.InitialGenerationID, issue.ExpectedWriterEpoch = candidate.FilesystemID, candidate.SourceGenerationID, f.writerEpoch
	issue.RuntimeNamespace, issue.RuntimeID, issue.RuntimeIncarnationID = target.AllocationNamespace, "slot", target.AllocationID
	issue.NodeName, issue.NodeUID, issue.NodeBootID = target.NodeID, target.NodeUID, target.NodeBootID
	issue.RuntimeGeneration = strconv.FormatInt(candidate.RuntimeGeneration, 10)
	bind := &BindRuntimeSlotWriterGrantRequest{SlotID: target.ID, OperationID: candidate.OperationID, ClaimID: target.ClaimID, GrantID: issue.GrantID}
	_, err = f.store.IssueAndBindRuntimeSlotWriterGrant(f.ctx, issue, bind)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict, "unverified image cannot receive a writer")
	_, err = f.store.GetRootFSWriterGrant(f.ctx, issue.GrantID)
	require.ErrorIs(t, err, ErrRootFSWriterGrantNotFound, "failed writer binding rolls back grant and epoch")
	digest, err := image.Digest()
	require.NoError(t, err)
	receipt := protocol.MigrationImagePrepared{RequestDigest: digest, ManifestDigest: image.Receipt.Reference.ManifestDigest, TotalBytes: 1024}
	changed := receipt
	changed.RequestDigest = cpu.Source.BindingDigest
	require.ErrorIs(t, f.store.CommitNomadCheckpointRestoreImage(f.ctx, *image, changed), ErrNomadCheckpointConflict)
	require.NoError(t, restarted.CommitNomadCheckpointRestoreImage(f.ctx, *image, receipt))
	require.NoError(t, f.store.CommitNomadCheckpointRestoreImage(f.ctx, *image, receipt))
	changed = receipt
	changed.TotalBytes++
	require.ErrorIs(t, f.store.CommitNomadCheckpointRestoreImage(f.ctx, *image, changed), ErrNomadCheckpointConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=evidence-'cpu' WHERE operation_id=$1`, candidate.OperationID)
	require.Error(t, err)
	_, err = f.store.IssueAndBindRuntimeSlotWriterGrant(f.ctx, issue, bind)
	require.NoError(t, err, "verified image permits ordinary atomic writer issuance")
	_, err = f.store.StartRuntimeSlot(f.ctx, &StartRuntimeSlotRequest{
		SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: candidate.OperationID, ClaimID: target.ClaimID, LaunchAttempt: "cold-fallback",
		RunscContainerID: protocol.NomadRunscContainerID(target.ID), RootFSBindingDigest: issue.BindingDigest,
		ClaimNetworkDigest: bytes.Repeat([]byte{0xa3}, 32), ResourceLeaseID: target.ResourceLease.LeaseID,
		ResourceLeaseDigest: target.ResourceLeaseDigest})
	require.ErrorContains(t, err, "requires image restore authority", "even a prepared image cannot silently take the cold-start path")
}

func TestNomadCheckpointRestoreCPUExpiryDoesNotRenewOnRetryIntegration(t *testing.T) {
	f, candidate, target := checkpointRestoreTargetFixture(t, "checkpoint-cpu-expiry")
	a := *candidate.Checkpoint
	cpu, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, *cpu, a, migrationCPUStoreResult(t, f, *cpu)))
	// Only this isolated test database bypasses the append-only guard to model time.
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_checkpoint_restores DISABLE TRIGGER runtime_checkpoint_restore_guard`)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores
		SET evidence=jsonb_set(evidence,'{cpu_requested_at}',to_jsonb(clock_timestamp()-INTERVAL '3 minutes')) WHERE operation_id=$1`, candidate.OperationID)
	require.NoError(t, err)
	_, err = tx.Exec(f.ctx, `ALTER TABLE manager.sandbox_runtime_checkpoint_restores ENABLE TRIGGER runtime_checkpoint_restore_guard`)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(f.ctx))
	_, err = f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, a, target.ID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
}

func TestNomadCheckpointRestoreTargetChangeAndHardTTLFailClosedIntegration(t *testing.T) {
	f, candidate, target := checkpointRestoreTargetFixture(t, "checkpoint-target-change")
	a := *candidate.Checkpoint
	_, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint='unix:///replacement.sock' WHERE slot_id=$1`, target.ID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.runtime_slots SET control_endpoint=$2 WHERE slot_id=$1`, target.ID, target.ControlEndpoint)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
}

func TestNomadCheckpointPreparationRetryReadsCustodyWithoutRewritingEvidenceIntegration(t *testing.T) {
	f, candidate, stage, _, start := checkpointExecutionStoreFixture(t, "checkpoint-preparation-retry")
	var before, after string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT xmin::text FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, candidate.OperationID).Scan(&before))
	evidence, err := f.store.GetNomadCheckpointRestorePreparation(f.ctx, *candidate.Checkpoint, start.SlotID)
	require.NoError(t, err)
	require.NotNil(t, evidence.Prepared, "writer issuance must not force another image download")
	require.NoError(t, evidence.Prepared.ValidateFor(*evidence.Image))
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT xmin::text FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, candidate.OperationID).Scan(&after))
	require.Equal(t, before, after, "read-only retries must not create JSONB/WAL churn")
	changed := *candidate.Checkpoint
	changed.LifecycleEpoch++
	_, err = f.store.GetNomadCheckpointRestorePreparation(f.ctx, changed, start.SlotID)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = f.store.GetNomadCheckpointRestorePreparation(f.ctx, *candidate.Checkpoint, f.slotID)
	require.Error(t, err, "source slot cannot acquire destination custody")
	restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, start.SlotID, stage)
	require.NoError(t, err)
	evidence, err = f.store.GetNomadCheckpointRestorePreparation(f.ctx, *candidate.Checkpoint, start.SlotID)
	require.NoError(t, err)
	require.Equal(t, restore, evidence.Restore)
}
