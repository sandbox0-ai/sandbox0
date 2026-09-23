package sandboxstore

import (
	"bytes"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func memoryForkRequest(t *testing.T, f *nomadPauseStoreFixture, sourceID, targetID, operation string) *NomadSandboxForkRequest {
	t.Helper()
	source, err := f.store.GetSandbox(f.ctx, sourceID)
	require.NoError(t, err)
	return &NomadSandboxForkRequest{OperationID: operation, SourceSandboxID: sourceID, ExpectedTeamID: source.TeamID,
		Target: nomadRunningForkTargetRecord(source, targetID), Memory: true}
}

func TestNomadCheckpointForkSharesImageAndSurvivesParentDeletionIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "memory-fork-shared")
	request := memoryForkRequest(t, f, f.sandboxID, "memory-fork-child", "memory-fork-operation")
	child, err := f.store.ForkNomadPausedSandbox(f.ctx, request)
	require.NoError(t, err)
	require.Zero(t, child.RuntimeGeneration)
	parentFS, err := f.store.GetRootFSFilesystem(f.ctx, f.sandboxID)
	require.NoError(t, err)
	childFS, err := f.store.GetRootFSFilesystem(f.ctx, child.ID)
	require.NoError(t, err)
	require.NotEqual(t, parentFS.ID, childFS.ID)
	require.Equal(t, parentFS.HeadGenerationID, childFS.HeadGenerationID)
	var parentImage, childImage string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT checkpoint_id FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&parentImage))
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT checkpoint_id FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, child.ID).Scan(&childImage))
	require.Equal(t, parentImage, childImage, "fork references image bytes rather than copying them")
	retry, err := f.store.ForkNomadPausedSandbox(f.ctx, request)
	require.NoError(t, err)
	require.Equal(t, child.ID, retry.ID)
	changed := *request
	changed.Memory = false
	_, err = f.store.ForkNomadPausedSandbox(f.ctx, &changed)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	nested := memoryForkRequest(t, f, child.ID, "memory-fork-grandchild", "memory-nested-fork")
	grandchild, err := f.store.ForkNomadPausedSandbox(f.ctx, nested)
	require.NoError(t, err)
	var grandchildImage string
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT checkpoint_id FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, grandchild.ID).Scan(&grandchildImage))
	require.Equal(t, parentImage, grandchildImage, "nested forks retain bounded image ancestry")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET desired_state='deleted',deleted_at=NOW() WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: child.ID, ExpectedTeamID: child.TeamID, Memory: true})
	require.NoError(t, err)
	require.Equal(t, runtimecontrol.CheckpointFork, candidate.Checkpoint.Assignment.Kind)
	require.Equal(t, int64(1), candidate.RuntimeGeneration)
	require.False(t, candidate.ResetCopiedSessionState, "memory fork must retain the captured live sessions")
	require.Equal(t, parentImage, candidate.Checkpoint.Retained.CheckpointID)
	target := acquireCheckpointRestoreTarget(t, f, candidate, "memory-fork-target")
	// Model epochs consumed and fenced by earlier failed attempts. The image
	// remains unchanged; this child's exact grant still comes from regional CAS.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_filesystems SET writer_epoch=writer_epoch+2 WHERE filesystem_id=$1`, child.ID)
	require.NoError(t, err)
	stage, issue, start := checkpointExecutionForTarget(t, f, candidate, target)
	require.Greater(t, stage.Identity.WriterEpoch, stage.Generation.WriterEpoch+1)
	require.Equal(t, child.ID, stage.Generation.FilesystemID)
	require.Equal(t, childFS.HeadGenerationID, stage.Generation.GenerationID)
	restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, target.ID, stage)
	require.NoError(t, err)
	consumeCheckpointRestoreWriter(t, f, stage, issue)
	start.MigrationRestoreDigest, err = restore.Digest()
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.NoError(t, err)
	observation := protocol.MigrationRestoreObservation{Request: *restore, RequestDigest: start.MigrationRestoreDigest, State: protocol.MigrationRestoreComplete}
	handover, err := f.store.AuthorizeNomadCheckpointHandover(f.ctx, observation)
	require.NoError(t, err)
	require.Equal(t, child.ID, handover.Restore.Target.SandboxID)
	require.Equal(t, candidate.Checkpoint.LifecycleEpoch, handover.LifecycleEpoch)
	responseDigest, err := handover.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointHandover(f.ctx, *handover, procdapi.RuntimeCheckpointResponse{
		InstanceID: handover.InstanceID, RequestDigest: responseDigest, RuntimeGeneration: 1, State: "ready"}))
	address, err := protocol.NomadProcdAddress(stage.ExpectedPolicyToken.SourceIP)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: candidate.OperationID, ClaimID: target.ClaimID, MigrationRestoreDigest: start.MigrationRestoreDigest,
		ProcdInstanceID: handover.InstanceID, ProcdAddress: address, CommandReadyDigest: bytes.Repeat([]byte{0x98}, 32)})
	require.NoError(t, err)
	completed, err := f.store.CompleteNomadSandboxResume(f.ctx, &CompleteNomadSandboxResumeRequest{
		SandboxID: child.ID, OperationID: candidate.OperationID, SlotID: target.ID, AllocationID: target.AllocationID,
		AllocationNamespace: target.AllocationNamespace, ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest})
	require.NoError(t, err)
	require.Equal(t, int64(1), completed.RuntimeGeneration)
	adoption, err := f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, target.ID)
	require.NoError(t, err)
	require.NoError(t, adoption.ValidateFor(observation))
	parentAfter, err := f.store.GetRootFSFilesystem(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, parentFS.WriterEpoch, parentAfter.WriterEpoch, "child writer cannot advance the parent's fence")
	grandchildResume, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: grandchild.ID, ExpectedTeamID: grandchild.TeamID, Memory: true})
	require.NoError(t, err, "another owner can still use the shared image after the first child's restore")
	require.Equal(t, candidate.Checkpoint.Retained, grandchildResume.Checkpoint.Retained)
}

func TestNomadCheckpointForkRollsBackMissingImageAndFailedCustodyIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "memory-fork-rollback")
	request := memoryForkRequest(t, f, f.sandboxID, "memory-fork-rollback-child", "memory-fork-rollback-operation")
	_, err := f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_memory_child() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.runtime_generation=0 THEN RAISE EXCEPTION 'injected memory custody failure'; END IF; RETURN NEW; END; $$;
		CREATE TRIGGER reject_test_memory_child BEFORE INSERT ON manager.sandbox_runtime_checkpoint_refs
		FOR EACH ROW EXECUTE FUNCTION manager.reject_test_memory_child()`)
	require.NoError(t, err)
	_, err = f.store.ForkNomadPausedSandbox(f.ctx, request)
	require.ErrorContains(t, err, "injected memory custody failure")
	missingTarget, err := f.store.GetSandbox(f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Nil(t, missingTarget)
	active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active)
	var filesystems int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, request.Target.ID).Scan(&filesystems))
	require.Zero(t, filesystems)
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_memory_child ON manager.sandbox_runtime_checkpoint_refs`)
	require.NoError(t, err)
	ordinary := *request
	ordinary.Memory = false
	created, err := f.store.ForkNomadPausedSandbox(f.ctx, &ordinary)
	require.NoError(t, err)
	var refs int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, created.ID).Scan(&refs))
	require.Zero(t, refs, "existing default remains filesystem-only even when the parent has memory")
	_, err = f.store.ForkNomadPausedSandbox(f.ctx, request)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "retry cannot change the committed mode")
	missing := memoryForkRequest(t, f, created.ID, "memory-fork-missing-image-child", "memory-fork-missing-image")
	_, err = f.store.ForkNomadPausedSandbox(f.ctx, missing)
	require.ErrorIs(t, err, ErrNomadCheckpointNotRetained)
	missingTarget, err = f.store.GetSandbox(f.ctx, missing.Target.ID)
	require.NoError(t, err)
	require.Nil(t, missingTarget)
}
