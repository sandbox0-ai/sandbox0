package sandboxstore

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func runningMemoryForkFixture(t *testing.T, name string, configure func(*NomadSandboxForkRequest)) (*checkpointPauseWorkerNode, *NomadSandboxForkRequest, *NomadRunningMemoryFork) {
	t.Helper()
	f, _ := migrationExecutionSource(t, newSandboxStoreIntegrationPool(t), name, "")
	request := memoryForkRequest(t, f, f.sandboxID, name+"-child", name+"-fork")
	if configure != nil {
		configure(request)
	}
	intent, err := f.store.RequestNomadSandboxRunningMemoryFork(f.ctx, request)
	require.NoError(t, err)
	inputs, err := f.store.GetRuntimeSlotClaimInputs(f.ctx, f.slotID)
	require.NoError(t, err)
	checkpoint, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, intent.CaptureOperationID, inputs.Runtime, inputs.NetworkPolicy)
	require.NoError(t, err)
	return &checkpointPauseWorkerNode{t: t, f: f, checkpoint: checkpoint, source: inputs.Runtime}, request, intent
}

func finishRunningMemoryForkCapture(t *testing.T, n *checkpointPauseWorkerNode, intent *NomadRunningMemoryFork) {
	t.Helper()
	advanceCheckpointPauseWorker(t, n, 6)
	cleanup, err := n.f.store.AuthorizeNomadCheckpointSourceFinalization(n.f.ctx, intent.CaptureOperationID)
	require.NoError(t, err)
	require.NoError(t, n.f.store.CommitNomadCheckpointSourceFinalization(n.f.ctx, *cleanup, migrationFinalizationStoreProof(t, *cleanup)))
	markCheckpointAllocationMissing(t, n.f)
}

func TestNomadRunningMemoryForkAtomicallyHandsPauseToChildAndParentResumeIntegration(t *testing.T) {
	n, request, intent := runningMemoryForkFixture(t, "running-memory-atomic", nil)
	f := n.f
	const count = 8
	results := make([]*NomadRunningMemoryFork, count)
	errs := make([]error, count)
	var wg sync.WaitGroup
	for i := range count {
		wg.Go(func() {
			results[i], errs[i] = NewPGSandboxStore(f.pool).RequestNomadSandboxRunningMemoryFork(f.ctx, request)
		})
	}
	wg.Wait()
	for i := range count {
		require.NoError(t, errs[i])
		require.Equal(t, intent, results[i])
	}
	child, err := f.store.GetSandbox(f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Nil(t, child, "an admitted capture is not a forked disk or memory image")
	_, err = f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
	require.ErrorIs(t, err, ErrNomadSandboxPauseConflict)
	finishRunningMemoryForkCapture(t, n, intent)
	before, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_running_memory_handoff() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.kind='resume' THEN RAISE EXCEPTION 'injected parent resume failure'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_running_memory_handoff BEFORE INSERT ON manager.sandbox_lifecycle_txns
        FOR EACH ROW EXECUTE FUNCTION manager.reject_test_running_memory_handoff()`)
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, intent.CaptureOperationID)
	require.ErrorContains(t, err, "injected parent resume failure")
	rolledBack, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, before.DesiredState, rolledBack.DesiredState)
	require.Equal(t, before.LifecycleEpoch, rolledBack.LifecycleEpoch)
	child, err = f.store.GetSandbox(f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Nil(t, child, "failed parent admission rolls back child ownership too")
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeResourceLeaseActive, slot.ResourceLeaseState)
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_running_memory_handoff ON manager.sandbox_lifecycle_txns`)
	require.NoError(t, err)
	for range 2 {
		slot, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, intent.CaptureOperationID)
		require.NoError(t, err)
		require.Equal(t, RuntimeResourceLeaseReleased, slot.ResourceLeaseState)
	}
	intent, err = NewPGSandboxStore(f.pool).GetNomadSandboxRunningMemoryFork(f.ctx, intent.OperationID)
	require.NoError(t, err)
	require.NotEmpty(t, intent.ParentResumeOperationID)
	work, err := NewPGSandboxStore(f.pool).ListNomadCheckpointResumes(f.ctx, "", 10)
	require.NoError(t, err)
	require.Equal(t, []NomadCheckpointResumeWork{{OperationID: intent.ParentResumeOperationID, SandboxID: f.sandboxID}}, work, "manager restart must discover the parent's admitted restore")
	child, err = f.store.GetSandbox(f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Equal(t, request.Target.ResourceMillicpu, child.ResourceMillicpu)
	require.Equal(t, request.Target.ResourceMemoryMiB, child.ResourceMemoryMiB)
	active, err := f.store.GetActiveLifecycleTxn(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, intent.ParentResumeOperationID, active.ID)
	require.Equal(t, SandboxLifecycleKindResume, active.Kind)
	require.Equal(t, before.RuntimeGeneration+1, active.ToGeneration)
	memory, found, err := f.store.RetryNomadSandboxResume(f.ctx, &RetryNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: request.ExpectedTeamID, Memory: true})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, intent.CaptureOperationID, memory.Checkpoint.Retained.CheckpointID)
	_, _, err = f.store.RetryNomadSandboxResume(f.ctx, &RetryNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: request.ExpectedTeamID})
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "a concurrent cold resume cannot take the handoff")
	var owners int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE checkpoint_id=$1`, intent.CaptureOperationID).Scan(&owners))
	require.Equal(t, 2, owners)
	target := acquireCheckpointRestoreTarget(t, f, memory, "running-memory-parent-restore")
	stage, issue, start := checkpointExecutionForTarget(t, f, memory, target)
	restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *memory.Checkpoint, target.ID, stage)
	require.NoError(t, err)
	consumeCheckpointRestoreWriter(t, f, stage, issue)
	start.MigrationRestoreDigest, err = restore.Digest()
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.NoError(t, err)
	observation := protocol.MigrationRestoreObservation{Request: *restore, RequestDigest: start.MigrationRestoreDigest, State: protocol.MigrationRestoreComplete}
	handover, err := f.store.AuthorizeNomadCheckpointHandover(f.ctx, observation)
	require.NoError(t, err)
	responseDigest, err := handover.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointHandover(f.ctx, *handover, procdapi.RuntimeCheckpointResponse{
		InstanceID: handover.InstanceID, RequestDigest: responseDigest, RuntimeGeneration: memory.RuntimeGeneration, State: "ready"}))
	address, err := protocol.NomadProcdAddress(stage.ExpectedPolicyToken.SourceIP)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: memory.OperationID, ClaimID: target.ClaimID, MigrationRestoreDigest: start.MigrationRestoreDigest,
		ProcdInstanceID: handover.InstanceID, ProcdAddress: address, CommandReadyDigest: bytes.Repeat([]byte{0x99}, 32)})
	require.NoError(t, err)
	completed, err := f.store.CompleteNomadSandboxResume(f.ctx, &CompleteNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, OperationID: memory.OperationID, SlotID: target.ID, AllocationID: target.AllocationID,
		AllocationNamespace: target.AllocationNamespace, ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest})
	require.NoError(t, err)
	require.Equal(t, before.ExpiresAt, completed.ExpiresAt, "fork cannot refresh the parent's soft TTL")
	require.Equal(t, before.RuntimeGeneration+1, completed.RuntimeGeneration)
	require.Equal(t, SandboxDesiredStateActive, completed.DesiredState)
	require.NotEqual(t, before.RuntimeID, completed.RuntimeID, "the source carrier is single-use")
	child, err = f.store.GetSandbox(f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, child.DesiredState)
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT count(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE checkpoint_id=$1`, intent.CaptureOperationID).Scan(&owners))
	require.Equal(t, 1, owners, "parent adoption retires only its own image reference")
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, intent.CaptureOperationID)
	require.NoError(t, err, "a lost completion reply must not recapture or create another parent restore")

}

func TestNomadRunningMemoryForkExpiredChildStillAdmitsParentRestoreIntegration(t *testing.T) {
	n, request, intent := runningMemoryForkFixture(t, "running-memory-expired-child", func(r *NomadSandboxForkRequest) {
		r.Target.HardExpiresAt = time.Now().UTC().Add(time.Second)
		r.Target.ExpiresAt = r.Target.HardExpiresAt
	})
	finishRunningMemoryForkCapture(t, n, intent)
	time.Sleep(max(time.Millisecond, time.Until(request.Target.HardExpiresAt)+20*time.Millisecond))
	_, err := n.f.store.CompleteNomadSandboxMemoryPause(n.f.ctx, intent.CaptureOperationID)
	require.NoError(t, err)
	actual, err := n.f.store.GetNomadSandboxRunningMemoryFork(n.f.ctx, intent.OperationID)
	require.NoError(t, err)
	require.NotEmpty(t, actual.ParentResumeOperationID)
	child, err := n.f.store.GetSandbox(n.f.ctx, request.Target.ID)
	require.NoError(t, err)
	require.Nil(t, child)
	outcome, err := n.f.store.GetLifecycleTxn(n.f.ctx, intent.OperationID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseAborted, outcome.Phase)
	parent, found, err := n.f.store.RetryNomadSandboxResume(n.f.ctx, &RetryNomadSandboxResumeRequest{SandboxID: n.f.sandboxID, ExpectedTeamID: request.ExpectedTeamID, Memory: true})
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, parent.Checkpoint)
}

func TestNomadRunningMemoryForkCannotChangeTargetOrOmitAtomicHandoffIntegration(t *testing.T) {
	n, request, intent := runningMemoryForkFixture(t, "running-memory-authority", nil)
	changed := *request
	target := *request.Target
	changed.Target = &target
	target.ResourceMemoryMiB++
	_, err := n.f.store.RequestNomadSandboxRunningMemoryFork(n.f.ctx, &changed)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	_, err = n.f.pool.Exec(n.f.ctx, `UPDATE manager.sandbox_runtime_running_memory_forks SET target_sandbox_id='another-child' WHERE operation_id=$1`, intent.OperationID)
	require.Error(t, err)
	finishRunningMemoryForkCapture(t, n, intent)
	// A failed child publication must keep both parent lifecycle and resource
	// release retryable rather than expose an unowned paused parent.
	_, err = n.f.pool.Exec(n.f.ctx, `CREATE FUNCTION manager.reject_test_running_child() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN IF NEW.runtime_generation=0 THEN RAISE EXCEPTION 'injected child retention failure'; END IF; RETURN NEW; END; $$;
        CREATE TRIGGER reject_test_running_child BEFORE INSERT ON manager.sandbox_runtime_checkpoint_refs
        FOR EACH ROW EXECUTE FUNCTION manager.reject_test_running_child()`)
	require.NoError(t, err)
	_, err = n.f.store.CompleteNomadSandboxMemoryPause(n.f.ctx, intent.CaptureOperationID)
	require.ErrorContains(t, err, "injected child retention failure")
	parent, err := n.f.store.GetSandbox(n.f.ctx, n.f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, parent.DesiredState)
	active, err := n.f.store.GetActiveLifecycleTxn(n.f.ctx, parent.ID)
	require.NoError(t, err)
	require.Equal(t, intent.CaptureOperationID, active.ID)
}

func TestNomadRunningMemoryForkDoesNotRestoreTerminatingOrExpiredParentIntegration(t *testing.T) {
	for _, state := range []string{SandboxDesiredStateActive, SandboxDesiredStateTerminating, SandboxDesiredStateDeleted} {
		t.Run(state, func(t *testing.T) {
			n, request, intent := runningMemoryForkFixture(t, "running-memory-expired-parent", nil)
			finishRunningMemoryForkCapture(t, n, intent)
			_, err := n.f.pool.Exec(n.f.ctx, `UPDATE manager.sandboxes
				SET desired_state=$2,hard_expires_at=clock_timestamp()-INTERVAL '1 second',
				deleted_at=CASE WHEN $2='deleted' THEN clock_timestamp() ELSE deleted_at END WHERE sandbox_id=$1`, n.f.sandboxID, state)
			require.NoError(t, err)
			terminal, err := n.f.store.CompleteNomadSandboxMemoryPause(n.f.ctx, intent.CaptureOperationID)
			require.NoError(t, err)
			require.Equal(t, RuntimeResourceLeaseReleased, terminal.ResourceLeaseState)
			actual, err := n.f.store.GetNomadSandboxRunningMemoryFork(n.f.ctx, intent.OperationID)
			require.NoError(t, err)
			require.Empty(t, actual.ParentResumeOperationID)
			child, err := n.f.store.GetSandbox(n.f.ctx, request.Target.ID)
			require.NoError(t, err)
			require.Nil(t, child)
			active, err := n.f.store.GetActiveLifecycleTxn(n.f.ctx, n.f.sandboxID)
			require.NoError(t, err)
			require.Nil(t, active, "cleanup cannot revive an expired or deleted owner")
		})
	}
}
