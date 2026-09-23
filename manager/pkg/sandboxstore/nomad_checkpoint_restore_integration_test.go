package sandboxstore

import (
	"bytes"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func checkpointExecutionStoreFixture(t *testing.T, name string, fork ...bool) (*nomadPauseStoreFixture, *NomadSandboxResumeCandidate, rootfshandoff.StageRequest, *IssueRootFSWriterGrantRequest, *StartRuntimeSlotRequest) {
	t.Helper()
	f := completedCheckpointStoreFixture(t, name)
	if len(fork) != 0 && fork[0] {
		request := memoryForkRequest(t, f, f.sandboxID, name+"-child", name+"-fork")
		child, err := f.store.ForkNomadPausedSandbox(f.ctx, request)
		require.NoError(t, err)
		// Keep the captured source slot for CPU provenance; subsequent lifecycle
		// assertions and commands belong to the independent child.
		f.sandboxID = child.ID
	}
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, ExpectedTeamID: "team-slot", Memory: true})
	require.NoError(t, err)
	target := acquireCheckpointRestoreTarget(t, f, candidate, name)
	stage, issue, start := checkpointExecutionForTarget(t, f, candidate, target)
	return f, candidate, stage, issue, start
}

func checkpointExecutionForTarget(t *testing.T, f *nomadPauseStoreFixture, candidate *NomadSandboxResumeCandidate, target *RuntimeSlot) (rootfshandoff.StageRequest, *IssueRootFSWriterGrantRequest, *StartRuntimeSlotRequest) {
	t.Helper()
	a := *candidate.Checkpoint
	cpu, err := f.store.AuthorizeNomadCheckpointRestoreCPU(f.ctx, a, target.ID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreCPU(f.ctx, *cpu, a, migrationCPUStoreResult(t, f, *cpu)))
	image, err := f.store.AuthorizeNomadCheckpointRestoreImage(f.ctx, a, target.ID)
	require.NoError(t, err)
	imageDigest, err := image.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointRestoreImage(f.ctx, *image, protocol.MigrationImagePrepared{
		RequestDigest: imageDigest, ManifestDigest: image.Receipt.Reference.ManifestDigest, TotalBytes: 1024}))
	issue := rootFSWriterGrantTestIssueRequest(candidate.SandboxID, "memory-restore-writer-"+target.ID, target.ClaimID, target.ID, nil)
	issue.OperationID = candidate.OperationID
	issue.ExpectedFilesystemID, issue.InitialGenerationID = target.FilesystemID, target.SourceGenerationID
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, candidate.SandboxID)
	require.NoError(t, err)
	issue.ExpectedWriterEpoch = filesystem.WriterEpoch
	issue.NodeUID, issue.NodeBootID, issue.NodeName = target.NodeUID, target.NodeBootID, target.NodeID
	issue.RuntimeNamespace, issue.RuntimeIncarnationID, issue.RuntimeID = target.AllocationNamespace, target.AllocationID, protocol.NomadTaskName
	issue.RuntimeGeneration, issue.ConsumeExpiresAt = strconv.FormatInt(candidate.RuntimeGeneration, 10), target.ClaimLeaseExpiresAt
	issue.GateParent = digest.FromString("memory-restore-parent-" + target.ID).String()
	cut := image.Publication.Capture.RootFS.Generation
	cut.FilesystemID = candidate.FilesystemID
	stage := rootfshandoff.StageRequest{BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent: issue.GateParent, InitialGeneration: cut.GenerationID, Generation: &cut,
		Identity: rootfshandoff.Identity{NodeUID: target.NodeUID, BootID: target.NodeBootID, RuntimeGeneration: issue.RuntimeGeneration,
			AllocationID: target.AllocationID, NetworkIncarnationID: "memory-network", TaskName: protocol.NomadTaskName,
			SourceOCIDigest: cut.SourceOCIDigest, RootFSDriver: "nomad-driver", RuntimeClass: "sandbox0-gvisor", SlotNonce: target.ID,
			ClaimID: target.ClaimID, LaunchAttempt: "memory-launch", RootFSID: cut.FilesystemID, WriterEpoch: filesystem.WriterEpoch + 1,
			WriterGrantID: issue.GrantID, WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(issue.RawToken)},
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{AllocationID: target.AllocationID, NetworkIncarnationID: "memory-network", ClaimID: target.ClaimID,
			NetworkEpoch: 1, PolicyDigest: target.ClaimNetworkPolicyDigest, SourceIP: "192.0.2.1", CtldGeneration: "ctld-1", NetNSIdentity: target.NetNSIdentity}}
	revision, err := a.Assignment.Target.Revision()
	require.NoError(t, err)
	resourceDigest, err := target.ResourceLease.Digest()
	require.NoError(t, err)
	stage.Labels = map[string]string{protocol.RuntimeAssignmentRevisionLabel: revision, protocol.RuntimeResourceLeaseDigestLabel: resourceDigest}
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	issue.BindingDigest = binding[:]
	_, err = f.store.IssueAndBindRuntimeSlotWriterGrant(f.ctx, issue, &BindRuntimeSlotWriterGrantRequest{
		SlotID: target.ID, OperationID: candidate.OperationID, ClaimID: target.ClaimID, GrantID: issue.GrantID})
	require.NoError(t, err)
	start := &StartRuntimeSlotRequest{SlotID: target.ID, AllocationID: target.AllocationID, NodeUID: target.NodeUID, NodeBootID: target.NodeBootID,
		OperationID: candidate.OperationID, ClaimID: target.ClaimID, LaunchAttempt: stage.Identity.LaunchAttempt,
		RunscContainerID: protocol.NomadRunscContainerID(target.ID), RootFSBindingDigest: binding[:],
		ClaimNetworkDigest: bytes.Repeat([]byte{0x71}, 32), ResourceLeaseID: target.ResourceLease.LeaseID, ResourceLeaseDigest: target.ResourceLeaseDigest}
	return stage, issue, start
}

func consumeCheckpointRestoreWriter(t *testing.T, f *nomadPauseStoreFixture, stage rootfshandoff.StageRequest, issue *IssueRootFSWriterGrantRequest) {
	t.Helper()
	_, err := f.store.ConsumeRootFSWriterGrant(f.ctx, &ConsumeRootFSWriterGrantRequest{GrantID: issue.GrantID, WriterEpoch: stage.Identity.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: issue.BindingVersion, BindingDigest: issue.BindingDigest, ConsumerNodeUID: issue.NodeUID, ConsumerAgentUID: "target-ctld", LeaseTTL: time.Minute})
	require.NoError(t, err)
}

func TestNomadCheckpointRestoreExecutionRequiresExactConsumedWriterIntegration(t *testing.T) {
	f, candidate, stage, issue, start := checkpointExecutionStoreFixture(t, "memory-execution")
	start.MigrationRestoreDigest = strings.Repeat("ab", 32)
	_, err := f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	a := *candidate.Checkpoint
	for _, change := range []func(*rootfshandoff.StageRequest){
		func(s *rootfshandoff.StageRequest) { s.Identity.WriterGrantToken = "must-not-persist" },
		func(s *rootfshandoff.StageRequest) { s.Identity.WriterGrantTokenDigest = strings.Repeat("ab", 32) },
		func(s *rootfshandoff.StageRequest) { s.Parent = digest.FromString("different-parent").String() },
	} {
		wrong := stage
		change(&wrong)
		_, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, a, start.SlotID, wrong)
		require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	}
	restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, a, start.SlotID, stage)
	require.NoError(t, err)
	start.MigrationRestoreDigest, err = restore.Digest()
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "issued grant does not permit execution")
	consumeCheckpointRestoreWriter(t, f, stage, issue)
	const workers = 8
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() { _, errs[i] = NewPGSandboxStore(f.pool).StartRuntimeSlot(f.ctx, start) })
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	stored, err := f.store.GetRuntimeSlot(f.ctx, start.SlotID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateStarting, stored.State)
	owner, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, owner.DesiredState)
	require.Empty(t, owner.RuntimeID)
	retry, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, a, start.SlotID, stage)
	require.NoError(t, err)
	require.Equal(t, restore, retry)
	_, err = f.store.AbortNomadSandboxResume(f.ctx, f.sandboxID, candidate.OperationID, "unsafe ordinary abort")
	require.Error(t, err, "authorized execution needs physical failure resolution")
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=evidence-'restore' WHERE operation_id=$1`, candidate.OperationID)
	require.Error(t, err)
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_filesystems SET writer_epoch=writer_epoch+1 WHERE filesystem_id=$1`, f.filesystem.ID)
	require.NoError(t, err)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict, "a consumed stale writer must not revive execution")
}

func TestNomadCheckpointHandoverGatesReadinessAndResumeCommitIntegration(t *testing.T) {
	f, candidate, stage, issue, start := checkpointExecutionStoreFixture(t, "memory-handover")
	restore, err := f.store.AuthorizeNomadCheckpointRestore(f.ctx, *candidate.Checkpoint, start.SlotID, stage)
	require.NoError(t, err)
	start.MigrationRestoreDigest, err = restore.Digest()
	require.NoError(t, err)
	consumeCheckpointRestoreWriter(t, f, stage, issue)
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.NoError(t, err)
	restored := protocol.MigrationRestoreObservation{Request: *restore, RequestDigest: start.MigrationRestoreDigest, State: protocol.MigrationRestoreComplete}
	address, err := protocol.NomadProcdAddress(stage.ExpectedPolicyToken.SourceIP)
	require.NoError(t, err)
	ready := &MarkRuntimeSlotCommandReadyRequest{SlotID: start.SlotID, AllocationID: start.AllocationID, NodeUID: start.NodeUID, NodeBootID: start.NodeBootID,
		OperationID: start.OperationID, ClaimID: start.ClaimID, MigrationRestoreDigest: start.MigrationRestoreDigest,
		ProcdInstanceID: restore.Image.Publication.Capture.Request.ProcdInstanceID, ProcdAddress: address, CommandReadyDigest: bytes.Repeat([]byte{0x81}, 32)}
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	uncertain := restored
	uncertain.State = protocol.MigrationRestoreUncertain
	_, err = f.store.AuthorizeNomadCheckpointHandover(f.ctx, uncertain)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	handover, err := f.store.AuthorizeNomadCheckpointHandover(f.ctx, restored)
	require.NoError(t, err)
	require.Equal(t, candidate.Checkpoint.LifecycleEpoch, handover.LifecycleEpoch)
	require.Equal(t, candidate.Checkpoint.Assignment, *handover.Restore)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	handoverDigest, err := handover.Digest()
	require.NoError(t, err)
	response := procdapi.RuntimeCheckpointResponse{InstanceID: handover.InstanceID, RequestDigest: handoverDigest, RuntimeGeneration: candidate.RuntimeGeneration, State: "ready"}
	wrong := response
	wrong.InstanceID = "replacement-procd"
	require.ErrorIs(t, f.store.CommitNomadCheckpointHandover(f.ctx, *handover, wrong), ErrNomadCheckpointConflict)
	require.NoError(t, f.store.CommitNomadCheckpointHandover(f.ctx, *handover, response))
	for _, change := range []func(*MarkRuntimeSlotCommandReadyRequest){
		func(r *MarkRuntimeSlotCommandReadyRequest) { r.ProcdInstanceID = "replacement-procd" },
		func(r *MarkRuntimeSlotCommandReadyRequest) { r.ProcdAddress = "http://192.0.2.99:49983" },
		func(r *MarkRuntimeSlotCommandReadyRequest) { r.NodeBootID = "replacement-boot" },
	} {
		changed := *ready
		change(&changed)
		_, err := f.store.MarkRuntimeSlotCommandReady(f.ctx, &changed)
		require.ErrorIs(t, err, ErrNomadCheckpointConflict)
	}
	const workers = 8
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := range workers {
		wg.Go(func() { _, errs[i] = NewPGSandboxStore(f.pool).MarkRuntimeSlotCommandReady(f.ctx, ready) })
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	owner, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, owner.DesiredState, "only resume completion publishes routing")
	complete := &CompleteNomadSandboxResumeRequest{SandboxID: f.sandboxID, OperationID: candidate.OperationID, SlotID: start.SlotID,
		AllocationID: start.AllocationID, AllocationNamespace: issue.RuntimeNamespace, ResourceLeaseID: start.ResourceLeaseID, ResourceLeaseDigest: start.ResourceLeaseDigest}
	_, err = f.pool.Exec(f.ctx, `CREATE FUNCTION manager.reject_test_checkpoint_adoption() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN IF NEW.evidence ? 'adoption' THEN RAISE EXCEPTION 'injected adoption failure'; END IF; RETURN NEW; END; $$;
		CREATE TRIGGER reject_test_checkpoint_adoption BEFORE UPDATE ON manager.sandbox_runtime_checkpoint_restores
		FOR EACH ROW EXECUTE FUNCTION manager.reject_test_checkpoint_adoption()`)
	require.NoError(t, err)
	_, err = f.store.CompleteNomadSandboxResume(f.ctx, complete)
	require.ErrorContains(t, err, "injected adoption failure")
	rolledBack, err := f.store.GetSandbox(f.ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, rolledBack.DesiredState)
	require.Empty(t, rolledBack.RuntimeID)
	missingAdoption, err := f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, start.SlotID)
	require.NoError(t, err)
	require.Nil(t, missingAdoption, "failed generation commit cannot authorize private-image cleanup")
	_, err = f.pool.Exec(f.ctx, `DROP TRIGGER reject_test_checkpoint_adoption ON manager.sandbox_runtime_checkpoint_restores`)
	require.NoError(t, err)
	owner, err = f.store.CompleteNomadSandboxResume(f.ctx, complete)
	require.NoError(t, err)
	require.Equal(t, candidate.RuntimeGeneration, owner.RuntimeGeneration)
	require.Equal(t, start.AllocationID, owner.RuntimeID)
	require.Equal(t, SandboxDesiredStateActive, owner.DesiredState)
	_, err = f.store.CompleteNomadSandboxResume(f.ctx, complete)
	require.NoError(t, err)
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, ready)
	require.NoError(t, err, "lost acknowledgement can retry after routing commit")
	_, err = f.store.StartRuntimeSlot(f.ctx, start)
	require.NoError(t, err, "same launch acknowledgement remains idempotent")
	var refs int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, f.sandboxID).Scan(&refs))
	require.Zero(t, refs, "completed owner can create a new independent memory pause")
	adoption, err := f.store.GetNomadSandboxMigrationAdoptionForSlot(f.ctx, start.SlotID)
	require.NoError(t, err)
	require.NotNil(t, adoption)
	require.NoError(t, adoption.ValidateFor(restored))
	adoptionDigest, err := adoption.Digest()
	require.NoError(t, err)
	proof := protocol.MigrationAdoptionProof{RequestDigest: adoptionDigest, ImageAbsent: true}
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, *adoption, proof))
	require.NoError(t, NewPGSandboxStore(f.pool).CommitNomadSandboxMigrationAdoption(f.ctx, *adoption, proof))
	// A later capture must retain the restored guest CPU profile and its exact
	// committed regional predecessor, including when the source node is reused.
	observation := restore.Image.Publication.CPULaunch.Observation
	observation.CPUSet = restore.Image.Resources.CPUSetCPUs
	launch, err := protocol.BindMigrationCPURestore(*restore, observation, observation)
	require.NoError(t, err)
	slot, err := f.store.GetRuntimeSlot(f.ctx, start.SlotID)
	require.NoError(t, err)
	tx, err := f.pool.Begin(f.ctx)
	require.NoError(t, err)
	defer tx.Rollback(f.ctx)
	require.NoError(t, validateNomadMigrationCPULineage(f.ctx, tx, slot, *launch))
	wrongLaunch := *launch
	wrongLaunch.Restored = nil
	require.ErrorIs(t, validateNomadMigrationCPULineage(f.ctx, tx, slot, wrongLaunch), ErrNomadSandboxMigrationConflict)
	require.NoError(t, tx.Rollback(f.ctx))
	next, err := f.store.ReserveNomadSandboxMemoryPause(f.ctx, "memory-pause-after-resume", candidate.Checkpoint.Assignment.Target,
		migrationSourcePolicy(f.sandboxID, candidate.Checkpoint.Assignment.Target.TeamID))
	require.NoError(t, err)
	nextDigest, err := next.Evidence.Preflight.Digest()
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointCPU(f.ctx, next.Evidence.Preflight, protocol.MigrationCPUPreflight{
		RequestDigest: nextDigest, Launch: *launch, Observation: observation}))
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.sandboxes SET hard_expires_at=NOW()-INTERVAL '1 second' WHERE sandbox_id=$1`, f.sandboxID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadSandboxMigrationAdoption(f.ctx, *adoption, proof), "historical cleanup survives later lifecycle and TTL")
}
