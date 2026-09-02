package sandboxstore

import (
	"bytes"
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNomadSandboxResumeOperationIDIsDeterministic(t *testing.T) {
	first := NomadSandboxResumeOperationID("sandbox-1", 7, "generation-1", 4)
	require.Equal(t, first, NomadSandboxResumeOperationID("sandbox-1", 7, "generation-1", 4))
	require.NotEqual(t, first, NomadSandboxResumeOperationID("sandbox-1", 8, "generation-1", 4))
	require.NotEqual(t, first, NomadSandboxResumeOperationID("sandbox-1", 7, "generation-2", 4))
	require.NotEqual(t, first, NomadSandboxResumeOperationID("sandbox-1", 7, "generation-1", 5))
}

func TestNomadSandboxResumeAbortIsExactAndAllowsNewAttemptIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume-abort")
	terminalizeNomadPauseFixture(t, fixture)
	limit := int64(10)
	first, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)

	aborted, err := fixture.store.AbortNomadSandboxResume(
		fixture.ctx, fixture.sandboxID, "nomad-resume-not-the-owner", "wrong operation",
	)
	require.NoError(t, err)
	require.False(t, aborted)
	active, err := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.NotNil(t, active)
	require.Equal(t, first.OperationID, active.ID)

	aborted, err = fixture.store.AbortNomadSandboxResume(
		fixture.ctx, fixture.sandboxID, first.OperationID, "planner did not reach command-ready",
	)
	require.NoError(t, err)
	require.True(t, aborted)
	aborted, err = fixture.store.AbortNomadSandboxResume(
		fixture.ctx, fixture.sandboxID, first.OperationID, "idempotent retry",
	)
	require.NoError(t, err)
	require.False(t, aborted)
	active, err = fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.Nil(t, active)
	closed, err := fixture.store.GetLifecycleTxn(fixture.ctx, first.OperationID)
	require.NoError(t, err)
	require.Equal(t, SandboxLifecyclePhaseAborted, closed.Phase)
	require.Equal(t, "planner did not reach command-ready", closed.Error)

	second, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)
	require.NotEqual(t, first.OperationID, second.OperationID)
	require.Greater(t, second.Record.LifecycleEpoch+1, first.Record.LifecycleEpoch+1)
}

func TestNomadSandboxResumeAbortCannotLeaveIssuedWriterUnboundIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume-atomic-writer")
	terminalizeNomadPauseFixture(t, fixture)
	requested, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot",
	})
	require.NoError(t, err)

	registration := runtimeSlotTestRegistration("slot-resume-atomic-writer", "allocation-resume-atomic-writer")
	_, err = registerRuntimeSlotWithTestCapacity(t, fixture.ctx, fixture.store, registration)
	require.NoError(t, err)
	readyProof := bytes.Repeat([]byte{0xa1}, 32)
	_, err = fixture.store.ReportRuntimeSlotReady(fixture.ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: readyProof, NetworkReadyDigest: readyProof,
		StorageReadyDigest: readyProof, HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	claimID := "claim-resume-atomic-writer"
	acquire := &AcquireRuntimeSlotRequest{
		OperationID: requested.OperationID, ClaimID: claimID, SandboxID: requested.SandboxID,
		FilesystemID: requested.FilesystemID, SourceGenerationID: requested.SourceGenerationID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
		Resources: runtimeSlotTestResources(),
	}
	claimed, err := fixture.store.AcquireRuntimeSlot(fixture.ctx, acquire)
	require.NoError(t, err)

	binding := bytes.Repeat([]byte{0xa2}, 32)
	issue := rootFSWriterGrantTestIssueRequest(
		fixture.sandboxID, "grant-resume-atomic-writer", claimID, registration.SlotID, binding,
	)
	issue.ExpectedFilesystemID = requested.FilesystemID
	issue.InitialGenerationID = requested.SourceGenerationID
	issue.ExpectedWriterEpoch = fixture.writerEpoch
	issue.RuntimeNamespace = registration.AllocationNamespace
	issue.RuntimeID = "slot"
	issue.RuntimeIncarnationID = registration.AllocationID
	issue.NodeName = registration.NodeID
	issue.NodeUID = registration.NodeUID
	issue.NodeBootID = registration.NodeBootID
	issue.RuntimeGeneration = strconv.FormatInt(requested.RuntimeGeneration, 10)

	aborted, err := fixture.store.AbortNomadSandboxResume(
		fixture.ctx, fixture.sandboxID, requested.OperationID, "concurrent planner lost authority",
	)
	require.NoError(t, err)
	require.True(t, aborted)
	_, err = fixture.store.IssueAndBindRuntimeSlotWriterGrant(
		fixture.ctx,
		issue,
		&BindRuntimeSlotWriterGrantRequest{
			SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: claimID, GrantID: issue.GrantID,
		},
	)
	require.ErrorIs(t, err, ErrRuntimeSlotInvalid)
	_, err = fixture.store.GetRootFSWriterGrant(fixture.ctx, issue.GrantID)
	require.ErrorIs(t, err, ErrRootFSWriterGrantNotFound)

	quiescing, err := fixture.store.GetRuntimeSlot(fixture.ctx, claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, quiescing.State)
	require.Empty(t, quiescing.WriterGrantID)
	filesystem, err := fixture.store.GetRootFSFilesystem(fixture.ctx, requested.FilesystemID)
	require.NoError(t, err)
	require.Equal(t, fixture.writerEpoch, filesystem.WriterEpoch,
		"failed atomic bind must roll back the writer epoch and grant")
}

func TestNomadSandboxResumePersistsClaimsAndCommitsExactRuntimeIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume")
	pause := terminalizeNomadPauseFixture(t, fixture)
	missingRetry, found, err := fixture.store.RetryNomadSandboxResume(fixture.ctx, &RetryNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot",
	})
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, missingRetry)

	limit := int64(10)
	requested, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)
	require.False(t, requested.AlreadyActive)
	require.Equal(t, SandboxLifecyclePhasePreparing, requested.LifecyclePhase)
	require.Equal(t, int64(2), requested.RuntimeGeneration)
	require.Equal(t, fixture.filesystem.ID, requested.FilesystemID)
	require.NotEqual(t, pause.OperationID, requested.OperationID)
	initialClaimLimit := int64(1)
	other := rootFSTestSandboxRecord("sandbox-blocked-by-resume", "team-slot")
	other.ClusterID = "cluster-a"
	other.RuntimeGeneration = 1
	_, err = fixture.store.ReserveSandboxClaim(fixture.ctx, &ReserveSandboxClaimRequest{
		Record: other, OperationID: "claim-blocked-by-resume", LeaseTTL: time.Minute,
		ActiveSandboxLimit: &initialClaimLimit,
	})
	require.ErrorIs(t, err, ErrActiveSandboxQuotaExceeded,
		"initial claims must count paused sandboxes with a reserved resume")

	retry, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)
	require.Equal(t, requested.OperationID, retry.OperationID)
	require.Equal(t, requested.SourceGenerationID, retry.SourceGenerationID)
	durableRetry, found, err := fixture.store.RetryNomadSandboxResume(fixture.ctx, &RetryNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot",
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, requested.OperationID, durableRetry.OperationID)
	require.Equal(t, requested.SourceGenerationID, durableRetry.SourceGenerationID)

	runtime := prepareNomadResumeRuntime(t, fixture, requested, "new")
	registration := runtime.registration
	acquire := runtime.acquire
	claimed := runtime.claimed
	_, err = fixture.store.MarkRuntimeSlotCommandReady(fixture.ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		ProcdInstanceID: "procd-nomad-resume", ProcdAddress: "http://192.0.2.10:49983",
		CommandReadyDigest: bytes.Repeat([]byte{0xa4}, 32),
	})
	require.NoError(t, err)

	completeRequest := &CompleteNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, OperationID: requested.OperationID, SlotID: claimed.ID,
		AllocationID: registration.AllocationID, AllocationNamespace: registration.AllocationNamespace,
		ResourceLeaseID: claimed.ResourceLease.LeaseID, ResourceLeaseDigest: claimed.ResourceLeaseDigest,
	}
	completed, err := fixture.store.CompleteNomadSandboxResume(fixture.ctx, completeRequest)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, completed.DesiredState)
	require.Equal(t, requested.RuntimeGeneration, completed.RuntimeGeneration)
	require.Equal(t, registration.AllocationID, completed.RuntimeID)
	require.Equal(t, registration.AllocationNamespace, completed.RuntimeNamespace)
	require.Equal(t, claimed.ResourceLease.CPUMillicores, completed.ResourceMillicpu)
	require.Equal(t, (claimed.ResourceLease.MemoryBytes+(1<<20)-1)/(1<<20), completed.ResourceMemoryMiB)
	wrongLease := *completeRequest
	wrongLease.ResourceLeaseDigest = bytes.Repeat([]byte{0xff}, 32)
	_, err = fixture.store.CompleteNomadSandboxResume(fixture.ctx, &wrongLease)
	require.ErrorIs(t, err, ErrNomadSandboxResumeConflict)

	completedRetry, err := fixture.store.CompleteNomadSandboxResume(fixture.ctx, completeRequest)
	require.NoError(t, err)
	require.Equal(t, completed.RuntimeID, completedRetry.RuntimeID)
	claimedRetry, err := fixture.store.AcquireRuntimeSlot(fixture.ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, claimedRetry.ID)
	alreadyActive, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)
	require.True(t, alreadyActive.AlreadyActive)
	alreadyActiveRetry, found, err := fixture.store.RetryNomadSandboxResume(fixture.ctx, &RetryNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot",
	})
	require.NoError(t, err)
	require.True(t, found)
	require.True(t, alreadyActiveRetry.AlreadyActive)
	pendingRecovery, err := fixture.store.IsRuntimeRecoveryPending(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	require.False(t, pendingRecovery)

	resumedPause, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, resumedPause.SlotID)
	require.Equal(t, requested.OperationID, resumedPause.ClaimOperationID,
		"pause must fence the current resumed slot operation")
	var admissionOperationID string
	require.NoError(t, fixture.pool.QueryRow(fixture.ctx, `
		SELECT operation_id FROM manager.sandbox_runtime_claims WHERE sandbox_id = $1
	`, fixture.sandboxID).Scan(&admissionOperationID))
	require.NotEqual(t, admissionOperationID, resumedPause.ClaimOperationID,
		"the logical admission workflow remains separate from runtime incarnations")
	cleanup, err := fixture.store.RequestSandboxRuntimeClaimCleanup(
		fixture.ctx, fixture.sandboxID, "delete resumed sandbox",
	)
	require.NoError(t, err)
	require.True(t, cleanup.PhysicalStateRequired)
	require.Equal(t, registration.SlotID, cleanup.SlotID,
		"cleanup must follow the current resumed slot rather than the initial claim operation")
	require.Equal(t, RuntimeSlotStateQuiescing, cleanup.SlotState)
}

func TestNomadSandboxResumeStartsNeverRunPausedForkIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume-fork-child")
	terminalizeNomadPauseFixture(t, fixture)
	source, err := fixture.store.GetSandbox(fixture.ctx, fixture.sandboxID)
	require.NoError(t, err)
	target := nomadRunningForkTargetRecord(source, "sandbox-nomad-resume-fork-child")
	operationID := "nomad-resume-fork-child-operation"
	_, err = fixture.store.ForkNomadPausedSandbox(fixture.ctx, &NomadSandboxForkRequest{
		OperationID: operationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: target,
	})
	require.NoError(t, err)

	targetFilesystem, err := fixture.store.GetRootFSFilesystem(fixture.ctx, target.ID)
	require.NoError(t, err)
	childFixture := *fixture
	childFixture.sandboxID = target.ID
	childFixture.filesystem = targetFilesystem
	childFixture.writerEpoch = targetFilesystem.WriterEpoch
	requested, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: target.ID, ExpectedTeamID: target.TeamID,
	})
	require.NoError(t, err)
	require.Equal(t, int64(1), requested.RuntimeGeneration)
	require.Equal(t, targetFilesystem.HeadGenerationID, requested.SourceGenerationID)
	require.True(t, requested.ResetCopiedSessionState)

	runtime := prepareNomadResumeRuntime(t, &childFixture, requested, "fork-child")
	_, err = fixture.store.MarkRuntimeSlotCommandReady(fixture.ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: runtime.claimed.ID, AllocationID: runtime.registration.AllocationID,
		NodeUID: runtime.registration.NodeUID, NodeBootID: runtime.registration.NodeBootID,
		OperationID: runtime.acquire.OperationID, ClaimID: runtime.acquire.ClaimID,
		ProcdInstanceID: "procd-nomad-resume-fork-child", ProcdAddress: "http://192.0.2.12:49983",
		CommandReadyDigest: bytes.Repeat([]byte{0xa5}, 32),
	})
	require.NoError(t, err)
	completed, err := fixture.store.CompleteNomadSandboxResume(fixture.ctx, &CompleteNomadSandboxResumeRequest{
		SandboxID: target.ID, OperationID: requested.OperationID, SlotID: runtime.claimed.ID,
		AllocationID:        runtime.registration.AllocationID,
		AllocationNamespace: runtime.registration.AllocationNamespace,
		ResourceLeaseID:     runtime.claimed.ResourceLease.LeaseID,
		ResourceLeaseDigest: runtime.claimed.ResourceLeaseDigest,
	})
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, completed.DesiredState)
	require.Equal(t, int64(1), completed.RuntimeGeneration)
}

func TestNomadSandboxResumeResetsSessionStateAfterCrossSandboxRestoreIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume-restored-child")
	terminalizeNomadPauseFixture(t, fixture)

	source := rootFSTestSandboxRecord("sandbox-nomad-resume-restore-source", "team-slot")
	source.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, fixture.store.UpsertSandbox(fixture.ctx, source))
	artifact, err := fixture.store.PutReadyRootFSBaseArtifact(fixture.ctx, readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	_, sourceGeneration, err := fixture.store.EnsureInitialRootFSGeneration(
		fixture.ctx,
		&EnsureInitialRootFSGenerationRequest{
			SandboxID: source.ID, TeamID: source.TeamID,
			SourceOCIRef: artifact.SourceOCIRef, SourceOCIDigest: artifact.SourceOCIDigest,
			BaseArtifactDigest: artifact.ArtifactDigest,
		},
	)
	require.NoError(t, err)
	snapshot, err := fixture.store.CreateRootFSSnapshot(fixture.ctx, &CreateRootFSSnapshotRequest{
		SandboxID: source.ID, SnapshotID: "snapshot-nomad-resume-restore-source",
	})
	require.NoError(t, err)
	require.Equal(t, sourceGeneration.ID, snapshot.HeadGenerationID)

	restored, err := fixture.store.RestoreRootFSFromSnapshot(fixture.ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: fixture.sandboxID, SnapshotID: snapshot.ID, TeamID: source.TeamID,
	})
	require.NoError(t, err)
	require.NotEqual(t, sourceGeneration.ID, restored.HeadGenerationID)

	requested, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: source.TeamID,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2), requested.RuntimeGeneration)
	require.Equal(t, restored.HeadGenerationID, requested.SourceGenerationID)
	require.True(t, requested.ResetCopiedSessionState)

	retry, found, err := fixture.store.RetryNomadSandboxResume(fixture.ctx, &RetryNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: source.TeamID,
	})
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, requested.OperationID, retry.OperationID)
	require.True(t, retry.ResetCopiedSessionState)
}

func TestNomadSandboxResumeFencesLateCommandReadyAfterAbortIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume-late-ready")
	terminalizeNomadPauseFixture(t, fixture)
	requested, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot",
	})
	require.NoError(t, err)
	runtime := prepareNomadResumeRuntime(t, fixture, requested, "late-ready")

	aborted, err := fixture.store.AbortNomadSandboxResume(
		fixture.ctx, fixture.sandboxID, requested.OperationID, "planner command-ready deadline elapsed",
	)
	require.NoError(t, err)
	require.True(t, aborted)
	quiescing, err := fixture.store.GetRuntimeSlot(fixture.ctx, runtime.claimed.ID)
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, quiescing.State)
	require.False(t, quiescing.HeartbeatExpiresAt.After(quiescing.AuthorityObservedAt))

	commandReady := &MarkRuntimeSlotCommandReadyRequest{
		SlotID: runtime.claimed.ID, AllocationID: runtime.registration.AllocationID,
		NodeUID: runtime.registration.NodeUID, NodeBootID: runtime.registration.NodeBootID,
		OperationID: runtime.acquire.OperationID, ClaimID: runtime.acquire.ClaimID,
		ProcdInstanceID: "late-procd", ProcdAddress: "http://192.0.2.11:49983",
		CommandReadyDigest: bytes.Repeat([]byte{0xb4}, 32),
	}
	_, err = fixture.store.MarkRuntimeSlotCommandReady(fixture.ctx, commandReady)
	require.ErrorIs(t, err, ErrRuntimeSlotConflict)

	// Recreate the state written by managers that predated the abort fence. A
	// live heartbeat and writer lease must not hide this durable contradiction.
	_, err = fixture.pool.Exec(fixture.ctx, `
		UPDATE manager.runtime_slots
		SET state = $2, revision = revision + 1,
			heartbeat_expires_at = NOW() + INTERVAL '1 minute', quiescing_at = NULL,
			procd_instance_id = $3, procd_address = $4, command_ready_digest = $5,
			command_ready_at = NOW(), updated_at = NOW()
		WHERE slot_id = $1
	`, runtime.claimed.ID, RuntimeSlotStateActive, commandReady.ProcdInstanceID,
		commandReady.ProcdAddress, commandReady.CommandReadyDigest)
	require.NoError(t, err)
	candidates, err := fixture.store.ListRuntimeSlotsForReconcile(fixture.ctx, 10)
	require.NoError(t, err)
	require.Len(t, candidates, 1)
	require.Equal(t, runtime.claimed.ID, candidates[0].ID)

	legacyActive, err := fixture.store.GetRuntimeSlot(fixture.ctx, runtime.claimed.ID)
	require.NoError(t, err)
	fenced, err := fixture.store.FenceRuntimeSlotForReconcile(fixture.ctx, &FenceRuntimeSlotForReconcileRequest{
		SlotID: legacyActive.ID, ExpectedRevision: legacyActive.Revision,
	})
	require.NoError(t, err)
	require.Equal(t, RuntimeSlotStateQuiescing, fenced.State)

	renewRequest := &RenewRootFSWriterGrantRequest{
		GrantID: runtime.issue.GrantID, WriterEpoch: runtime.issued.Grant.WriterEpoch,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: runtime.binding,
		ConsumerNodeUID: runtime.registration.NodeUID,
	}
	policy := RootFSWriterLeaseRenewalPolicy{LeaseTTL: time.Minute, GracePeriod: time.Second}
	_, err = fixture.store.RenewRootFSWriterGrant(fixture.ctx, renewRequest, policy)
	require.ErrorIs(t, err, ErrRootFSWriterGrantInvalidState)
	batch, err := fixture.store.RenewRootFSWriterGrants(
		fixture.ctx, []*RenewRootFSWriterGrantRequest{renewRequest}, policy,
	)
	require.NoError(t, err)
	require.Len(t, batch, 1)
	require.ErrorIs(t, batch[0].Err, ErrRootFSWriterGrantInvalidState)

	crashOperationID := "reconcile-writer-late-ready"
	err = fixture.store.WithSandboxLock(fixture.ctx, fixture.sandboxID, func(
		lockCtx context.Context,
		tx SandboxStoreTx,
		_ *SandboxRecord,
	) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID: crashOperationID, SandboxID: fixture.sandboxID,
			Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing,
			Source: SandboxLifecycleSourceCrash, Cancelable: false,
			FromGeneration:       requested.RuntimeGeneration,
			FromRuntimeNamespace: runtime.registration.AllocationNamespace,
			FromRuntimeID:        runtime.registration.AllocationID,
			ExpectedGenerationID: requested.SourceGenerationID,
		})
	})
	require.NoError(t, err)
	begun, err := fixture.store.BeginRootFSWriterCrashAbandon(fixture.ctx, &BeginRootFSWriterCrashAbandonRequest{
		GrantID: runtime.issue.GrantID, WriterEpoch: runtime.issued.Grant.WriterEpoch,
		OperationID: crashOperationID, BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: runtime.binding, NodeUID: runtime.registration.NodeUID,
		NodeBootID:              runtime.registration.NodeBootID,
		ExpectedOldGenerationID: requested.SourceGenerationID,
	})
	require.NoError(t, err)
	require.Equal(t, RootFSWriterGrantStateRetiring, begun.State,
		"the durable slot fence must revoke a live writer lease without waiting for wall-clock expiry")
}

func TestNomadSandboxResumeWaitsForTerminalRuntimeAndReservesQuotaIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume-gates")
	pause, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	fixture.publishPlannedPause(t, pause.OperationID)
	limit := int64(10)
	_, err = fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.ErrorIs(t, err, ErrNomadSandboxResumeNotReady)
	terminalizeNomadPauseSlot(t, fixture, pause)

	zero := int64(0)
	_, err = fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &zero,
	})
	require.ErrorIs(t, err, ErrActiveSandboxQuotaExceeded)
	active, getErr := fixture.store.GetActiveLifecycleTxn(fixture.ctx, fixture.sandboxID)
	require.NoError(t, getErr)
	require.Nil(t, active)
}

type preparedNomadResumeRuntime struct {
	registration *RegisterRuntimeSlotRequest
	acquire      *AcquireRuntimeSlotRequest
	claimed      *RuntimeSlot
	issue        *IssueRootFSWriterGrantRequest
	issued       *IssuedRootFSWriterGrant
	binding      []byte
}

func prepareNomadResumeRuntime(
	t *testing.T,
	fixture *nomadPauseStoreFixture,
	requested *NomadSandboxResumeCandidate,
	suffix string,
) *preparedNomadResumeRuntime {
	t.Helper()
	registration := runtimeSlotTestRegistration("slot-nomad-resume-"+suffix, "allocation-nomad-resume-"+suffix)
	registration.AllocationNamespace = "nomad"
	_, err := registerRuntimeSlotWithTestCapacity(t, fixture.ctx, fixture.store, registration)
	require.NoError(t, err)
	readyProof := bytes.Repeat([]byte{0xa1}, 32)
	_, err = fixture.store.ReportRuntimeSlotReady(fixture.ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: readyProof, NetworkReadyDigest: readyProof,
		StorageReadyDigest: readyProof, HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	claimID := "claim-nomad-resume-" + suffix
	acquire := &AcquireRuntimeSlotRequest{
		OperationID: requested.OperationID, ClaimID: claimID, SandboxID: requested.SandboxID,
		FilesystemID: requested.FilesystemID, SourceGenerationID: requested.SourceGenerationID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
		Resources: runtimeSlotTestResources(),
	}
	claimed, err := fixture.store.AcquireRuntimeSlot(fixture.ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, registration.SlotID, claimed.ID)

	binding := bytes.Repeat([]byte{0xa2}, 32)
	issue := rootFSWriterGrantTestIssueRequest(
		fixture.sandboxID, "grant-nomad-resume-"+suffix, claimID, registration.SlotID, binding,
	)
	issue.ExpectedFilesystemID = requested.FilesystemID
	issue.InitialGenerationID = requested.SourceGenerationID
	issue.ExpectedWriterEpoch = fixture.writerEpoch
	issue.RuntimeNamespace = registration.AllocationNamespace
	issue.RuntimeID = "slot"
	issue.RuntimeIncarnationID = registration.AllocationID
	issue.NodeName = registration.NodeID
	issue.NodeUID = registration.NodeUID
	issue.NodeBootID = registration.NodeBootID
	issue.RuntimeGeneration = strconv.FormatInt(requested.RuntimeGeneration, 10)
	issued, err := fixture.store.IssueRootFSWriterGrant(fixture.ctx, issue)
	require.NoError(t, err)
	_, err = fixture.store.BindRuntimeSlotWriterGrant(fixture.ctx, &BindRuntimeSlotWriterGrantRequest{
		SlotID: claimed.ID, OperationID: acquire.OperationID, ClaimID: claimID, GrantID: issue.GrantID,
	})
	require.NoError(t, err)
	_, err = fixture.store.ConsumeRootFSWriterGrant(fixture.ctx, &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding,
		ConsumerNodeUID: registration.NodeUID, ConsumerAgentUID: "ctld-resume", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = fixture.store.StartRuntimeSlot(fixture.ctx, &StartRuntimeSlotRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		LaunchAttempt: "launch-nomad-resume-" + suffix, RunscContainerID: "runsc-nomad-resume-" + suffix,
		RootFSBindingDigest: binding, ClaimNetworkDigest: bytes.Repeat([]byte{0xa3}, 32),
		ResourceLeaseID: claimed.ResourceLease.LeaseID, ResourceLeaseDigest: claimed.ResourceLeaseDigest,
	})
	require.NoError(t, err)
	return &preparedNomadResumeRuntime{
		registration: registration, acquire: acquire, claimed: claimed,
		issue: issue, issued: issued, binding: binding,
	}
}

func terminalizeNomadPauseFixture(t *testing.T, fixture *nomadPauseStoreFixture) *NomadSandboxPauseCandidate {
	t.Helper()
	pause, err := fixture.store.RequestNomadSandboxPause(
		fixture.ctx, fixture.sandboxID, SandboxLifecycleSourceManual,
	)
	require.NoError(t, err)
	fixture.publishPlannedPause(t, pause.OperationID)
	terminalizeNomadPauseSlot(t, fixture, pause)
	return pause
}

func terminalizeNomadPauseSlot(
	t *testing.T,
	fixture *nomadPauseStoreFixture,
	pause *NomadSandboxPauseCandidate,
) {
	t.Helper()
	slot, err := fixture.store.GetRuntimeSlot(fixture.ctx, pause.SlotID)
	require.NoError(t, err)
	_, err = fixture.store.BeginRuntimeSlotQuiesce(fixture.ctx, &BeginRuntimeSlotQuiesceRequest{
		SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
	})
	require.NoError(t, err)
	_, err = fixture.store.MarkRuntimeSlotAllocationMissing(fixture.ctx, &MarkRuntimeSlotAllocationMissingRequest{
		SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID,
		NodeBootID: slot.NodeBootID, ObservationDigest: bytes.Repeat([]byte{0xb1}, 32),
	})
	require.NoError(t, err)
	_, err = fixture.store.FinalizeRuntimeSlot(fixture.ctx, &FinalizeRuntimeSlotRequest{
		SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
		Reason: "planned_publish", ProofDigest: bytes.Repeat([]byte{0xb2}, 32),
		ResourceLeaseID:     slot.ResourceLease.LeaseID,
		ResourceLeaseDigest: slot.ResourceLeaseDigest, ResourceCgroupAbsent: true,
	})
	require.NoError(t, err)
}
