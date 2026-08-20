package sandboxstore

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNomadSandboxResumeOperationIDIsDeterministic(t *testing.T) {
	first := NomadSandboxResumeOperationID("sandbox-1", 7, "generation-1")
	require.Equal(t, first, NomadSandboxResumeOperationID("sandbox-1", 7, "generation-1"))
	require.NotEqual(t, first, NomadSandboxResumeOperationID("sandbox-1", 8, "generation-1"))
	require.NotEqual(t, first, NomadSandboxResumeOperationID("sandbox-1", 7, "generation-2"))
}

func TestNomadSandboxResumePersistsClaimsAndCommitsExactRuntimeIntegration(t *testing.T) {
	fixture := newNomadPauseStoreFixture(t, "resume")
	pause := terminalizeNomadPauseFixture(t, fixture)

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
	other.RuntimeBackend = SandboxRuntimeBackendNomad
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

	registration := runtimeSlotTestRegistration("slot-nomad-resume-new", "allocation-nomad-resume-new")
	registration.AllocationNamespace = "nomad"
	_, err = fixture.store.RegisterRuntimeSlot(fixture.ctx, registration)
	require.NoError(t, err)
	readyProof := bytes.Repeat([]byte{0xa1}, 32)
	_, err = fixture.store.ReportRuntimeSlotReady(fixture.ctx, &ReportRuntimeSlotReadyRequest{
		SlotID: registration.SlotID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		RuntimeReadyDigest: readyProof, NetworkReadyDigest: readyProof,
		StorageReadyDigest: readyProof, HeartbeatTTL: time.Minute,
	})
	require.NoError(t, err)
	claimID := "claim-nomad-resume-new"
	acquire := &AcquireRuntimeSlotRequest{
		OperationID: requested.OperationID, ClaimID: claimID, SandboxID: requested.SandboxID,
		FilesystemID: requested.FilesystemID, SourceGenerationID: requested.SourceGenerationID,
		CompatibilityDigest: registration.CompatibilityDigest, ClusterID: registration.ClusterID,
		RuntimeAssignmentRevision: strings.Repeat("ab", 32),
		NetworkPolicyDigest:       "sha256:" + strings.Repeat("cd", 32), ClaimTTL: time.Minute,
	}
	claimed, err := fixture.store.AcquireRuntimeSlot(fixture.ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, registration.SlotID, claimed.ID)

	binding := bytes.Repeat([]byte{0xa2}, 32)
	issue := rootFSWriterGrantTestIssueRequest(
		fixture.sandboxID, "grant-nomad-resume-new", claimID, registration.SlotID, binding,
	)
	issue.ExpectedFilesystemID = requested.FilesystemID
	issue.InitialGenerationID = requested.SourceGenerationID
	issue.ExpectedWriterEpoch = fixture.writerEpoch
	issue.PodNamespace = registration.AllocationNamespace
	issue.PodName = "slot"
	issue.PodUID = registration.AllocationID
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
		ConsumerNodeUID: registration.NodeUID, ConsumerCtldPodUID: "ctld-resume", LeaseTTL: time.Minute,
	})
	require.NoError(t, err)
	_, err = fixture.store.StartRuntimeSlot(fixture.ctx, &StartRuntimeSlotRequest{
		SlotID: claimed.ID, AllocationID: registration.AllocationID,
		NodeUID: registration.NodeUID, NodeBootID: registration.NodeBootID,
		OperationID: acquire.OperationID, ClaimID: acquire.ClaimID,
		LaunchAttempt: "launch-nomad-resume", RunscContainerID: "runsc-nomad-resume",
		RootFSBindingDigest: binding, ClaimNetworkDigest: bytes.Repeat([]byte{0xa3}, 32),
	})
	require.NoError(t, err)
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
	}
	completed, err := fixture.store.CompleteNomadSandboxResume(fixture.ctx, completeRequest)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, completed.DesiredState)
	require.Equal(t, requested.RuntimeGeneration, completed.RuntimeGeneration)
	require.Equal(t, registration.AllocationID, completed.CurrentPodName)
	require.Equal(t, registration.AllocationNamespace, completed.CurrentPodNamespace)

	completedRetry, err := fixture.store.CompleteNomadSandboxResume(fixture.ctx, completeRequest)
	require.NoError(t, err)
	require.Equal(t, completed.CurrentPodName, completedRetry.CurrentPodName)
	claimedRetry, err := fixture.store.AcquireRuntimeSlot(fixture.ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, claimed.ID, claimedRetry.ID)
	alreadyActive, err := fixture.store.RequestNomadSandboxResume(fixture.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: fixture.sandboxID, ExpectedTeamID: "team-slot", ActiveSandboxLimit: &limit,
	})
	require.NoError(t, err)
	require.True(t, alreadyActive.AlreadyActive)
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
	})
	require.NoError(t, err)
}
