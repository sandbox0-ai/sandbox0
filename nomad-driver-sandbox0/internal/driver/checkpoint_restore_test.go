package driver

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRestoreUsesSharedRestoreWithoutColdStartFallback(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) { testCheckpointRestoreClaim(t, kind) })
	}
}

func testCheckpointRestoreClaim(t *testing.T, kind runtimecontrol.CheckpointRestoreKind) {
	t.Helper()
	h, claim, runner, custodian, fixture := migrationRestoreHandleCPUFixture(t, true, kind)
	image := claim.MigrationRestore.Image
	preflight := protocol.MigrationCPUPreflightRequest{
		Checkpoint: &image.Checkpoint.Assignment,
		Target:     image.Target, Destination: image.Target, DestinationResources: image.Resources,
		Source: image.Publication.Capture.Request, SourceResources: image.Publication.CPULaunch.Resources,
		Launch: image.Publication.CPULaunch,
	}
	observed, err := h.PreflightMigrationCPU(t.Context(), preflight)
	require.NoError(t, err)
	require.NoError(t, observed.ValidateFor(preflight))
	require.Empty(t, runner.callsSnapshot(), "CPU admission cannot start or restore a workload")
	require.NoError(t, h.Claim(claim))
	require.NoError(t, h.Claim(claim))
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	require.NotContains(t, runner.callsSnapshot(), "start")
	persisted, err := readPersistedState(h.statePath())
	require.NoError(t, err)
	require.NoError(t, persisted.Claim.MigrationCPULaunch.ValidateRestore(*claim.MigrationRestore))
	require.Equal(t, kind, persisted.Claim.MigrationCPULaunch.Restored.CheckpointKind)
	require.Equal(t, image.Publication.CPULaunch.GuestCPUProfile(), persisted.Claim.MigrationCPULaunch.GuestCPUProfile())
	ready := commandReadyProof(fixture, *claim.Stage)
	ready.ProcdInstanceID = image.Publication.Capture.Request.ProcdInstanceID
	require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: ready}))
	custody, err := custodian.GetMigrationDestination(t.Context(), image.Target.SlotID)
	require.NoError(t, err)
	require.True(t, custody.Adopted())
	require.NotEmpty(t, custody.Adoption.Request.CheckpointRestoreDigest)
	metadata := h.PersistedState().Claim
	next := protocol.MigrationCaptureRequest{Target: image.Target, OperationID: "next-memory-pause", LifecycleEpoch: 4,
		SandboxID: metadata.SandboxID, SourceGeneration: claim.Runtime.RuntimeGeneration, AssignmentRevision: metadata.RuntimeRevision,
		BindingDigest: metadata.RootFSBindingDigest, ResourceLeaseDigest: metadata.ResourceLeaseDigest, ProcdInstanceID: ready.ProcdInstanceID}
	_, err = h.PreflightMigrationCPU(t.Context(), protocol.MigrationCPUPreflightRequest{CaptureOnly: true,
		Target: image.Target, Source: next, SourceResources: claim.Resources})
	require.NoError(t, err, "future pause or migration must preserve the original guest CPU lineage")
	require.NoError(t, h.Signal("USR1"), "adopted memory runtime follows ordinary process lifecycle")
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
}

func TestCheckpointResumeUncertainExecutionNeverReplaysImage(t *testing.T) {
	h, claim, runner, custodian, _ := migrationRestoreHandleCPUFixture(t, true, runtimecontrol.CheckpointResume)
	custodian.failComplete = true
	require.ErrorContains(t, h.Claim(claim), "completion response lost")
	require.ErrorContains(t, h.Claim(claim), "current phase migrating")
	require.Eventually(t, func() bool { return contains(runner.callsSnapshot(), "kill:KILL") }, 3*time.Second, 10*time.Millisecond)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	require.NotContains(t, runner.callsSnapshot(), "start")
	custody, err := custodian.GetMigrationDestination(t.Context(), claim.MigrationRestore.Image.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
	require.Nil(t, custody.Adoption)
}

func TestCheckpointReadinessCanPrecedeRegionalCommitWithoutReleasingCustody(t *testing.T) {
	for _, kind := range []runtimecontrol.CheckpointRestoreKind{runtimecontrol.CheckpointResume, runtimecontrol.CheckpointFork} {
		t.Run(string(kind), func(t *testing.T) {
			h, claim, runner, custodian, fixture := migrationRestoreHandleCPUFixture(t, true, kind)
			fixture.authority.mu.Lock()
			adoption := fixture.authority.adoption
			fixture.authority.adoption = nil
			fixture.authority.mu.Unlock()
			require.NoError(t, h.Claim(claim))
			ready := commandReadyProof(fixture, *claim.Stage)
			ready.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
			require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: ready}))
			require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: ready}), "lost ready reply must not require an uncommitted adoption")
			custody, err := custodian.GetMigrationDestination(t.Context(), claim.MigrationRestore.Image.Target.SlotID)
			require.NoError(t, err)
			require.False(t, custody.Adopted())
			require.Nil(t, h.PersistedState().Claim.MigrationAdoption)
			require.Error(t, h.Signal("USR1"), "readiness alone cannot unlock ordinary controls")
			changed := ready
			changed.ProcdInstanceID = "replacement-procd"
			require.Error(t, h.CommandReady(CommandReadyRequest{Proof: changed}))
			fixture.authority.mu.Lock()
			fixture.authority.adoption = adoption
			fixture.authority.mu.Unlock()
			require.NoError(t, h.CommandReady(CommandReadyRequest{Proof: ready}))
			custody, err = custodian.GetMigrationDestination(t.Context(), claim.MigrationRestore.Image.Target.SlotID)
			require.NoError(t, err)
			require.True(t, custody.Adopted())
			require.NoError(t, h.Signal("USR1"))
			require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
		})
	}
}

func TestMigrationReadinessStillRequiresRegionalAdoption(t *testing.T) {
	h, claim, _, _, fixture := migrationRestoreHandleCPUFixture(t, true, "")
	fixture.authority.mu.Lock()
	fixture.authority.adoption = nil
	fixture.authority.mu.Unlock()
	require.NoError(t, h.Claim(claim))
	ready := commandReadyProof(fixture, *claim.Stage)
	ready.ProcdInstanceID = claim.MigrationRestore.Image.Publication.Capture.Request.ProcdInstanceID
	require.ErrorContains(t, h.CommandReady(CommandReadyRequest{Proof: ready}), "lacks exact migration adoption")
}
