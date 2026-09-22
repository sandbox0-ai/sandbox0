package driver

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestMigrationRestoredCPULineagePreservesGuestOnWiderHost(t *testing.T) {
	h, claim, runner, c, fixture := migrationRestoreHandleFixture(t)
	source := claim.MigrationRestore.Image.Publication.CPULaunch.Clone()
	runner.cpuObservation.Profile.Features = append(append([]string(nil), source.Observation.Profile.Features...), "zzz_extra")
	require.NoError(t, h.Claim(claim))
	state, err := readPersistedState(h.statePath())
	require.NoError(t, err)
	launch := state.Claim.MigrationCPULaunch
	require.NoError(t, launch.ValidateRestore(*claim.MigrationRestore))
	require.Equal(t, source.GuestCPUProfile(), launch.GuestCPUProfile())
	require.Contains(t, launch.Observation.Profile.Features, "zzz_extra")
	require.NotContains(t, launch.GuestCPUProfile().Features, "zzz_extra")
	clone := launch.Clone()
	clone.Restored.GuestProfile.Features[0] = "changed"
	require.Equal(t, source.GuestCPUProfile(), launch.GuestCPUProfile(), "lineage snapshots must own their feature slices")

	adoption := installBackgroundAdoption(t, claim, c, fixture)
	h.closeMu.Lock()
	err = h.refreshMigrationAdoption()
	h.closeMu.Unlock()
	require.NoError(t, err)
	metadata := h.PersistedState().Claim
	capture := protocol.MigrationCaptureRequest{Target: adoption.Target, OperationID: "next-migration", LifecycleEpoch: 4,
		SandboxID: metadata.SandboxID, SourceGeneration: claim.Runtime.RuntimeGeneration, AssignmentRevision: metadata.RuntimeRevision,
		BindingDigest: metadata.RootFSBindingDigest, ResourceLeaseDigest: metadata.ResourceLeaseDigest, ProcdInstanceID: adoption.ProcdInstanceID}
	require.NoError(t, h.checkMigrationCaptureCPU(t.Context(), capture, false), "a restored source validates against its current host, while retaining original guest exposure")
	for _, mode := range []string{"missing-lineage", "widened-guest", "narrowed-guest", "other-source", "other-restore"} {
		t.Run(mode, func(t *testing.T) {
			changed := launch.Clone()
			switch mode {
			case "missing-lineage":
				changed.Restored = nil
			case "widened-guest":
				changed.Restored.GuestProfile = changed.Observation.Profile
			case "narrowed-guest":
				changed.Restored.GuestProfile.Features = []string{"aes"}
			case "other-source":
				changed.Restored.SourceLaunchDigest = "sha256:" + strings.Repeat("f", 64)
			case "other-restore":
				changed.Restored.RestoreRequestDigest = strings.Repeat("f", 64)
			}
			require.NoError(t, changed.Validate(), "plausible standalone shape does not establish restore provenance")
			h.mu.Lock()
			h.claim.MigrationCPULaunch = changed
			h.mu.Unlock()
			require.ErrorIs(t, h.checkMigrationCaptureCPU(t.Context(), capture, false), errdefs.ErrFailedPrecondition)
		})
	}
	h.mu.Lock()
	h.claim.MigrationCPULaunch = launch
	h.mu.Unlock()
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
	require.NoError(t, h.Claim(claim), "an exact active retry must not remeasure or replace lineage")
	retried := h.PersistedState().Claim.MigrationCPULaunch
	require.Equal(t, launch, retried)
	payload, err := json.Marshal(retried)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "writer_grant_token")
	require.Equal(t, 1, strings.Count(string(payload), "source_launch_digest"), "only one predecessor link is carried")
}

func TestMigrationRestoreRejectsCompatibleHostChangesDuringExecution(t *testing.T) {
	h, claim, runner, c, _ := migrationRestoreHandleFixture(t)
	runner.beforeRestore = func() {
		runner.cpuObservation.Profile.Features = append(runner.cpuObservation.Profile.Features, "zzz_extra")
	}
	require.ErrorContains(t, h.Claim(claim), "destination CPU profile or coverage changed during restore")
	require.Nil(t, h.PersistedState().Claim.MigrationCPULaunch)
	custody, err := c.GetMigrationDestination(t.Context(), claim.Resources.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "restore"))
	require.NotContains(t, runner.callsSnapshot(), "start")
}
