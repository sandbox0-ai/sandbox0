package driver

import (
	"strings"
	"testing"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestMigrationCaptureCPUFailureBeforeIntentLeavesSourceUntouched(t *testing.T) {
	for _, mode := range []string{"unqualified-release", "missing-history", "changed-binary", "changed-cpu", "changed-lease", "fenced-during-probe", "history-changed-during-probe"} {
		t.Run(mode, func(t *testing.T) {
			h, request, runner, custodian := migrationHandleFixture(t)
			switch mode {
			case "unqualified-release":
				h.claim.MigrationCPULaunch.Observation.Profile.RunscVersion = "runsc version release-20260817.0"
			case "missing-history":
				h.claim.MigrationCPULaunch = nil
			case "changed-binary":
				runner.executable = "sha256:" + strings.Repeat("f", 64)
			case "changed-cpu":
				runner.cpuObservation.Profile.Features = []string{"fp"}
			case "changed-lease":
				h.claim.MigrationCPULaunch.Resources.CPUSetCPUs = "0-7"
			case "fenced-during-probe":
				runner.cpuObserve = func() { h.mu.Lock(); h.migrationAdmissionFenced = true; h.mu.Unlock() }
			case "history-changed-during-probe":
				runner.cpuObserve = func() { h.mu.Lock(); h.claim.MigrationCPULaunch = nil; h.mu.Unlock() }
			}
			_, err := h.CaptureMigration(t.Context(), request)
			require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
			require.Nil(t, h.PersistedState().Migration)
			require.Equal(t, phaseActive, h.PersistedState().Phase)
			custody, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
			require.NoError(t, err)
			require.Nil(t, custody)
			require.Empty(t, runner.callsSnapshot(), "a rejected CPU check cannot signal or checkpoint the guest")
		})
	}
}

func TestMigrationCaptureRechecksCPUAfterIntentAndNeverReplays(t *testing.T) {
	h, request, runner, custodian := migrationHandleFixture(t)
	calls := 0
	runner.cpuObserve = func() {
		calls++
		if calls == 2 {
			runner.executable = "sha256:" + strings.Repeat("f", 64)
		}
	}
	result, err := awaitMigrationCapture(t, h, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, result.State)
	require.Equal(t, 2, calls)
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
	custody, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, custody.Capture.State)
	for range 2 {
		retry, err := h.CaptureMigration(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, result, retry)
	}
	require.Equal(t, 2, calls, "recovery observes custody; it does not repeat CPU validation or execution")
}

func TestMigrationRestoreRechecksCPUAcrossExecutionBoundaries(t *testing.T) {
	for _, boundary := range []int{1, 2, 3} {
		name := map[int]string{1: "before-intent", 2: "before-restore", 3: "after-restore"}[boundary]
		t.Run(name, func(t *testing.T) {
			h, claim, runner, custodian, _ := migrationRestoreHandleFixture(t)
			calls := 0
			runner.cpuObserve = func() {
				calls++
				if calls == boundary {
					runner.cpuObservation.Profile.Features = []string{"fp"}
				}
			}
			require.ErrorIs(t, h.Claim(claim), errdefs.ErrFailedPrecondition)
			require.Equal(t, boundary, calls)
			wantRestores := 0
			if boundary == 3 {
				wantRestores = 1
			}
			require.Equal(t, wantRestores, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.Error(t, h.Claim(claim))
			require.Equal(t, wantRestores, countMigrationCall(runner.callsSnapshot(), "restore"))
			require.NotContains(t, runner.callsSnapshot(), "start")
			custody, err := custodian.GetMigrationDestination(t.Context(), claim.Resources.SlotID)
			require.NoError(t, err)
			require.NotNil(t, custody.Restore)
			require.Equal(t, protocol.MigrationRestoreUncertain, custody.Restore.State)
			require.Equal(t, phaseMigrating, h.PersistedState().Phase)
		})
	}
}

func TestMigrationRestoreRequiresPublishedSourceCPUHistory(t *testing.T) {
	h, claim, runner, _, _ := migrationRestoreHandleCPUFixture(t, false)
	require.NoError(t, claim.MigrationRestore.Validate(), "legacy metadata remains readable")
	require.ErrorContains(t, h.Claim(claim), "restore lacks source CPU launch history")
	require.NotContains(t, runner.callsSnapshot(), "create")
	require.NotContains(t, runner.callsSnapshot(), "restore")
	require.NotContains(t, runner.callsSnapshot(), "start")
}

func TestMigrationPublicationBindsFullSourceCPUHistory(t *testing.T) {
	_, claim, _, _, _ := migrationRestoreHandleFixture(t)
	publication := claim.MigrationRestore.Image.Publication
	before, err := publication.Digest()
	require.NoError(t, err)
	launch := *publication.CPULaunch
	publication.CPULaunch = &launch
	launch.Resources.CPUSetCPUs = "0-7"
	_, err = publication.Binding()
	require.Error(t, err)
	launch = *claim.MigrationRestore.Image.Publication.CPULaunch
	publication.CPUFeaturesDigest = "sha256:" + strings.Repeat("f", 64)
	_, err = publication.Binding()
	require.Error(t, err)
	publication.CPUFeaturesDigest = claim.MigrationRestore.Image.Publication.CPUFeaturesDigest
	launch.ExecutableDigest = "sha256:" + strings.Repeat("a", 64)
	after, err := publication.Digest()
	require.NoError(t, err)
	require.NotEqual(t, before, after, "the publication receipt binds binary history as well as feature names")
	require.Error(t, claim.MigrationRestore.Image.Receipt.ValidateFor(publication))
}
