package gvisorcli

import (
	"context"
	"errors"
	"strings"
	"testing"

	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type preflightCPUObserver struct {
	executable             string
	executableCalls        int
	changeAfterObservation bool
	cancelAfterObservation context.CancelFunc
	calls                  int
	cpus                   string
	result                 protocol.MigrationCPUObservation
	err                    error
	cancel                 context.CancelFunc
}

func (o *preflightCPUObserver) ExecutableDigest(context.Context) (string, error) {
	o.executableCalls++
	if o.calls > 0 && o.cancelAfterObservation != nil {
		o.cancelAfterObservation()
	}
	if o.changeAfterObservation && o.calls > 0 {
		return "sha256:" + strings.Repeat("f", 64), nil
	}
	if o.executable != "" {
		return o.executable, nil
	}
	return "sha256:" + strings.Repeat("e", 64), nil
}

func (o *preflightCPUObserver) CPUCoverage(_ context.Context, cpus string) (protocol.MigrationCPUObservation, error) {
	o.calls++
	o.cpus = cpus
	if o.cancel != nil {
		o.cancel()
	}
	return o.result, o.err
}
func cpuPreflightFixture(t *testing.T) (*protocol.MigrationCPULaunch, protocol.MigrationCaptureRequest, protocol.RuntimeResourceLease) {
	t.Helper()
	resources, err := protocol.NewRuntimeResourceLease("claim-operation", "claim", "slot", "cluster", "node", "uid", "boot",
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit}, "0-3", "0")
	require.NoError(t, err)
	leaseDigest, err := resources.Digest()
	require.NoError(t, err)
	digest := strings.TrimPrefix(leaseDigest, "sha256:")
	launch := &protocol.MigrationCPULaunch{Version: protocol.MigrationCPULaunchVersion, ExecutableDigest: "sha256:" + strings.Repeat("e", 64),
		Target:    protocol.NodeChannelTarget{SlotID: "slot", ClusterID: "cluster", AllocationID: "alloc", NodeID: "node", NodeUID: "uid", NodeBootID: "boot", ControlEndpoint: "unix:///private/source.sock"},
		SandboxID: "sandbox", RuntimeGeneration: 1, LaunchAttempt: "launch", BindingDigest: strings.Repeat("a", 64), ResourceLeaseDigest: digest, Resources: resources, AssignmentRevision: strings.Repeat("b", 64),
		Observation: protocol.MigrationCPUObservation{CPUSet: "0-3", Profile: protocol.MigrationCPUProfile{Version: 1, Architecture: "arm64", RunscVersion: cpuLaunchSupportedVersion, Features: []string{"aes", "fp"}}}}
	capture := protocol.MigrationCaptureRequest{Target: launch.Target, OperationID: "migration", LifecycleEpoch: 2, SandboxID: "sandbox", SourceGeneration: 1,
		AssignmentRevision: launch.AssignmentRevision, BindingDigest: launch.BindingDigest, ResourceLeaseDigest: digest, ProcdInstanceID: "procd"}
	require.NoError(t, launch.ValidateCapture(capture, "launch", resources))
	return launch, capture, resources
}

func TestRestoredCPUPreflightUsesGuestExposureForDestination(t *testing.T) {
	launch, capture, resources := cpuPreflightFixture(t)
	guest := launch.GuestCPUProfile()
	launch.RuntimeGeneration, capture.SourceGeneration = 2, 2
	launch.Restored = &protocol.MigrationCPURestoreLineage{SourceLaunchDigest: "sha256:" + strings.Repeat("c", 64), RestoreRequestDigest: strings.Repeat("d", 64), GuestProfile: guest}
	launch.Observation.Profile.Features = append(append([]string(nil), guest.Features...), "zzz_host_only")
	observer := &preflightCPUObserver{result: protocol.MigrationCPUObservation{CPUSet: resources.CPUSetCPUs, Profile: guest}}
	_, err := CheckMigrationTargetCPU(t.Context(), observer, launch, resources)
	require.NoError(t, err, "the next host does not need features never exposed to the guest")
	_, err = CheckMigrationSourceCPU(t.Context(), observer, launch, capture, launch.LaunchAttempt, resources)
	require.Error(t, err, "the current source must still match its own restore-time host observation")
	observer.result = launch.Observation
	_, err = CheckMigrationSourceCPU(t.Context(), observer, launch, capture, launch.LaunchAttempt, resources)
	require.NoError(t, err)
	observer.result.Profile.Features = []string{"fp"}
	_, err = CheckMigrationTargetCPU(t.Context(), observer, launch, resources)
	require.Error(t, err, "inherited guest features remain mandatory")
}

func TestSourceCPUPreflightRejectsMissingOrChangedHistoryBeforeMeasurement(t *testing.T) {
	launch, capture, resources := cpuPreflightFixture(t)
	observer := &preflightCPUObserver{result: launch.Observation}
	_, err := CheckMigrationSourceCPU(t.Context(), observer, nil, capture, "launch", resources)
	require.Error(t, err)
	changed := capture
	changed.Target.NodeBootID = "new-boot"
	_, err = CheckMigrationSourceCPU(t.Context(), observer, launch, changed, "launch", resources)
	require.Error(t, err)
	_, err = CheckMigrationSourceCPU(t.Context(), observer, launch, capture, "restarted", resources)
	require.Error(t, err)
	require.Zero(t, observer.calls)
	current, err := CheckMigrationSourceCPU(t.Context(), observer, launch, capture, "launch", resources)
	require.NoError(t, err)
	require.Equal(t, "0-3", observer.cpus)
	require.Equal(t, launch.Observation, *current)
	observer.result.Profile.Features = []string{"aes", "fp", "new"}
	_, err = CheckMigrationSourceCPU(t.Context(), observer, launch, capture, "launch", resources)
	require.Error(t, err)
	require.Equal(t, []string{"aes", "fp"}, launch.Observation.Profile.Features)
}

func TestTargetCPUPreflightUsesTargetCoverageAndAllowsFeatureSuperset(t *testing.T) {
	launch, _, source := cpuPreflightFixture(t)
	target, err := protocol.NewRuntimeResourceLease("migration", "target-claim", "target-slot", source.ClusterID, "target-node", "target-uid", "target-boot",
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit}, "8-11", "0")
	require.NoError(t, err)
	observer := &preflightCPUObserver{result: protocol.MigrationCPUObservation{CPUSet: "8-11", Profile: launch.Observation.Profile}}
	observer.result.Profile.Features = []string{"aes", "fp", "sha2"}
	result, err := CheckMigrationTargetCPU(t.Context(), observer, launch, target)
	require.NoError(t, err)
	require.Equal(t, "8-11", observer.cpus)
	require.Equal(t, "8-11", result.CPUSet)
	observer.result.CPUSet = "8-9"
	result, err = CheckMigrationTargetCPU(t.Context(), observer, launch, target)
	require.Error(t, err)
	require.Nil(t, result)
	observer.result.CPUSet = "8-11"
	observer.result.Profile.Features = []string{"fp"}
	_, err = CheckMigrationTargetCPU(t.Context(), observer, launch, target)
	require.Error(t, err)
}

func TestPlanningTargetCPUPreflightRequiresWholeFixedCPUSetAndGuestFeatures(t *testing.T) {
	launch, _, _ := cpuPreflightFixture(t)
	observer := &preflightCPUObserver{result: protocol.MigrationCPUObservation{
		CPUSet: "0-7", Profile: launch.Observation.Profile,
	}}
	result, err := CheckMigrationPlanningTargetCPU(t.Context(), observer, launch, "0-7")
	require.NoError(t, err)
	require.Equal(t, "0-7", observer.cpus)
	require.Equal(t, "0-7", result.CPUSet)
	observer.result.CPUSet = "0-3"
	_, err = CheckMigrationPlanningTargetCPU(t.Context(), observer, launch, "0-7")
	require.Error(t, err)
	observer.result.CPUSet = "0-7"
	observer.result.Profile.Features = []string{"fp"}
	_, err = CheckMigrationPlanningTargetCPU(t.Context(), observer, launch, "0-7")
	require.Error(t, err)
}

func TestCPUPreflightNeverReturnsPartialOrCanceledEvidence(t *testing.T) {
	launch, capture, resources := cpuPreflightFixture(t)
	for _, target := range []bool{false, true} {
		check := func(ctx context.Context, observer *preflightCPUObserver) (*protocol.MigrationCPUObservation, error) {
			if target {
				return CheckMigrationTargetCPU(ctx, observer, launch, resources)
			}
			return CheckMigrationSourceCPU(ctx, observer, launch, capture, "launch", resources)
		}
		observer := &preflightCPUObserver{result: launch.Observation, err: errors.New("observation failed")}
		result, err := check(t.Context(), observer)
		require.Error(t, err)
		require.Nil(t, result)
		ctx, cancel := context.WithCancel(t.Context())
		observer = &preflightCPUObserver{result: launch.Observation, cancel: cancel}
		result, err = check(ctx, observer)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result)
		_, err = check(ctx, observer)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, observer.calls, "already-canceled requests never measure")
		ctx, cancel = context.WithCancel(t.Context())
		observer = &preflightCPUObserver{result: launch.Observation, cancelAfterObservation: cancel}
		result, err = check(ctx, observer)
		require.ErrorIs(t, err, context.Canceled)
		require.Nil(t, result, "final executable check cannot return canceled evidence")
	}
}

func TestCPUPreflightRejectsArtifactReplacementBeforeAndDuringObservation(t *testing.T) {
	launch, capture, resources := cpuPreflightFixture(t)
	observer := &preflightCPUObserver{result: launch.Observation, executable: "sha256:" + strings.Repeat("f", 64)}
	result, err := CheckMigrationSourceCPU(t.Context(), observer, launch, capture, "launch", resources)
	require.Error(t, err)
	require.Nil(t, result)
	require.Zero(t, observer.calls)
	observer = &preflightCPUObserver{result: launch.Observation, changeAfterObservation: true}
	result, err = CheckMigrationTargetCPU(t.Context(), observer, launch, resources)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 1, observer.calls)
}

func TestCPUPreflightRejectsUnqualifiedReleaseWithoutCaptureOrObservation(t *testing.T) {
	for _, version := range []string{"runsc version release-20260817.0", "runsc version release-20260921.0", "runsc test"} {
		launch, capture, resources := cpuPreflightFixture(t)
		launch.Observation.Profile.RunscVersion = version
		require.NoError(t, launch.Validate(), "historical metadata remains readable for recovery")
		observer := &preflightCPUObserver{result: launch.Observation}
		_, err := CheckMigrationSourceCPU(t.Context(), observer, launch, capture, "launch", resources)
		require.ErrorContains(t, err, "qualified stock runsc")
		_, err = CheckMigrationTargetCPU(t.Context(), observer, launch, resources)
		require.ErrorContains(t, err, "qualified stock runsc")
		require.Zero(t, observer.calls)
		require.Zero(t, observer.executableCalls)
	}
}
