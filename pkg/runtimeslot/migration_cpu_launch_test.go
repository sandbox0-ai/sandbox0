package runtimeslot

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func cpuLaunchFixture(t *testing.T) (MigrationCPULaunch, MigrationCaptureRequest, NodeClaimControlRequest) {
	t.Helper()
	claim := testNodeClaimControlRequest()
	claim.Stage.Identity.RuntimeGeneration = "1"
	claim.Stage.Identity.TaskName = NomadTaskName
	target := testNodeChannelTarget(true)
	observation := MigrationCPUObservation{Profile: testMigrationCPUProfile(), CPUSet: "0-3"}
	launch, err := BindMigrationCPULaunch(target, claim, observation, "sha256:"+strings.Repeat("e", 64))
	require.NoError(t, err)
	capture := MigrationCaptureRequest{Target: target, OperationID: "migration-next", LifecycleEpoch: 2,
		SandboxID: launch.SandboxID, SourceGeneration: launch.RuntimeGeneration, AssignmentRevision: launch.AssignmentRevision,
		BindingDigest: launch.BindingDigest, ResourceLeaseDigest: launch.ResourceLeaseDigest, ProcdInstanceID: "original-procd"}
	require.NoError(t, launch.ValidateCapture(capture, claim.Stage.Identity.LaunchAttempt, claim.Resources))
	return *launch, capture, claim
}

func TestCPULaunchRetainsOnlyBoundEvidence(t *testing.T) {
	launch, capture, claim := cpuLaunchFixture(t)
	payload, err := json.Marshal(launch)
	require.NoError(t, err)
	require.NotContains(t, string(payload), claim.PolicyToken)
	require.NotContains(t, string(payload), "env_vars")
	var restored MigrationCPULaunch
	require.NoError(t, json.Unmarshal(payload, &restored))
	require.NoError(t, restored.ValidateCapture(capture, claim.Stage.Identity.LaunchAttempt, claim.Resources))
	digest, err := restored.Digest()
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(digest, "sha256:"))
	profile := MigrationCPUObservation{Profile: testMigrationCPUProfile(), CPUSet: "0-3"}
	bound, err := BindMigrationCPULaunch(launch.Target, claim, profile, launch.ExecutableDigest)
	require.NoError(t, err)
	original, _ := bound.Digest()
	profile.Profile.Features[0] = "changed"
	after, err := bound.Digest()
	require.NoError(t, err)
	require.Equal(t, original, after, "binding owns its immutable feature list")
}

func TestCPULaunchRejectsSourceIdentityReuse(t *testing.T) {
	launch, capture, claim := cpuLaunchFixture(t)
	for _, mutate := range []func(*MigrationCPULaunch){
		func(l *MigrationCPULaunch) { l.ExecutableDigest = "" },
		func(l *MigrationCPULaunch) { l.ExecutableDigest = "sha512:" + strings.Repeat("e", 128) },
		func(l *MigrationCPULaunch) { l.Target.NodeBootID = "next-boot" },
		func(l *MigrationCPULaunch) { l.Target.NodeUID = "other-node" },
		func(l *MigrationCPULaunch) { l.Target.AllocationID = "replacement" },
		func(l *MigrationCPULaunch) { l.Target.ControlEndpoint = "unix:///other.sock" },
		func(l *MigrationCPULaunch) { l.Target.ClusterID = "other-cluster" },
		func(l *MigrationCPULaunch) { l.SandboxID = "foreign" },
		func(l *MigrationCPULaunch) { l.RuntimeGeneration++ },
		func(l *MigrationCPULaunch) { l.LaunchAttempt = "second-launch" },
		func(l *MigrationCPULaunch) { l.BindingDigest = strings.Repeat("a", 64) },
		func(l *MigrationCPULaunch) { l.AssignmentRevision = strings.Repeat("b", 64) },
		func(l *MigrationCPULaunch) { l.ResourceLeaseDigest = strings.Repeat("c", 64) },
		func(l *MigrationCPULaunch) { l.Observation.CPUSet = "0-1" },
		func(l *MigrationCPULaunch) { l.Resources.CPUSetCPUs = "0-7" },
		func(l *MigrationCPULaunch) { l.Resources = RuntimeResourceLease{} },
	} {
		changed := launch
		mutate(&changed)
		require.Error(t, changed.ValidateCapture(capture, claim.Stage.Identity.LaunchAttempt, claim.Resources))
	}
	changedResources := claim.Resources
	changedResources.MemoryBytes++
	require.Error(t, launch.ValidateCapture(capture, claim.Stage.Identity.LaunchAttempt, changedResources))
	claim.Stage.Identity.RuntimeGeneration = "2"
	_, err := BindMigrationCPULaunch(launch.Target, claim, launch.Observation, launch.ExecutableDigest)
	require.Error(t, err, "assignment and Stage generation must agree")
}

func TestCPULaunchCurrentSourceCannotBackfillOrWidenHistory(t *testing.T) {
	launch, _, _ := cpuLaunchFixture(t)
	current := MigrationCPUObservation{Profile: testMigrationCPUProfile(), CPUSet: "0-7"}
	require.NoError(t, launch.CheckCurrentSource(current, "0-3"))
	require.Error(t, launch.CheckCurrentSource(current, "0-7"), "new observation cannot widen historical coverage")
	require.Error(t, (MigrationCPULaunch{}).CheckCurrentSource(current, "0-3"))
	current.Profile.Features = append(current.Profile.Features, "zzz_new_feature")
	require.NoError(t, CheckMigrationCPUProfiles(launch.Observation.Profile, current.Profile), "destination may support additional features")
	require.Error(t, launch.CheckCurrentSource(current, "0-3"), "source observation must preserve its launch profile")
	current = MigrationCPUObservation{Profile: testMigrationCPUProfile(), CPUSet: "0-1"}
	require.Error(t, launch.CheckCurrentSource(current, "0-3"))
}
