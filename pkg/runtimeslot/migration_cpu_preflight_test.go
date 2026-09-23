package runtimeslot

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	"github.com/stretchr/testify/require"
)

func TestCheckpointRestoreCPUPreflightUsesArchivedSourceAndNewCarrier(t *testing.T) {
	launch, source, claim := cpuLaunchFixture(t)
	capture, err := runtimecontrol.NewCheckpointCaptureAssignment(source.OperationID, *claim.Runtime)
	require.NoError(t, err)
	assignment := *claim.Runtime
	assignment.RuntimeGeneration++
	restore := runtimecontrol.CheckpointRestoreAssignment{OperationID: "later-resume", Kind: runtimecontrol.CheckpointResume, Capture: capture, Target: assignment}
	target := source.Target
	target.SlotID, target.AllocationID, target.ControlEndpoint = "restore-slot", "restore-allocation", "unix:///restore/control.sock"
	resources, err := NewRuntimeResourceLease(restore.OperationID, "restore-claim", target.SlotID,
		target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		RuntimeResourceRequest{Version: RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: DefaultRuntimePIDsLimit}, launch.Observation.CPUSet, "0")
	require.NoError(t, err)
	request := MigrationCPUPreflightRequest{Checkpoint: &restore, Target: target, Destination: target,
		DestinationResources: resources, Source: source, SourceResources: claim.Resources, Launch: &launch}
	digest, err := request.Digest()
	require.NoError(t, err)
	result := MigrationCPUPreflight{RequestDigest: digest, Launch: launch, Observation: launch.Observation}
	require.NoError(t, result.ValidateFor(request))
	command, err := NewNodeChannelMigrationCPUPreflightCommand(request)
	require.NoError(t, err)
	require.NoError(t, command.Validate())
	for _, mutate := range []func(*MigrationCPUPreflightRequest){
		func(r *MigrationCPUPreflightRequest) { r.Checkpoint = nil },
		func(r *MigrationCPUPreflightRequest) { r.Target = source.Target },
		func(r *MigrationCPUPreflightRequest) { r.CaptureOnly = true },
		func(r *MigrationCPUPreflightRequest) { r.Checkpoint.Capture.Revision = assignment.SecurityClass },
		func(r *MigrationCPUPreflightRequest) { r.Checkpoint.OperationID = source.OperationID },
		func(r *MigrationCPUPreflightRequest) { r.Checkpoint.Target.TeamID = "another-team" },
		func(r *MigrationCPUPreflightRequest) { r.Launch = nil },
	} {
		copy := request
		a := *request.Checkpoint
		copy.Checkpoint = &a
		mutate(&copy)
		require.Error(t, copy.Validate())
	}
}

func TestCheckpointCPUPreflightDoesNotReserveADestination(t *testing.T) {
	paired, oldResult := cpuPreflightProtocolFixture(t)
	request := paired
	request.CaptureOnly = true
	request.Destination = NodeChannelTarget{}
	request.DestinationResources = RuntimeResourceLease{}
	digest, err := request.Digest()
	require.NoError(t, err)
	result := oldResult
	result.RequestDigest = digest
	require.NoError(t, result.ValidateFor(request))
	require.Error(t, oldResult.ValidateFor(request), "paired evidence cannot authorize a different observation")
	command, err := NewNodeChannelMigrationCPUPreflightCommand(request)
	require.NoError(t, err)
	require.NoError(t, command.Validate())

	for _, mutate := range []func(*MigrationCPUPreflightRequest){
		func(r *MigrationCPUPreflightRequest) { r.CaptureOnly = false },
		func(r *MigrationCPUPreflightRequest) { r.Target = paired.Destination },
		func(r *MigrationCPUPreflightRequest) { r.Destination = paired.Destination },
		func(r *MigrationCPUPreflightRequest) { r.DestinationResources = paired.DestinationResources },
		func(r *MigrationCPUPreflightRequest) { r.Launch = &result.Launch },
		func(r *MigrationCPUPreflightRequest) { r.SourceResources.MemoryBytes++ },
		func(r *MigrationCPUPreflightRequest) { r.SourceResources.NodeBootID = "rebooted" },
	} {
		changed := request
		mutate(&changed)
		require.Error(t, changed.Validate())
	}
	payload, err := json.Marshal(paired)
	require.NoError(t, err)
	require.NotContains(t, string(payload), "capture_only", "old migration command digests remain unchanged")
}

func cpuPreflightProtocolFixture(t *testing.T) (MigrationCPUPreflightRequest, MigrationCPUPreflight) {
	t.Helper()
	launch, source, claim := cpuLaunchFixture(t)
	destination := NodeChannelTarget{SlotID: "target-slot", AllocationID: "target-allocation", ClusterID: source.Target.ClusterID,
		NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", ControlEndpoint: "unix:///private/target.sock"}
	resources, err := NewRuntimeResourceLease(source.OperationID, "target-claim", destination.SlotID, destination.ClusterID,
		destination.NodeID, destination.NodeUID, destination.NodeBootID,
		RuntimeResourceRequest{Version: RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: DefaultRuntimePIDsLimit}, "8-11", "0")
	require.NoError(t, err)
	request := MigrationCPUPreflightRequest{Target: source.Target, Source: source, SourceResources: claim.Resources, Destination: destination, DestinationResources: resources}
	digest, err := request.Digest()
	require.NoError(t, err)
	result := MigrationCPUPreflight{RequestDigest: digest, Launch: launch, Observation: launch.Observation}
	require.NoError(t, result.ValidateFor(request))
	return request, result
}

func TestMigrationCPUPreflightBindsBothNodesAndHistoricalEvidence(t *testing.T) {
	source, sourceResult := cpuPreflightProtocolFixture(t)
	for _, mutate := range []func(*MigrationCPUPreflightRequest){
		func(r *MigrationCPUPreflightRequest) { r.Source.LifecycleEpoch++ },
		func(r *MigrationCPUPreflightRequest) {
			r.Destination.NodeBootID = "rebooted"
			r.DestinationResources.NodeBootID = "rebooted"
		},
		func(r *MigrationCPUPreflightRequest) {
			r.Destination.ControlEndpoint = "unix:///private/replacement.sock"
		},
		func(r *MigrationCPUPreflightRequest) { r.DestinationResources.CPUSetCPUs = "12-15" },
		func(r *MigrationCPUPreflightRequest) { r.SourceResources.MemoryBytes++ },
	} {
		changed := source
		mutate(&changed)
		require.Error(t, sourceResult.ValidateFor(changed))
	}
	changed := source
	changed.Launch = &sourceResult.Launch
	require.Error(t, changed.Validate(), "source cannot accept caller-supplied launch history")
	changed = source
	changed.Destination.NodeUID = source.Source.Target.NodeUID
	changed.DestinationResources.NodeUID = source.Source.Target.NodeUID
	require.Error(t, changed.Validate(), "different Nomad IDs cannot migrate within the same physical node")
	target := source
	target.Target, target.Launch = target.Destination, &sourceResult.Launch
	digest, err := target.Digest()
	require.NoError(t, err)
	result := MigrationCPUPreflight{RequestDigest: digest, Launch: sourceResult.Launch, Observation: sourceResult.Observation}
	result.Observation.CPUSet = target.DestinationResources.CPUSetCPUs
	require.NoError(t, result.ValidateFor(target))
	result.Launch.ExecutableDigest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.Error(t, result.ValidateFor(target), "destination cannot substitute its own source history")
}

type cpuPreflightChannelExecutor struct {
	result   *MigrationCPUPreflight
	err      error
	deadline time.Duration
}

func (e *cpuPreflightChannelExecutor) PreflightMigrationCPU(ctx context.Context, _ MigrationCPUPreflightRequest) (*MigrationCPUPreflight, error) {
	deadline, _ := ctx.Deadline()
	e.deadline = time.Until(deadline)
	return e.result, e.err
}

func TestMigrationCPUPreflightChannelCannotGrantExecutionOrLeakPartialResults(t *testing.T) {
	request, observation := cpuPreflightProtocolFixture(t)
	command, err := NewNodeChannelMigrationCPUPreflightCommand(request)
	require.NoError(t, err)
	executor := &cpuPreflightChannelExecutor{result: &observation}
	agent := &NodeChannelAgent{config: NodeChannelAgentConfig{OperationTimeout: time.Second, MigrationCPUPreflightExecutor: executor}}
	result := agent.execute(t.Context(), command)
	require.NoError(t, result.ValidateFor(command))
	require.Greater(t, executor.deadline, time.Minute)
	require.LessOrEqual(t, executor.deadline, MigrationCPUPreflightTimeout)
	result.ControlResponse = &NodeControlResponse{Phase: string(StateActive)}
	require.Error(t, result.ValidateFor(command))
	control := NodeControlResponse{Phase: "cpu_preflight", MigrationCPUPreflight: &observation}
	require.NoError(t, control.validateCPUPreflightResult(request))
	require.Error(t, control.Validate())
	control.Migration = &MigrationCapture{}
	require.Error(t, control.validateCPUPreflightResult(request))
	executor.err = errors.New("observation interrupted")
	result = agent.execute(t.Context(), command)
	require.NotEmpty(t, result.Error)
	require.Nil(t, result.MigrationCPUPreflight)
	require.NoError(t, result.ValidateFor(command))
	command.MigrationCapture = &request.Source
	require.Error(t, command.Validate())
}
