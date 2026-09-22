package runtimeslot

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
