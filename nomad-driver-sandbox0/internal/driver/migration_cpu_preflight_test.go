package driver

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type preflightCPURunsc struct {
	*launchCPURunsc
	observe func()
	calls   int
}

func (r *preflightCPURunsc) ExecutableDigest(context.Context) (string, error) {
	r.calls++
	return "sha256:" + strings.Repeat("e", 64), nil
}

func (r *preflightCPURunsc) CPUCoverage(context.Context, string) (protocol.MigrationCPUObservation, error) {
	r.calls++
	if r.observe != nil {
		r.observe()
	}
	observation, _, err := r.Complete(context.Background())
	return *observation, err
}

var _ gvisorcli.CPUPreflightRunsc = (*preflightCPURunsc)(nil)

func migrationCPUPreflightHandle(t *testing.T) (*taskHandle, protocol.MigrationCPUPreflightRequest, *preflightCPURunsc) {
	t.Helper()
	fixture := newRuntimeSlotPluginFixture(t)
	runner := &preflightCPURunsc{launchCPURunsc: &launchCPURunsc{Runsc: fixture.runner, recorder: fixture.runner}}
	fixture.plugin.newRunner = func(PluginConfig) Runsc { return runner }
	handle, stage, token, policy, _ := prepareRuntimeSlotClaim(t, fixture)
	t.Cleanup(func() {
		handle.stopExitWatch()
		handle.stopConsumerRenewal()
		fixture.plugin.cancel()
		handle.stopControl()
	})
	stage.Identity.RuntimeGeneration = "1"
	resources := runtimeSlotResourceLease(t, fixture, stage)
	require.NoError(t, handle.Claim(ClaimRequest{OperationID: "operation-1", ClaimID: "claim-1", PolicyToken: token,
		WriterEpoch: "1", Stage: &stage, NetworkPolicy: policy, Runtime: runtimeSlotAssignment(), Resources: resources}))
	proof := commandReadyProof(fixture, stage)
	require.NoError(t, handle.CommandReady(CommandReadyRequest{Proof: proof}))
	handle.mu.Lock()
	launch := *handle.claim.MigrationCPULaunch
	handle.mu.Unlock()
	source := protocol.MigrationCaptureRequest{Target: launch.Target, OperationID: "migration-1", LifecycleEpoch: 2,
		SandboxID: launch.SandboxID, SourceGeneration: launch.RuntimeGeneration, ProcdInstanceID: proof.ProcdInstanceID,
		AssignmentRevision: launch.AssignmentRevision, BindingDigest: launch.BindingDigest, ResourceLeaseDigest: launch.ResourceLeaseDigest}
	target := protocol.NodeChannelTarget{SlotID: "target-slot", AllocationID: "target-allocation", ClusterID: launch.Target.ClusterID,
		NodeID: "target-node", NodeUID: "target-uid", NodeBootID: "target-boot", ControlEndpoint: "unix:///private/target.sock"}
	targetResources, err := protocol.NewRuntimeResourceLease(source.OperationID, "target-claim", target.SlotID, target.ClusterID,
		target.NodeID, target.NodeUID, target.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit}, "0-3", "0")
	require.NoError(t, err)
	request := protocol.MigrationCPUPreflightRequest{Target: source.Target, Source: source, SourceResources: resources,
		Destination: target, DestinationResources: targetResources}
	require.NoError(t, request.Validate())
	return handle, request, runner
}

func TestMigrationCPUPreflightUsesDurableSourceWithoutChangingExecution(t *testing.T) {
	handle, request, runner := migrationCPUPreflightHandle(t)
	before, err := readPersistedState(handle.statePath())
	require.NoError(t, err)
	result, err := handle.PreflightMigrationCPU(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.ValidateFor(request))
	require.Equal(t, *before.Claim.MigrationCPULaunch, result.Launch)
	require.Equal(t, 3, runner.calls)
	after, err := readPersistedState(handle.statePath())
	require.NoError(t, err)
	require.Equal(t, before, after, "read-only evidence must not change durable runtime authority")
	require.Equal(t, 1, countMigrationCall(runner.recorder.callsSnapshot(), "create"))
	require.Equal(t, 1, countMigrationCall(runner.recorder.callsSnapshot(), "start"))
	require.NotContains(t, runner.recorder.callsSnapshot(), "checkpoint")
	// Changing even a structurally valid source must be rejected before probing.
	request.Source.ProcdInstanceID = "replacement-procd"
	result, err = handle.PreflightMigrationCPU(t.Context(), request)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 3, runner.calls)
}

func TestMigrationCPUPreflightCannotInventHistoryOrRaceAdmissionFence(t *testing.T) {
	for _, mode := range []string{"missing-history", "busy", "fenced-during-observation", "history-changed-during-observation"} {
		t.Run(mode, func(t *testing.T) {
			handle, request, runner := migrationCPUPreflightHandle(t)
			switch mode {
			case "missing-history":
				handle.mu.Lock()
				handle.claim.MigrationCPULaunch = nil
				handle.mu.Unlock()
			case "busy":
				handle.closeMu.Lock()
				defer handle.closeMu.Unlock()
			default:
				runner.observe = func() {
					handle.mu.Lock()
					defer handle.mu.Unlock()
					if mode == "fenced-during-observation" {
						handle.migrationAdmissionFenced = true
					} else {
						handle.claim.MigrationCPULaunch.ExecutableDigest = "sha256:" + strings.Repeat("a", 64)
					}
				}
			}
			result, err := handle.PreflightMigrationCPU(t.Context(), request)
			require.Error(t, err)
			require.Nil(t, result)
			if mode == "missing-history" || mode == "busy" {
				require.Zero(t, runner.calls)
			}
		})
	}
}

func TestMigrationCPUPreflightDestinationRemainsUnclaimed(t *testing.T) {
	source, request, _ := migrationCPUPreflightHandle(t)
	observed, err := source.PreflightMigrationCPU(t.Context(), request)
	require.NoError(t, err)
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.task.ID, fixture.task.AllocID, fixture.task.NodeID = "target-slot", "target-allocation", "target-node"
	fixture.authority.heartbeatTTL = runtimeSlotMaxHeartbeatTTL
	runner := &preflightCPURunsc{launchCPURunsc: &launchCPURunsc{Runsc: fixture.runner, recorder: fixture.runner}}
	fixture.plugin.newRunner = func(PluginConfig) Runsc { return runner }
	_, _, err = fixture.plugin.StartTask(fixture.task)
	require.NoError(t, err)
	handle, ok := fixture.plugin.tasks.Get(fixture.task.ID)
	require.True(t, ok)
	t.Cleanup(func() { fixture.plugin.cancel(); handle.stopControl() })
	request.Destination.ControlEndpoint = "unix://" + handle.socketPath
	request.Destination.NodeBootID, request.DestinationResources.NodeBootID = "boot-1", "boot-1"
	request.Target, request.Launch = request.Destination, &observed.Launch
	result, err := handle.PreflightMigrationCPU(t.Context(), request)
	require.NoError(t, err)
	require.NoError(t, result.ValidateFor(request))
	state, err := readPersistedState(handle.statePath())
	require.NoError(t, err)
	require.Equal(t, phaseWarm, state.Phase)
	require.Nil(t, state.Claim)
	require.NotContains(t, fixture.runner.callsSnapshot(), "create")
	require.NotContains(t, fixture.runner.callsSnapshot(), "start")
	// Once another claim owns this carrier, prior target evidence is unusable.
	handle.mu.Lock()
	handle.phase = phaseActive
	handle.mu.Unlock()
	result, err = handle.PreflightMigrationCPU(t.Context(), request)
	require.Error(t, err)
	require.Nil(t, result)
	require.Equal(t, 3, runner.calls)
}

func TestMigrationCPUPreflightControlSocketReturnsSeparateEvidence(t *testing.T) {
	handle, request, _ := migrationCPUPreflightHandle(t)
	client := unixHTTPClient(handle.socketPath)
	body, err := json.Marshal(request)
	require.NoError(t, err)
	response, err := awaitControl(client, http.MethodPut, protocol.NodeMigrationCPUPreflightControlPath, body, protocol.MigrationCPUPreflightTimeout)
	require.NoError(t, err)
	var result protocol.NodeControlResponse
	require.NoError(t, json.Unmarshal(response, &result))
	require.Equal(t, "cpu_preflight", result.Phase)
	require.NotNil(t, result.MigrationCPUPreflight)
	require.NoError(t, result.MigrationCPUPreflight.ValidateFor(request))
	require.Error(t, result.Validate(), "CPU evidence cannot be accepted as ordinary readiness")
}
