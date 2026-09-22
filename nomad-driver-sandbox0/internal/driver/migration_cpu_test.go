package driver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type launchCPURunsc struct {
	Runsc
	recorder *fakeRunsc
	mode     string
}

func TestMigrationCPULaunchRewarmsRecoveredUnusedCarrier(t *testing.T) {
	for _, mode := range []string{"success", "warm-failure"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newRuntimeSlotPluginFixture(t)
			original, stage, token, policy, _ := prepareRuntimeSlotClaim(t, fixture)
			nomad := drivers.NewTaskHandle(taskHandleVersion)
			nomad.Config = fixture.task
			require.NoError(t, nomad.SetDriverState(original.PersistedState()))
			nomad = roundTripNomadTaskHandleDatabase(t, nomad)
			fixture.plugin.cancel()
			original.stopControl()
			raw := newFakeRunsc()
			runner := &launchCPURunsc{Runsc: raw, recorder: raw, mode: mode}
			plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return runner }).(*Plugin)
			plugin.config, plugin.rootfs = fixture.config, fixture.rootfs
			plugin.rootfsOnce.Do(func() {})
			plugin.newSlotAuthority = func(*PluginConfig) (runtimeSlotAuthority, error) { return fixture.authority, nil }
			t.Cleanup(plugin.cancel)
			require.NoError(t, plugin.RecoverTask(nomad))
			require.Equal(t, []string{"cpu-warm"}, raw.callsSnapshot(), "recovery must prepare CPU evidence without creating a guest")
			require.NoError(t, plugin.RecoverTask(nomad))
			require.Equal(t, 1, countMigrationCall(raw.callsSnapshot(), "cpu-warm"))
			handle, ok := plugin.tasks.Get(fixture.task.ID)
			require.True(t, ok)
			handle.mounter = &fakeMounter{}
			t.Cleanup(func() { handle.stopExitWatch(); handle.stopConsumerRenewal(); handle.stopControl() })
			stage.Identity.RuntimeGeneration = "1"
			require.NoError(t, handle.Claim(ClaimRequest{OperationID: "operation-1", ClaimID: "claim-1", PolicyToken: token, WriterEpoch: "1",
				Stage: &stage, NetworkPolicy: policy, Runtime: runtimeSlotAssignment(), Resources: runtimeSlotResourceLease(t, fixture, stage)}))
			state, err := readPersistedState(handle.statePath())
			require.NoError(t, err)
			if mode == "success" {
				require.NotNil(t, state.Claim.MigrationCPULaunch)
				require.NoError(t, state.Claim.MigrationCPULaunch.Validate())
			} else {
				require.Nil(t, state.Claim.MigrationCPULaunch, "failed warming cannot invent migration eligibility")
			}
			require.Equal(t, phaseActive, state.Phase)
			require.Equal(t, 1, countMigrationCall(raw.callsSnapshot(), "create"))
			require.Equal(t, 1, countMigrationCall(raw.callsSnapshot(), "start"))
		})
	}
}

func (r *launchCPURunsc) PrepareCPULaunch(context.Context) error {
	r.recorder.record("cpu-warm")
	if r.mode == "warm-failure" {
		return errors.New("observation unavailable")
	}
	return nil
}

func (r *launchCPURunsc) BeginCPULaunch(ctx context.Context, _ string) (gvisorcli.CPULaunchVerifier, error) {
	r.recorder.record("cpu-begin")
	if r.mode == "warm-failure" || r.mode == "begin-failure" {
		return nil, errors.New("no trustworthy warm observation")
	}
	if r.mode == "begin-timeout" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r, nil
}

func (r *launchCPURunsc) Complete(context.Context) (*protocol.MigrationCPUObservation, string, error) {
	r.recorder.record("cpu-complete")
	if r.mode == "complete-failure" {
		return nil, "", errors.New("launch capabilities changed")
	}
	observation := &protocol.MigrationCPUObservation{CPUSet: "0-3", Profile: protocol.MigrationCPUProfile{
		Version: protocol.MigrationCPUProfileVersion, Architecture: "arm64", RunscVersion: "runsc version release-20260914.0", Features: []string{"aes"},
	}}
	if r.mode == "invalid-evidence" {
		observation.CPUSet = "0"
	}
	return observation, "sha256:" + strings.Repeat("e", 64), nil
}

func (r *launchCPURunsc) Create(ctx context.Context, bundle, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.Runsc.Create(ctx, bundle, id)
}

// This test goes through real warm registration, claim validation and durable
// state writes. Losing a claim response must not fabricate another observation.
func TestMigrationCPULaunchRecordsActualStartAndPreservesOrdinaryClaims(t *testing.T) {
	for _, mode := range []string{"success", "warm-failure", "begin-failure", "begin-timeout", "complete-failure", "invalid-evidence", "create-failure", "start-failure"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newRuntimeSlotPluginFixture(t)
			runner := &launchCPURunsc{Runsc: fixture.runner, recorder: fixture.runner, mode: mode}
			fixture.plugin.newRunner = func(PluginConfig) Runsc { return runner }
			// Warm evidence is optional and precedes regional readiness.
			fixture.authority.readyHook = func() {
				require.Contains(t, fixture.runner.callsSnapshot(), "cpu-warm")
			}
			handle, stage, token, policy, _ := prepareRuntimeSlotClaim(t, fixture)
			t.Cleanup(func() {
				handle.stopExitWatch()
				handle.stopConsumerRenewal()
				fixture.plugin.cancel()
				handle.stopControl()
			})
			stage.Identity.RuntimeGeneration = "1"
			claim := ClaimRequest{OperationID: "operation-1", ClaimID: "claim-1", PolicyToken: token, WriterEpoch: "1",
				Stage: &stage, NetworkPolicy: policy, Runtime: runtimeSlotAssignment(), Resources: runtimeSlotResourceLease(t, fixture, stage)}
			if mode == "create-failure" {
				fixture.runner.createErr = errors.New("create failed")
			}
			if mode == "start-failure" {
				fixture.runner.startErr = errors.New("start failed")
			}
			err := handle.Claim(claim)
			failed := mode == "create-failure" || mode == "start-failure"
			if failed {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			state, err := readPersistedState(handle.statePath())
			require.NoError(t, err)
			require.NotNil(t, state.Claim)
			calls := fixture.runner.callsSnapshot()
			if mode == "success" {
				launch := state.Claim.MigrationCPULaunch
				require.NotNil(t, launch)
				require.NoError(t, launch.Validate())
				require.Equal(t, stage.Identity.LaunchAttempt, launch.LaunchAttempt)
				require.Equal(t, "unix://"+handle.socketPath, launch.Target.ControlEndpoint)
				require.Equal(t, fixture.task.ID, launch.Target.SlotID)
				var sequence []string
				for _, call := range calls {
					if call == "create" || call == "start" || strings.HasPrefix(call, "cpu-") {
						sequence = append(sequence, call)
					}
				}
				require.Equal(t, []string{"cpu-warm", "cpu-begin", "create", "start", "cpu-complete"}, sequence)
			} else {
				require.Nil(t, state.Claim.MigrationCPULaunch)
			}
			if failed {
				require.NotContains(t, calls, "cpu-complete", "failed launch cannot produce history")
				return
			}
			require.Equal(t, phaseActive, state.Phase)
			require.NoError(t, handle.Claim(claim))
			retried, err := readPersistedState(handle.statePath())
			require.NoError(t, err)
			require.Equal(t, state.Claim.MigrationCPULaunch, retried.Claim.MigrationCPULaunch)
			require.Equal(t, 1, countMigrationCall(fixture.runner.callsSnapshot(), "cpu-begin"))
			require.Equal(t, 1, countMigrationCall(fixture.runner.callsSnapshot(), "create"))
		})
	}
}

type restoredLaunchCPURunsc struct {
	*launchCPURunsc
	restored *restoreRunsc
}

func (r *restoredLaunchCPURunsc) Restore(ctx context.Context, id, directory string) error {
	return r.restored.Restore(ctx, id, directory)
}

func (r *restoredLaunchCPURunsc) Checkpoint(ctx context.Context, id, directory string) error {
	return r.restored.Checkpoint(ctx, id, directory)
}

func (r *restoredLaunchCPURunsc) ExecutableDigest(ctx context.Context) (string, error) {
	return r.restored.ExecutableDigest(ctx)
}
func (r *restoredLaunchCPURunsc) CPUCoverage(ctx context.Context, cpus string) (protocol.MigrationCPUObservation, error) {
	return r.restored.CPUCoverage(ctx, cpus)
}

var _ gvisorcli.CheckpointRunsc = (*restoredLaunchCPURunsc)(nil)

func TestMigrationRestoredClaimCannotInventOrdinaryCPULaunch(t *testing.T) {
	handle, claim, runner, _, _ := migrationRestoreHandleFixture(t)
	handle.runner = &restoredLaunchCPURunsc{launchCPURunsc: &launchCPURunsc{Runsc: runner, recorder: runner.fakeRunsc}, restored: runner}
	require.NoError(t, handle.Claim(claim))
	state, err := readPersistedState(handle.statePath())
	require.NoError(t, err)
	require.NotNil(t, state.Claim.MigrationCPULaunch)
	require.NotNil(t, state.Claim.MigrationCPULaunch.Restored)
	require.NoError(t, state.Claim.MigrationCPULaunch.ValidateRestore(*claim.MigrationRestore))
	require.Equal(t, claim.MigrationRestore.Image.Publication.CPULaunch.GuestCPUProfile(), state.Claim.MigrationCPULaunch.GuestCPUProfile())
	require.NotContains(t, runner.callsSnapshot(), "cpu-begin")
	require.NotContains(t, runner.callsSnapshot(), "cpu-complete")
	require.Contains(t, runner.callsSnapshot(), "restore")
}
