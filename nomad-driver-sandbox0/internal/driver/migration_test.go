package driver

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type migrationRunsc struct {
	*fakeRunsc
	before         func()
	failure        error
	cpuObserve     func()
	cpuObservation protocol.MigrationCPUObservation
	executable     string
}

func (r *migrationRunsc) ExecutableDigest(ctx context.Context) (string, error) {
	return r.executable, ctx.Err()
}

func (r *migrationRunsc) CPUCoverage(ctx context.Context, cpus string) (protocol.MigrationCPUObservation, error) {
	if r.cpuObserve != nil {
		r.cpuObserve()
	}
	result := r.cpuObservation
	result.CPUSet = cpus
	return result, ctx.Err()
}

// Controlled evidence for lifecycle fault tests, not host CPU attestation.
func migrationExecutionCPUFixture(t *testing.T, capture *protocol.MigrationCaptureRequest, attempt string) *protocol.MigrationCPULaunch {
	t.Helper()
	target := capture.Target
	resources, err := protocol.NewRuntimeResourceLease("source-claim-operation", "source-claim", target.SlotID, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID,
		protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: protocol.DefaultRuntimePIDsLimit}, "0-3", "0")
	require.NoError(t, err)
	resourceDigest, err := resources.Digest()
	require.NoError(t, err)
	capture.ResourceLeaseDigest = strings.TrimPrefix(resourceDigest, "sha256:")
	launch := &protocol.MigrationCPULaunch{Version: protocol.MigrationCPULaunchVersion, ExecutableDigest: "sha256:" + strings.Repeat("e", 64),
		Target: target, SandboxID: capture.SandboxID, RuntimeGeneration: capture.SourceGeneration, LaunchAttempt: attempt,
		BindingDigest: capture.BindingDigest, ResourceLeaseDigest: capture.ResourceLeaseDigest, Resources: resources, AssignmentRevision: capture.AssignmentRevision,
		Observation: protocol.MigrationCPUObservation{CPUSet: "0-3", Profile: protocol.MigrationCPUProfile{Version: 1, Architecture: "arm64", RunscVersion: "runsc version release-20260914.0", Features: []string{"aes", "fp"}}}}
	require.NoError(t, launch.ValidateCapture(*capture, attempt, resources))
	return launch
}

func (r *migrationRunsc) Checkpoint(ctx context.Context, id, path string) error {
	r.record("checkpoint")
	if r.before != nil {
		r.before()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Mkdir(path, 0o700); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(path, "checkpoint.img"), []byte("private-memory"), 0o600); err != nil {
		return err
	}
	if r.failure != nil {
		return r.failure
	}
	return r.Kill(ctx, id, "KILL")
}

func (r *migrationRunsc) Restore(context.Context, string, string) error {
	r.record("restore")
	return nil
}

type migrationCustodian struct {
	RootFSRuntime
	mu            sync.Mutex
	root          string
	custody       *nomadruntime.MigrationCaptureCustody
	recordFailure error
}

func (r *migrationCustodian) GetMigrationCapture(context.Context, string) (*nomadruntime.MigrationCaptureCustody, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.custody == nil {
		return nil, nil
	}
	clone := *r.custody
	return &clone, nil
}

func (r *migrationCustodian) RecordMigrationCapture(_ context.Context, capture protocol.MigrationCapture) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.custody != nil && r.custody.Capture.RequestDigest != capture.RequestDigest {
		return errdefs.ErrAlreadyExists
	}
	r.custody = &nomadruntime.MigrationCaptureCustody{Capture: capture, ImageDirectory: filepath.Join(r.root, capture.RequestDigest)}
	return r.recordFailure
}

func migrationHandleFixture(t *testing.T) (*taskHandle, protocol.MigrationCaptureRequest, *migrationRunsc, *migrationCustodian) {
	t.Helper()
	stage, _, _ := newAuthorizedRootFSStage(t, t.TempDir())
	stage.Identity.RuntimeGeneration = "1"
	stage = stage.WithoutWriterGrantToken()
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	runner := &migrationRunsc{fakeRunsc: newFakeRunsc()}
	runner.setState("running")
	custodian := &migrationCustodian{RootFSRuntime: &fakeRootFSRuntime{}, root: t.TempDir()}
	bundle := t.TempDir()
	handle := newTaskHandle(taskHandleOptions{
		taskConfig: &drivers.TaskConfig{ID: "slot-1", AllocID: "alloc-1", NodeID: "node-1", Namespace: "default", Name: "warm-slot",
			AllocDir: bundle, NetworkIsolation: &drivers.NetworkIsolationSpec{Mode: drivers.NetIsolationModeGroup, Path: filepath.Join(bundle, "network.ns")}},
		driverConfig: TaskConfig{Command: "/procd", SecurityClass: "privileged"},
		bundleDir:    bundle, rootMount: filepath.Join(bundle, "rootfs"), containerID: "fake",
		socketPath: filepath.Join(bundle, "control.sock"), runner: runner, rootfs: custodian, mounter: &fakeMounter{},
	})
	request := protocol.MigrationCaptureRequest{Target: protocol.NodeChannelTarget{
		SlotID: "slot-1", AllocationID: "alloc-1", ClusterID: "cluster-1", NodeID: "node-1", NodeUID: "node-1", NodeBootID: "boot-1", ControlEndpoint: "unix://" + handle.socketPath,
	}, OperationID: "migration-1", SandboxID: "sandbox-1", SourceGeneration: 1, LifecycleEpoch: 2,
		AssignmentRevision: strings.Repeat("ab", 32), BindingDigest: hex.EncodeToString(binding[:]), ResourceLeaseDigest: strings.Repeat("cd", 32), ProcdInstanceID: "procd-1"}
	launch := migrationExecutionCPUFixture(t, &request, stage.Identity.LaunchAttempt)
	runner.executable, runner.cpuObservation = launch.ExecutableDigest, launch.Observation
	handle.claim = &claimMetadata{MigrationCPULaunch: launch, LaunchAttempt: stage.Identity.LaunchAttempt, SandboxID: request.SandboxID, RuntimeRevision: request.AssignmentRevision, RootFSBindingDigest: request.BindingDigest,
		ResourceLeaseDigest: request.ResourceLeaseDigest, ProcdInstanceID: request.ProcdInstanceID, Stage: &stage}
	handle.stage = &stage
	handle.phase = phaseActive
	handle.rootMounted = true
	require.NoError(t, handle.persist())
	t.Cleanup(handle.stopExitWatch)
	return handle, request, runner, custodian
}

func TestMigrationCaptureJournalsBeforeCheckpointAndSuppressesExpectedExit(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	runner.before = func() {
		state, err := readPersistedState(handle.statePath())
		require.NoError(t, err)
		require.Equal(t, phaseMigrating, state.Phase)
		require.Equal(t, protocol.MigrationCaptureIntent, state.Migration.State)
		custody, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
		require.NoError(t, err)
		require.Equal(t, *state.Migration, custody.Capture)
		require.NotContains(t, custody.ImageDirectory, handle.bundleDir)
	}
	result, err := awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureComplete, result.State)
	// Feed the expected runsc exit through the same driver watcher used by Nomad.
	handle.waitForExit(t.Context())
	select {
	case <-handle.done:
		t.Fatal("checkpoint exit incorrectly completed the Nomad carrier")
	default:
	}
	require.True(t, handle.IsRunning())
	require.Equal(t, phaseMigrating, handle.PersistedState().Phase)
	for range 3 {
		retry, err := handle.CaptureMigration(t.Context(), request)
		require.NoError(t, err)
		require.Equal(t, result, retry)
	}
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "checkpoint"))
	require.ErrorIs(t, handle.Close(true), errdefs.ErrFailedPrecondition)
	require.ErrorIs(t, handle.Stop(0, "KILL"), errdefs.ErrFailedPrecondition)
	require.Error(t, handle.Signal("TERM"))
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	changed := request
	changed.LifecycleEpoch++
	_, err = handle.CaptureMigration(t.Context(), changed)
	require.ErrorIs(t, err, errdefs.ErrAlreadyExists)
}

func TestMigrationCaptureRejectsSourceAfterAuthorityLoss(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	// An active guest still uses the writer's ordinary lease-loss path, but
	// lost node authority must immediately close admission to new captures.
	handle.runtimeSlotHeartbeatLost(errors.New("node authority expired"))
	_, err := handle.CaptureMigration(t.Context(), request)
	require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
	custody, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, custody)
}

type delayedMigrationKill struct{ *migrationRunsc }

func (r *delayedMigrationKill) Kill(context.Context, string, string) error {
	r.record("signal-accepted")
	return nil
}

func TestMigrationCaptureFenceRequiresStoppedObservation(t *testing.T) {
	handle, _, runner, _ := migrationHandleFixture(t)
	handle.runner = &delayedMigrationKill{migrationRunsc: runner}
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, handle.stopMigrationExecution(ctx), context.DeadlineExceeded,
		"accepted KILL must not be treated as stopped execution")
	require.Contains(t, runner.callsSnapshot(), "signal-accepted")
	runner.setState("stopped")
	require.NoError(t, handle.stopMigrationExecution(t.Context()))
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
}

func TestMigrationCaptureFailureRetainsPartialImageWithoutRetry(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	runner.failure = errors.New("checkpoint RPC interrupted")
	result, err := awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, result.State)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "checkpoint"))
	custody, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(custody.ImageDirectory, "checkpoint.img"))
	require.NoError(t, err, "a partial directory is retained, never promoted to completed capture")
	require.NotContains(t, runner.callsSnapshot(), "start")
}

func TestMigrationCaptureRecoveryUsesNodeIntentMissingFromDriverSnapshot(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	state := handle.PersistedState()
	digest, err := request.Digest()
	require.NoError(t, err)
	require.NoError(t, custodian.RecordMigrationCapture(t.Context(), protocol.MigrationCapture{Request: request, RequestDigest: digest, State: protocol.MigrationCaptureIntent}))
	require.NoError(t, handle.Recover(state))
	require.Equal(t, protocol.MigrationCaptureUncertain, handle.PersistedState().Migration.State)
	require.Contains(t, runner.callsSnapshot(), "kill:KILL")
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.True(t, handle.IsRunning())
}

func TestMigrationCaptureRecoversCompleteImageAfterDriverRestart(t *testing.T) {
	handle, request, runner, _ := migrationHandleFixture(t)
	_, err := awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	state := handle.PersistedState()
	require.NoError(t, handle.Recover(state))
	require.Equal(t, protocol.MigrationCaptureComplete, handle.PersistedState().Migration.State)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "checkpoint"))
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
}

type migrationSealingCustodian struct {
	*migrationCustodian
	seal func(context.Context, protocol.MigrationCaptureRequest) (rootfshandoff.MigrationRootFSCut, error)
}

func (c *migrationSealingCustodian) SealMigrationRootFS(ctx context.Context, request protocol.MigrationCaptureRequest) (rootfshandoff.MigrationRootFSCut, error) {
	return c.seal(ctx, request)
}

func TestMigrationCaptureSealsRootFSOnlyAfterDurableMemoryCapture(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	sealed := make(chan struct{}, 1)
	handle.rootfs = &migrationSealingCustodian{migrationCustodian: custodian, seal: func(ctx context.Context, received protocol.MigrationCaptureRequest) (rootfshandoff.MigrationRootFSCut, error) {
		if received != request {
			return rootfshandoff.MigrationRootFSCut{}, errors.New("changed capture request")
		}
		stored, err := custodian.GetMigrationCapture(ctx, request.Target.SlotID)
		if err != nil || stored == nil || stored.Capture.State != protocol.MigrationCaptureComplete {
			return rootfshandoff.MigrationRootFSCut{}, errors.New("filesystem cut preceded successful memory capture")
		}
		cutRequest := rootfshandoff.MigrationRootFSCutRequest{OperationID: request.OperationID, CaptureRequestDigest: stored.Capture.RequestDigest,
			SourceBindingDigest: request.BindingDigest, GenerationID: "migration-" + stored.Capture.RequestDigest}
		generation := *handle.stage.Generation
		generation.GenerationID, generation.WriterEpoch = cutRequest.GenerationID, handle.stage.Identity.WriterEpoch
		generation.LocatorVersion++
		cut, err := rootfshandoff.NewMigrationRootFSCut(cutRequest, generation, 0)
		if err == nil {
			sealed <- struct{}{}
		}
		return cut, err
	}}
	result, err := awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureComplete, result.State)
	select {
	case <-sealed:
	case <-time.After(time.Second):
		t.Fatal("filesystem sealing did not follow memory capture")
	}
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "checkpoint"))
	require.Nil(t, result.RootFS, "ctld owns the additional filesystem proof")
}

func TestMigrationCaptureRecoveryHonorsNodeExecutionInvalidation(t *testing.T) {
	handle, request, _, custodian := migrationHandleFixture(t)
	_, err := awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	state := handle.PersistedState()
	custodian.mu.Lock()
	custodian.custody.ExecutionInvalidated = true
	custodian.custody.Capture.State = protocol.MigrationCaptureUncertain
	custodian.mu.Unlock()
	require.NoError(t, handle.Recover(state))
	require.Equal(t, protocol.MigrationCaptureUncertain, handle.PersistedState().Migration.State,
		"the driver's older complete snapshot cannot revive invalidated node custody")
}

func TestMigrationCaptureJournalFailureNeverInvokesCheckpoint(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	custodian.recordFailure = errors.New("ctld journal acknowledgement lost")
	_, err := handle.CaptureMigration(t.Context(), request)
	require.Error(t, err)
	custodian.recordFailure = nil
	result, err := handle.CaptureMigration(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, result.State)
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
}

func TestMigrationCaptureRejectsChangedSourceBeforeTakingCustody(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	for _, mutate := range []func(*protocol.MigrationCaptureRequest){
		func(r *protocol.MigrationCaptureRequest) { r.Target.NodeBootID = "rebooted" },
		func(r *protocol.MigrationCaptureRequest) { r.ProcdInstanceID = "restarted" },
		func(r *protocol.MigrationCaptureRequest) { r.BindingDigest = strings.Repeat("ef", 32) },
		func(r *protocol.MigrationCaptureRequest) { r.SourceGeneration++ },
		func(r *protocol.MigrationCaptureRequest) { r.SandboxID = "another" },
	} {
		changed := request
		mutate(&changed)
		_, err := handle.CaptureMigration(t.Context(), changed)
		require.ErrorIs(t, err, errdefs.ErrFailedPrecondition)
	}
	state, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	require.Nil(t, state)
	require.NotContains(t, runner.callsSnapshot(), "checkpoint")
}

func countMigrationCall(calls []string, wanted string) int {
	n := 0
	for _, call := range calls {
		if call == wanted {
			n++
		}
	}
	return n
}

func awaitMigrationCapture(t *testing.T, handle *taskHandle, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		result, err := handle.CaptureMigration(t.Context(), request)
		if err != nil || result.State != protocol.MigrationCaptureIntent {
			return result, err
		}
		if time.Now().After(deadline) {
			t.Fatal("checkpoint worker did not complete")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMigrationCaptureContinuesAcrossControlConnectionLoss(t *testing.T) {
	handle, request, runner, _ := migrationHandleFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	runner.before = func() { close(entered); <-release }
	ctx, cancel := context.WithCancel(t.Context())
	result, err := handle.CaptureMigration(ctx, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureIntent, result.State)
	<-entered
	cancel()
	result, err = handle.CaptureMigration(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureIntent, result.State)
	close(release)
	result, err = awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureComplete, result.State)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "checkpoint"))
}

func TestMigrationCaptureConcurrentRetriesThroughPrivateSocket(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("root-owned driver socket requires root")
	}
	handle, request, runner, _ := migrationHandleFixture(t)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go handle.ServeControl(ctx)
	require.NoError(t, handle.waitControlReady(ctx))
	client, err := protocol.NewNodeClient(protocol.NodeClientConfig{AllowedSocketRoot: filepath.Dir(handle.socketPath), Timeout: time.Second * 10})
	require.NoError(t, err)
	const workers = 8
	var group sync.WaitGroup
	type outcome struct {
		capture *protocol.MigrationCapture
		err     error
	}
	results := make(chan outcome, workers)
	for range workers {
		group.Go(func() { capture, err := client.CaptureMigration(ctx, request); results <- outcome{capture, err} })
	}
	group.Wait()
	close(results)
	for result := range results {
		require.NoError(t, result.err)
		require.Contains(t, []string{protocol.MigrationCaptureIntent, protocol.MigrationCaptureComplete}, result.capture.State)
	}
	_, err = awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	require.Equal(t, 1, countMigrationCall(runner.callsSnapshot(), "checkpoint"))
}

func TestMigrationCaptureLeaseLossStopsExecutionWithoutDiscardingCustody(t *testing.T) {
	handle, request, runner, custodian := migrationHandleFixture(t)
	runner.before = func() { handle.handleWriterLeaseLoss(errors.New("regional writer lease expired")) }
	result, err := awaitMigrationCapture(t, handle, request)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, result.State)
	require.Eventually(t, func() bool { return contains(runner.callsSnapshot(), "kill:KILL") }, time.Second, time.Millisecond)
	state, err := custodian.GetMigrationCapture(t.Context(), request.Target.SlotID)
	require.NoError(t, err)
	require.Equal(t, protocol.MigrationCaptureUncertain, state.Capture.State)
	require.NotContains(t, runner.callsSnapshot(), "delete:force")
	require.ErrorIs(t, handle.Close(true), errdefs.ErrFailedPrecondition)
	select {
	case <-handle.done:
		t.Fatal("lease loss released migration image custody")
	default:
	}
}
