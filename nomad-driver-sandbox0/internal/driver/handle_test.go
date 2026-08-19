// Copyright 2026 Sandbox0 Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package driver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"

	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/rootfsbuilder"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	rootfshandoff "github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
)

type fakeRunsc struct {
	mu           sync.Mutex
	state        string
	calls        []string
	createErr    error
	startErr     error
	waitErr      error
	waitResult   WaitResult
	stateErr     error
	deleteErr    error
	waitReleased chan struct{}
	releaseOnce  sync.Once
}

func newFakeRunsc() *fakeRunsc {
	return &fakeRunsc{state: "", waitReleased: make(chan struct{})}
}

func (r *fakeRunsc) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *fakeRunsc) callsSnapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.calls...)
}

func (r *fakeRunsc) setState(state string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.state = state
}

func (r *fakeRunsc) Create(context.Context, string, string) error {
	r.record("create")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.createErr != nil {
		return r.createErr
	}
	r.state = "created"
	return nil
}

func (r *fakeRunsc) Start(context.Context, string) error {
	r.record("start")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.startErr != nil {
		return r.startErr
	}
	r.state = "running"
	return nil
}

func (r *fakeRunsc) Wait(context.Context, string) (WaitResult, error) {
	r.record("wait")
	r.mu.Lock()
	result := r.waitResult
	err := r.waitErr
	released := r.waitReleased
	r.mu.Unlock()
	if err != nil {
		return result, err
	}
	<-released
	return result, nil
}

func (r *fakeRunsc) Kill(_ context.Context, _, signal string) error {
	r.record("kill:" + signal)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.releaseOnce.Do(func() { close(r.waitReleased) })
	r.state = "stopped"
	return nil
}

func (r *fakeRunsc) Delete(_ context.Context, _ string, force bool) error {
	if force {
		r.record("delete:force")
	} else {
		r.record("delete")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.deleteErr != nil {
		return r.deleteErr
	}
	r.state = "deleted"
	select {
	case <-r.waitReleased:
	default:
		r.releaseOnce.Do(func() { close(r.waitReleased) })
	}
	return nil
}

func (r *fakeRunsc) State(context.Context, string) (RunscState, error) {
	r.record("state")
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stateErr != nil {
		return RunscState{}, r.stateErr
	}
	return RunscState{ID: "fake", Status: r.state}, nil
}

func (r *fakeRunsc) Version(context.Context) (string, error) {
	return "runsc version fake", nil
}

type fakeMounter struct {
	mu       sync.Mutex
	binds    [][2]string
	unmounts []string
	bindErr  error
}

type fakeRootFSRuntime struct {
	mu            sync.Mutex
	source        string
	pingErr       error
	ensureErr     error
	crashErr      error
	retireErr     error
	ensureCalls   int
	retireCalls   int
	crashCalls    int
	lastParent    string
	lastOperation string
	leaseLoss     func(error)
}

func (r *fakeRootFSRuntime) Ping(context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.pingErr
}

type fakeNetworkRuntime struct {
	mu       sync.Mutex
	applies  []NetworkPolicy
	cleanups int
}

func (r *fakeNetworkRuntime) Apply(_ context.Context, _, _ string, policy NetworkPolicy) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.applies = append(r.applies, policy)
	return nil
}

func (r *fakeNetworkRuntime) Cleanup(context.Context, string, string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cleanups++
	return nil
}

func (r *fakeNetworkRuntime) snapshot() ([]NetworkPolicy, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]NetworkPolicy(nil), r.applies...), r.cleanups
}

type noOpCommandRunner struct{}

func (noOpCommandRunner) Run(context.Context, string, ...string) error { return nil }

func (r *fakeRootFSRuntime) Ensure(
	_ context.Context,
	request rootfshandoff.StageRequest,
	leaseLoss func(error),
) (rootfssession.Mount, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ensureCalls++
	r.lastParent = request.Parent
	r.leaseLoss = leaseLoss
	if r.ensureErr != nil {
		return rootfssession.Mount{}, r.ensureErr
	}
	return rootfssession.Mount{Source: r.source, Type: "bind"}, nil
}

func (r *fakeRootFSRuntime) RegisterConsumer(
	_ context.Context,
	_ rootfshandoff.StageRequest,
	_ RootFSConsumerRequest,
) (RootFSConsumerLease, error) {
	return RootFSConsumerLease{LeaseID: "fake-consumer", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (r *fakeRootFSRuntime) RenewConsumer(
	_ context.Context,
	_ rootfshandoff.StageRequest,
	lease RootFSConsumerLease,
) (RootFSConsumerLease, error) {
	lease.ExpiresAt = time.Now().Add(time.Hour)
	return lease, nil
}

func (r *fakeRootFSRuntime) CaptureRunningFork(
	_ context.Context,
	_ rootfshandoff.StageRequest,
	request rootfshandoff.RunningForkCheckpointRequest,
) (rootfshandoff.RunningForkCheckpointResult, error) {
	return rootfshandoff.RunningForkCheckpointResult{
		Proof: rootfshandoff.RunningForkCheckpointProof{OperationID: request.OperationID},
	}, nil
}

func (r *fakeRootFSRuntime) RecoverySessions() ([]rootfssession.RecoverySession, error) {
	return nil, nil
}

func (r *fakeRootFSRuntime) Retire(_ context.Context, request rootfshandoff.StageRequest, operationID string) (rootfssession.RetireResult, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retireCalls++
	r.lastParent = request.Parent
	r.lastOperation = operationID
	if r.retireErr != nil {
		return rootfssession.RetireResult{}, r.retireErr
	}
	return rootfssession.RetireResult{Parent: request.Parent, OperationID: operationID}, nil
}

func (r *fakeRootFSRuntime) CrashFence(
	_ context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
	_ crashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.crashCalls++
	r.lastParent = request.Parent
	r.lastOperation = operationID
	if r.crashErr != nil {
		return rootfshandoff.CrashFenceProof{}, r.crashErr
	}
	return rootfshandoff.CrashFenceProof{OperationID: operationID}, nil
}

func (r *fakeRootFSRuntime) snapshot() (ensureCalls, retireCalls int, parent, operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensureCalls, r.retireCalls, r.lastParent, r.lastOperation
}

func (m *fakeMounter) Bind(source, target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.bindErr != nil {
		return m.bindErr
	}
	m.binds = append(m.binds, [2]string{source, target})
	return nil
}

func (m *fakeMounter) Unmount(target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unmounts = append(m.unmounts, target)
	return nil
}

func (m *fakeMounter) snapshot() (binds [][2]string, unmounts []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][2]string(nil), m.binds...), append([]string(nil), m.unmounts...)
}

type testFixture struct {
	tempDir     string
	bundleDir   string
	rootMount   string
	socketPath  string
	allowedRoot string
	rootfs      string
	runner      *fakeRunsc
	mounter     *fakeMounter
	handle      *taskHandle
	taskConfig  *drivers.TaskConfig
}

func newTestFixture(t *testing.T) *testFixture {
	t.Helper()
	tempDir := t.TempDir()
	taskConfig := &drivers.TaskConfig{
		ID:            "task-1",
		AllocID:       "alloc-1",
		Name:          "warm-slot",
		TaskGroupName: "default",
		AllocDir:      filepath.Join(tempDir, "alloc"),
		Resources:     &drivers.Resources{},
		NetworkIsolation: &drivers.NetworkIsolationSpec{
			Mode: drivers.NetIsolationModeGroup,
			Path: filepath.Join(tempDir, "netns"),
		},
	}
	if err := taskConfig.EncodeConcreteDriverConfig(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("encode task driver config: %v", err)
	}
	if err := os.MkdirAll(taskConfig.NetworkIsolation.Path, 0o755); err != nil {
		t.Fatalf("create netns path: %v", err)
	}

	runner := newFakeRunsc()
	mounter := &fakeMounter{}
	bundleDir := filepath.Join(tempDir, "bundle")
	rootMount := filepath.Join(bundleDir, "rootfs")
	socketPath := filepath.Join(tempDir, "control", "task-1.sock")
	allowedRoot := filepath.Join(tempDir, "allowed-rootfs")
	rootfs := filepath.Join(allowedRoot, "generation-1")
	for _, path := range []string{allowedRoot, rootfs} {
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create %s: %v", path, err)
		}
	}
	handle := newTaskHandle(taskHandleOptions{
		taskConfig:  taskConfig,
		bundleDir:   bundleDir,
		containerID: "s0-task-1",
		rootMount:   rootMount,
		socketPath:  socketPath,
		runner:      runner,
		mounter:     mounter,
		allowedRoot: allowedRoot,
		logger:      hclog.NewNullLogger(),
	})
	return &testFixture{
		tempDir:     tempDir,
		bundleDir:   bundleDir,
		rootMount:   rootMount,
		socketPath:  socketPath,
		allowedRoot: allowedRoot,
		rootfs:      rootfs,
		runner:      runner,
		mounter:     mounter,
		handle:      handle,
		taskConfig:  taskConfig,
	}
}

func TestPrepareCreatesGenericWarmSlotWithoutRunsc(t *testing.T) {
	fixture := newTestFixture(t)
	var config TaskConfig
	if err := fixture.taskConfig.DecodeDriverConfig(&config); err != nil {
		t.Fatalf("decode task config: %v", err)
	}
	if err := fixture.handle.Prepare(config); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if calls := fixture.runner.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("warm runner calls = %v, want none", calls)
	}
	if _, err := os.Stat(filepath.Join(fixture.bundleDir, "config.json")); !os.IsNotExist(err) {
		t.Fatalf("warm OCI config exists before claim: %v", err)
	}
	if status := fixture.handle.TaskStatus(); status.DriverAttributes["phase"] != string(phaseWarm) {
		t.Fatalf("phase = %s, want warm", status.DriverAttributes["phase"])
	}
	persisted, err := readPersistedState(filepath.Join(fixture.bundleDir, ".sandbox0-driver-state.json"))
	if err != nil || persisted.Phase != phaseWarm {
		t.Fatalf("persisted state = %+v, err=%v; want warm state on disk", persisted, err)
	}
}

func TestClaimAttachesRootBeforeCreateAndStart(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := fixture.handle.Claim(ClaimRequest{
		RootfsPath:  fixture.rootfs,
		PolicyToken: "one-shot",
		WriterEpoch: "epoch-7",
	}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	binds, unmounts := fixture.mounter.snapshot()
	if len(binds) != 1 || binds[0][0] != fixture.rootfs || binds[0][1] != fixture.rootMount {
		t.Fatalf("binds = %v, want generation bound to OCI root", binds)
	}
	if len(unmounts) != 0 {
		t.Fatalf("claim unexpectedly unmounted %v", unmounts)
	}
	if status := fixture.handle.TaskStatus(); status.DriverAttributes["phase"] != string(phaseActive) {
		t.Fatalf("phase = %s, want active", status.DriverAttributes["phase"])
	}
	data, err := os.ReadFile(filepath.Join(fixture.bundleDir, "config.json"))
	if err != nil {
		t.Fatalf("read claim OCI config: %v", err)
	}
	var spec map[string]any
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("decode claim OCI config: %v", err)
	}
	root := spec["root"].(map[string]any)
	readonly, hasReadonly := root["readonly"]
	if root["path"] != "rootfs" || (hasReadonly && readonly != false) {
		t.Fatalf("OCI root = %v, want writable initial root", root)
	}
	persisted, err := readPersistedState(filepath.Join(fixture.bundleDir, ".sandbox0-driver-state.json"))
	if err != nil || persisted.Phase != phaseActive || !persisted.RootMounted {
		t.Fatalf("persisted state = %+v, err=%v; want durable active state", persisted, err)
	}
	var calls []string
	deadline := time.Now().Add(2 * time.Second)
	for deadline.After(time.Now()) {
		calls = fixture.runner.callsSnapshot()
		if contains(calls, "start") && contains(calls, "wait") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !contains(calls, "create") || !contains(calls, "start") || !contains(calls, "wait") {
		t.Fatalf("runner calls = %v, want create, start, and wait", calls)
	}
}

func TestClaimIsOneShotAndRejectsRootfsOutsideAllowedRoot(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	outside := t.TempDir()
	err := fixture.handle.Claim(ClaimRequest{RootfsPath: outside, PolicyToken: "token", WriterEpoch: "epoch"})
	if err == nil || !strings.Contains(err.Error(), "outside allowed root") {
		t.Fatalf("outside root error = %v, want outside allowed root", err)
	}
	if err := fixture.handle.Claim(ClaimRequest{RootfsPath: fixture.rootfs, PolicyToken: "token", WriterEpoch: "epoch"}); err != nil {
		t.Fatalf("first claim error = %v", err)
	}
	err = fixture.handle.Claim(ClaimRequest{RootfsPath: fixture.rootfs, PolicyToken: "token2", WriterEpoch: "epoch2"})
	if err == nil || !strings.Contains(err.Error(), "only valid in warm phase") {
		t.Fatalf("second claim error = %v, want one-shot rejection", err)
	}
}

func TestClaimUsesAuthorizedRootFSSessionAndRetiresOnClose(t *testing.T) {
	fixture := newTestFixture(t)
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)

	runtime := &fakeRootFSRuntime{source: source}
	fixture.handle.rootfs = runtime
	fixture.handle.rootfsAllowedRoot = filepath.Dir(source)
	// Keep the fake session source outside the development allowed root to prove
	// that the session mount uses the RootFS-specific allowlist.
	if _, err := validateRootfsPath(source, fixture.allowedRoot); err == nil {
		t.Fatal("test source unexpectedly resolved under development root")
	}
	if err := fixture.handle.Prepare(TaskConfig{Command: "/bin/sh", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := fixture.handle.Claim(ClaimRequest{
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
	}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	ensureCalls, retireCalls, parent, _ := runtime.snapshot()
	if ensureCalls != 1 || retireCalls != 0 || parent != stage.Parent {
		t.Fatalf("runtime calls=(%d,%d,%q), want one ensure and no retire", ensureCalls, retireCalls, parent)
	}
	persisted := fixture.handle.PersistedState()
	if persisted.Claim == nil || persisted.Claim.Stage == nil ||
		persisted.Claim.Stage.Identity.WriterGrantToken != "" {
		t.Fatalf("persisted claim = %+v, want tokenless durable stage", persisted.Claim)
	}
	if err := fixture.handle.Close(true); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	ensureCalls, retireCalls, parent, operation := runtime.snapshot()
	if ensureCalls != 1 || retireCalls != 1 || parent != stage.Parent || operation == "" {
		t.Fatalf("runtime calls=(%d,%d,%q,%q), want one ensure and one retire", ensureCalls, retireCalls, parent, operation)
	}
}

func TestWriterLeaseLossKillsTaskAndCrashAbandonsRootFS(t *testing.T) {
	fixture := newTestFixture(t)
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)
	runtime := &fakeRootFSRuntime{source: source}
	fixture.handle.rootfs = runtime
	fixture.handle.rootfsAllowedRoot = filepath.Dir(source)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/bin/sh", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := fixture.handle.Claim(ClaimRequest{
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
	}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	runtime.mu.Lock()
	leaseLoss := runtime.leaseLoss
	runtime.mu.Unlock()
	if leaseLoss == nil {
		t.Fatal("RootFS runtime did not receive a lease-loss handler")
	}
	leaseLoss(errors.New("regional writer lease expired"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.mu.Lock()
		crashCalls := runtime.crashCalls
		retireCalls := runtime.retireCalls
		runtime.mu.Unlock()
		status := fixture.handle.TaskStatus()
		if crashCalls == 1 && retireCalls == 0 &&
			status.DriverAttributes["phase"] == string(phasePoisoned) &&
			status.DriverAttributes["root_mounted"] == "false" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	runtime.mu.Lock()
	crashCalls, retireCalls, operation := runtime.crashCalls, runtime.retireCalls, runtime.lastOperation
	runtime.mu.Unlock()
	if crashCalls != 1 || retireCalls != 0 || operation != crashOperationID(stage) {
		t.Fatalf("runtime crash=%d retire=%d operation=%q", crashCalls, retireCalls, operation)
	}
	if calls := fixture.runner.callsSnapshot(); !contains(calls, "kill:KILL") || !contains(calls, "delete:force") {
		t.Fatalf("runner calls = %v, want forced writer removal", calls)
	}
}

func newAuthorizedRootFSStage(t *testing.T, source string) (rootfshandoff.StageRequest, string, string) {
	t.Helper()
	store := objectstore.NewMemoryStore(t.Name()).(objectstore.ConditionalStore)
	descriptor, err := rootfsbuilder.Build(context.Background(), store, rootfsbuilder.Options{
		SourceRoot: source, ImagePath: filepath.Join(t.TempDir(), "base.xfs"),
		RootFSID: "rootfs-1", ObjectPrefix: "driver-test/rootfs", Runner: noOpCommandRunner{},
	})
	if err != nil {
		t.Fatalf("build descriptor: %v", err)
	}
	token := "one-shot-writer-token"
	zeroDigest := "sha256:" + strings.Repeat("0", 64)
	networkPolicy := `{"mode":"allow-all"}`
	stage := rootfshandoff.StageRequest{
		BindingVersion: rootfshandoff.WriterBindingVersion,
		Parent:         zeroDigest, InitialGeneration: descriptor.GenerationID,
		Generation: &descriptor,
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			PodUID: "alloc-1", PodSandboxID: "sandbox-1", ClaimID: "claim-1",
			NetworkEpoch: 1, PolicyDigest: digestString(networkPolicy), PodIP: "172.26.64.2",
			CtldGeneration: "ctld-1", NetNSIdentity: "netns-1",
		},
		Identity: rootfshandoff.Identity{
			NodeUID: "node-1", BootID: "boot-1", RuntimeGeneration: "runtime-1",
			PodUID: "alloc-1", PodSandboxID: "sandbox-1", ContainerName: "warm-slot",
			Image: "image-1", Snapshotter: "nomad-driver", RuntimeName: PluginName,
			SlotNonce: "slot-1", ClaimID: "claim-1", LaunchAttempt: "attempt-1",
			RootFSID: "rootfs-1", WriterEpoch: 1, WriterGrantID: "grant-1",
			WriterGrantTokenDigest: rootfshandoff.WriterGrantTokenDigest(token),
			WriterGrantToken:       token,
		},
	}
	if err := stage.Validate(); err != nil {
		t.Fatalf("stage validation: %v", err)
	}
	return stage, token, networkPolicy
}

func TestClaimCrashAbandonsConsumedWriterWhenRootFSAttachFails(t *testing.T) {
	fixture := newTestFixture(t)
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)
	runtime := &fakeRootFSRuntime{
		source:    source,
		ensureErr: &consumedRootFSAttachError{err: errors.New("NBD device is unavailable")},
	}
	fixture.handle.rootfs = runtime
	fixture.handle.rootfsAllowedRoot = filepath.Dir(source)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/bin/sh", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	err := fixture.handle.Claim(ClaimRequest{
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
	})
	if err == nil || !strings.Contains(err.Error(), "writer was crash-abandoned") {
		t.Fatalf("Claim() error = %v, want crash-abandoned attach failure", err)
	}
	status := fixture.handle.TaskStatus()
	if status.DriverAttributes["phase"] != string(phasePoisoned) || status.DriverAttributes["root_mounted"] != "false" {
		t.Fatalf("status = %+v, want poisoned and detached", status.DriverAttributes)
	}
	runtime.mu.Lock()
	ensureCalls, retireCalls, crashCalls, operation := runtime.ensureCalls, runtime.retireCalls, runtime.crashCalls, runtime.lastOperation
	runtime.mu.Unlock()
	if ensureCalls != 1 || retireCalls != 0 || crashCalls != 1 || operation != crashOperationID(stage) {
		t.Fatalf("runtime ensure=%d retire=%d crash=%d operation=%q", ensureCalls, retireCalls, crashCalls, operation)
	}
	if calls := fixture.runner.callsSnapshot(); contains(calls, "create") || contains(calls, "start") {
		t.Fatalf("runner calls = %v, runsc must not start after RootFS attach failure", calls)
	}
	_, unmounts := fixture.mounter.snapshot()
	if len(unmounts) != 1 || unmounts[0] != fixture.rootMount {
		t.Fatalf("unmounts = %v, want crash cleanup", unmounts)
	}
}

func TestClaimReturnsWarmWhenWriterGrantWasNotConsumed(t *testing.T) {
	fixture := newTestFixture(t)
	source := t.TempDir()
	if err := os.MkdirAll(filepath.Join(source, "bin"), 0o755); err != nil {
		t.Fatal(err)
	}
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)
	runtime := &fakeRootFSRuntime{source: source, ensureErr: errors.New("writer authority unavailable")}
	fixture.handle.rootfs = runtime
	fixture.handle.rootfsAllowedRoot = filepath.Dir(source)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/bin/sh", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	err := fixture.handle.Claim(ClaimRequest{
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
	})
	if err == nil || !strings.Contains(err.Error(), "writer authority unavailable") {
		t.Fatalf("Claim() error = %v, want authority failure", err)
	}
	status := fixture.handle.TaskStatus()
	if status.DriverAttributes["phase"] != string(phaseWarm) || status.DriverAttributes["root_mounted"] != "false" {
		t.Fatalf("status = %+v, want reusable warm slot", status.DriverAttributes)
	}
	runtime.mu.Lock()
	crashCalls := runtime.crashCalls
	runtime.mu.Unlock()
	if crashCalls != 0 {
		t.Fatalf("crash calls = %d, writer was never consumed", crashCalls)
	}
}

func TestClaimStartFailureUnmountsAndPoisonsSlot(t *testing.T) {
	fixture := newTestFixture(t)
	fixture.runner.startErr = errors.New("sentry boot failed")
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	err := fixture.handle.Claim(ClaimRequest{RootfsPath: fixture.rootfs, PolicyToken: "token", WriterEpoch: "epoch"})
	if err == nil || !strings.Contains(err.Error(), "sentry boot failed") {
		t.Fatalf("claim error = %v, want start failure", err)
	}
	binds, unmounts := fixture.mounter.snapshot()
	if len(binds) != 1 || len(unmounts) != 1 || unmounts[0] != fixture.rootMount {
		t.Fatalf("binds=%v unmounts=%v, want failed claim rollback", binds, unmounts)
	}
	if calls := fixture.runner.callsSnapshot(); !contains(calls, "delete:force") {
		t.Fatalf("runner calls = %v, want force delete", calls)
	}
	if status := fixture.handle.TaskStatus(); status.DriverAttributes["phase"] != string(phasePoisoned) {
		t.Fatalf("phase = %s, want poisoned", status.DriverAttributes["phase"])
	}
}

func TestNetworkPolicyStartsBlockAllAndAppliesClaimPolicy(t *testing.T) {
	fixture := newTestFixture(t)
	network := &fakeNetworkRuntime{}
	fixture.handle.network = network
	if err := fixture.handle.Prepare(TaskConfig{Command: "/bin/sh", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	policy := `{"mode":"block-all","allow":[{"protocol":"tcp","host":"203.0.113.7","port":443}]}`
	if err := fixture.handle.Claim(ClaimRequest{
		RootfsPath: fixture.rootfs, PolicyToken: "token", WriterEpoch: "epoch", NetworkPolicy: policy,
	}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	applies, cleanups := network.snapshot()
	if len(applies) != 2 || applies[0].Mode != networkPolicyBlockAll ||
		applies[1].Mode != networkPolicyBlockAll || len(applies[1].Allow) != 1 {
		t.Fatalf("network applications = %+v", applies)
	}
	if err := fixture.handle.Close(true); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if _, cleanups = network.snapshot(); cleanups != 1 {
		t.Fatalf("cleanups = %d, want one", cleanups)
	}
}

func TestRecoverWarmSlotWithoutRootfs(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	state := fixture.handle.PersistedState()
	fixture.runner.setState("created")
	recovered := newTaskHandle(taskHandleOptions{
		taskConfig:  state.TaskConfig,
		bundleDir:   state.BundleDir,
		containerID: state.ContainerID,
		rootMount:   state.RootMount,
		socketPath:  filepath.Join(t.TempDir(), "recovered.sock"),
		runner:      fixture.runner,
		mounter:     fixture.mounter,
		allowedRoot: fixture.allowedRoot,
		logger:      hclog.NewNullLogger(),
	})
	if err := recovered.Recover(state); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if status := recovered.TaskStatus(); status.DriverAttributes["phase"] != string(phaseWarm) {
		t.Fatalf("recovered phase = %s, want warm", status.DriverAttributes["phase"])
	}
}

func TestRecoverPoisonsInterruptedClaim(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	state := fixture.handle.PersistedState()
	state.RootMounted = true
	state.Phase = phaseClaiming
	if err := writePersistedState(state, filepath.Join(state.BundleDir, ".sandbox0-driver-state.json")); err != nil {
		t.Fatalf("write interrupted state: %v", err)
	}
	fixture.runner.setState("created")
	recovered := newTaskHandle(taskHandleOptions{
		taskConfig:  state.TaskConfig,
		bundleDir:   state.BundleDir,
		containerID: state.ContainerID,
		rootMount:   state.RootMount,
		socketPath:  filepath.Join(t.TempDir(), "recovered.sock"),
		runner:      fixture.runner,
		mounter:     fixture.mounter,
		allowedRoot: fixture.allowedRoot,
		logger:      hclog.NewNullLogger(),
	})
	if err := recovered.Recover(state); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	if status := recovered.TaskStatus(); status.DriverAttributes["phase"] != string(phasePoisoned) {
		t.Fatalf("recovered phase = %s, want poisoned", status.DriverAttributes["phase"])
	}
}

func TestRecoverCrashAbandonsActiveRootFSWriter(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	stage := rootfshandoff.StageRequest{
		BindingVersion:    rootfshandoff.WriterBindingVersion,
		Parent:            "sha256:" + strings.Repeat("a", 64),
		InitialGeneration: "generation-active",
		Identity: rootfshandoff.Identity{
			RootFSID: "rootfs-active", WriterEpoch: 4, WriterGrantID: "grant-active",
		},
	}
	state := fixture.handle.PersistedState()
	state.RootMounted = true
	state.Phase = phaseActive
	state.Claim = &claimMetadata{WriterEpoch: "4", Stage: &stage}
	if err := writePersistedState(state, filepath.Join(state.BundleDir, ".sandbox0-driver-state.json")); err != nil {
		t.Fatalf("write active state: %v", err)
	}
	runtime := &fakeRootFSRuntime{}
	recovered := newTaskHandle(taskHandleOptions{
		taskConfig: state.TaskConfig, bundleDir: state.BundleDir, containerID: state.ContainerID,
		rootMount: state.RootMount, socketPath: filepath.Join(t.TempDir(), "recovered.sock"),
		runner: fixture.runner, mounter: fixture.mounter, rootfs: runtime,
		allowedRoot: fixture.allowedRoot, logger: hclog.NewNullLogger(),
	})
	if err := recovered.Recover(state); err != nil {
		t.Fatalf("Recover() error = %v", err)
	}
	status := recovered.TaskStatus()
	if status.DriverAttributes["phase"] != string(phasePoisoned) || status.DriverAttributes["root_mounted"] != "false" {
		t.Fatalf("recovered status = %+v, want poisoned and detached", status.DriverAttributes)
	}
	runtime.mu.Lock()
	crashCalls, retireCalls, operation := runtime.crashCalls, runtime.retireCalls, runtime.lastOperation
	runtime.mu.Unlock()
	if crashCalls != 1 || retireCalls != 0 || operation != crashOperationID(stage) {
		t.Fatalf("runtime crash=%d retire=%d operation=%q", crashCalls, retireCalls, operation)
	}
	if calls := fixture.runner.callsSnapshot(); !contains(calls, "kill:KILL") || !contains(calls, "delete:force") {
		t.Fatalf("runner calls = %v, want forced runtime removal", calls)
	}
	_, unmounts := fixture.mounter.snapshot()
	if len(unmounts) != 1 || unmounts[0] != fixture.rootMount {
		t.Fatalf("unmounts = %v, want task root detached", unmounts)
	}
}

func TestStopActiveContainerAndCloseCleanup(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if err := fixture.handle.Claim(ClaimRequest{RootfsPath: fixture.rootfs, PolicyToken: "token", WriterEpoch: "epoch"}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if err := fixture.handle.Stop(2*time.Second, "TERM"); err != nil {
		t.Fatalf("Stop() error = %v", err)
	}
	if err := fixture.handle.Close(true); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	_, unmounts := fixture.mounter.snapshot()
	if len(unmounts) != 1 {
		t.Fatalf("unmounts = %v, want RootFS cleanup", unmounts)
	}
	if _, err := os.Stat(fixture.bundleDir); !os.IsNotExist(err) {
		t.Fatalf("bundle dir still exists after Close(): %v", err)
	}
}

func TestCloseRetriesRootFSRetirementWithStableOperation(t *testing.T) {
	fixture := newTestFixture(t)
	retireFailure := errors.New("XFS mount is busy")
	runtime := &fakeRootFSRuntime{retireErr: retireFailure}
	stage := rootfshandoff.StageRequest{
		Parent: "sha256:" + strings.Repeat("d", 64),
		Identity: rootfshandoff.Identity{
			RootFSID: "retry-rootfs", WriterEpoch: 9, WriterGrantID: "retry-grant",
		},
	}
	fixture.handle.mu.Lock()
	fixture.handle.rootfs = runtime
	fixture.handle.stage = &stage
	fixture.handle.rootMounted = true
	fixture.handle.phase = phaseExited
	fixture.handle.mu.Unlock()

	if err := fixture.handle.Close(true); !errors.Is(err, retireFailure) {
		t.Fatalf("first Close() error = %v, want %v", err, retireFailure)
	}
	runtime.mu.Lock()
	firstOperation := runtime.lastOperation
	runtime.retireErr = nil
	runtime.mu.Unlock()
	if firstOperation != retireOperationID(stage) {
		t.Fatalf("first retire operation = %q, want %q", firstOperation, retireOperationID(stage))
	}
	if err := fixture.handle.Close(true); err != nil {
		t.Fatalf("retry Close() error = %v", err)
	}
	runtime.mu.Lock()
	retireCalls, secondOperation := runtime.retireCalls, runtime.lastOperation
	runtime.mu.Unlock()
	if retireCalls != 2 || secondOperation != firstOperation {
		t.Fatalf("retire calls = %d, operation = %q, want two calls with %q", retireCalls, secondOperation, firstOperation)
	}
}

func TestValidateRootfsPathRejectsTraversal(t *testing.T) {
	base := t.TempDir()
	rootfs := filepath.Join(base, "rootfs")
	if err := os.MkdirAll(rootfs, 0o755); err != nil {
		t.Fatalf("create rootfs: %v", err)
	}
	if got, err := validateRootfsPath(rootfs, base); err != nil || got != rootfs {
		t.Fatalf("validateRootfsPath() = %s, %v", got, err)
	}
	if _, err := validateRootfsPath(filepath.Join(base, "..", filepath.Base(rootfs)), base); err == nil {
		t.Fatal("validateRootfsPath accepted traversal outside allowed root")
	}
}

func TestCommandRunscUsesPersistentRootFlags(t *testing.T) {
	config := defaultPluginConfig()
	runner := NewCommandRunsc(*config).(*CommandRunsc)
	flags := runner.globalArgs()
	for _, wanted := range []string{
		"--platform=systrap",
		"--overlay2=none",
		"--file-access=shared",
		"--directfs=true",
	} {
		if !contains(flags, wanted) {
			t.Fatalf("runsc flags = %v, want %s", flags, wanted)
		}
	}
}

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
