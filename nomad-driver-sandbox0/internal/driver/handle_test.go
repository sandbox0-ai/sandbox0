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

func TestPrepareCreatesWritableWarmOCIBundle(t *testing.T) {
	fixture := newTestFixture(t)
	var config TaskConfig
	if err := fixture.taskConfig.DecodeDriverConfig(&config); err != nil {
		t.Fatalf("decode task config: %v", err)
	}
	if err := fixture.handle.Prepare(config); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if calls := fixture.runner.callsSnapshot(); len(calls) != 1 || calls[0] != "create" {
		t.Fatalf("runner calls = %v, want only create", calls)
	}

	data, err := os.ReadFile(filepath.Join(fixture.bundleDir, "config.json"))
	if err != nil {
		t.Fatalf("read OCI config: %v", err)
	}
	var spec map[string]any
	if err := json.Unmarshal(data, &spec); err != nil {
		t.Fatalf("decode OCI config: %v", err)
	}
	root := spec["root"].(map[string]any)
	readonly, hasReadonly := root["readonly"]
	if root["path"] != "rootfs" || (hasReadonly && readonly != false) {
		t.Fatalf("OCI root = %v, want writable rootfs", root)
	}
	if got := spec["process"].(map[string]any)["args"].([]any); len(got) != 1 || got[0] != "/procd" {
		t.Fatalf("OCI argv = %v, want /procd", got)
	}
	if status := fixture.handle.TaskStatus(); status.DriverAttributes["phase"] != string(phaseWarm) {
		t.Fatalf("phase = %s, want warm", status.DriverAttributes["phase"])
	}
	persisted, err := readPersistedState(filepath.Join(fixture.bundleDir, ".sandbox0-driver-state.json"))
	if err != nil || persisted.Phase != phaseWarm {
		t.Fatalf("persisted state = %+v, err=%v; want warm state on disk", persisted, err)
	}
}

func TestClaimBindsRootAndStartsPrecreatedContainer(t *testing.T) {
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
	if !contains(calls, "start") || !contains(calls, "wait") {
		t.Fatalf("runner calls = %v, want start and wait", calls)
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

func TestRecoverWarmCreatedSlotWithoutRootfs(t *testing.T) {
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
