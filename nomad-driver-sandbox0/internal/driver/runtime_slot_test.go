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
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type fakeRuntimeSlotAuthority struct {
	mu sync.Mutex

	state           protocol.State
	revision        int64
	heartbeatTTL    time.Duration
	registerErr     error
	readyErr        error
	heartbeatErrors []error
	calls           []string
	registrations   []protocol.RegistrationRequest
	readiness       []protocol.ReadinessRequest
	heartbeats      []protocol.HeartbeatRequest
	heartbeatNotify chan struct{}
}

func newFakeRuntimeSlotAuthority() *fakeRuntimeSlotAuthority {
	return &fakeRuntimeSlotAuthority{
		state: protocol.StateRegistered, heartbeatTTL: 500 * time.Millisecond,
		heartbeatNotify: make(chan struct{}, 16),
	}
}

func (a *fakeRuntimeSlotAuthority) observationLocked(slotID string) protocol.Observation {
	a.revision++
	now := time.Now().UTC()
	observation := protocol.Observation{
		SlotID: slotID, State: a.state, Revision: a.revision,
		ServerTime: now, HeartbeatExpiresAt: now.Add(a.heartbeatTTL),
	}
	switch a.state {
	case protocol.StateClaiming, protocol.StateStarting, protocol.StateActive, protocol.StateQuiescing:
		expiresAt := now.Add(time.Minute)
		observation.ClaimOperationID = "operation-1"
		observation.ClaimID = "claim-1"
		observation.ClaimLeaseExpiresAt = &expiresAt
	}
	return observation
}

func (a *fakeRuntimeSlotAuthority) Register(
	_ context.Context,
	slotID string,
	request protocol.RegistrationRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, "register")
	a.registrations = append(a.registrations, request)
	if a.registerErr != nil {
		return protocol.Observation{}, a.registerErr
	}
	return a.observationLocked(slotID), nil
}

func (a *fakeRuntimeSlotAuthority) Observe(_ context.Context, slotID string) (protocol.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, "observe")
	return a.observationLocked(slotID), nil
}

func (a *fakeRuntimeSlotAuthority) Ready(
	_ context.Context,
	slotID string,
	request protocol.ReadinessRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, "ready")
	a.readiness = append(a.readiness, request)
	if a.readyErr != nil {
		return protocol.Observation{}, a.readyErr
	}
	a.state = protocol.StateFastpathReady
	return a.observationLocked(slotID), nil
}

func (a *fakeRuntimeSlotAuthority) Heartbeat(
	_ context.Context,
	slotID string,
	request protocol.HeartbeatRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	a.calls = append(a.calls, "heartbeat")
	a.heartbeats = append(a.heartbeats, request)
	var err error
	if len(a.heartbeatErrors) != 0 {
		err = a.heartbeatErrors[0]
		a.heartbeatErrors = a.heartbeatErrors[1:]
	}
	observation := a.observationLocked(slotID)
	a.mu.Unlock()
	select {
	case a.heartbeatNotify <- struct{}{}:
	default:
	}
	if err != nil {
		return protocol.Observation{}, err
	}
	return observation, nil
}

func (a *fakeRuntimeSlotAuthority) Starting(
	_ context.Context,
	slotID string,
	_ protocol.StartingRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, "starting")
	a.state = protocol.StateStarting
	return a.observationLocked(slotID), nil
}

func (a *fakeRuntimeSlotAuthority) CommandReady(
	_ context.Context,
	slotID string,
	_ protocol.CommandReadyRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls = append(a.calls, "command-ready")
	a.state = protocol.StateActive
	return a.observationLocked(slotID), nil
}

func (a *fakeRuntimeSlotAuthority) snapshot() (
	[]string,
	[]protocol.RegistrationRequest,
	[]protocol.ReadinessRequest,
	[]protocol.HeartbeatRequest,
) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.calls...),
		append([]protocol.RegistrationRequest(nil), a.registrations...),
		append([]protocol.ReadinessRequest(nil), a.readiness...),
		append([]protocol.HeartbeatRequest(nil), a.heartbeats...)
}

type runtimeSlotPluginFixture struct {
	plugin    *Plugin
	config    *PluginConfig
	task      *drivers.TaskConfig
	runner    *fakeRunsc
	rootfs    *fakeRootFSRuntime
	network   *fakeNetworkRuntime
	authority *fakeRuntimeSlotAuthority
}

func newRuntimeSlotPluginFixture(t *testing.T) *runtimeSlotPluginFixture {
	t.Helper()
	tempDir := t.TempDir()
	controlDir, err := os.MkdirTemp("", "s0-slot-control-")
	if err != nil {
		t.Fatalf("create short control directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(controlDir) })
	bootIDPath := filepath.Join(tempDir, "boot-id")
	if err := os.WriteFile(bootIDPath, []byte("boot-1\n"), 0o600); err != nil {
		t.Fatalf("write boot ID: %v", err)
	}
	config := defaultPluginConfig()
	config.ControlDir = controlDir
	config.AllowedRootfsDir = filepath.Join(tempDir, "development-rootfs")
	config.NetworkPolicyEnabled = true
	config.RootFSEnabled = true
	config.RootFSSessiondSocket = filepath.Join(tempDir, "sessiond.sock")
	config.RootFSConsumerMountRoot = filepath.Join(tempDir, "consumer-mounts")
	config.RootFSMountRoot = filepath.Join(tempDir, "rootfs-mounts")
	config.RootFSAuthorityURL = "https://regional.example.test"
	config.RootFSAuthorityCAFile = filepath.Join(tempDir, "ca.pem")
	config.RootFSAuthorityClientCertFile = filepath.Join(tempDir, "client.pem")
	config.RootFSAuthorityClientKeyFile = filepath.Join(tempDir, "client-key.pem")
	config.RootFSAuthorityTokenFile = filepath.Join(tempDir, "token")
	config.RuntimeSlotEnabled = true
	config.RuntimeSlotClusterID = "cluster-1"
	config.RuntimeSlotNodeBootIDFile = bootIDPath

	netnsPath := filepath.Join(tempDir, "allocation.netns")
	if err := os.WriteFile(netnsPath, []byte("netns"), 0o600); err != nil {
		t.Fatalf("create network namespace identity: %v", err)
	}
	task := &drivers.TaskConfig{
		ID: "slot-1", AllocID: "allocation-1", Namespace: "default", NodeID: "node-1",
		Name: "runtime-slot", AllocDir: filepath.Join(tempDir, "allocation"),
		Resources: &drivers.Resources{},
		NetworkIsolation: &drivers.NetworkIsolationSpec{
			Mode: drivers.NetIsolationModeGroup,
			Path: netnsPath,
		},
	}
	if err := task.EncodeConcreteDriverConfig(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("encode task config: %v", err)
	}
	runner := newFakeRunsc()
	rootfs := &fakeRootFSRuntime{}
	network := &fakeNetworkRuntime{}
	authority := newFakeRuntimeSlotAuthority()
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return runner }).(*Plugin)
	plugin.config = config
	plugin.rootfs = rootfs
	plugin.rootfsOnce.Do(func() {})
	plugin.newNetwork = func(*PluginConfig) NetworkRuntime { return network }
	plugin.newSlotAuthority = func(*PluginConfig) (runtimeSlotAuthority, error) { return authority, nil }
	return &runtimeSlotPluginFixture{
		plugin: plugin, config: config, task: task, runner: runner,
		rootfs: rootfs, network: network, authority: authority,
	}
}

func TestStartTaskRegistersReadyRuntimeSlotBeforeReturning(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.authority.heartbeatTTL = 300 * time.Millisecond

	handle, _, err := fixture.plugin.StartTask(fixture.task)
	if err != nil {
		t.Fatalf("StartTask() error = %v", err)
	}
	calls, registrations, readiness, _ := fixture.authority.snapshot()
	if !reflect.DeepEqual(calls, []string{"register", "ready"}) {
		t.Fatalf("authority calls at return = %v, want register then ready", calls)
	}
	if len(registrations) != 1 || len(readiness) != 1 {
		t.Fatalf("registrations = %d, readiness = %d, want one each", len(registrations), len(readiness))
	}
	registration := registrations[0]
	if registration.ClusterID != "cluster-1" || registration.AllocationID != fixture.task.AllocID ||
		registration.NodeID != fixture.task.NodeID || registration.NodeBootID != "boot-1" {
		t.Fatalf("registration = %+v", registration)
	}
	if registration.ControlEndpoint != "unix://"+controlSocketPath(fixture.config.ControlDir, fixture.task.ID) ||
		!strings.HasPrefix(registration.RuntimeCompatibility, "sha256:") {
		t.Fatalf("registration endpoint or compatibility = %+v", registration)
	}
	if err := readiness[0].Validate(); err != nil {
		t.Fatalf("readiness proof is invalid: %v", err)
	}

	client := unixHTTPClient(controlSocketPath(fixture.config.ControlDir, fixture.task.ID))
	request, err := http.NewRequest(http.MethodGet, "http://sandbox0/status", nil)
	if err != nil {
		t.Fatalf("build status request: %v", err)
	}
	response, err := client.Do(request)
	if err != nil {
		t.Fatalf("control endpoint was not ready when StartTask returned: %v", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("control status = %s", response.Status)
	}

	select {
	case <-fixture.authority.heartbeatNotify:
	case <-time.After(2 * time.Second):
		t.Fatal("runtime slot heartbeat was not started")
	}
	_, _, _, heartbeats := fixture.authority.snapshot()
	if len(heartbeats) == 0 || heartbeats[0].AllocationID != fixture.task.AllocID || heartbeats[0].NodeBootID != "boot-1" {
		t.Fatalf("heartbeats = %+v", heartbeats)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
	var persisted PersistedState
	if err := handle.GetDriverState(&persisted); err != nil || persisted.Phase != phaseWarm {
		t.Fatalf("Nomad state = %+v, error = %v", persisted, err)
	}
}

func TestStartTaskRegistrationFailureCleansLocalSlot(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.authority.registerErr = fmtErrorUnavailable("regional outage")

	_, _, err := fixture.plugin.StartTask(fixture.task)
	if !errdefs.IsUnavailable(err) {
		t.Fatalf("StartTask() error = %v, want unavailable", err)
	}
	if _, ok := fixture.plugin.tasks.Get(fixture.task.ID); ok {
		t.Fatal("failed slot was added to the task store")
	}
	socketPath := controlSocketPath(fixture.config.ControlDir, fixture.task.ID)
	if _, statErr := os.Stat(socketPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("control socket still exists after failed registration: %v", statErr)
	}
	bundleDir := filepath.Join(fixture.task.TaskDir().Dir, "gvisor-bundle")
	if _, statErr := os.Stat(bundleDir); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("bundle still exists after failed registration: %v", statErr)
	}
	_, cleanups := fixture.network.snapshot()
	if cleanups != 1 {
		t.Fatalf("network cleanups = %d, want 1", cleanups)
	}
}

func TestStartTaskDoesNotRegisterBeforeControlEndpointIsReady(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.config.ControlDir = filepath.Join(t.TempDir(), strings.Repeat("control", 24))

	_, _, err := fixture.plugin.StartTask(fixture.task)
	if err == nil || !strings.Contains(err.Error(), "start task control endpoint") {
		t.Fatalf("StartTask() error = %v, want control endpoint failure", err)
	}
	calls, _, _, _ := fixture.authority.snapshot()
	if len(calls) != 0 {
		t.Fatalf("authority calls = %v, slot was registered without a ready control endpoint", calls)
	}
	if _, ok := fixture.plugin.tasks.Get(fixture.task.ID); ok {
		t.Fatal("slot with failed control endpoint was exposed")
	}
}

func TestRecoverTaskResumesExactRuntimeSlotHeartbeat(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.authority.heartbeatTTL = 5 * time.Second
	handle, _, err := fixture.plugin.StartTask(fixture.task)
	if err != nil {
		t.Fatalf("StartTask() error = %v", err)
	}
	fixture.plugin.cancel()

	fixture.authority.mu.Lock()
	fixture.authority.calls = nil
	fixture.authority.registrations = nil
	fixture.authority.readiness = nil
	fixture.authority.heartbeats = nil
	fixture.authority.state = protocol.StateFastpathReady
	fixture.authority.heartbeatTTL = 300 * time.Millisecond
	fixture.authority.mu.Unlock()
	recoveredRunner := newFakeRunsc()
	recoveredNetwork := &fakeNetworkRuntime{}
	recovered := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return recoveredRunner }).(*Plugin)
	recovered.config = fixture.config
	recovered.rootfs = fixture.rootfs
	recovered.rootfsOnce.Do(func() {})
	recovered.newNetwork = func(*PluginConfig) NetworkRuntime { return recoveredNetwork }
	recovered.newSlotAuthority = func(*PluginConfig) (runtimeSlotAuthority, error) { return fixture.authority, nil }

	if err := recovered.RecoverTask(handle); err != nil {
		t.Fatalf("RecoverTask() error = %v", err)
	}
	calls, registrations, readiness, _ := fixture.authority.snapshot()
	if !reflect.DeepEqual(calls, []string{"register"}) {
		t.Fatalf("recovery authority calls = %v, want exact register only", calls)
	}
	if len(registrations) != 1 || len(readiness) != 0 {
		t.Fatalf("registrations = %d, readiness = %d", len(registrations), len(readiness))
	}
	if registrations[0].NetNSIdentity == "" || registrations[0].ControlEndpoint == "" {
		t.Fatalf("recovered registration = %+v", registrations[0])
	}
	recoveredHandle, ok := recovered.tasks.Get(fixture.task.ID)
	if !ok {
		t.Fatal("recovered runtime slot was not stored")
	}
	if recoveredHandle.driverConfig.Command != "/procd" || !recoveredHandle.driverConfig.WaitForClaim {
		t.Fatalf("recovered driver config = %+v", recoveredHandle.driverConfig)
	}
	select {
	case <-fixture.authority.heartbeatNotify:
	case <-time.After(2 * time.Second):
		t.Fatal("recovered runtime slot did not resume heartbeat")
	}
	if err := recovered.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestRecoverTaskRegistrationFailureDoesNotDestroyActiveRuntime(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.runner.setState("running")
	fixture.authority.registerErr = fmtErrorUnavailable("regional outage")
	bundleDir := filepath.Join(fixture.task.TaskDir().Dir, "gvisor-bundle")
	rootMount := filepath.Join(bundleDir, "rootfs")
	if err := os.MkdirAll(rootMount, 0o755); err != nil {
		t.Fatalf("create recovered root mount: %v", err)
	}
	nomadHandle := drivers.NewTaskHandle(taskHandleVersion)
	nomadHandle.Config = fixture.task
	if err := nomadHandle.SetDriverState(PersistedState{
		TaskConfig: fixture.task, ContainerID: safeContainerID(fixture.task.ID),
		BundleDir: bundleDir, RootMount: rootMount, StartedAt: time.Now(),
		Phase: phaseActive, RootMounted: true,
	}); err != nil {
		t.Fatalf("encode active driver state: %v", err)
	}

	err := fixture.plugin.RecoverTask(nomadHandle)
	if !errdefs.IsUnavailable(err) {
		t.Fatalf("RecoverTask() error = %v, want unavailable", err)
	}
	if _, ok := fixture.plugin.tasks.Get(fixture.task.ID); ok {
		t.Fatal("failed recovered task was exposed")
	}
	for _, call := range fixture.runner.callsSnapshot() {
		if call == "delete" || call == "delete:force" || strings.HasPrefix(call, "kill:") {
			t.Fatalf("runsc call %q destroyed active runtime after authority failure", call)
		}
	}
	if _, err := os.Stat(bundleDir); err != nil {
		t.Fatalf("active bundle was removed after authority failure: %v", err)
	}
	if _, err := os.Stat(controlSocketPath(fixture.config.ControlDir, fixture.task.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed recovery control socket was retained: %v", err)
	}
	_ = fixture.runner.Delete(context.Background(), safeContainerID(fixture.task.ID), true)
}

func TestRuntimeSlotHeartbeatLossPoisonsOnlyWarmSlot(t *testing.T) {
	tests := []struct {
		name       string
		phase      slotPhase
		wantClosed bool
	}{
		{name: "warm", phase: phaseWarm, wantClosed: true},
		{name: "claimed", phase: phaseActive, wantClosed: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority := newFakeRuntimeSlotAuthority()
			authority.state = protocol.StateFastpathReady
			authority.heartbeatTTL = 250 * time.Millisecond
			authority.heartbeatErrors = []error{fmtErrorPermissionDenied("revoked node")}
			handle := newTaskHandle(taskHandleOptions{
				taskConfig: &drivers.TaskConfig{ID: "slot-1"}, bundleDir: t.TempDir(),
				containerID: "s0-slot-1", rootMount: filepath.Join(t.TempDir(), "root"),
				socketPath: filepath.Join(t.TempDir(), "control.sock"), runner: newFakeRunsc(),
				mounter: &fakeMounter{}, logger: hclog.NewNullLogger(),
			})
			handle.setPhase(test.phase)
			lifecycle := &runtimeSlotLifecycle{
				authority: authority, slotID: "slot-1",
				heartbeat: protocol.HeartbeatRequest{AllocationID: "allocation-1", NodeBootID: "boot-1"},
				logger:    hclog.NewNullLogger(),
			}
			initial := authority.observationLocked("slot-1")
			exited := make(chan struct{})
			go func() {
				defer close(exited)
				lifecycle.runHeartbeat(context.Background(), handle.done, initial, handle.runtimeSlotHeartbeatLost)
			}()
			select {
			case <-exited:
			case <-time.After(2 * time.Second):
				t.Fatal("heartbeat loop did not terminate after terminal authority error")
			}
			select {
			case <-handle.done:
				if !test.wantClosed {
					t.Fatal("claimed slot was poisoned by registry heartbeat loss")
				}
			default:
				if test.wantClosed {
					t.Fatal("warm slot was not poisoned by registry heartbeat loss")
				}
			}
			handle.mu.Lock()
			phase := handle.phase
			handle.mu.Unlock()
			if test.wantClosed && phase != phasePoisoned {
				t.Fatalf("phase = %s, want poisoned", phase)
			}
			if !test.wantClosed && phase != phaseActive {
				t.Fatalf("phase = %s, want active", phase)
			}
		})
	}
}

func TestRuntimeSlotHeartbeatRetriesTransientFailureWithinLease(t *testing.T) {
	authority := newFakeRuntimeSlotAuthority()
	authority.state = protocol.StateFastpathReady
	authority.heartbeatTTL = time.Second
	authority.heartbeatErrors = []error{fmtErrorUnavailable("temporary outage")}
	lifecycle := &runtimeSlotLifecycle{
		authority: authority, slotID: "slot-1",
		heartbeat: protocol.HeartbeatRequest{AllocationID: "allocation-1", NodeBootID: "boot-1"},
		logger:    hclog.NewNullLogger(),
	}
	initial := authority.observationLocked("slot-1")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lost := make(chan error, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		lifecycle.runHeartbeat(ctx, make(chan struct{}), initial, func(err error) { lost <- err })
	}()
	for index := 0; index < 2; index++ {
		select {
		case <-authority.heartbeatNotify:
		case <-time.After(2 * time.Second):
			t.Fatalf("heartbeat attempt %d did not occur", index+1)
		}
	}
	cancel()
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat loop did not stop after cancellation")
	}
	select {
	case err := <-lost:
		t.Fatalf("transient heartbeat failure incorrectly exhausted lease: %v", err)
	default:
	}
	_, _, _, heartbeats := authority.snapshot()
	if len(heartbeats) != 2 {
		t.Fatalf("heartbeat attempts = %d, want one failure and one retry", len(heartbeats))
	}
}

func TestRuntimeSlotNetNSReplacementChangesReadinessIdentity(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle := newTaskHandle(taskHandleOptions{
		taskConfig: fixture.task, bundleDir: filepath.Join(t.TempDir(), "bundle"),
		containerID: safeContainerID(fixture.task.ID), rootMount: filepath.Join(t.TempDir(), "root"),
		socketPath: controlSocketPath(fixture.config.ControlDir, fixture.task.ID), runner: fixture.runner,
		mounter: &fakeMounter{}, rootfs: fixture.rootfs, network: fixture.network, logger: hclog.NewNullLogger(),
	})
	if err := os.MkdirAll(handle.rootMount, 0o755); err != nil {
		t.Fatalf("create root mount: %v", err)
	}
	first, err := newRuntimeSlotLifecycle(context.Background(), fixture.config, handle, fixture.rootfs, fixture.authority)
	if err != nil {
		t.Fatalf("first lifecycle: %v", err)
	}
	oldPath := fixture.task.NetworkIsolation.Path + ".old"
	if err := os.Rename(fixture.task.NetworkIsolation.Path, oldPath); err != nil {
		t.Fatalf("retain old network namespace identity: %v", err)
	}
	if err := os.WriteFile(fixture.task.NetworkIsolation.Path, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("replace network namespace identity: %v", err)
	}
	second, err := newRuntimeSlotLifecycle(context.Background(), fixture.config, handle, fixture.rootfs, fixture.authority)
	if err != nil {
		t.Fatalf("second lifecycle: %v", err)
	}
	if first.registration.NetNSIdentity == second.registration.NetNSIdentity {
		t.Fatalf("network namespace identity did not change: %q", first.registration.NetNSIdentity)
	}
	if first.readiness.NetworkReadyDigest == second.readiness.NetworkReadyDigest {
		t.Fatalf("network readiness digest did not change after namespace replacement: %q", first.readiness.NetworkReadyDigest)
	}
}

func TestValidateRuntimeSlotTaskConfigRequiresGenericProcdSlot(t *testing.T) {
	config := &PluginConfig{RuntimeSlotEnabled: true}
	tests := []struct {
		name string
		task TaskConfig
	}{
		{name: "eager", task: TaskConfig{Command: "/procd"}},
		{name: "different command", task: TaskConfig{Command: "/bin/sh", WaitForClaim: true}},
		{name: "arguments", task: TaskConfig{Command: "/procd", Args: []string{"--unsafe"}, WaitForClaim: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateRuntimeSlotTaskConfig(config, test.task); err == nil {
				t.Fatal("invalid regional runtime slot task was accepted")
			}
		})
	}
	if err := validateRuntimeSlotTaskConfig(config, TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("valid regional runtime slot task was rejected: %v", err)
	}
}

func TestValidateRuntimeSlotConfigRequiresProductionDependencies(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	if err := validateRuntimeSlotConfig(fixture.config); err != nil {
		t.Fatalf("valid runtime slot config was rejected: %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*PluginConfig)
		match  string
	}{
		{name: "cluster", mutate: func(config *PluginConfig) { config.RuntimeSlotClusterID = "" }, match: "cluster_id"},
		{name: "session daemon", mutate: func(config *PluginConfig) { config.RootFSEnabled = false }, match: "session daemon"},
		{name: "network", mutate: func(config *PluginConfig) { config.NetworkPolicyEnabled = false }, match: "network_policy_enabled"},
		{name: "authority", mutate: func(config *PluginConfig) { config.RootFSAuthorityURL = "" }, match: "authority_url"},
		{name: "boot identity", mutate: func(config *PluginConfig) { config.RuntimeSlotNodeBootIDFile = "relative" }, match: "node_boot_id_file"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := *fixture.config
			test.mutate(&config)
			err := validateRuntimeSlotConfig(&config)
			if err == nil || !strings.Contains(err.Error(), test.match) {
				t.Fatalf("validateRuntimeSlotConfig() error = %v, want %q", err, test.match)
			}
		})
	}
}

func fmtErrorUnavailable(message string) error {
	return errors.Join(errors.New(message), errdefs.ErrUnavailable)
}

func fmtErrorPermissionDenied(message string) error {
	return errors.Join(errors.New(message), errdefs.ErrPermissionDenied)
}
