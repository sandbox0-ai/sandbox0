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
	"encoding/hex"
	"encoding/json"
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
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
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
	startingErrors  []error
	commandErrors   []error
	calls           []string
	registrations   []protocol.RegistrationRequest
	readiness       []protocol.ReadinessRequest
	heartbeats      []protocol.HeartbeatRequest
	starting        []protocol.StartingRequest
	commands        []protocol.CommandReadyRequest
	heartbeatNotify chan struct{}
	startingHook    func(protocol.StartingRequest)
	commandHook     func(protocol.CommandReadyRequest)
	readyHook       func()
	claimOperation  string
	claimID         string
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
		operationID := a.claimOperation
		if operationID == "" {
			operationID = "operation-1"
		}
		claimID := a.claimID
		if claimID == "" {
			claimID = "claim-1"
		}
		observation.ClaimOperationID = operationID
		observation.ClaimID = claimID
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
	if a.readyHook != nil {
		a.readyHook()
	}
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
	request protocol.StartingRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	a.calls = append(a.calls, "starting")
	a.starting = append(a.starting, request)
	a.claimOperation = request.OperationID
	a.claimID = request.ClaimID
	var err error
	if len(a.startingErrors) != 0 {
		err = a.startingErrors[0]
		a.startingErrors = a.startingErrors[1:]
	}
	hook := a.startingHook
	if err != nil {
		if errdefs.IsUnavailable(err) {
			a.state = protocol.StateStarting
		}
		a.mu.Unlock()
		if hook != nil {
			hook(request)
		}
		return protocol.Observation{}, err
	}
	a.state = protocol.StateStarting
	observation := a.observationLocked(slotID)
	a.mu.Unlock()
	if hook != nil {
		hook(request)
	}
	return observation, nil
}

func (a *fakeRuntimeSlotAuthority) startingSnapshot() []protocol.StartingRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]protocol.StartingRequest(nil), a.starting...)
}

func (a *fakeRuntimeSlotAuthority) commandSnapshot() []protocol.CommandReadyRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]protocol.CommandReadyRequest(nil), a.commands...)
}

func (a *fakeRuntimeSlotAuthority) CommandReady(
	_ context.Context,
	slotID string,
	request protocol.CommandReadyRequest,
) (protocol.Observation, error) {
	a.mu.Lock()
	a.calls = append(a.calls, "command-ready")
	a.commands = append(a.commands, request)
	a.claimOperation = request.OperationID
	a.claimID = request.ClaimID
	var err error
	if len(a.commandErrors) != 0 {
		err = a.commandErrors[0]
		a.commandErrors = a.commandErrors[1:]
	}
	hook := a.commandHook
	if err != nil {
		if errdefs.IsUnavailable(err) {
			a.state = protocol.StateActive
		}
		a.mu.Unlock()
		if hook != nil {
			hook(request)
		}
		return protocol.Observation{}, err
	}
	a.state = protocol.StateActive
	observation := a.observationLocked(slotID)
	a.mu.Unlock()
	if hook != nil {
		hook(request)
	}
	return observation, nil
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
	config.RootFSNodeSocket = filepath.Join(tempDir, "ctld-runtime.sock")
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
		Name: protocol.NomadTaskName, AllocDir: filepath.Join(tempDir, "allocation"),
		Env: map[string]string{
			"NOMAD_ALLOC_ADDR_" + protocol.NomadProcdPortLabel: "172.26.64.2:49983",
			"UNTRUSTED_TASK_ENV":                               "must-not-enter-procd",
		},
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
	journalReady := false
	fixture.authority.readyHook = func() {
		fixture.rootfs.mu.Lock()
		journalReady = len(fixture.rootfs.journalRecords) == 1
		fixture.rootfs.mu.Unlock()
	}

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
	if !journalReady {
		t.Fatal("regional readiness preceded durable node runtime-slot registration")
	}
	fixture.rootfs.mu.Lock()
	nodeRecords := append([]runtimeSlotJournalRegistration(nil), fixture.rootfs.journalRecords...)
	fixture.rootfs.mu.Unlock()
	if len(nodeRecords) != 1 || nodeRecords[0].SlotID != fixture.task.ID ||
		nodeRecords[0].RunscContainerID != protocol.NomadRunscContainerID(fixture.task.ID) ||
		nodeRecords[0].StableMountID == "" || nodeRecords[0].MountNamespaceID == "" {
		t.Fatalf("node runtime-slot registrations = %+v", nodeRecords)
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
	if cleanups != 0 {
		t.Fatalf("legacy network cleanups = %d, want 0 for a ctld-owned runtime slot", cleanups)
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

func TestStartTaskRejectsUnaddressableNomadProcdBeforeRegistration(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*drivers.TaskConfig)
	}{
		{name: "task name", mutate: func(task *drivers.TaskConfig) { task.Name = "warm-slot" }},
		{name: "missing address", mutate: func(task *drivers.TaskConfig) {
			delete(task.Env, "NOMAD_ALLOC_ADDR_"+protocol.NomadProcdPortLabel)
		}},
		{name: "wrong port", mutate: func(task *drivers.TaskConfig) {
			task.Env["NOMAD_ALLOC_ADDR_"+protocol.NomadProcdPortLabel] = "172.26.64.2:49984"
		}},
		{name: "noncanonical address", mutate: func(task *drivers.TaskConfig) {
			task.Env["NOMAD_ALLOC_ADDR_"+protocol.NomadProcdPortLabel] = " 172.26.64.2:49983"
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeSlotPluginFixture(t)
			test.mutate(fixture.task)
			if _, _, err := fixture.plugin.StartTask(fixture.task); err == nil {
				t.Fatal("unaddressable Nomad procd task was accepted")
			}
			calls, _, _, _ := fixture.authority.snapshot()
			if len(calls) != 0 {
				t.Fatalf("authority calls = %v, invalid task was registered", calls)
			}
		})
	}
}

func TestStartTaskDoesNotExposeSlotWhenNodeJournalFails(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	fixture.rootfs.journalErr = fmtErrorUnavailable("journal unavailable")
	if _, _, err := fixture.plugin.StartTask(fixture.task); err == nil {
		t.Fatal("StartTask() accepted an unjournaled runtime slot")
	}
	calls, _, _, _ := fixture.authority.snapshot()
	if len(calls) != 0 {
		t.Fatalf("authority calls = %v, unjournaled slot reached region", calls)
	}
}

func prepareRuntimeSlotClaim(
	t *testing.T,
	fixture *runtimeSlotPluginFixture,
) (*taskHandle, rootfshandoff.StageRequest, string, string, *fakeMounter) {
	t.Helper()
	fixture.authority.heartbeatTTL = runtimeSlotMaxHeartbeatTTL
	if _, _, err := fixture.plugin.StartTask(fixture.task); err != nil {
		t.Fatalf("StartTask() error = %v", err)
	}
	handle, ok := fixture.plugin.tasks.Get(fixture.task.ID)
	if !ok {
		t.Fatal("runtime slot handle was not stored")
	}
	mounter := &fakeMounter{}
	handle.mounter = mounter
	source := filepath.Join(fixture.config.RootFSMountRoot, "claim-source")
	if err := os.MkdirAll(filepath.Join(source, "bin"), 0o755); err != nil {
		t.Fatalf("create RootFS claim source: %v", err)
	}
	stage, token, networkPolicy := newAuthorizedRootFSStage(t, source)
	netnsIdentity, err := networkNamespaceIdentity(fixture.task.NetworkIsolation.Path)
	if err != nil {
		t.Fatalf("derive claim netns identity: %v", err)
	}
	stage.ExpectedPolicyToken.PodUID = fixture.task.AllocID
	stage.ExpectedPolicyToken.ClaimID = "claim-1"
	stage.ExpectedPolicyToken.NetNSIdentity = netnsIdentity
	stage.Identity.NodeUID = fixture.task.NodeID
	stage.Identity.BootID = "boot-1"
	stage.Identity.PodUID = fixture.task.AllocID
	stage.Identity.ContainerName = fixture.task.Name
	stage.Identity.SlotNonce = fixture.task.ID
	stage.Identity.ClaimID = "claim-1"
	runtimeRevision, err := runtimeSlotAssignment().Revision()
	if err != nil {
		t.Fatalf("derive runtime assignment revision: %v", err)
	}
	stage.Labels = map[string]string{protocol.RuntimeAssignmentRevisionLabel: runtimeRevision}
	if err := stage.Validate(); err != nil {
		t.Fatalf("validate regional runtime slot stage: %v", err)
	}
	fixture.rootfs.mu.Lock()
	fixture.rootfs.source = source
	fixture.rootfs.mu.Unlock()
	fixture.authority.mu.Lock()
	fixture.authority.state = protocol.StateClaiming
	fixture.authority.claimOperation = "operation-1"
	fixture.authority.claimID = "claim-1"
	fixture.authority.mu.Unlock()
	return handle, stage, token, networkPolicy, mounter
}

func runtimeSlotAssignment() *runtimecontrol.Assignment {
	return &runtimecontrol.Assignment{
		SandboxID: "sandbox-1", TeamID: "team-1", RuntimeGeneration: 1,
		EnvVars: map[string]string{runtimecontrol.EnvSandboxID: "sandbox-1"},
	}
}

func TestRuntimeSlotClaimRetriesStartingBeforeRunscCreate(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, _ := prepareRuntimeSlotClaim(t, fixture)
	fixture.authority.mu.Lock()
	fixture.authority.startingErrors = []error{fmtErrorUnavailable("response lost")}
	startedTooEarly := false
	fixture.authority.startingHook = func(protocol.StartingRequest) {
		if contains(fixture.runner.callsSnapshot(), "create") {
			startedTooEarly = true
		}
	}
	fixture.authority.mu.Unlock()

	err := handle.Claim(ClaimRequest{
		OperationID: "operation-1", ClaimID: "claim-1",
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	})
	if err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	if startedTooEarly {
		t.Fatal("runsc create happened before the regional starting transition")
	}
	requests := fixture.authority.startingSnapshot()
	if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) {
		t.Fatalf("starting requests = %+v, want one byte-stable retry", requests)
	}
	request := requests[0]
	if request.OperationID != "operation-1" || request.ClaimID != "claim-1" ||
		request.LaunchAttempt != stage.Identity.LaunchAttempt || request.RunscContainerID != safeContainerID(fixture.task.ID) {
		t.Fatalf("starting request identity = %+v", request)
	}
	bindingDigest, err := stage.BindingDigest()
	if err != nil {
		t.Fatalf("derive expected binding digest: %v", err)
	}
	if request.RootFSBindingDigest != hex.EncodeToString(bindingDigest[:]) {
		t.Fatalf("RootFS binding digest = %q", request.RootFSBindingDigest)
	}
	if _, err := protocol.DecodeProof("claim_network_digest", request.ClaimNetworkDigest); err != nil {
		t.Fatalf("claim network proof = %q: %v", request.ClaimNetworkDigest, err)
	}
	persisted := handle.PersistedState()
	runtimeRevision, err := runtimeSlotAssignment().Revision()
	if err != nil {
		t.Fatalf("derive expected runtime revision: %v", err)
	}
	if persisted.Claim == nil || persisted.Claim.OperationID != request.OperationID ||
		persisted.Claim.ClaimID != request.ClaimID ||
		persisted.Claim.RootFSBindingDigest != request.RootFSBindingDigest ||
		persisted.Claim.ClaimNetworkDigest != request.ClaimNetworkDigest ||
		persisted.Claim.RuntimeRevision != runtimeRevision {
		t.Fatalf("persisted claim = %+v", persisted.Claim)
	}
	if calls := fixture.runner.callsSnapshot(); !contains(calls, "create") || !contains(calls, "start") {
		t.Fatalf("runsc calls = %v, want create and start after regional starting", calls)
	}
	configPayload, err := os.ReadFile(filepath.Join(handle.bundleDir, "config.json"))
	if err != nil {
		t.Fatalf("read claimed OCI config: %v", err)
	}
	var spec specs.Spec
	if err := json.Unmarshal(configPayload, &spec); err != nil {
		t.Fatalf("decode claimed OCI config: %v", err)
	}
	assignmentPayload, err := json.Marshal(runtimeSlotAssignment())
	if err != nil {
		t.Fatalf("encode expected runtime assignment: %v", err)
	}
	for _, expected := range []string{
		"http_port=49983",
		runtimecontrol.EnvControlMode + "=" + runtimecontrol.ControlModeStatic,
		runtimecontrol.EnvStaticAssignment + "=" + string(assignmentPayload),
	} {
		if !contains(spec.Process.Env, expected) {
			t.Fatalf("OCI environment = %q, missing %q", spec.Process.Env, expected)
		}
	}
	for _, value := range spec.Process.Env {
		if strings.HasPrefix(value, "UNTRUSTED_TASK_ENV=") || strings.HasPrefix(value, "NOMAD_ALLOC_ADDR_") {
			t.Fatalf("Nomad task environment leaked into procd OCI environment: %q", value)
		}
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestRuntimeSlotClaimStartingRejectionPoisonsWithoutRunsc(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, mounter := prepareRuntimeSlotClaim(t, fixture)
	fixture.authority.mu.Lock()
	fixture.authority.startingErrors = []error{fmtErrorPermissionDenied("claim revoked")}
	fixture.authority.mu.Unlock()

	err := handle.Claim(ClaimRequest{
		OperationID: "operation-1", ClaimID: "claim-1",
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	})
	if !errdefs.IsPermissionDenied(err) {
		t.Fatalf("Claim() error = %v, want permission denied", err)
	}
	if calls := fixture.runner.callsSnapshot(); contains(calls, "create") || contains(calls, "start") {
		t.Fatalf("runsc calls = %v, rejected starting transition launched runsc", calls)
	}
	if phase := handle.TaskStatus().DriverAttributes["phase"]; phase != string(phasePoisoned) {
		t.Fatalf("phase = %s, want poisoned", phase)
	}
	_, unmounts := mounter.snapshot()
	if len(unmounts) == 0 || unmounts[len(unmounts)-1] != handle.rootMount {
		t.Fatalf("unmounts = %v, want claimed root detached", unmounts)
	}
	_, retireCalls, _, _ := fixture.rootfs.snapshot()
	if retireCalls != 1 {
		t.Fatalf("RootFS retire calls = %d, want consumed writer retired", retireCalls)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestRuntimeSlotClaimAcceptsExactRetryAfterResponseLoss(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, _ := prepareRuntimeSlotClaim(t, fixture)
	request := ClaimRequest{
		OperationID: "operation-1", ClaimID: "claim-1",
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	}
	if err := handle.Claim(request); err != nil {
		t.Fatalf("first Claim() error = %v", err)
	}
	if err := handle.Claim(request); err != nil {
		t.Fatalf("exact Claim() retry error = %v", err)
	}
	ensureCalls, _, _, _ := fixture.rootfs.snapshot()
	if ensureCalls != 1 {
		t.Fatalf("RootFS Ensure calls = %d, exact retry consumed the writer twice", ensureCalls)
	}
	if starting := fixture.authority.startingSnapshot(); len(starting) != 1 {
		t.Fatalf("regional starting calls = %d, exact local retry reported another launch", len(starting))
	}

	changedStage := stage
	changedPolicy := `{"mode":"block-all"}`
	changedStage.ExpectedPolicyToken.PolicyDigest = digestString(changedPolicy)
	changed := request
	changed.Stage = &changedStage
	changed.NetworkPolicy = changedPolicy
	if err := handle.Claim(changed); err == nil {
		t.Fatal("changed active claim retry was accepted")
	}
	changed = request
	changed.Runtime = runtimeSlotAssignment()
	changed.Runtime.RuntimeGeneration++
	if err := handle.Claim(changed); err == nil {
		t.Fatal("changed active runtime assignment retry was accepted")
	}
	ensureCalls, _, _, _ = fixture.rootfs.snapshot()
	if ensureCalls != 1 {
		t.Fatalf("RootFS Ensure calls = %d after changed retry", ensureCalls)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestRuntimeSlotClaimRequiresRegionalIdentityBeforeConsumingWriter(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, _ := prepareRuntimeSlotClaim(t, fixture)

	err := handle.Claim(ClaimRequest{
		ClaimID: "claim-1", PolicyToken: token, WriterEpoch: "1",
		Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	})
	if err == nil || !strings.Contains(err.Error(), "operation_id") {
		t.Fatalf("Claim() error = %v, want missing operation ID", err)
	}
	ensureCalls, retireCalls, _, _ := fixture.rootfs.snapshot()
	if ensureCalls != 0 || retireCalls != 0 {
		t.Fatalf("RootFS calls = ensure %d retire %d, invalid claim consumed writer", ensureCalls, retireCalls)
	}
	if phase := handle.TaskStatus().DriverAttributes["phase"]; phase != string(phaseWarm) {
		t.Fatalf("phase = %s, want reusable warm slot before writer consumption", phase)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestRuntimeSlotClaimRejectsNetworkTokenForAnotherAllocationAddress(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, _ := prepareRuntimeSlotClaim(t, fixture)
	stage.ExpectedPolicyToken.PodIP = "172.26.64.3"

	err := handle.Claim(ClaimRequest{
		OperationID: "operation-1", ClaimID: "claim-1",
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	})
	if !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("Claim() error = %v, want failed precondition", err)
	}
	ensureCalls, retireCalls, _, _ := fixture.rootfs.snapshot()
	if ensureCalls != 0 || retireCalls != 0 {
		t.Fatalf("RootFS calls = ensure %d retire %d, mismatched address consumed writer", ensureCalls, retireCalls)
	}
	if phase := handle.TaskStatus().DriverAttributes["phase"]; phase != string(phaseWarm) {
		t.Fatalf("phase = %s, want warm", phase)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func commandReadyProof(fixture *runtimeSlotPluginFixture, stage rootfshandoff.StageRequest) protocol.CommandReadyProof {
	return protocol.CommandReadyProof{
		Version: protocol.CommandReadyProofVersion, SlotID: fixture.task.ID,
		OperationID: "operation-1", ClaimID: "claim-1", LaunchAttempt: stage.Identity.LaunchAttempt,
		RunscContainerID: safeContainerID(fixture.task.ID), ProcdInstanceID: "procd-instance-1",
		ProcdAddress:  "http://172.26.64.2:49983",
		RequestMethod: "PUT", RequestPath: protocol.ProcdCommandReadyProbePath, ResponseStatus: http.StatusOK,
		ResponseBodyDigest: strings.Repeat("ab", 32),
	}
}

func TestRuntimeSlotCommandReadyRetriesAcceptedResponseLoss(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, _ := prepareRuntimeSlotClaim(t, fixture)
	if err := handle.Claim(ClaimRequest{
		OperationID: "operation-1", ClaimID: "claim-1",
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	fixture.authority.mu.Lock()
	fixture.authority.commandErrors = []error{fmtErrorUnavailable("active response lost")}
	fixture.authority.mu.Unlock()
	proof := commandReadyProof(fixture, stage)
	wrongAddress := proof
	wrongAddress.ProcdAddress = "http://172.26.64.3:49983"
	if err := handle.CommandReady(CommandReadyRequest{Proof: wrongAddress}); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("wrong-address CommandReady() error = %v, want failed precondition", err)
	}
	if got := len(fixture.authority.commandSnapshot()); got != 0 {
		t.Fatalf("authority command-ready calls = %d, wrong procd address reached region", got)
	}
	payload, err := json.Marshal(CommandReadyRequest{Proof: proof})
	if err != nil {
		t.Fatalf("encode command-ready request: %v", err)
	}
	client := unixHTTPClient(controlSocketPath(fixture.config.ControlDir, fixture.task.ID))
	if _, err := awaitControl(client, http.MethodPut, protocol.NodeCommandReadyControlPath, payload, 2*time.Second); err != nil {
		t.Fatalf("command-ready control request: %v", err)
	}
	requests := fixture.authority.commandSnapshot()
	if len(requests) != 2 || !reflect.DeepEqual(requests[0], requests[1]) {
		t.Fatalf("regional command-ready requests = %+v, want exact response-loss retry", requests)
	}
	expectedDigest, err := proof.Digest()
	if err != nil {
		t.Fatalf("derive expected command-ready digest: %v", err)
	}
	if requests[0].ProcdInstanceID != proof.ProcdInstanceID || requests[0].ProcdAddress != proof.ProcdAddress ||
		requests[0].CommandReadyDigest != expectedDigest {
		t.Fatalf("regional command-ready request = %+v", requests[0])
	}
	persisted := handle.PersistedState()
	if persisted.Claim == nil || persisted.Claim.ProcdInstanceID != proof.ProcdInstanceID ||
		persisted.Claim.CommandReadyDigest != expectedDigest {
		t.Fatalf("persisted command readiness = %+v", persisted.Claim)
	}
	changed := proof
	changed.ProcdInstanceID = "another-procd-instance"
	if err := handle.CommandReady(CommandReadyRequest{Proof: changed}); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("changed CommandReady() error = %v, want failed precondition", err)
	}
	if got := len(fixture.authority.commandSnapshot()); got != 2 {
		t.Fatalf("authority command-ready calls = %d, changed local proof reached region", got)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestRuntimeSlotCommandReadyRejectionFencesWriter(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	handle, stage, token, networkPolicy, _ := prepareRuntimeSlotClaim(t, fixture)
	if err := handle.Claim(ClaimRequest{
		OperationID: "operation-1", ClaimID: "claim-1",
		PolicyToken: token, WriterEpoch: "1", Stage: &stage, NetworkPolicy: networkPolicy,
		Runtime: runtimeSlotAssignment(),
	}); err != nil {
		t.Fatalf("Claim() error = %v", err)
	}
	fixture.authority.mu.Lock()
	fixture.authority.commandErrors = []error{fmtErrorPermissionDenied("claim was revoked")}
	fixture.authority.mu.Unlock()

	err := handle.CommandReady(CommandReadyRequest{Proof: commandReadyProof(fixture, stage)})
	if !errdefs.IsPermissionDenied(err) {
		t.Fatalf("CommandReady() error = %v, want permission denied", err)
	}
	select {
	case <-handle.done:
	case <-time.After(2 * time.Second):
		t.Fatal("regional command-ready rejection did not poison the writer")
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		fixture.rootfs.mu.Lock()
		crashCalls := fixture.rootfs.crashCalls
		fixture.rootfs.mu.Unlock()
		if crashCalls == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	fixture.rootfs.mu.Lock()
	crashCalls := fixture.rootfs.crashCalls
	fixture.rootfs.mu.Unlock()
	if crashCalls != 1 {
		t.Fatalf("RootFS crash-fence calls = %d, want 1", crashCalls)
	}
	if err := fixture.plugin.DestroyTask(fixture.task.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
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

func TestRecoveredCrashFencedClaimRemainsInspectable(t *testing.T) {
	for _, state := range []protocol.State{
		protocol.StateClaiming, protocol.StateStarting, protocol.StateActive, protocol.StateQuiescing,
	} {
		if err := validateRecoveredRuntimeSlotState(state, phasePoisoned); err != nil {
			t.Fatalf("state %s rejected crash-fenced local claim: %v", state, err)
		}
	}
	if err := validateRecoveredRuntimeSlotState(protocol.StateFastpathReady, phasePoisoned); err == nil {
		t.Fatal("unclaimed fast-path slot accepted poisoned local state")
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
		{name: "ctld runtime", mutate: func(config *PluginConfig) { config.RootFSEnabled = false }, match: "ctld-owned Nomad runtime"},
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
