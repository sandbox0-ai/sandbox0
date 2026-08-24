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
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/nomad-driver-sandbox0/internal/rootfsbuilder"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
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

func (r *fakeRunsc) Wait(ctx context.Context, _ string) (WaitResult, error) {
	r.record("wait")
	r.mu.Lock()
	result := r.waitResult
	err := r.waitErr
	released := r.waitReleased
	r.mu.Unlock()
	if err != nil {
		return result, err
	}
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case <-released:
		return result, nil
	}
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

func (r *fakeRunsc) Stats(context.Context, string) (RunscStats, error) {
	r.record("stats")
	return RunscStats{Type: "stats", ID: "fake"}, nil
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
	mu               sync.Mutex
	source           string
	pingErr          error
	ensureErr        error
	consumerErr      error
	crashErr         error
	retireErr        error
	ensureCalls      int
	retireCalls      int
	crashCalls       int
	externalReclaims int
	journalRecords   []runtimeSlotJournalRegistration
	journalErr       error
	lastParent       string
	lastOperation    string
	leaseLoss        func(error)
	pressureSignal   chan struct{}
	pressures        []rootfssession.DirtyTailPressureSession
	pressurePlans    []rootfssession.DirtyTailPressureSession
	pressurePlanErr  error
	recoverySessions []rootfssession.RecoverySession
	runtimeInfo      nomadruntime.RuntimeInfo
}

func (r *fakeRootFSRuntime) RuntimeInfo(context.Context) (nomadruntime.RuntimeInfo, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.pingErr != nil {
		return nomadruntime.RuntimeInfo{}, r.pingErr
	}
	if r.runtimeInfo.Version != 0 {
		return r.runtimeInfo, nil
	}
	return nomadruntime.RuntimeInfo{
		Version: nomadruntime.RuntimeInfoVersion, MountRoot: "/run/sandbox0/rootfs",
		MaxDirtyTailBytes:               rootfssession.DefaultMaxDirtyTailBytes,
		MaxNodeDirtyTailBytes:           rootfssession.DefaultMaxNodeDirtyTailBytes,
		DirtyTailRetirementReserveBytes: rootfssession.DefaultDirtyTailRetirementReserveBytes,
	}, nil
}

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
	if r.consumerErr != nil {
		return RootFSConsumerLease{}, r.consumerErr
	}
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
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rootfssession.RecoverySession(nil), r.recoverySessions...), nil
}

func (r *fakeRootFSRuntime) DirtyTailPressureSignal() <-chan struct{} {
	return r.pressureSignal
}

func (r *fakeRootFSRuntime) DirtyTailPressureSessions() ([]rootfssession.DirtyTailPressureSession, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]rootfssession.DirtyTailPressureSession(nil), r.pressures...), nil
}

func (r *fakeRootFSRuntime) PlanDirtyTailPressure(
	_ context.Context,
	pressure rootfssession.DirtyTailPressureSession,
) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pressurePlans = append(r.pressurePlans, pressure)
	if r.pressurePlanErr != nil {
		return "", r.pressurePlanErr
	}
	return rootfshandoff.PlannedRetireOperationID(
		pressure.Stage.Parent, pressure.Stage.Identity.WriterGrantID, pressure.Stage.Identity.WriterEpoch,
	), nil
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

func (r *fakeRootFSRuntime) FenceLocalRootFSWriter(
	ctx context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
	observation crashTaskObservation,
) (rootfshandoff.CrashFenceProof, error) {
	return r.CrashFence(ctx, request, operationID, observation)
}

func (r *fakeRootFSRuntime) ReclaimExternallyRetired(
	context.Context,
	rootfshandoff.StageRequest,
) (bool, error) {
	r.mu.Lock()
	r.externalReclaims++
	r.mu.Unlock()
	return true, nil
}

func (r *fakeRootFSRuntime) ReclaimVerifiedTerminal(
	context.Context,
	rootfshandoff.StageRequest,
) error {
	return nil
}

func (r *fakeRootFSRuntime) RegisterRuntimeSlot(
	_ context.Context,
	registration runtimeSlotJournalRegistration,
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.journalRecords = append(r.journalRecords, registration)
	return r.journalErr
}

func (r *fakeRootFSRuntime) snapshot() (ensureCalls, retireCalls int, parent, operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensureCalls, r.retireCalls, r.lastParent, r.lastOperation
}

func (r *fakeRootFSRuntime) terminalCallsSnapshot() (retireCalls, crashCalls int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.retireCalls, r.crashCalls
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

type noOpCommandRunner struct{}

func (noOpCommandRunner) Run(context.Context, string, ...string) error { return nil }

func newAuthorizedRootFSStage(t *testing.T, source string) (rootfshandoff.StageRequest, string, string) {
	t.Helper()
	store := objectstore.NewMemoryStore(t.Name()).(objectstore.ContextConditionalStore)
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
		Parent:         zeroDigest, InitialGeneration: descriptor.GenerationID, Generation: &descriptor,
		ExpectedPolicyToken: rootfshandoff.NetworkPolicyToken{
			AllocationID: "alloc-1", NetworkIncarnationID: "sandbox-1", ClaimID: "claim-1",
			NetworkEpoch: 1, PolicyDigest: digestString(networkPolicy), SourceIP: "172.26.64.2",
			CtldGeneration: "ctld-1", NetNSIdentity: "netns-1",
		},
		Identity: rootfshandoff.Identity{
			NodeUID: "node-1", BootID: "boot-1", RuntimeGeneration: "runtime-1",
			AllocationID: "alloc-1", NetworkIncarnationID: "sandbox-1", TaskName: "warm-slot",
			SourceOCIDigest: descriptor.SourceOCIDigest, RootFSDriver: "nomad-driver", RuntimeClass: PluginName,
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

func contains(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}
