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

package nomadruntime

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

type fakeRunsc struct {
	mu           sync.Mutex
	state        string
	calls        []string
	stateErr     error
	deleteErr    error
	waitReleased chan struct{}
	releaseOnce  sync.Once
}

func newFakeRunsc() *fakeRunsc {
	return &fakeRunsc{waitReleased: make(chan struct{})}
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

func (r *fakeRunsc) Create(context.Context, string, string) error {
	r.record("create")
	return nil
}

func (r *fakeRunsc) Start(context.Context, string) error {
	r.record("start")
	return nil
}

func (r *fakeRunsc) Wait(ctx context.Context, _ string) (WaitResult, error) {
	r.record("wait")
	select {
	case <-ctx.Done():
		return WaitResult{}, ctx.Err()
	case <-r.waitReleased:
		return WaitResult{}, nil
	}
}

func (r *fakeRunsc) Kill(_ context.Context, _, signal string) error {
	r.record("kill:" + signal)
	r.releaseOnce.Do(func() { close(r.waitReleased) })
	return nil
}

func (r *fakeRunsc) Delete(_ context.Context, _ string, force bool) error {
	if force {
		r.record("delete:force")
	} else {
		r.record("delete")
	}
	if r.deleteErr != nil {
		return r.deleteErr
	}
	r.releaseOnce.Do(func() { close(r.waitReleased) })
	return nil
}

func (r *fakeRunsc) State(context.Context, string) (RunscState, error) {
	r.record("state")
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
	unmounts []string
}

func (*fakeMounter) Bind(string, string) error { return nil }

func (m *fakeMounter) Unmount(target string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unmounts = append(m.unmounts, target)
	return nil
}

type fakeRootFSRuntime struct {
	mu               sync.Mutex
	source           string
	ensureErr        error
	consumerErr      error
	crashErr         error
	retireErr        error
	ensureCalls      int
	retireCalls      int
	crashCalls       int
	externalReclaims int
	lastParent       string
	lastOperation    string
	leaseLoss        func(error)
	pressureSignal   chan struct{}
	pressures        []rootfssession.DirtyTailPressureSession
	pressurePlans    []rootfssession.DirtyTailPressureSession
	pressurePlanErr  error
	recoverySessions []rootfssession.RecoverySession
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
	context.Context,
	rootfshandoff.StageRequest,
	ConsumerRequest,
) (ConsumerLease, error) {
	if r.consumerErr != nil {
		return ConsumerLease{}, r.consumerErr
	}
	return ConsumerLease{LeaseID: "fake-consumer", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (r *fakeRootFSRuntime) RenewConsumer(
	_ context.Context,
	_ rootfshandoff.StageRequest,
	lease ConsumerLease,
) (ConsumerLease, error) {
	lease.ExpiresAt = time.Now().Add(time.Hour)
	return lease, nil
}

func (*fakeRootFSRuntime) CaptureRunningFork(
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
		pressure.Stage.Parent,
		pressure.Stage.Identity.WriterGrantID,
		pressure.Stage.Identity.WriterEpoch,
	), nil
}

func (r *fakeRootFSRuntime) Retire(
	_ context.Context,
	request rootfshandoff.StageRequest,
	operationID string,
) (rootfssession.RetireResult, error) {
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
	_ CrashTaskObservation,
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
	observation CrashTaskObservation,
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

func (*fakeRootFSRuntime) ReclaimVerifiedTerminal(context.Context, rootfshandoff.StageRequest) error {
	return nil
}

func (r *fakeRootFSRuntime) snapshot() (ensureCalls, retireCalls int, parent, operation string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.ensureCalls, r.retireCalls, r.lastParent, r.lastOperation
}

type fakeCtldNetwork struct {
	client   *protocol.RuntimeSlotNetworkClient
	mu       sync.Mutex
	cleanups int
}

func newFakeCtldNetwork(t *testing.T) *fakeCtldNetwork {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "ctld-network.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0o600))
	fake := &fakeCtldNetwork{}
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		writer.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case protocol.RuntimeSlotNetworkHealthPath:
			_, _ = writer.Write([]byte("{}"))
		case protocol.RuntimeSlotNetworkRegisterPath:
			_ = json.NewEncoder(writer).Encode(protocol.RuntimeSlotNetworkRegistrationResponse{NetworkPolicyApplied: true})
		case protocol.RuntimeSlotNetworkCleanupPath:
			fake.mu.Lock()
			fake.cleanups++
			fake.mu.Unlock()
			_ = json.NewEncoder(writer).Encode(protocol.RuntimeSlotNetworkCleanupResponse{NetworkPolicyAbsent: true})
		default:
			http.NotFound(writer, request)
		}
	})}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { require.NoError(t, server.Close()) })
	fake.client, err = protocol.NewRuntimeSlotNetworkClient(socket, time.Second)
	require.NoError(t, err)
	return fake
}

func (f *fakeCtldNetwork) cleanupCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cleanups
}
