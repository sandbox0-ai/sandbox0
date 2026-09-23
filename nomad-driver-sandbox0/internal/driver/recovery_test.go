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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-msgpack/v2/codec"
	trstate "github.com/hashicorp/nomad/client/allocrunner/taskrunner/state"
	nomadstate "github.com/hashicorp/nomad/client/state"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/plugins/drivers"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Nomad's boltdd Bucket.Put/Get uses this codec for LocalState. A direct handle
// copy misses the loss of TaskConfig's unexported rawDriverConfig on restart.
func roundTripNomadTaskHandle(t *testing.T, handle *drivers.TaskHandle) *drivers.TaskHandle {
	t.Helper()
	var encoded bytes.Buffer
	if err := codec.NewEncoder(&encoded, structs.MsgpackHandle).Encode(&trstate.LocalState{TaskHandle: handle}); err != nil {
		t.Fatalf("encode Nomad LocalState: %v", err)
	}
	var restored trstate.LocalState
	if err := codec.NewDecoderBytes(encoded.Bytes(), structs.MsgpackHandle).Decode(&restored); err != nil {
		t.Fatalf("decode Nomad LocalState: %v", err)
	}
	if restored.TaskHandle == nil || restored.TaskHandle.Config == nil {
		t.Fatal("Nomad persistence lost public task config")
	}
	return restored.TaskHandle
}

func roundTripNomadTaskHandleDatabase(t *testing.T, handle *drivers.TaskHandle) *drivers.TaskHandle {
	t.Helper()
	dir := t.TempDir()
	db, err := nomadstate.NewBoltStateDB(hclog.NewNullLogger(), dir)
	if err != nil {
		t.Fatal(err)
	}
	err = db.PutTaskRunnerLocalState(handle.Config.AllocID, handle.Config.Name, &trstate.LocalState{TaskHandle: handle})
	closeErr := db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("persist/close Nomad state database: %v / %v", err, closeErr)
	}
	db, err = nomadstate.NewBoltStateDB(hclog.NewNullLogger(), dir)
	if err != nil {
		t.Fatal(err)
	}
	state, _, err := db.GetTaskRunnerState(handle.Config.AllocID, handle.Config.Name)
	closeErr = db.Close()
	if err != nil || closeErr != nil {
		t.Fatalf("restore/close Nomad state database: %v / %v", err, closeErr)
	}
	if state == nil || state.TaskHandle == nil || state.TaskHandle.Config == nil {
		t.Fatal("Nomad database lost persisted task handle")
	}
	return state.TaskHandle
}

func recoveryTestHandle(t *testing.T, fixture *runtimeSlotPluginFixture) (*drivers.TaskHandle, PersistedState) {
	t.Helper()
	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = fixture.task
	bundle := filepath.Join(fixture.task.TaskDir().Dir, "gvisor-bundle")
	state := PersistedState{
		TaskConfig: fixture.task.Copy(), DriverConfig: &TaskConfig{Command: "/procd", SecurityClass: "privileged"},
		ContainerID: safeContainerID(fixture.task.ID), BundleDir: bundle,
		RootMount: filepath.Join(bundle, "rootfs"), StartedAt: time.Now(), Phase: phaseWarm,
	}
	if err := handle.SetDriverState(state); err != nil {
		t.Fatal(err)
	}
	return roundTripNomadTaskHandle(t, handle), state
}

func TestRecoverTaskRejectsInvalidPersistedConfigurationAndIdentity(t *testing.T) {
	tests := []struct {
		name   string
		change func(*drivers.TaskHandle, *PersistedState)
	}{
		{"legacy version", func(h *drivers.TaskHandle, _ *PersistedState) { h.Version = 1 }},
		{"zero version", func(h *drivers.TaskHandle, _ *PersistedState) { h.Version = 0 }},
		{"future version", func(h *drivers.TaskHandle, _ *PersistedState) { h.Version = taskHandleVersion + 1 }},
		{"missing driver config", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig = nil }},
		{"missing command", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig.Command = "" }},
		{"missing security class", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig.SecurityClass = "" }},
		{"unnormalized command", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig.Command = " /procd" }},
		{"different command", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig.Command = "/bin/sh" }},
		{"unknown security class", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig.SecurityClass = "host" }},
		{"arguments", func(_ *drivers.TaskHandle, s *PersistedState) { s.DriverConfig.Args = []string{"--unsafe"} }},
		{"missing task", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig = nil }},
		{"task ID", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.ID += "-other" }},
		{"allocation ID", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.AllocID += "-other" }},
		{"node ID", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.NodeID += "-other" }},
		{"namespace", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.Namespace += "-other" }},
		{"task name", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.Name += "-other" }},
		{"allocation directory", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.AllocDir += "-other" }},
		{"netns path", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.NetworkIsolation.Path += "-other" }},
		{"netns mode", func(_ *drivers.TaskHandle, s *PersistedState) {
			s.TaskConfig.NetworkIsolation.Mode = drivers.NetIsolationModeHost
		}},
		{"missing netns", func(_ *drivers.TaskHandle, s *PersistedState) { s.TaskConfig.NetworkIsolation = nil }},
		{"container ID", func(_ *drivers.TaskHandle, s *PersistedState) { s.ContainerID += "-other" }},
		{"bundle directory", func(_ *drivers.TaskHandle, s *PersistedState) { s.BundleDir += "-other" }},
		{"root mount", func(_ *drivers.TaskHandle, s *PersistedState) { s.RootMount += "-other" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newRuntimeSlotPluginFixture(t)
			t.Cleanup(fixture.plugin.cancel)
			handle, state := recoveryTestHandle(t, fixture)
			test.change(handle, &state)
			if err := handle.SetDriverState(state); err != nil {
				t.Fatal(err)
			}
			err := fixture.plugin.RecoverTask(handle)
			if err == nil {
				t.Fatal("invalid persisted recovery was accepted")
			}
			assertRejectedRecovery(t, fixture)
		})
	}
}

func assertRejectedRecovery(t *testing.T, fixture *runtimeSlotPluginFixture) {
	t.Helper()
	if _, exists := fixture.plugin.tasks.Get(fixture.task.ID); exists {
		t.Fatal("rejected task was exposed")
	}
	if calls := fixture.runner.callsSnapshot(); len(calls) != 0 {
		t.Fatalf("rejected recovery called runsc: %v", calls)
	}
	if calls, _, _, _ := fixture.authority.snapshot(); len(calls) != 0 {
		t.Fatalf("rejected recovery contacted authority: %v", calls)
	}
	if _, err := os.Stat(controlSocketPath(fixture.config.ControlDir, fixture.task.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected recovery retained a control socket: %v", err)
	}
}

func TestRecoverTaskRejectsConflictingLocalState(t *testing.T) {
	for _, field := range []string{"task", "allocation", "node", "netns", "container", "bundle", "root", "missing config", "security class", "corrupt JSON"} {
		t.Run(field, func(t *testing.T) {
			fixture := newRuntimeSlotPluginFixture(t)
			t.Cleanup(fixture.plugin.cancel)
			handle, local := recoveryTestHandle(t, fixture)
			path := filepath.Join(local.BundleDir, ".sandbox0-driver-state.json")
			if err := os.MkdirAll(local.BundleDir, 0700); err != nil {
				t.Fatal(err)
			}
			switch field {
			case "task":
				local.TaskConfig.ID += "-other"
			case "allocation":
				local.TaskConfig.AllocID += "-other"
			case "node":
				local.TaskConfig.NodeID += "-other"
			case "netns":
				local.TaskConfig.NetworkIsolation.Path += "-other"
			case "container":
				local.ContainerID += "-other"
			case "bundle":
				local.BundleDir += "-other"
			case "root":
				local.RootMount += "-other"
			case "missing config":
				local.DriverConfig = nil
			case "security class":
				local.DriverConfig.SecurityClass = "privileged"
			}
			if field == "corrupt JSON" {
				if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
					t.Fatal(err)
				}
			} else if err := writePersistedState(local, path); err != nil {
				t.Fatal(err)
			}
			if err := fixture.plugin.RecoverTask(handle); err == nil {
				t.Fatal("conflicting local state was accepted")
			}
			assertRejectedRecovery(t, fixture)
		})
	}
}

func TestRecoverTaskUsesNewerMatchingLocalState(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	t.Cleanup(fixture.plugin.cancel)
	handle, local := recoveryTestHandle(t, fixture)
	local.Phase, local.RootMounted = phaseActive, true
	fixture.runner.setState("running")
	fixture.authority.state = protocol.StateActive
	if err := os.MkdirAll(local.RootMount, 0700); err != nil {
		t.Fatal(err)
	}
	if err := writePersistedState(local, filepath.Join(local.BundleDir, ".sandbox0-driver-state.json")); err != nil {
		t.Fatal(err)
	}
	if err := fixture.plugin.RecoverTask(handle); err != nil {
		t.Fatal(err)
	}
	recovered, ok := fixture.plugin.tasks.Get(fixture.task.ID)
	if !ok {
		t.Fatal("matching local state was not recovered")
	}
	t.Cleanup(func() { recovered.stopExitWatch(); fixture.plugin.stopTaskControl(recovered) })
	if got := recovered.PersistedState(); got.Phase != phaseActive || !got.RootMounted || got.DriverConfig.SecurityClass != "privileged" {
		t.Fatal("newer local lifecycle state or normalized driver config was lost")
	}
}

type exactRecoveryAuthority struct {
	runtimeSlotAuthority
	expected protocol.RegistrationRequest
	observed protocol.RegistrationRequest
}

func (a *exactRecoveryAuthority) Register(ctx context.Context, id string, request protocol.RegistrationRequest) (protocol.Observation, error) {
	a.observed = request
	if !reflect.DeepEqual(a.expected, request) {
		return protocol.Observation{}, errdefs.ErrFailedPrecondition
	}
	return a.runtimeSlotAuthority.Register(ctx, id, request)
}

func TestRecoverTaskPersistenceDoesNotRebindBootOrNetNS(t *testing.T) {
	for _, field := range []string{"boot", "netns"} {
		t.Run(field, func(t *testing.T) {
			fixture := newRuntimeSlotPluginFixture(t)
			handle, _, err := fixture.plugin.StartTask(fixture.task)
			if err != nil {
				t.Fatal(err)
			}
			fixture.plugin.cancel()
			_, registrations, _, _ := fixture.authority.snapshot()
			if len(registrations) != 1 {
				t.Fatal("missing original registration")
			}
			authority := &exactRecoveryAuthority{runtimeSlotAuthority: fixture.authority, expected: registrations[0]}
			if field == "boot" {
				if err := os.WriteFile(fixture.config.RuntimeSlotNodeBootIDFile, []byte("boot-2\n"), 0600); err != nil {
					t.Fatal(err)
				}
			} else {
				path := fixture.task.NetworkIsolation.Path
				if err := os.Rename(path, path+".old"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte("new-namespace"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			runner := newFakeRunsc()
			recovered := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return runner }).(*Plugin)
			t.Cleanup(recovered.cancel)
			recovered.config, recovered.rootfs = fixture.config, fixture.rootfs
			recovered.rootfsOnce.Do(func() {})
			recovered.newSlotAuthority = func(*PluginConfig) (runtimeSlotAuthority, error) { return authority, nil }
			err = recovered.RecoverTask(roundTripNomadTaskHandle(t, handle))
			if !errdefs.IsFailedPrecondition(err) {
				t.Fatalf("changed incarnation recovery = %v", err)
			}
			if field == "boot" && authority.observed.NodeBootID != "boot-2" {
				t.Fatal("recovery did not use current boot")
			}
			if field == "netns" && authority.observed.NetNSIdentity == authority.expected.NetNSIdentity {
				t.Fatal("recovery reused stale netns identity")
			}
			if _, exists := recovered.tasks.Get(fixture.task.ID); exists {
				t.Fatal("rejected incarnation was exposed")
			}
			if calls := runner.callsSnapshot(); len(calls) != 0 {
				t.Fatalf("unexpected recovery runsc operations: %v", calls)
			}
			if _, err := os.Stat(controlSocketPath(fixture.config.ControlDir, fixture.task.ID)); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("rejected recovery retained control socket")
			}
		})
	}
}

func TestRecoveryConfigurationSurvivesBothPersistenceEncodings(t *testing.T) {
	fixture := newRuntimeSlotPluginFixture(t)
	t.Cleanup(fixture.plugin.cancel)
	if err := fixture.task.EncodeConcreteDriverConfig(TaskConfig{Command: "/procd", SecurityClass: "privileged"}); err != nil {
		t.Fatal(err)
	}
	handle, _, err := fixture.plugin.StartTask(fixture.task)
	if err != nil {
		t.Fatal(err)
	}
	restored := roundTripNomadTaskHandle(t, handle)
	var raw TaskConfig
	if err := restored.Config.DecodeDriverConfig(&raw); !errors.Is(err, io.EOF) {
		t.Fatalf("test did not cross missing raw config boundary: %v", err)
	}
	state, err := recoveredDriverState(fixture.config, restored)
	if err != nil {
		t.Fatal(err)
	}
	local, err := readPersistedState(filepath.Join(state.BundleDir, ".sandbox0-driver-state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !sameRecoveryIdentity(state, local) || state.DriverConfig.SecurityClass != "privileged" {
		t.Fatal("driver configuration changed across MsgPack/JSON")
	}
}

func TestRecoverTaskRejectsCorruptOpaqueState(t *testing.T) {
	for _, data := range [][]byte{nil, {0xc1}, []byte("{")} {
		fixture := newRuntimeSlotPluginFixture(t)
		t.Cleanup(fixture.plugin.cancel)
		handle, _ := recoveryTestHandle(t, fixture)
		handle.DriverState = data
		if err := fixture.plugin.RecoverTask(handle); err == nil {
			t.Fatal("corrupt driver state was accepted")
		}
		assertRejectedRecovery(t, fixture)
	}
}

func TestPersistedDriverConfigurationIsAnIndependentSnapshot(t *testing.T) {
	handle := newTaskHandle(taskHandleOptions{driverConfig: TaskConfig{
		Command: "/procd", SecurityClass: "privileged", Args: []string{"snapshot-only"},
	}})
	first := handle.PersistedState()
	first.DriverConfig.Command = "/changed"
	first.DriverConfig.Args[0] = "changed"
	next := handle.PersistedState()
	if next.DriverConfig.Command != "/procd" || next.DriverConfig.Args[0] != "snapshot-only" {
		t.Fatal("persisted snapshot aliases live driver configuration")
	}
}
