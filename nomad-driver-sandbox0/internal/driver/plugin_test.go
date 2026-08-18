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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
)

func TestStartTaskReturnsWarmSlotAndServesControlSocket(t *testing.T) {
	tempDir := t.TempDir()
	runner := newFakeRunsc()
	config := defaultPluginConfig()
	config.ControlDir = filepath.Join(tempDir, "control")
	config.AllowedRootfsDir = filepath.Join(tempDir, "rootfs")

	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return runner }).(*Plugin)
	plugin.config = config

	taskConfig := &drivers.TaskConfig{
		ID:       "plugin-task-1",
		AllocID:  "plugin-alloc-1",
		Name:     "warm-slot",
		AllocDir: filepath.Join(tempDir, "alloc"),
	}
	if err := taskConfig.EncodeConcreteDriverConfig(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("encode task config: %v", err)
	}
	handle, network, err := plugin.StartTask(taskConfig)
	if err != nil {
		t.Fatalf("StartTask() error = %v", err)
	}
	if network != nil {
		t.Fatalf("network = %+v, want nil", network)
	}
	var persisted PersistedState
	if err := handle.GetDriverState(&persisted); err != nil {
		t.Fatalf("get persisted state: %v", err)
	}
	if persisted.Phase != phaseWarm || persisted.ContainerID == "" {
		t.Fatalf("persisted state = %+v, want warm slot with container ID", persisted)
	}

	client := unixHTTPClient(controlSocketPath(config.ControlDir, taskConfig.ID))
	response, err := awaitControl(client, http.MethodGet, "/status", nil, 2*time.Second)
	if err != nil {
		t.Fatalf("control status: %v", err)
	}
	var status statusResponse
	if err := json.Unmarshal(response, &status); err != nil {
		t.Fatalf("decode control status: %v", err)
	}
	if status.Phase != string(phaseWarm) || status.ContainerID != persisted.ContainerID {
		t.Fatalf("status = %+v, want warm slot with persisted container ID", status)
	}
	if err := plugin.DestroyTask(taskConfig.ID, true); err != nil {
		t.Fatalf("DestroyTask() error = %v", err)
	}
}

func TestControlClaimStartsSlot(t *testing.T) {
	fixture := newTestFixture(t)
	if err := fixture.handle.Prepare(TaskConfig{Command: "/procd", WaitForClaim: true}); err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	go fixture.handle.ServeControl(context.Background())
	client := unixHTTPClient(fixture.socketPath)
	body := fmt.Sprintf(`{"rootfs_path":%q,"policy_token":"token","writer_epoch":"epoch"}`, fixture.rootfs)
	response, err := awaitControl(client, http.MethodPost, "/claim", []byte(body), 2*time.Second)
	if err != nil {
		t.Fatalf("control claim: %v", err)
	}
	var result claimResponse
	if err := json.Unmarshal(response, &result); err != nil {
		t.Fatalf("decode control claim: %v", err)
	}
	if result.Phase != string(phaseActive) {
		t.Fatalf("claim response = %+v, want active", result)
	}
	if err := fixture.handle.Close(true); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}

func TestSetConfigRejectsNilConfig(t *testing.T) {
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	if err := plugin.SetConfig(nil); err == nil {
		t.Fatal("SetConfig(nil) succeeded")
	}
}

func TestStartTaskRejectsDevSmokeByDefault(t *testing.T) {
	tempDir := t.TempDir()
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	config := defaultPluginConfig()
	config.ControlDir = filepath.Join(tempDir, "control")
	config.AllowedRootfsDir = filepath.Join(tempDir, "rootfs")
	plugin.config = config

	taskConfig := &drivers.TaskConfig{
		ID:       "dev-smoke-rejected",
		AllocID:  "alloc",
		Name:     "warm-slot",
		AllocDir: filepath.Join(tempDir, "alloc"),
	}
	if err := taskConfig.EncodeConcreteDriverConfig(TaskConfig{
		Command: "/bin/sh", WaitForClaim: false, RootfsPath: "/bin",
	}); err != nil {
		t.Fatalf("encode task config: %v", err)
	}
	if _, _, err := plugin.StartTask(taskConfig); err == nil || !strings.Contains(err.Error(), "dev_smoke_enabled") {
		t.Fatalf("StartTask() error = %v, want dev smoke rejection", err)
	}
}

func unixHTTPClient(socketPath string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, "unix", socketPath)
			},
		},
	}
}

func awaitControl(client *http.Client, method, path string, body []byte, timeout time.Duration) ([]byte, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		var request *http.Request
		var err error
		if body == nil {
			request, err = http.NewRequest(method, "http://sandbox0"+path, nil)
		} else {
			request, err = http.NewRequest(method, "http://sandbox0"+path, bytes.NewReader(body))
		}
		if err != nil {
			return nil, err
		}
		response, err := client.Do(request)
		if err != nil {
			lastErr = err
			time.Sleep(5 * time.Millisecond)
			continue
		}
		data, readErr := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("control %s returned %s: %s", path, response.Status, data)
		}
		if readErr != nil {
			return nil, readErr
		}
		return data, nil
	}
	return nil, lastErr
}
