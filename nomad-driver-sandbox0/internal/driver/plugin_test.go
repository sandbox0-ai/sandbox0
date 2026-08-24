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
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestDestroyTaskRetainsHandleUntilCleanupSucceeds(t *testing.T) {
	runner := newFakeRunsc()
	cleanupFailure := fmt.Errorf("delete runsc state")
	runner.deleteErr = cleanupFailure
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return runner }).(*Plugin)
	taskID := "cleanup-retry-task"
	handle := newTaskHandle(taskHandleOptions{
		taskConfig: &drivers.TaskConfig{ID: taskID}, bundleDir: t.TempDir(),
		containerID: safeContainerID(taskID), rootMount: filepath.Join(t.TempDir(), "root"),
		socketPath: filepath.Join(t.TempDir(), "control.sock"), runner: runner,
		mounter: &fakeMounter{}, logger: hclog.NewNullLogger(),
	})
	handle.setPhase(phaseExited)
	plugin.tasks.Set(taskID, handle)

	if err := plugin.DestroyTask(taskID, true); !errors.Is(err, cleanupFailure) {
		t.Fatalf("first DestroyTask() error = %v, want %v", err, cleanupFailure)
	}
	if _, ok := plugin.tasks.Get(taskID); !ok {
		t.Fatal("DestroyTask removed the handle after failed cleanup")
	}
	runner.mu.Lock()
	runner.deleteErr = nil
	runner.mu.Unlock()
	if err := plugin.DestroyTask(taskID, true); err != nil {
		t.Fatalf("retry DestroyTask() error = %v", err)
	}
	if _, ok := plugin.tasks.Get(taskID); ok {
		t.Fatal("DestroyTask retained the handle after successful cleanup")
	}
}

func TestSetConfigRejectsNilConfig(t *testing.T) {
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	if err := plugin.SetConfig(nil); err == nil {
		t.Fatal("SetConfig(nil) succeeded")
	}
}

func TestSetConfigLoadsProcdInternalJWTPublicKey(t *testing.T) {
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	config := validPluginConfigForSetConfig(t)
	var encoded []byte
	if err := base.MsgPackEncode(&encoded, config); err != nil {
		t.Fatal(err)
	}
	if err := plugin.SetConfig(&base.Config{PluginConfig: encoded}); err != nil {
		t.Fatalf("SetConfig() error = %v", err)
	}
	if plugin.config.ProcdInternalJWTPublicKeyFile != config.ProcdInternalJWTPublicKeyFile {
		t.Fatalf("configured procd public key path = %q", plugin.config.ProcdInternalJWTPublicKeyFile)
	}
}

func TestSetConfigRejectsInvalidProcdInternalJWTPublicKey(t *testing.T) {
	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	config := validPluginConfigForSetConfig(t)
	if err := os.WriteFile(config.ProcdInternalJWTPublicKeyFile, []byte("not a public key\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var encoded []byte
	if err := base.MsgPackEncode(&encoded, config); err != nil {
		t.Fatal(err)
	}
	err := plugin.SetConfig(&base.Config{PluginConfig: encoded})
	if err == nil || !strings.Contains(err.Error(), "load procd internal JWT public key") {
		t.Fatalf("SetConfig() error = %v", err)
	}
}

func validPluginConfigForSetConfig(t *testing.T) *PluginConfig {
	t.Helper()
	directory := t.TempDir()
	_, publicKey, err := internalauth.GenerateEd25519KeyPair()
	if err != nil {
		t.Fatal(err)
	}
	publicKeyPath := filepath.Join(directory, "data-public.pem")
	if err := os.WriteFile(publicKeyPath, publicKey, 0o644); err != nil {
		t.Fatal(err)
	}
	config := defaultPluginConfig()
	config.RootFSAuthorityURL = "https://manager.internal:9444"
	config.RootFSAuthorityCAFile = filepath.Join(directory, "ca.pem")
	config.RootFSAuthorityClientCertFile = filepath.Join(directory, "client.pem")
	config.RootFSAuthorityClientKeyFile = filepath.Join(directory, "client-key.pem")
	config.RootFSAuthorityTokenFile = filepath.Join(directory, "manager.token")
	config.ProcdInternalJWTPublicKeyFile = publicKeyPath
	config.RuntimeSlotClusterID = "cluster-1"
	return config
}

func TestRunscOperationTimeoutBounds(t *testing.T) {
	if got := defaultPluginConfig().RunscOperationTimeoutSeconds; got != 30 {
		t.Fatalf("default runsc operation timeout = %d seconds", got)
	}
	for _, seconds := range []int64{1, 30, 120} {
		if err := validateRunscOperationTimeout(seconds); err != nil {
			t.Fatalf("validateRunscOperationTimeout(%d) error = %v", seconds, err)
		}
	}
	for _, seconds := range []int64{0, 121} {
		if err := validateRunscOperationTimeout(seconds); err == nil {
			t.Fatalf("validateRunscOperationTimeout(%d) succeeded", seconds)
		}
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
