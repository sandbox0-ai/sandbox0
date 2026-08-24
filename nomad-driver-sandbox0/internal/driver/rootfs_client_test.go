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
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
)

func TestRootFSConfigRequiresCtldNodeRuntimeOnly(t *testing.T) {
	config := defaultPluginConfig()
	require.NoError(t, validateRootFSConfig(config))

	config.RootFSNodeSocket = ""
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_node_socket")
}

func TestPluginFingerprintRequiresHealthyCtldNomadRuntime(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "ctld-runtime.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0o600))
	var includeInfo atomic.Bool
	includeInfo.Store(true)
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut || request.URL.Path != "/v1/health" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		if !includeInfo.Load() {
			_, _ = writer.Write([]byte("{}"))
			return
		}
		_ = json.NewEncoder(writer).Encode(map[string]any{"info": nomadruntime.RuntimeInfo{
			Version: nomadruntime.RuntimeInfoVersion, MountRoot: "/run/sandbox0/rootfs",
			MaxDirtyTailBytes:               rootfssession.DefaultMaxDirtyTailBytes,
			MaxNodeDirtyTailBytes:           rootfssession.DefaultMaxNodeDirtyTailBytes,
			DirtyTailRetirementReserveBytes: rootfssession.DefaultDirtyTailRetirementReserveBytes,
		}})
	})}
	go func() { _ = server.Serve(listener) }()

	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	plugin.config.RootFSNodeSocket = socket
	fingerprint := plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateHealthy, fingerprint.Health)
	ctldRuntime, ok := fingerprint.Attributes["driver.sandbox0_gvisor.ctld_runtime"].GetBool()
	require.True(t, ok)
	require.True(t, ctldRuntime)

	includeInfo.Store(false)
	fingerprint = plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateUndetected, fingerprint.Health)
	includeInfo.Store(true)

	require.NoError(t, server.Close())
	fingerprint = plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateUndetected, fingerprint.Health)
}
