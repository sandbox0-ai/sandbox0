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
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/stretchr/testify/require"
)

func TestRootFSConfigRequiresCtldNodeRuntimeOnly(t *testing.T) {
	config := defaultPluginConfig()
	config.RootFSEnabled = true
	config.RootFSMountRoot = "/run/sandbox0/rootfs"
	require.NoError(t, validateRootFSConfig(config))

	config.RootFSNodeSocket = ""
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_node_socket")
	config.RootFSNodeSocket = "/run/sandbox0/ctld-nomad-runtime.sock"

	config.RootFSMaxDirtyTailBytes = -1
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_max_dirty_tail_bytes")
	config.RootFSMaxDirtyTailBytes = 0
	config.RootFSMaxNodeDirtyTailBytes = -1
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_max_node_dirty_tail_bytes")
	config.RootFSMaxNodeDirtyTailBytes = 0
	config.RootFSDirtyTailRetirementReserveBytes = -1
	require.ErrorContains(t, validateRootFSConfig(config), "rootfs_dirty_tail_retirement_reserve_bytes")
}

func TestPluginFingerprintRequiresHealthyCtldNomadRuntime(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "ctld-runtime.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	require.NoError(t, os.Chmod(socket, 0o600))
	server := &http.Server{Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPut || request.URL.Path != "/v1/health" {
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte("{}"))
	})}
	go func() { _ = server.Serve(listener) }()

	plugin := newPlugin(hclog.NewNullLogger(), func(PluginConfig) Runsc { return newFakeRunsc() }).(*Plugin)
	plugin.config.RootFSEnabled = true
	plugin.config.RootFSNodeSocket = socket
	fingerprint := plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateHealthy, fingerprint.Health)
	ctldRuntime, ok := fingerprint.Attributes["driver.sandbox0_gvisor.ctld_runtime"].GetBool()
	require.True(t, ok)
	require.True(t, ctldRuntime)

	require.NoError(t, server.Close())
	fingerprint = plugin.buildFingerprint()
	require.Equal(t, drivers.HealthStateUndetected, fingerprint.Health)
}
