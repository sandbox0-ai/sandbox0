//go:build linux

package driver

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/pkg/procdartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

func TestClaimBundlePinsHostProcdAcrossRuntimeUpgrades(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root-owned executable cache")
	}
	root, err := os.MkdirTemp("/root", "driver-procd-test-")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(root) })
	cache := filepath.Join(root, "cache")
	source := filepath.Join(root, "source")
	lease, err := protocol.NewRuntimeResourceLease("op", "claim", "slot-1", "cluster-1", "node-1", "node-1", "boot-1", protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion, CPUMillicores: 500, MemoryBytes: 256 << 20, PIDsLimit: protocol.DefaultRuntimePIDsLimit}, "0-1", "0")
	require.NoError(t, err)
	var paths []string
	for i, body := range []string{"old node daemon", "new node daemon"} {
		require.NoError(t, os.WriteFile(source, []byte(body), 0755))
		artifact := procdartifact.Artifact{Digest: digest.FromString(body).String(), Protocol: "sandbox0.procd.v3"}
		path, err := procdartifact.Install(cache, source, artifact.Digest)
		require.NoError(t, err)
		paths = append(paths, path)
		bundle := filepath.Join(root, body)
		require.NoError(t, os.MkdirAll(bundle, 0755))
		// Model a pre-existing RootFS with an obsolete executable and user data.
		require.NoError(t, os.MkdirAll(filepath.Join(bundle, "rootfs"), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(bundle, "rootfs/procd"), []byte("obsolete embedded daemon"), 0555))
		h := newTaskHandle(taskHandleOptions{taskConfig: &drivers.TaskConfig{ID: "slot-1", NodeID: "node-1", AllocID: "alloc-1"}, driverConfig: TaskConfig{Command: "/procd", SecurityClass: "privileged"}, bundleDir: bundle, resourceCgroupRoot: "/sys/fs/cgroup/sandbox0", procdPort: protocol.NomadProcdPort, procdArtifactDir: cache})
		assignment := &runtimecontrol.Assignment{SandboxID: "sandbox-1", RuntimeGeneration: int64(i + 1), SecurityClass: "privileged", Procd: &artifact}
		require.NoError(t, h.writeClaimBundle(assignment, lease))
		raw, err := os.ReadFile(filepath.Join(bundle, "config.json"))
		require.NoError(t, err)
		var spec specs.Spec
		require.NoError(t, json.Unmarshal(raw, &spec))
		matched := false
		for _, m := range spec.Mounts {
			if m.Destination == "/procd" {
				require.Equal(t, path, m.Source)
				require.Contains(t, m.Options, "ro")
				matched = true
			}
		}
		require.True(t, matched)
		unchanged, err := os.ReadFile(filepath.Join(bundle, "rootfs/procd"))
		require.NoError(t, err)
		require.Equal(t, "obsolete embedded daemon", string(unchanged))
		artifact.Digest = digest.FromString("missing").String()
		require.Error(t, h.writeClaimBundle(assignment, lease), "missing pin must not fall back to embedded /procd")
	}
	require.NotEqual(t, paths[0], paths[1])
	old, err := os.ReadFile(paths[0])
	require.NoError(t, err)
	require.Equal(t, "old node daemon", string(old))
}
