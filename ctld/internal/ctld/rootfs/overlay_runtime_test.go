package rootfs

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/stretchr/testify/assert"
)

func TestRebasePath(t *testing.T) {
	path, ok := rebasePath("/var/lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs", "/var/lib/containerd", "/host-var-lib/containerd")
	assert.True(t, ok)
	assert.Equal(t, "/host-var-lib/containerd/io.containerd.snapshotter.v1.overlayfs/snapshots/1/fs", path)

	_, ok = rebasePath("/var/lib/other", "/var/lib/containerd", "/host-var-lib/containerd")
	assert.False(t, ok)
}

func TestRootFSExcludedPathsIncludeRuntimeAndPortalMounts(t *testing.T) {
	got := rootFSExcludedPathsWithPortals(
		[]string{"/workspace/volume"},
		[]ctldapi.RootFSPortalPath{
			{MountPath: "/workspace/session", BackingPath: "/portal/session"},
			{MountPath: "/tmp/ignored", BackingPath: "/portal/ignored"},
		},
	)

	assert.ElementsMatch(t, []string{"/procd", "/tmp", "/workspace/volume", "/workspace/session"}, got)
	assert.True(t, rootFSPathExcluded("/workspace/volume/child", []string{"/workspace/volume"}))
	assert.False(t, rootFSPathExcluded("/workspace/volume-other", []string{"/workspace/volume"}))
}
