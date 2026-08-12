package rootfs

import (
	"path/filepath"
	"testing"

	"github.com/moby/sys/mountinfo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestActiveMergedRootPath(t *testing.T) {
	got, err := activeMergedRootPath("unix:///host-run/containerd/containerd.sock", "k8s.io", "container-id")
	require.NoError(t, err)
	assert.Equal(t, "/host-run/containerd/io.containerd.runtime.v2.task/k8s.io/container-id/rootfs", got)
}

func TestActiveMergedRootPathRejectsUnsafeComponents(t *testing.T) {
	_, err := activeMergedRootPath("/host-run/containerd/containerd.sock", "../escape", "container-id")
	assert.Error(t, err)
	_, err = activeMergedRootPath("/host-run/containerd/containerd.sock", "k8s.io", "../escape")
	assert.Error(t, err)
}

func TestValidateMergedRootMountMatchesUpperdir(t *testing.T) {
	upper := t.TempDir()
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{})
	err := runtime.validateMergedRootMount(&mountinfo.Info{
		Mountpoint: "/host-run/containerd/task/rootfs",
		FSType:     "overlay",
		VFSOptions: "rw,lowerdir=/lower,upperdir=" + upper + ",workdir=/work",
	}, upper)
	require.NoError(t, err)
}

func TestValidateMergedRootMountRejectsDifferentUpperdir(t *testing.T) {
	upper := t.TempDir()
	other := t.TempDir()
	runtime := NewContainerdRuntime(ContainerdRuntimeConfig{})
	err := runtime.validateMergedRootMount(&mountinfo.Info{
		Mountpoint: "/host-run/containerd/task/rootfs",
		FSType:     "overlay",
		VFSOptions: "rw,upperdir=" + other,
	}, upper)
	assert.ErrorContains(t, err, "does not match")
	assert.NotEqual(t, filepath.Clean(upper), filepath.Clean(other))
}
