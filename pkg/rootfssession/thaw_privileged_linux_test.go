//go:build linux

package session

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// The probe allocates only its own loop device and temporary XFS image. It does
// not use ctld's NBD pool or any mounted workload filesystem.
func TestPrivilegedThawShutdownXFS(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_XFS_THAW_TEST") != "1" {
		t.Skip("set SANDBOX0_RUN_XFS_THAW_TEST=1 on an isolated Linux host")
	}
	require.Zero(t, os.Geteuid())
	run := func(name string, args ...string) string {
		t.Helper()
		output, err := exec.Command(name, args...).CombinedOutput()
		require.NoError(t, err, "%s: %s", name, output)
		return strings.TrimSpace(string(output))
	}
	root := t.TempDir()
	backing := filepath.Join(root, "private.xfs")
	file, err := os.Create(backing)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(512<<20))
	require.NoError(t, file.Close())
	device := run("losetup", "--find", "--show", backing)
	t.Cleanup(func() { run("losetup", "--detach", device) })
	run("mkfs.xfs", "-f", device)
	target := filepath.Join(root, "mount")
	require.NoError(t, os.Mkdir(target, 0700))
	runtime := &LinuxRuntime{}
	require.NoError(t, runtime.MountXFS(device, target))
	mounted := true
	t.Cleanup(func() {
		if mounted {
			_ = runtime.ThawXFS(target)
			require.NoError(t, unix.Unmount(target, 0))
		}
	})
	require.NoError(t, runtime.FreezeXFS(target))
	run("xfs_io", "-x", "-c", "shutdown -f", target)
	_, err = os.Lstat(target)
	require.ErrorIs(t, err, unix.EIO, "probe must exercise a shutdown filesystem")
	require.NoError(t, runtime.ThawXFS(target), "thaw cannot depend on XFS getattr")
	require.NoError(t, unix.Unmount(target, 0))
	mounted = false
}

func TestXFSFreezeRejectsSymlinkAndNonDirectory(t *testing.T) {
	root := t.TempDir()
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(root, alias))
	require.Error(t, ioctlXFSFreeze(alias, fsIOCThaw, "thaw"))
	file := filepath.Join(root, "file")
	require.NoError(t, os.WriteFile(file, nil, 0600))
	require.Error(t, ioctlXFSFreeze(file, fsIOCThaw, "thaw"))
}
