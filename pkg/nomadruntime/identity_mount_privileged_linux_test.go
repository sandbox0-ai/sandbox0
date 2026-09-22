//go:build linux

package nomadruntime

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// This probe uses only a new private temporary tree, never a runtime mount or NBD.
func TestPrivilegedStableMountUnderlyingIdentity(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_MOUNT_IDENTITY_TEST") != "1" {
		t.Skip("set SANDBOX0_RUN_MOUNT_IDENTITY_TEST=1 on an isolated Linux host")
	}
	require.Zero(t, os.Geteuid())
	root := t.TempDir()
	parent := filepath.Join(root, "bundle")
	target := filepath.Join(parent, "rootfs")
	source := filepath.Join(root, "source")
	require.NoError(t, os.MkdirAll(target, 0700))
	require.NoError(t, os.Mkdir(source, 0700))
	original, err := stableMountIdentity(target)
	require.NoError(t, err)
	registration := RuntimeSlotRegistration{StableMount: target, StableMountID: original}
	d := &nodeRuntime{config: Config{RootFSConsumerMountRoot: root}}
	mounted := false
	t.Cleanup(func() {
		if mounted {
			require.NoError(t, unix.Unmount(target, 0))
		}
	})
	mount := func() {
		require.NoError(t, unix.Mount(source, target, "", unix.MS_BIND, ""))
		mounted = true
	}
	mount()
	visible, err := stableMountIdentity(target)
	require.NoError(t, err)
	require.NotEqual(t, original, visible)
	underlying, err := stableMountUnderlyingIdentity(target)
	require.NoError(t, err)
	require.Equal(t, original, underlying)
	path, err := d.runtimeSlotStableMountPath(registration)
	require.NoError(t, err)
	require.Equal(t, target, path)
	attached, err := hostMountAttached(target)
	require.NoError(t, err)
	require.True(t, attached, "identity inspection must leave the real bind mount attached")
	outside := d.config.RootFSConsumerMountRoot
	d.config.RootFSConsumerMountRoot = filepath.Join(root, "source")
	_, err = d.runtimeSlotStableMountPath(registration)
	require.Error(t, err, "a mounted path must still be contained by the configured root")
	d.config.RootFSConsumerMountRoot = outside
	alias := filepath.Join(root, "alias")
	require.NoError(t, os.Symlink(parent, alias))
	_, err = stableMountCanonicalPath(filepath.Join(alias, "rootfs"), root)
	require.Error(t, err, "a symlinked parent cannot substitute a canonical carrier path")

	require.NoError(t, unix.Unmount(target, 0))
	mounted = false
	require.NoError(t, os.Rename(target, target+"-retired"))
	require.NoError(t, os.Mkdir(target, 0700))
	mount()
	_, err = d.runtimeSlotStableMountPath(registration)
	require.ErrorContains(t, err, "incarnation changed")
	attached, err = hostMountAttached(target)
	require.NoError(t, err)
	require.True(t, attached, "a different underlying directory must not be unmounted")
}
