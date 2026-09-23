//go:build linux

package session

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/hostmount"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Keep the NBD/XFS consumer mount visible until its last kernel user exits.
// Otherwise a lazy detach hides the retry target while XFS still owns NBD.
func TestPrivilegedBusyConsumerMountKeepsNBDRetirementRetryable(t *testing.T) {
	devicePath := strings.TrimSpace(os.Getenv(privilegedNBDDeviceEnv))
	if devicePath == "" {
		t.Skipf("set %s to an unused /dev/nbdN device", privilegedNBDDeviceEnv)
	}
	if os.Geteuid() != 0 {
		t.Fatal("NBD/XFS mount test requires root")
	}
	if _, err := exec.LookPath("mkfs.xfs"); err != nil {
		t.Fatalf("mkfs.xfs is required: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	root := t.TempDir()
	branch, err := rootfsblock.OpenBranch(filepath.Join(root, "branch.wal"), rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: "busy-consumer",
		GenerationID: "base", WriterEpoch: 1, LogicalSizeBytes: 512 << 20,
		BaseRootDigest: digest.FromString("zero-base").String(),
	}, zeroBlockReaderAt{size: 512 << 20})
	require.NoError(t, err)
	defer branch.Close()
	device, err := rootfsblock.StartKernelNBD(ctx, ctx, branch, rootfsblock.KernelNBDOptions{
		DevicePath: devicePath, RequestTimeout: 10 * time.Second, ReadyTimeout: 10 * time.Second,
	})
	require.NoError(t, err)
	defer device.Close()
	output, err := runPrivilegedCommand(ctx, "mkfs.xfs", "-f", "-m", "reflink=0", devicePath)
	require.NoError(t, err, output)
	xfsRoot := filepath.Join(root, "xfs")
	stableRoot := filepath.Join(root, "task-root")
	require.NoError(t, os.Mkdir(xfsRoot, 0o700))
	require.NoError(t, os.Mkdir(stableRoot, 0o700))
	require.NoError(t, unix.Mount(devicePath, xfsRoot, "xfs", xfsMountFlags, xfsMountData))
	defer unix.Unmount(xfsRoot, 0)
	require.NoError(t, unix.Mount(xfsRoot, stableRoot, "", unix.MS_BIND, ""))
	defer unix.Unmount(stableRoot, 0)
	consumer, err := os.Open(stableRoot)
	require.NoError(t, err)
	defer consumer.Close()

	require.ErrorIs(t, (hostmount.System{}).Unmount(stableRoot), unix.EBUSY)
	require.NoError(t, unix.Unmount(xfsRoot, 0))
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{devicePath}})
	require.NoError(t, err)
	waitCtx, waitCancel := context.WithTimeout(ctx, 50*time.Millisecond)
	require.ErrorContains(t, runtime.WaitFilesystemRelease(waitCtx, devicePath), "remains attached")
	waitCancel()
	require.NoError(t, consumer.Close())
	require.NoError(t, (hostmount.System{}).Unmount(stableRoot))
	require.NoError(t, runtime.WaitFilesystemRelease(ctx, devicePath))
	require.NoError(t, device.Close())
	require.NoError(t, branch.Close())
}
