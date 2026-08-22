//go:build linux

package rootfsblock

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestValidateNBDDevicePath(t *testing.T) {
	path, name, err := validateNBDDevicePath(" /dev/nbd12 ")
	require.NoError(t, err)
	require.Equal(t, "/dev/nbd12", path)
	require.Equal(t, "nbd12", name)
	for _, value := range []string{"", "/dev/nbd", "/tmp/nbd0", "/dev/../tmp/nbd0", "/dev/nbd0/child"} {
		_, _, err := validateNBDDevicePath(value)
		require.Error(t, err, value)
	}
}

func TestKernelNBDGeometryUsesFilesystemSectorSize(t *testing.T) {
	geometry, err := kernelNBDGeometry(3 * LogicalBlockSize)
	require.NoError(t, err)
	require.Equal(t, NBDDeviceSectorSize, geometry.sectorSize)
	require.Equal(t, 3*LogicalBlockSize/NBDDeviceSectorSize, geometry.sectorCount)
	require.NotEqual(t, LogicalBlockSize, geometry.sectorSize)

	_, err = kernelNBDGeometry(LogicalBlockSize - NBDDeviceSectorSize)
	require.ErrorContains(t, err, "positive multiple")
	_, err = kernelNBDGeometry(0)
	require.ErrorContains(t, err, "positive multiple")
}

func TestKernelNBDCapacityErrorDoesNotPoisonRetirement(t *testing.T) {
	device := &KernelNBDDevice{}
	device.recordRequestError(fmt.Errorf("write: %w", &DirtyTailCapacityError{
		UsedBytes: LogicalBlockSize, RequestedBytes: LogicalBlockSize, LimitBytes: LogicalBlockSize,
	}))
	require.NoError(t, device.requestErr)

	device.recordRequestError(fmt.Errorf("write: %w", syscall.EIO))
	require.ErrorIs(t, device.requestErr, syscall.EIO)
}

func TestKernelNBDWaitBlocksAsynchronousSignals(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	var before unix.Sigset_t
	require.NoError(t, unix.PthreadSigmask(unix.SIG_BLOCK, nil, &before))

	sentinel := errors.New("operation completed")
	err := withKernelNBDSignalMask(func() error {
		var current unix.Sigset_t
		require.NoError(t, unix.PthreadSigmask(unix.SIG_BLOCK, nil, &current))
		for _, signal := range []unix.Signal{unix.SIGHUP, unix.SIGUSR1, unix.SIGCHLD, unix.SIGURG, unix.SIGTERM} {
			require.True(t, signalMaskContains(&current, signal), "signal %d remains unblocked", signal)
		}
		require.False(t, signalMaskContains(&current, unix.SIGKILL))
		require.False(t, signalMaskContains(&current, unix.SIGSTOP))
		return sentinel
	})
	require.ErrorIs(t, err, sentinel)
	var after unix.Sigset_t
	require.NoError(t, unix.PthreadSigmask(unix.SIG_BLOCK, nil, &after))
	require.Equal(t, before, after)
}

func TestOrphanNBDIsUnusedRequiresZeroPIDAndSize(t *testing.T) {
	root := t.TempDir()
	pidPath := filepath.Join(root, "pid")
	sizePath := filepath.Join(root, "size")
	require.NoError(t, os.WriteFile(pidPath, []byte("42\n"), 0o600))
	require.NoError(t, os.WriteFile(sizePath, []byte("0\n"), 0o600))
	unused, err := orphanNBDIsUnused(pidPath, sizePath)
	require.NoError(t, err)
	require.False(t, unused)

	require.NoError(t, os.WriteFile(pidPath, []byte("0\n"), 0o600))
	require.NoError(t, os.WriteFile(sizePath, []byte("128\n"), 0o600))
	unused, err = orphanNBDIsUnused(pidPath, sizePath)
	require.NoError(t, err)
	require.False(t, unused)

	require.NoError(t, os.WriteFile(sizePath, []byte("0\n"), 0o600))
	unused, err = orphanNBDIsUnused(pidPath, sizePath)
	require.NoError(t, err)
	require.True(t, unused)

	require.NoError(t, os.Remove(pidPath))
	unused, err = orphanNBDIsUnused(pidPath, sizePath)
	require.NoError(t, err)
	require.True(t, unused)
}

func TestRecoverOrphanKernelNBDAcceptsMissingKernelEndpoint(t *testing.T) {
	require.NoError(t, RecoverOrphanKernelNBD(t.Context(), "/dev/nbd999", t.TempDir()))
}

func TestRequireUnusedNBD(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "pid")
	require.NoError(t, requireUnusedNBD(pidPath))
	require.NoError(t, os.WriteFile(pidPath, []byte("0\n"), 0o600))
	require.NoError(t, requireUnusedNBD(pidPath))
	require.NoError(t, os.WriteFile(pidPath, []byte("123\n"), 0o600))
	require.ErrorContains(t, requireUnusedNBD(pidPath), "already owned")
}

func TestWaitNBDReady(t *testing.T) {
	pidPath := filepath.Join(t.TempDir(), "pid")
	done := make(chan struct{})
	writeResult := make(chan error, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		writeResult <- os.WriteFile(pidPath, []byte("321\n"), 0o600)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	require.NoError(t, waitNBDReady(ctx, pidPath, done, func() error { return nil }))
	require.NoError(t, <-writeResult)
}
