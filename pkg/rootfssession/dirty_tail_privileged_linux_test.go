//go:build linux

package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const privilegedNBDDeviceEnv = "SANDBOX0_PRIVILEGED_NBD_DEVICE"

func TestPrivilegedNodeDirtyTailHeadroomAllowsCleanXFSRetirement(t *testing.T) {
	devicePath := strings.TrimSpace(os.Getenv(privilegedNBDDeviceEnv))
	if devicePath == "" {
		t.Skipf("set %s to an unused /dev/nbdN device to run the privileged XFS test", privilegedNBDDeviceEnv)
	}
	if os.Geteuid() != 0 {
		t.Fatalf("%s requires root", t.Name())
	}
	if _, err := exec.LookPath("mkfs.xfs"); err != nil {
		t.Fatalf("mkfs.xfs is required: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	root := t.TempDir()
	branchPath := filepath.Join(root, "branch.wal")
	logicalSize := int64(512 << 20)
	identity := rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: "privileged-dirty-tail",
		GenerationID: "privileged-generation", WriterEpoch: 1,
		LogicalSizeBytes: logicalSize, BaseRootDigest: digest.FromString("privileged-zero-base").String(),
	}
	base := zeroBlockReaderAt{size: logicalSize}

	formatBranch, err := rootfsblock.OpenBranch(branchPath, identity, base)
	require.NoError(t, err)
	formatDevice, err := rootfsblock.StartKernelNBD(ctx, ctx, formatBranch, rootfsblock.KernelNBDOptions{
		DevicePath: devicePath, RequestTimeout: 10 * time.Second, ReadyTimeout: 10 * time.Second,
	})
	if err != nil {
		_ = formatBranch.Close()
		t.Fatalf("attach format NBD: %v", err)
	}
	formatOutput, formatCommandErr := runPrivilegedCommand(ctx, "mkfs.xfs", "-f", "-m", "reflink=0", devicePath)
	formatDeviceErr := formatDevice.Close()
	formatUsage := formatBranch.DirtyTailUsage().DirtyBytes
	formatBranchErr := formatBranch.Close()
	if err := errors.Join(formatCommandErr, formatDeviceErr, formatBranchErr); err != nil {
		t.Fatalf("format XFS over RootFS NBD: %v\n%s", err, formatOutput)
	}
	require.Positive(t, formatUsage)

	const (
		guestAllowance    = int64(32 << 20)
		retirementReserve = int64(16 << 20)
	)
	nodeLimit := formatUsage + guestAllowance + retirementReserve
	budget, err := rootfsblock.NewDirtyTailBudgetWithReserve(nodeLimit, retirementReserve)
	require.NoError(t, err)
	require.NoError(t, budget.Preload(branchPath, formatUsage))
	pressureObserved := make(chan rootfsblock.DirtyTailPressure, 1)
	branch, err := rootfsblock.OpenBranchWithOptions(branchPath, identity, base, rootfsblock.BranchOptions{
		MaxDirtyTailBytes:      nodeLimit,
		RetirementReserveBytes: retirementReserve,
		NodeDirtyBudget:        budget,
		PressureObserver: func(pressure rootfsblock.DirtyTailPressure) {
			pressureObserved <- pressure
		},
	})
	require.NoError(t, err)
	device, err := rootfsblock.StartKernelNBD(ctx, ctx, branch, rootfsblock.KernelNBDOptions{
		DevicePath: devicePath, RequestTimeout: 10 * time.Second, ReadyTimeout: 10 * time.Second,
	})
	if err != nil {
		_ = branch.Close()
		t.Fatalf("reattach formatted NBD: %v", err)
	}

	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{devicePath}})
	require.NoError(t, err)
	xfsRoot := filepath.Join(root, "xfs")
	mergedRoot := filepath.Join(root, "merged")
	mountedXFS := false
	mountedOverlay := false
	defer func() {
		if mountedOverlay {
			_ = unix.Unmount(mergedRoot, unix.MNT_DETACH)
			_ = unix.Unmount(filepath.Join(xfsRoot, "lower"), unix.MNT_DETACH)
		}
		if mountedXFS {
			_ = unix.Unmount(xfsRoot, unix.MNT_DETACH)
		}
		_ = device.Close()
		_ = branch.Close()
	}()
	require.NoError(t, runtime.MountXFS(devicePath, xfsRoot))
	mountedXFS = true
	for _, name := range []string{"lower", "upper", "work"} {
		require.NoError(t, os.MkdirAll(filepath.Join(xfsRoot, name), 0o700))
	}
	sentinel := []byte("retirement-reserve-survives")
	require.NoError(t, os.WriteFile(filepath.Join(xfsRoot, "lower", "sentinel"), sentinel, 0o600))
	require.NoError(t, runtime.MountOverlay(xfsRoot, mergedRoot))
	mountedOverlay = true

	fill, err := os.OpenFile(filepath.Join(mergedRoot, "fill"), os.O_CREATE|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	chunk := make([]byte, 1<<20)
	normalLimit := nodeLimit - retirementReserve
	for budget.Usage().UsedBytes < normalLimit-(8<<20) {
		_, err = fill.Write(chunk)
		require.NoError(t, err)
		require.NoError(t, fill.Sync())
	}
	require.NoError(t, fill.Close())
	actualSentinel, err := os.ReadFile(filepath.Join(mergedRoot, "sentinel"))
	require.NoError(t, err)
	require.Equal(t, sentinel, actualSentinel)
	usageBeforeRetire := budget.Usage()
	require.Equal(t, retirementReserve, usageBeforeRetire.ReservedBytes)
	require.LessOrEqual(t, usageBeforeRetire.UsedBytes+usageBeforeRetire.ReservedBytes, nodeLimit)
	writeDone := make(chan error, 1)
	go func() {
		pressured, openErr := os.OpenFile(filepath.Join(mergedRoot, "pressure"), os.O_CREATE|os.O_WRONLY, 0o600)
		if openErr != nil {
			writeDone <- openErr
			return
		}
		payload := make([]byte, 1<<20)
		for index := range payload {
			payload[index] = 0x5a
		}
		var writeErr error
		for range 9 {
			if _, writeErr = pressured.Write(payload); writeErr != nil {
				break
			}
		}
		if writeErr == nil {
			writeErr = pressured.Sync()
		}
		writeDone <- errors.Join(writeErr, pressured.Close())
	}()
	select {
	case pressure := <-pressureObserved:
		require.Equal(t, "node", pressure.Scope)
	case <-time.After(time.Second):
		t.Fatal("normal admission did not trigger node pressure")
	}
	select {
	case err := <-writeDone:
		t.Fatalf("pressured write returned through NBD error boundary: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	require.NoError(t, branch.BeginRetirement())
	select {
	case err := <-writeDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("pressured write did not resume as retirement I/O")
	}
	require.NoError(t, runtime.UnmountOverlay(mergedRoot, true),
		"protected capacity must let OverlayFS synchronize and unmount")
	mountedOverlay = false
	require.NoError(t, runtime.UnmountXFS(xfsRoot, true),
		"protected capacity must let XFS journal synchronize and unmount")
	mountedXFS = false
	require.NoError(t, branch.Flush())
	require.NoError(t, device.Close())
	require.NoError(t, branch.Close())
	require.LessOrEqual(t, budget.Usage().UsedBytes, nodeLimit)
}

type zeroBlockReaderAt struct{ size int64 }

func (r zeroBlockReaderAt) ReadAt(payload []byte, offset int64) (int, error) {
	if offset < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if offset >= r.size {
		return 0, io.EOF
	}
	length := len(payload)
	if remaining := r.size - offset; int64(length) > remaining {
		length = int(remaining)
	}
	clear(payload[:length])
	if length < len(payload) {
		return length, io.EOF
	}
	return length, nil
}

func runPrivilegedCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	output, err := command.CombinedOutput()
	if err != nil {
		return output, fmt.Errorf("%s %s: %w", name, strings.Join(args, " "), err)
	}
	return output, nil
}
