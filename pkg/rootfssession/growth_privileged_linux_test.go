//go:build linux

package session

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/sys/mountinfo"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// This gate proves mounted NBD/XFS growth and WAL rotation. The separate
// runsc/regional acceptance gate must prove the complete product integration.
func TestPrivilegedXFSGrowthAndJournalRotationKeepWriterRunning(t *testing.T) {
	devicePath := strings.TrimSpace(os.Getenv(privilegedNBDDeviceEnv))
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_PRIVILEGED") != "1" || devicePath == "" {
		t.Skip("requires explicit automatic-growth acceptance and an exclusive unused NBD device")
	}
	require.Zero(t, os.Geteuid())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	root, err := os.MkdirTemp("", "sandbox0-autogrow-")
	require.NoError(t, err)
	t.Cleanup(func() {
		mounts, err := mountinfo.GetMounts(nil)
		if err != nil {
			t.Errorf("retain fixture after mount inspection failure: %v", err)
			return
		}
		for _, mount := range mounts {
			if mount.Mountpoint == root || strings.HasPrefix(mount.Mountpoint, root+"/") {
				t.Errorf("retain fixture with unreleased mount %s", mount.Mountpoint)
				return
			}
		}
		require.NoError(t, os.RemoveAll(root))
	})
	runtime, err := NewLinuxRuntime(LinuxRuntimeConfig{DevicePaths: []string{devicePath}})
	require.NoError(t, err)
	require.NoError(t, demandReadUnusedDevice(runtime, devicePath))
	source := filepath.Join(root, "source")
	require.NoError(t, os.Mkdir(source, 0o700))
	imagePath := filepath.Join(root, "base.xfs")
	initialBytes := int64(300 << 20)
	require.NoError(t, (rootfsartifact.XFSBuilder{}).Build(ctx, source, imagePath, initialBytes))
	objects := newSessionObjectStore()
	image, err := os.Open(imagePath)
	require.NoError(t, err)
	base, err := rootfsblock.BuildMaterializedFileGeneration(ctx, image, initialBytes, objects, rootfsblock.BuildOptions{}, nil)
	require.NoError(t, errors.Join(err, image.Close()))
	require.NoError(t, os.Remove(imagePath))
	grown, err := rootfsblock.GrowSparseGeneration(ctx, objects, base.Descriptor, 64<<40, objects, rootfsblock.BuildOptions{})
	require.NoError(t, err)
	require.Less(t, grown.Bytes, int64(4096))
	cache, err := rootfsblock.NewReadCache(rootfsblock.DefaultReadCacheBytes)
	require.NoError(t, err)
	defer cache.Close()
	reader, err := rootfsblock.NewReaderWithCache(objects, grown.Descriptor, cache)
	require.NoError(t, err)
	identity := rootfsblock.BranchIdentity{Version: rootfsblock.BranchFormatVersion, RootFSID: t.Name(), GenerationID: "initial", WriterEpoch: 1,
		LogicalSizeBytes: grown.Descriptor.LogicalSizeBytes, BaseRootDigest: grown.Descriptor.MappingRoot.RootDigest}
	budget, err := rootfsblock.NewDirtyTailBudgetWithReserve(256<<20, 16<<20)
	require.NoError(t, err)
	branch, err := rootfsblock.OpenBranchWithOptions(filepath.Join(root, "branch-0.wal"), identity, reader, rootfsblock.BranchOptions{MaxDirtyTailBytes: 192 << 20, NodeDirtyBudget: budget})
	require.NoError(t, err)
	device, err := rootfsblock.StartKernelNBD(ctx, ctx, branch, rootfsblock.KernelNBDOptions{DevicePath: devicePath, RequestTimeout: 30 * time.Second})
	require.NoError(t, err)
	xfsRoot := filepath.Join(root, "xfs")
	mergedRoot := filepath.Join(root, "merged")
	finished := make(chan error, 1)
	writerStarted := false
	defer func() {
		if writerStarted {
			select {
			case <-finished:
			case <-time.After(10 * time.Second):
				t.Fatal("retain fixture: writer did not exit before mount cleanup")
			}
		}
		cleanup := errors.Join(runtime.UnmountOverlay(mergedRoot, true), runtime.UnmountXFS(xfsRoot, true))
		if device != nil {
			cleanup = errors.Join(cleanup, device.Close())
		}
		if branch != nil {
			cleanup = errors.Join(cleanup, branch.Close())
		}
		require.NoError(t, cleanup)
	}()
	require.NoError(t, runtime.MountXFS(devicePath, xfsRoot))
	require.NoError(t, runtime.MountOverlay(xfsRoot, mergedRoot))
	usage, err := runtime.InspectXFS(devicePath, xfsRoot)
	require.NoError(t, err)
	require.Equal(t, initialBytes, usage.SizeBytes)
	// The process retains one open file and heap marker for the entire test.
	// Each chunk has a distinct value so suffix overwrites cannot hide loss.
	writerCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	command := exec.CommandContext(writerCtx, "python3", "-c", `import os,sys,time
marker=bytearray(b'rootfs-growth-heap-marker')
with open(sys.argv[1], 'wb', buffering=0) as f:
 for i in range(600):
  f.write(bytes([i%251])*1048576)
  os.fsync(f.fileno())
  time.sleep(.008)
assert marker==b'rootfs-growth-heap-marker'
print('writer_pid=%d chunks=600 marker_ok=true'%os.getpid())`, filepath.Join(mergedRoot, "growing.bin"))
	var output strings.Builder
	command.Stdout, command.Stderr = &output, &output
	require.NoError(t, command.Start())
	writerStarted = true
	writerPID := command.Process.Pid
	go func() {
		finished <- command.Wait()
		close(finished)
	}()
	baseDescriptor := grown.Descriptor
	rotations, growths := 0, 0
	peakWAL := int64(0)
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	running := true
	for running {
		select {
		case err := <-finished:
			require.NoError(t, err, output.String())
			running = false
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-ticker.C:
			usage, err = runtime.InspectXFS(devicePath, xfsRoot)
			require.NoError(t, err)
			target, targetErr := XFSGrowthTarget(usage, branch.Size())
			require.NoError(t, targetErr)
			if target > 0 {
				usage, err = runtime.GrowXFS(ctx, devicePath, xfsRoot, target)
				require.NoError(t, err)
				growths++
			}
			peakWAL = max(peakWAL, budget.Usage().UsedBytes)
			if branch.DirtyTailUsage().DirtyBytes < 16<<20 {
				continue
			}
			checkpoint, checkpointErr := branch.Checkpoint()
			require.NoError(t, checkpointErr)
			cut := checkpoint.Cut()
			committed, buildErr := rootfsblock.BuildIncrementalGenerationFromBlockReader(ctx, objects, baseDescriptor, checkpoint, objects, rootfsblock.BuildOptions{})
			require.NoError(t, errors.Join(buildErr, checkpoint.Close()))
			committedReader, readerErr := rootfsblock.NewReaderWithCache(objects, committed.Descriptor, cache)
			require.NoError(t, readerErr)
			rotations++
			path := filepath.Join(root, fmt.Sprintf("branch-%d.wal", rotations))
			// Production installation is a durable session-journal transaction.
			// This isolated primitive fixture stores its selected identity/base.
			err = branch.RotateCommitted(cut, committedReader, committed.Descriptor.MappingRoot.RootDigest, path, func(next rootfsblock.BranchIdentity) error {
				payload, marshalErr := json.Marshal(struct {
					Path       string
					Identity   rootfsblock.BranchIdentity
					Descriptor rootfsblock.Descriptor
				}{path, next, committed.Descriptor})
				if marshalErr != nil {
					return marshalErr
				}
				file, openErr := os.OpenFile(filepath.Join(root, "installation.json"), os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
				if openErr != nil {
					return openErr
				}
				_, writeErr := file.Write(payload)
				return errors.Join(writeErr, file.Sync(), file.Close())
			})
			require.NoError(t, err)
			baseDescriptor = committed.Descriptor
		}
	}
	require.Contains(t, output.String(), fmt.Sprintf("writer_pid=%d chunks=600 marker_ok=true", writerPID))
	require.GreaterOrEqual(t, growths, 2)
	require.Greater(t, rotations, 2)
	require.Less(t, peakWAL, int64(192<<20))
	file, err := os.Open(filepath.Join(mergedRoot, "growing.bin"))
	require.NoError(t, err)
	for index := range 600 {
		var sample [1 << 20]byte
		_, err = file.ReadAt(sample[:], int64(index)*(1<<20))
		require.NoError(t, err)
		require.True(t, bytes.Equal(sample[:], bytes.Repeat([]byte{byte(index % 251)}, len(sample))), "written chunk %d is incorrect", index)
	}
	require.NoError(t, file.Close())
	t.Logf("sparse_device_bytes=%d initial_xfs_bytes=%d final_xfs_bytes=%d growths=%d rotations=%d peak_wal_payload_bytes=%d writer_pid=%d", branch.Size(), initialBytes, usage.SizeBytes, growths, rotations, peakWAL, writerPID)
	// Restore the last live block cut with a fresh writable branch and no old
	// WAL. Mounting XFS replays any log transactions captured during writes.
	// This cut was made without a guest freeze, so it proves crash consistency
	// at the filesystem layer, independently of a clean final unmount image.
	require.NoError(t, runtime.UnmountOverlay(mergedRoot, true))
	require.NoError(t, runtime.UnmountXFS(xfsRoot, true))
	require.NoError(t, device.Close())
	require.NoError(t, branch.Close())
	require.NoError(t, demandReadUnusedDevice(runtime, devicePath))
	restoredReader, err := rootfsblock.NewReaderWithCache(objects, baseDescriptor, cache)
	require.NoError(t, err)
	identity.GenerationID, identity.WriterEpoch = "restored-live-cut", 2
	identity.BaseRootDigest = baseDescriptor.MappingRoot.RootDigest
	branch, err = rootfsblock.OpenBranchWithOptions(filepath.Join(root, "restored.wal"), identity, restoredReader,
		rootfsblock.BranchOptions{MaxDirtyTailBytes: 192 << 20, NodeDirtyBudget: budget})
	require.NoError(t, err)
	device, err = rootfsblock.StartKernelNBD(ctx, ctx, branch, rootfsblock.KernelNBDOptions{DevicePath: devicePath, RequestTimeout: 30 * time.Second})
	require.NoError(t, err)
	require.NoError(t, runtime.MountXFS(devicePath, xfsRoot))
	require.NoError(t, runtime.MountOverlay(xfsRoot, mergedRoot))
	restored, err := os.Open(filepath.Join(mergedRoot, "growing.bin"))
	require.NoError(t, err)
	defer restored.Close()
	info, err := restored.Stat()
	require.NoError(t, err)
	chunks := int(info.Size() / (1 << 20))
	require.Greater(t, chunks, 0)
	require.LessOrEqual(t, chunks, 600)
	for index := range chunks {
		var sample [1 << 20]byte
		_, err = restored.ReadAt(sample[:], int64(index)*(1<<20))
		require.NoError(t, err)
		require.True(t, bytes.Equal(sample[:], bytes.Repeat([]byte{byte(index % 251)}, len(sample))), "restored chunk %d is incorrect", index)
	}
	t.Logf("live_cut_restored_without_original_wal=true complete_chunks=%d", chunks)
}
