//go:build linux

package session

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/moby/sys/mountinfo"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsartifact"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/rootfswriterauthority"
	"github.com/stretchr/testify/require"
)

// This gate exercises the actual session journal and growth ownership with
// Linux NBD/XFS. Its authority and immutable store remain test doubles; the
// separate remote-service/runsc gate owns product-level acceptance.
func TestPrivilegedSessionAutoGrowthAndLiveCommitsKeepWriterRunning(t *testing.T) {
	devicePath := strings.TrimSpace(os.Getenv(privilegedNBDDeviceEnv))
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_PRIVILEGED") != "1" || devicePath == "" {
		t.Skip("requires explicit automatic-growth acceptance and an exclusive unused NBD device")
	}
	require.Zero(t, os.Geteuid())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	root, err := os.MkdirTemp("", "sandbox0-session-autogrow-")
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
	sparse, err := rootfsblock.GrowSparseGeneration(ctx, objects, base.Descriptor, 64<<40, objects, rootfsblock.BuildOptions{})
	require.NoError(t, err)
	stage := testStageRequest(t, objects, "privileged-session-autogrow")
	stage.Generation.Descriptor = sparse.Payload
	stage.Generation.BaseBlockRoot, stage.Generation.CurrentBlockHead = sparse.Descriptor.MappingRoot.RootDigest, sparse.Descriptor.MappingRoot.RootDigest
	stage = stage.WithoutWriterGrantToken()
	require.NoError(t, stage.ValidateDurableBinding())
	authority := &nodeLiveAuthority{state: "consumed", progress: protocol.LiveCommitProgress{CommittedGenerationID: stage.InitialGeneration, InstalledGenerationID: stage.InitialGeneration}}
	m, err := New(Config{StatePath: filepath.Join(root, "state.db"), BranchRoot: filepath.Join(root, "branches"), MountRoot: filepath.Join(root, "mounts"),
		MaxDirtyTailBytes: 32 << 20, MaxNodeDirtyTailBytes: 64 << 20, DirtyTailRetirementReserveBytes: 8 << 20,
		Source: objects, Publisher: objects, Runtime: runtime, LiveCommitAuthority: authority})
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()
	_, err = m.Ensure(ctx, stage)
	require.NoError(t, err)
	stored, err := m.load(stage.Parent)
	require.NoError(t, err)
	branch := m.live[stage.Parent].branch
	defer func() {
		// Cleanup is a planned physical retirement of this isolated fixture,
		// after the writer has exited; it retains the session's normal ordering.
		require.NoError(t, m.SettleLiveCommit(context.Background(), stage))
		require.NoError(t, m.BeginRetire(stage.Parent, stage.Identity, "fixture-cleanup"))
		require.NoError(t, m.Release(context.Background(), stage.Identity))
	}()
	writerCtx, stopWriter := context.WithCancel(ctx)
	defer stopWriter()
	command := exec.CommandContext(writerCtx, "python3", "-c", `import os,sys,time
marker=bytearray(b'session-growth-heap-marker')
with open(sys.argv[1], 'wb', buffering=0) as f:
 for i in range(600):
  f.write(bytes([i%251])*1048576)
  os.fsync(f.fileno())
  time.sleep(.005)
assert marker==b'session-growth-heap-marker'
print('writer_pid=%d chunks=600 marker_ok=true'%os.getpid())`, filepath.Join(stored.MergedRoot, "growing.bin"))
	var output strings.Builder
	command.Stdout, command.Stderr = &output, &output
	require.NoError(t, command.Start())
	finished := make(chan error, 1)
	go func() { finished <- command.Wait(); close(finished) }()
	defer func() {
		stopWriter()
		select {
		case <-finished:
		case <-time.After(10 * time.Second):
			t.Fatal("retain fixture: writer did not exit")
		}
	}()
	writerPID := command.Process.Pid
	growths, commits := 0, 0
	peakWAL := int64(0)
	ticker := time.NewTicker(time.Second)
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
			peakWAL = max(peakWAL, m.NodeDirtyTailUsage().UsedBytes)
			grew, growErr := m.GrowLiveXFS(ctx, stage)
			require.NoError(t, growErr)
			if grew {
				growths++
			}
			hints, hintErr := m.LiveCommitSessions(time.Now())
			require.NoError(t, hintErr)
			if len(hints) != 0 {
				commitErr := m.CommitLive(ctx, hints[0].Stage)
				var capacity *rootfsblock.DirtyTailCapacityError
				if commitErr != nil && !errdefs.IsUnavailable(commitErr) && !errors.As(commitErr, &capacity) {
					require.NoError(t, commitErr)
				}
				if commitErr == nil {
					commits++
				}
			}
		}
	}
	require.Contains(t, output.String(), fmt.Sprintf("writer_pid=%d chunks=600 marker_ok=true", writerPID))
	require.GreaterOrEqual(t, growths, 2)
	require.Greater(t, commits, 10)
	require.LessOrEqual(t, peakWAL, int64(32<<20))
	require.Same(t, branch, m.live[stage.Parent].branch)
	file, err := os.Open(filepath.Join(stored.MergedRoot, "growing.bin"))
	require.NoError(t, err)
	defer file.Close()
	for i := range 600 {
		var chunk [1 << 20]byte
		_, err := file.ReadAt(chunk[:], int64(i)*(1<<20))
		require.NoError(t, err)
		require.True(t, bytes.Equal(chunk[:], bytes.Repeat([]byte{byte(i % 251)}, len(chunk))), "incorrect chunk %d", i)
	}
	usage, err := runtime.InspectXFS(stored.DevicePath, stored.XFSRoot)
	require.NoError(t, err)
	current, err := m.load(stage.Parent)
	require.NoError(t, err)
	require.Equal(t, stage.InitialGeneration, current.Stage.InitialGeneration)
	t.Logf("session_owned_growth=true sparse_device_bytes=%d initial_xfs_bytes=%d final_xfs_bytes=%d growths=%d foreground_commits=%d peak_wal_payload_bytes=%d writer_pid=%d", branch.Size(), initialBytes, usage.SizeBytes, growths, commits, peakWAL, writerPID)
}
