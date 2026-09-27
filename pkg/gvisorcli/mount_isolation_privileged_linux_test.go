//go:build linux

package gvisorcli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type mountIsolationZeroReader struct{}

func (mountIsolationZeroReader) ReadAt(p []byte, off int64) (int, error) {
	clear(p)
	return len(p), nil
}

// A running guest must not pin an unrelated, already unmounted XFS filesystem.
// This opt-in probe owns the supplied unused NBD device and all its mount paths.
func TestPrivilegedRunscUnrelatedXFSRelease(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_PRIVILEGED_MOUNT_ISOLATION") != "1" {
		t.Skip("requires an isolated Linux test host")
	}
	require.Zero(t, os.Geteuid())
	devicePath := os.Getenv("SANDBOX0_MOUNT_ISOLATION_NBD")
	require.NotEmpty(t, devicePath, "supply an unused NBD device")
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	for _, directFS := range []bool{true, false} {
		t.Run(fmt.Sprintf("directfs_%t", directFS), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			root := t.TempDir()
			mountRoot := filepath.Join(root, "rootfs-mounts")
			victim := filepath.Join(mountRoot, "unrelated", "xfs")
			require.NoError(t, os.MkdirAll(victim, 0700))
			branch, err := rootfsblock.OpenBranch(filepath.Join(root, "branch.wal"), rootfsblock.BranchIdentity{
				Version: rootfsblock.BranchFormatVersion, RootFSID: "unrelated", GenerationID: "base", WriterEpoch: 1,
				LogicalSizeBytes: 512 << 20, BaseRootDigest: digest.FromString("empty").String()}, mountIsolationZeroReader{})
			require.NoError(t, err)
			defer branch.Close()
			device, err := rootfsblock.StartKernelNBD(ctx, ctx, branch, rootfsblock.KernelNBDOptions{
				DevicePath: devicePath, RequestTimeout: 10 * time.Second, ReadyTimeout: 10 * time.Second})
			require.NoError(t, err)
			defer device.Close()
			output, err := exec.CommandContext(ctx, "mkfs.xfs", "-f", "-m", "reflink=0", devicePath).CombinedOutput()
			require.NoError(t, err, string(output))
			require.NoError(t, unix.Mount(devicePath, victim, "xfs", 0, "nouuid"))
			defer unix.Unmount(victim, 0)
			require.NoError(t, os.WriteFile(filepath.Join(victim, "identity"), []byte("unrelated-data"), 0600))

			// Include an overlay and a bind alias outside the attested mount root.
			oldMerged := filepath.Join(mountRoot, "unrelated", "merged")
			oldAlias := filepath.Join(root, "old-consumer")
			for _, p := range []string{oldMerged, oldAlias, filepath.Join(victim, "lower"), filepath.Join(victim, "upper"), filepath.Join(victim, "work")} {
				require.NoError(t, os.MkdirAll(p, 0700))
			}
			require.NoError(t, unix.Mount("overlay", oldMerged, "overlay", 0, "lowerdir="+filepath.Join(victim, "lower")+",upperdir="+filepath.Join(victim, "upper")+",workdir="+filepath.Join(victim, "work")))
			defer unix.Unmount(oldMerged, 0)
			require.NoError(t, unix.Mount(oldMerged, oldAlias, "", unix.MS_BIND, ""))
			defer unix.Unmount(oldAlias, 0)
			rootfs := filepath.Join(root, "guest-root")
			evidence := filepath.Join(root, "evidence")
			for _, dir := range []string{rootfs, evidence, filepath.Join(rootfs, "proc"), filepath.Join(rootfs, "dev"), filepath.Join(rootfs, "tmp"), filepath.Join(rootfs, "evidence")} {
				require.NoError(t, os.MkdirAll(dir, 0700))
			}
			binary, err := os.Executable()
			require.NoError(t, err)
			input, err := os.Open(binary)
			require.NoError(t, err)
			out, err := os.OpenFile(filepath.Join(rootfs, "payload"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0700)
			require.NoError(t, err)
			_, err = io.Copy(out, input)
			require.NoError(t, err)
			require.NoError(t, input.Close())
			require.NoError(t, out.Close())
			// The current guest also uses an overlay with an external consumer bind.
			ownMerged := filepath.Join(mountRoot, "current", "merged")
			ownConsumer := filepath.Join(root, "current-consumer")
			for _, p := range []string{ownMerged, ownConsumer, filepath.Join(root, "ownupper"), filepath.Join(root, "ownwork")} {
				require.NoError(t, os.MkdirAll(p, 0700))
			}
			require.NoError(t, unix.Mount("overlay", ownMerged, "overlay", 0, "lowerdir="+rootfs+",upperdir="+filepath.Join(root, "ownupper")+",workdir="+filepath.Join(root, "ownwork")))
			defer unix.Unmount(ownMerged, 0)
			require.NoError(t, unix.Mount(ownMerged, ownConsumer, "", unix.MS_BIND, ""))
			defer unix.Unmount(ownConsumer, 0)
			bundle := filepath.Join(root, "bundle")
			require.NoError(t, os.Mkdir(bundle, 0700))
			spec := specs.Spec{Version: specs.Version, Root: &specs.Root{Path: ownConsumer, Readonly: true},
				Process: &specs.Process{Cwd: "/", Args: []string{"/payload", "-test.run=^TestCheckpointPayload$"}, Env: checkpointProbeEnvironment(t)},
				Mounts: []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}, {Destination: "/dev", Type: "tmpfs", Source: "tmpfs"},
					{Destination: "/tmp", Type: "tmpfs", Source: "tmpfs"}, {Destination: "/evidence", Type: "bind", Source: evidence, Options: []string{"rbind", "rw"}}},
				Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}}}}
			payload, err := json.Marshal(spec)
			require.NoError(t, err)
			require.NoError(t, os.WriteFile(filepath.Join(bundle, "config.json"), payload, 0600))
			config := Config{Path: runsc, Root: filepath.Join(root, "runsc"), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: directFS, RootFSMountRoot: mountRoot}
			runner := New(config).(CheckpointRunsc)
			defer func() {
				cleanup, done := context.WithTimeout(context.Background(), 15*time.Second)
				defer done()
				_ = runner.Delete(cleanup, "guest", true)
				_ = unix.Unmount(filepath.Join(config.Root, "null-netns"), 0)
			}()
			require.NoError(t, runner.Create(ctx, bundle, "guest"))
			require.NoError(t, runner.Start(ctx, "guest"))
			before := waitCheckpointEvidence(t, ctx, evidence, 2)
			identity, err := os.ReadFile(filepath.Join(victim, "identity"))
			require.NoError(t, err)
			require.Equal(t, "unrelated-data", string(identity), "launch must leave the host mount intact")
			require.NoError(t, unix.Unmount(oldAlias, 0))
			require.NoError(t, unix.Unmount(oldMerged, 0))
			require.NoError(t, unix.Unmount(victim, 0), "normal host unmount must succeed")
			require.Eventually(t, func() bool {
				_, e := os.Stat(filepath.Join("/sys/fs/xfs", filepath.Base(devicePath)))
				return os.IsNotExist(e)
			}, 3*time.Second, 25*time.Millisecond,
				"running stock-runsc guest retained an unrelated XFS superblock")
			after := waitCheckpointEvidence(t, ctx, evidence, before.Counter+2)
			require.Equal(t, before.Token, after.Token)
			require.Equal(t, before.MemorySHA256, after.MemorySHA256)
			require.NoError(t, device.Close())
			require.NoError(t, branch.Close())
		})
	}
}
