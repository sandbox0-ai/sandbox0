//go:build linux

package gvisorcli

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/containerd/errdefs"
	"github.com/opencontainers/go-digest"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/pkg/migrationstaging"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Exercise the actual staged publisher, retained cache, chunk cloner, XFS quota
// and qualified stock runsc. The probe owns its loop filesystem and guests;
// it never opens a production checkpoint, allocation or device. This is runtime
// acceptance, not a measurement of the entire authenticated Sandpi/API path.
func TestPrivilegedRetainedCheckpointRestore(t *testing.T) {
	if os.Getenv("SANDBOX0_RUN_RETAINED_RESTORE") != "1" {
		t.Skip("explicit isolated Linux retained-image acceptance only")
	}
	require.Zero(t, os.Geteuid())
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Minute)
	defer cancel()
	run := func(name string, args ...string) string {
		t.Helper()
		output, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
		require.NoError(t, err, "%s: %s", name, output)
		return strings.TrimSpace(string(output))
	}
	runsc, err := exec.LookPath("runsc")
	require.NoError(t, err)
	require.Contains(t, run(runsc, "--version"), "release-20260914.0")
	binary, err := os.Open(runsc)
	require.NoError(t, err)
	hash := sha256.New()
	_, err = io.Copy(hash, binary)
	require.NoError(t, err)
	require.NoError(t, binary.Close())
	require.Equal(t, "c0f4ec0ac1198975d5cf919a78f2302426de096f69eebd33e50125c3ca42d699", fmt.Sprintf("%x", hash.Sum(nil)), "qualified stock runsc only")

	work := t.TempDir()
	backing := filepath.Join(work, "owned-xfs.img")
	file, err := os.OpenFile(backing, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o600)
	require.NoError(t, err)
	require.NoError(t, file.Truncate(4<<30))
	require.NoError(t, file.Close())
	loop := run("losetup", "--find", "--show", "--direct-io=on", backing)
	require.True(t, strings.HasPrefix(loop, "/dev/loop") && !strings.ContainsAny(loop, " \t\r\n"))
	t.Cleanup(func() {
		output, err := exec.Command("losetup", "--detach", loop).CombinedOutput()
		require.NoError(t, err, "%s", output)
	})
	require.Equal(t, "1", run("losetup", "--list", "--noheadings", "--output", "DIO", loop), "backing page cache must not hide cold reads")
	run("mkfs.xfs", "-f", loop)
	mount := filepath.Join(work, "mnt")
	require.NoError(t, os.Mkdir(mount, 0o700))
	run("mount", "-o", "prjquota", loop, mount)
	t.Cleanup(func() { require.NoError(t, unix.Unmount(mount, 0)) })
	root := filepath.Join(mount, "probe")
	require.NoError(t, os.Mkdir(root, 0o700))
	t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_MIB", "512")
	t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_PATTERN", "random")
	t.Setenv("SANDBOX0_CHECKPOINT_MEMORY_VERIFY_ON_REQUEST", "1")
	prepareCrossHostBundle(t, root)
	configPath := filepath.Join(root, "bundle", "config.json")
	payload, err := os.ReadFile(configPath)
	require.NoError(t, err)
	var spec specs.Spec
	require.NoError(t, json.Unmarshal(payload, &spec))
	period, quota, memory := uint64(100000), int64(100000), int64(2<<30)
	spec.Linux.CgroupsPath = "/sandbox0-" + filepath.Base(work)
	spec.Linux.Resources = &specs.LinuxResources{
		CPU: &specs.LinuxCPU{Period: &period, Quota: &quota}, Memory: &specs.LinuxMemory{Limit: &memory},
	}
	payload, err = json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, payload, 0o600))
	makeRunner := func(id string) *Command {
		config := Config{Path: runsc, Root: filepath.Join(root, id), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true}
		runner := New(config).(*Command)
		t.Cleanup(func() {
			cleanup, done := context.WithTimeout(context.Background(), 20*time.Second)
			defer done()
			require.NoError(t, runner.Delete(cleanup, id, true))
			if err := unix.Unmount(filepath.Join(config.Root, "null-netns"), 0); err != nil && !errors.Is(err, unix.ENOENT) && !errors.Is(err, unix.EINVAL) {
				t.Error(err)
			}
		})
		return runner
	}
	staging := filepath.Join(root, "staging")
	require.NoError(t, os.Mkdir(staging, 0o700))
	require.False(t, strings.ContainsAny(staging, " \t\n'\""))
	limits := migrationstaging.Limits{ProjectID: 1395654658, Bytes: 2 << 30, Inodes: 4096}
	run("xfs_quota", "-x", "-c", fmt.Sprintf("project -s -p %s %d", staging, limits.ProjectID), mount)
	run("xfs_quota", "-x", "-c", fmt.Sprintf("limit -p bhard=2g ihard=4096 %d", limits.ProjectID), mount)
	guard, err := migrationstaging.Open(staging, limits)
	require.NoError(t, err)
	admit := func(bytes int64, inodes uint64) error { return guard.AdmitBudget(staging, bytes, inodes) }
	cache, err := rootfsblock.NewReadCacheWithDisk(0, rootfsblock.DiskCacheConfig{Directory: filepath.Join(root, "chunks"), MaxBytes: 1 << 30})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, cache.Close()) })
	objects := objectstore.NewMemoryStore("")
	retention := runtimecheckpoint.RetainedImageLimits{Directory: filepath.Join(staging, "retained-images"), Bytes: 1 << 30, Inodes: 1024, Entries: 2, Lifetime: 24 * time.Hour}
	newStore := func(retain bool) *runtimecheckpoint.Store {
		store, err := runtimecheckpoint.NewWithChunkCache(objects, 1<<30, cache)
		require.NoError(t, err)
		t.Cleanup(store.Close)
		if retain {
			require.NoError(t, store.ConfigureRetainedImages(retention))
		}
		return store
	}
	store := newStore(true)
	source := makeRunner("source")
	bundle := filepath.Join(root, "bundle")
	evidence := filepath.Join(root, "evidence")
	require.NoError(t, source.Create(ctx, bundle, "source"))
	require.NoError(t, source.Start(ctx, "source"))
	before := waitCheckpointEvidence(t, ctx, evidence, 2)
	image := filepath.Join(staging, "source-image")
	require.NoError(t, source.Checkpoint(ctx, "source", image))
	require.NoError(t, source.Delete(ctx, "source", true))
	payload, err = os.ReadFile(filepath.Join(evidence, "state.json"))
	require.NoError(t, err)
	var cut checkpointPayloadEvidence
	require.NoError(t, json.Unmarshal(payload, &cut))
	require.Equal(t, before.Token, cut.Token)
	original, err := os.Stat(filepath.Join(image, "pages.img"))
	require.NoError(t, err)
	d := digest.FromString("isolated-retained-restore").String()
	binding := runtimecheckpoint.Binding{OperationID: "retained-probe", SandboxID: "probe", TeamID: "probe",
		SourceBindingDigest: d, RuntimeCompatibilityDigest: d, AssignmentRevision: d, CPUFeaturesDigest: d,
		RootFSGenerationID: "probe-readonly-rootfs", RootFSDescriptorDigest: d}
	scopeBinding := binding
	scopeBinding.RootFSGenerationID, scopeBinding.RootFSDescriptorDigest = "", ""
	scope, err := runtimecheckpoint.NewCaptureScope(scopeBinding)
	require.NoError(t, err)
	stage, err := store.OpenCaptureStaging(ctx, scope, 1<<30)
	require.NoError(t, err)
	plan, err := stage.PlanLocal(ctx, binding, image)
	require.NoError(t, err)
	ref, err := stage.PublishPlanned(ctx, binding, plan, image)
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(image))
	store.Close()
	store = newStore(true) // Persistent hints must work after source/service loss.
	cold := newStore(false)
	_, err = migrationstaging.Open(staging, limits)
	require.NoError(t, err, "retained links must remain inside the enforced project after restart")

	for round := range 3 {
		for step := range 2 {
			retained := (round+step)%2 == 1 // Rotate order to avoid fixed cohort ordering.
			id := fmt.Sprintf("restore-%d-%d", round, step)
			target := makeRunner(id)
			destination := filepath.Join(staging, id+"-image")
			reader := cold
			if retained {
				reader = store
			}
			started := time.Now()
			manifest, stats, err := reader.DownloadWithAdmissionStats(ctx, binding, ref, destination, admit)
			require.NoError(t, err)
			prepared := time.Since(started)
			var bytes int64
			for _, file := range manifest.Files {
				bytes += file.Size
			}
			info, err := os.Stat(filepath.Join(destination, "pages.img"))
			require.NoError(t, err)
			require.Equal(t, retained, stats.RetainedImage)
			require.Equal(t, retained, os.SameFile(original, info))
			if retained {
				require.Equal(t, bytes, stats.RetainedBytes)
			} else {
				require.Equal(t, bytes, stats.ClonedBytes, "baseline must actually clone every verified chunk")
			}
			verified := time.Now()
			_, err = reader.VerifyLocal(ctx, binding, ref, destination)
			require.NoError(t, err)
			verification := time.Since(verified)
			resident, pages := retainedProbeResidency(t, filepath.Join(destination, "pages.img"))
			if retained {
				require.Equal(t, pages, resident)
			} else {
				require.Zero(t, resident, "fresh cloned inode must start cold")
			}
			require.NoError(t, os.Remove(filepath.Join(evidence, "state.json")))
			require.NoError(t, target.Create(ctx, bundle, id))
			restoring := time.Now()
			require.NoError(t, target.Restore(ctx, id, destination))
			restoreDuration := time.Since(restoring)
			after := waitCheckpointEvidence(t, ctx, evidence, cut.Counter+1)
			ready := time.Since(started)
			var nonce [16]byte
			_, err = rand.Read(nonce[:])
			require.NoError(t, err)
			challenge := fmt.Sprintf("%x", nonce)
			require.NoError(t, os.WriteFile(filepath.Join(evidence, "verify-memory.new"), []byte(challenge), 0o600))
			require.NoError(t, os.Rename(filepath.Join(evidence, "verify-memory.new"), filepath.Join(evidence, "verify-memory")))
			for after.MemoryVerification != challenge {
				after = waitCheckpointEvidence(t, ctx, evidence, after.Counter+1)
			}
			require.Equal(t, cut.Token, after.Token)
			require.Equal(t, cut.PID, after.PID)
			require.Equal(t, cut.Offset, after.Offset)
			require.Equal(t, cut.Temp, after.Temp)
			require.Equal(t, cut.CPUFlagsDigest, after.CPUFlagsDigest)
			require.Equal(t, cut.MemorySHA256, after.MemorySHA256, "fresh challenge must scan all 512 MiB after restore")
			t.Logf("retained_restore round=%d retained=%t image_bytes=%d resident_pages=%d total_pages=%d preparation_us=%d admission_verification_us=%d runsc_restore_us=%d prepare_to_ready_us=%d prepare_to_verified_us=%d", round, retained, bytes, resident, pages, prepared.Microseconds(), verification.Microseconds(), restoreDuration.Microseconds(), ready.Microseconds(), time.Since(started).Microseconds())
			require.NoError(t, target.Delete(ctx, id, true))
			require.NoError(t, os.RemoveAll(destination))
		}
	}
	// No guest, source image or destination owns these bytes now: retention alone
	// must charge the kernel quota, and eviction must return usable capacity.
	require.ErrorIs(t, guard.AdmitBudget(staging, 7<<28, 8), errdefs.ErrResourceExhausted)
	require.NoError(t, store.EvictRetainedImages(ctx))
	require.NoError(t, guard.AdmitBudget(staging, 7<<28, 8))
}

func retainedProbeResidency(t *testing.T, name string) (resident, pages int) {
	t.Helper()
	file, err := os.Open(name)
	require.NoError(t, err)
	defer file.Close()
	info, err := file.Stat()
	require.NoError(t, err)
	require.Positive(t, info.Size())
	mapping, err := unix.Mmap(int(file.Fd()), 0, int(info.Size()), unix.PROT_NONE, unix.MAP_SHARED)
	require.NoError(t, err)
	defer func() { require.NoError(t, unix.Munmap(mapping)) }()
	vector := make([]byte, (len(mapping)+os.Getpagesize()-1)/os.Getpagesize())
	_, _, errno := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&mapping[0])), uintptr(len(mapping)), uintptr(unsafe.Pointer(&vector[0])))
	require.Zero(t, errno)
	for _, state := range vector {
		if state&1 != 0 {
			resident++
		}
	}
	return resident, len(vector)
}
