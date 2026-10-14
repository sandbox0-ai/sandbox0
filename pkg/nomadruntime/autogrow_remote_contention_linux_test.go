//go:build linux

package nomadruntime

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
	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/runtime-spec/specs-go"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// Both guests share one manager, node WAL budget, cache, upload admission and
// real regional authority. Separate test processes would not prove contention.
func TestPrivilegedRemoteAutogrowTwoWritersShareNodeBudget(t *testing.T) {
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_CONTENTION") != "1" {
		t.Skip("requires exclusive two-device remote contention fixture")
	}
	require.Zero(t, os.Geteuid())
	require.Equal(t, "1", os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REMOTE"))
	require.Equal(t, "1", os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OUTAGE"))
	require.NotEqual(t, "1", os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK"))
	devices := []string{os.Getenv("SANDBOX0_PRIVILEGED_NBD_DEVICE"), os.Getenv("SANDBOX0_PRIVILEGED_NBD_RESTORE_DEVICE")}
	for _, device := range devices {
		require.NotEmpty(t, device)
	}
	require.NotEqual(t, devices[0], devices[1])
	require.NotEmpty(t, os.Getenv("SANDBOX0_TEST_RUNSC"))
	ctx, cancel := context.WithTimeout(t.Context(), 18*time.Minute)
	defer cancel()
	root, err := os.MkdirTemp("", "sandbox0-autogrow-contention-")
	require.NoError(t, err)
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("retain failed contention fixture at %s", root)
			return
		}
		mounts, err := mountinfo.GetMounts(nil)
		require.NoError(t, err)
		for _, mount := range mounts {
			require.False(t, mount.Mountpoint == root || strings.HasPrefix(mount.Mountpoint, root+"/"), "retain mounted fixture %s", mount.Mountpoint)
		}
		require.NoError(t, os.RemoveAll(root))
	})
	for _, dir := range []string{"source", "a", "a/evidence", "a/bundle", "b", "b/evidence", "b/bundle"} {
		require.NoError(t, os.Mkdir(filepath.Join(root, dir), 0700))
	}
	program := filepath.Join(root, "writer.c")
	require.NoError(t, os.WriteFile(program, []byte(autogrowWriterSource), 0600))
	output, err := exec.CommandContext(ctx, "gcc", "-static", "-O2", "-Wall", "-Wextra", "-o", filepath.Join(root, "source/writer"), program).CombinedOutput()
	require.NoError(t, err, "%s", output)
	store := sandboxstore.NewPGSandboxStore(autogrowRemotePool(t))
	runID := digest.FromString(fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano())).Encoded()
	bucket := "s0-autogrow-contention-" + runID[:16]
	t.Logf("isolated_object_bucket=%s", bucket)
	endpoint, outage := autogrowObjectEndpoint(t, os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"))
	require.NotNil(t, outage)
	defer outage.down.Store(false)
	objects, err := objectstore.Create(objectstore.Config{Type: objectstore.TypeS3, Bucket: bucket, Region: "us-east-1", Endpoint: endpoint,
		AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY")})
	require.NoError(t, err)
	require.NoError(t, objects.Create())
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("retain failed contention objects for custody inspection")
			return
		}
		for {
			items, more, _, err := objects.List("", "", "", "", 1000)
			require.NoError(t, err)
			for _, item := range items {
				require.NoError(t, objects.Delete(item.Key))
			}
			if !more {
				break
			}
		}
	})
	artifact := autogrowRemoteBase(t, root, runID, store, conditional)
	client, loss := autogrowRemoteAuthority(t, root, store)
	host, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{DevicePaths: devices})
	require.NoError(t, err)
	publisher := rootfsblock.ObjectStorePublisher{Store: conditional}
	m, err := rootfssession.New(rootfssession.Config{StatePath: filepath.Join(root, "sessions.db"), BranchRoot: filepath.Join(root, "branches"), MountRoot: filepath.Join(root, "mounts"),
		MaxDirtyTailBytes: 32 << 20, MaxNodeDirtyTailBytes: 40 << 20, DirtyTailRetirementReserveBytes: 8 << 20,
		ReadCacheBytes: 64 << 20, ReadDiskCache: rootfsblock.DiskCacheConfig{Directory: filepath.Join(root, "read-cache"), MaxBytes: 32 << 20},
		Source: conditional, Publisher: publisher, Runtime: host, LiveCommitAuthority: liveCommitAuthorityAdapter{client: client},
		PublisherForOperation: func(s rootfshandoff.StageRequest, op string) rootfsblock.ImmutableObjectPublisher {
			return nodeJournalPublisher{authority: client, stage: s, operation: op, publisher: publisher}
		}})
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()
	runtime := &rootfsRuntime{sessions: m, authority: client, liveCommits: true, autoGrow: true, logger: newLogger(zap.NewNop())}
	type writer struct {
		name, root, mount string
		stage             rootfshandoff.StageRequest
		first, last       autogrowGuestEvidence
		done              bool
	}
	var writers []*writer
	runner := gvisorcli.New(gvisorcli.Config{Path: os.Getenv("SANDBOX0_TEST_RUNSC"), Root: filepath.Join(root, "runsc"), Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true, RootFSMountRoot: filepath.Join(root, "mounts")})
	defer func() {
		outage.down.Store(false)
		cleanup, done := context.WithTimeout(context.Background(), 90*time.Second)
		defer done()
		for _, writer := range writers {
			require.NoError(t, runner.Delete(cleanup, writer.name, true))
			operation := rootfshandoff.PlannedRetireOperationID(writer.stage.Parent, writer.stage.Identity.WriterGrantID, writer.stage.Identity.WriterEpoch)
			if err := runtime.PlanRetire(cleanup, writer.stage, operation); err != nil {
				t.Errorf("retain writer %s retirement: %v", writer.name, err)
				continue
			}
			_, err := runtime.Retire(cleanup, writer.stage, operation)
			require.NoError(t, err)
		}
		if err := unix.Unmount(filepath.Join(root, "runsc/null-netns"), 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("retain null network namespace: %v", err)
		}
	}()
	type completed struct {
		index int
		err   error
	}
	finished := make(chan completed, 2)
	for index, name := range []string{"a", "b"} {
		stage := autogrowRemoteNamedStage(t, store, artifact, name)
		_, err = client.ConsumeWriterGrant(ctx, stage)
		require.NoError(t, err)
		stage = stage.WithoutWriterGrantToken()
		mount, err := m.Ensure(ctx, stage)
		require.NoError(t, err)
		w := &writer{name: name, root: filepath.Join(root, name), mount: mount.Source, stage: stage}
		writers = append(writers, w)
		spec := specs.Spec{Version: specs.Version, Root: &specs.Root{Path: mount.Source}, Process: &specs.Process{Cwd: "/", Args: []string{"/writer", "stress"}, Env: []string{"PATH=/"}},
			Mounts: []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}, {Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
				{Destination: "/evidence", Type: "bind", Source: filepath.Join(w.root, "evidence"), Options: []string{"rbind", "rw"}}},
			Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}}}}
		payload, err := json.Marshal(spec)
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(w.root, "bundle/config.json"), payload, 0600))
		require.NoError(t, runner.Create(ctx, filepath.Join(w.root, "bundle"), name))
		require.NoError(t, runner.Start(ctx, name))
		go func(index int, name string) {
			result, err := runner.Wait(ctx, name)
			if err == nil && result.ExitStatus != 0 {
				err = fmt.Errorf("guest %s exit=%d", name, result.ExitStatus)
			}
			finished <- completed{index, err}
		}(index, name)
	}
	d := &nodeRuntime{runtime: runtime, logger: newLogger(zap.NewNop())}
	control, stopControl := context.WithCancel(ctx)
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-control.Done():
				return
			case <-tick.C:
				d.scanLiveCommits(control)
				d.scanRootFSGrowth(control)
			}
		}
	}()
	defer func() { outage.down.Store(false); stopControl(); d.wg.Wait() }()
	peakWAL, peakArtifacts := int64(0), int64(0)
	samples, remaining := 0, 2
	outageStarted, outageRestored := false, false
	var pressuredAt, lastSample, lastRenewal time.Time
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
	for remaining > 0 {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case completion := <-finished:
			detail, _ := os.ReadFile(filepath.Join(writers[completion.index].root, "evidence/error.txt"))
			require.NoError(t, completion.err, "%s", detail)
			writers[completion.index].done = true
			remaining--
		case <-tick.C:
			if time.Since(lastRenewal) >= 30*time.Second {
				for _, writer := range writers {
					_, err := client.RenewWriterGrant(ctx, writer.stage)
					require.NoError(t, err)
				}
				lastRenewal = time.Now()
			}
			peakWAL = max(peakWAL, m.NodeDirtyTailUsage().UsedBytes)
			if time.Since(lastSample) >= time.Second {
				peakArtifacts = max(peakArtifacts, autogrowNodeArtifactBytes(t, root))
				samples++
				lastSample = time.Now()
			}
			ready := true
			for _, writer := range writers {
				payload, err := os.ReadFile(filepath.Join(writer.root, "evidence/state.json"))
				if os.IsNotExist(err) {
					ready = false
					continue
				}
				require.NoError(t, err)
				require.NoError(t, json.Unmarshal(payload, &writer.last))
				if writer.first.Token == "" {
					writer.first = writer.last
				}
				require.Equal(t, writer.first.PID, writer.last.PID)
				require.Equal(t, writer.first.Token, writer.last.Token)
				require.Equal(t, writer.first.Inode, writer.last.Inode)
				require.True(t, writer.last.MarkerOK)
				require.Equal(t, int64(writer.last.Chunks)<<20, writer.last.Offset)
				ready = ready && writer.last.Chunks >= 80 && !writer.done
			}
			if !outageStarted && ready {
				outageStarted = true
				outage.down.Store(true)
			}
			if outageStarted && !outageRestored {
				pressure, err := m.DirtyTailPressureSessions()
				require.NoError(t, err)
				if pressuredAt.IsZero() && len(pressure) == 2 && outage.rejected.Load() > 0 {
					pressuredAt = time.Now()
					require.LessOrEqual(t, m.NodeDirtyTailUsage().UsedBytes, int64(32<<20), "normal contention cannot consume protected retirement space")
				}
				if !pressuredAt.IsZero() && time.Since(pressuredAt) >= time.Minute {
					for _, writer := range writers {
						grant, err := store.GetRootFSWriterGrant(ctx, writer.stage.Identity.WriterGrantID)
						require.NoError(t, err)
						require.Equal(t, sandboxstore.RootFSWriterGrantStateConsumed, grant.State)
					}
					outage.down.Store(false)
					outageRestored = true
					t.Logf("both_writers_pressured=true outage_pressure_hold=1m rejected_requests=%d", outage.rejected.Load())
				}
			}
		}
	}
	require.True(t, outageRestored, "both live writers must block and recover through the shared budget")
	require.True(t, loss.publish.Load())
	require.True(t, loss.install.Load())
	for _, writer := range writers {
		file, err := os.Open(filepath.Join(writer.mount, "growing.bin"))
		require.NoError(t, err)
		var chunk [1 << 20]byte
		for index := range 600 {
			_, err = file.ReadAt(chunk[:], int64(index)<<20)
			require.NoError(t, err)
			require.True(t, bytes.Equal(chunk[:], autogrowChunkPattern(index, true, true)), "writer=%s chunk=%d", writer.name, index)
		}
		require.NoError(t, file.Close())
		turnover, err := os.ReadFile(filepath.Join(writer.mount, "turnover.bin"))
		require.NoError(t, err)
		require.Equal(t, autogrowChunkPattern(77, true, false)[:65536], turnover)
		xfsRoot := filepath.Dir(writer.mount) + "/xfs"
		mounts, err := mountinfo.GetMounts(nil)
		require.NoError(t, err)
		device := ""
		for _, mount := range mounts {
			if mount.Mountpoint == xfsRoot && mount.FSType == "xfs" {
				device = mount.Source
			}
		}
		require.Contains(t, devices, device, "exact fixture-owned XFS device")
		usage, err := host.InspectXFS(device, xfsRoot)
		require.NoError(t, err)
		require.GreaterOrEqual(t, usage.SizeBytes, int64(1200<<20))
		observation, err := client.ObserveLiveCommit(ctx, writer.stage)
		require.NoError(t, err)
		require.Positive(t, observation.Progress.InstalledSequence)
		t.Logf("writer=%s bytes_verified=%d pid=%d inode=%d installed_sequence=%d xfs_bytes=%d", writer.name, 600<<20, writer.first.PID, writer.first.Inode, observation.Progress.InstalledSequence, usage.SizeBytes)
	}
	peakArtifacts = max(peakArtifacts, autogrowNodeArtifactBytes(t, root))
	require.LessOrEqual(t, peakWAL, int64(40<<20))
	require.LessOrEqual(t, peakArtifacts, int64(128<<20))
	t.Logf("two_real_runsc_writers=true aggregate_data_verified=%d node_wal_budget_bytes=%d peak_node_wal_bytes=%d artifact_samples=%d peak_artifact_allocated_bytes=%d", 1200<<20, 40<<20, peakWAL, samples, peakArtifacts)
}
