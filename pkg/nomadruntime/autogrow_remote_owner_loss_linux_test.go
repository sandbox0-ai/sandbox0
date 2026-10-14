//go:build linux

package nomadruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/sys/mountinfo"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

// A separate runner SIGKILLs the source test process after this durable receipt.
// PostgreSQL and the object service survive. This is process-owner loss on the
// isolated host, not replacement-machine or process-memory recovery acceptance.
type autogrowOwnerLossReceipt struct {
	PID                 int                        `json:"pid"`
	Root                string                     `json:"root"`
	Bucket              string                     `json:"bucket"`
	Namespace           string                     `json:"namespace"`
	MachineBoot         string                     `json:"machine_boot"`
	Stage               rootfshandoff.StageRequest `json:"stage"`
	CommittedGeneration string                     `json:"committed_generation"`
	CommittedSequence   uint64                     `json:"committed_sequence"`
}

func autogrowWaitForOwnerLoss(t *testing.T, ctx context.Context, root, bucket, mount string,
	m *rootfssession.Manager, client *managerauthority.ManagerClient, stage rootfshandoff.StageRequest,
) {
	t.Helper()
	path := os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_RECEIPT")
	require.NotEmpty(t, path)
	require.NotEqual(t, "1", os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK"))
	fd, err := unix.Open(mount, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	require.NoError(t, err)
	require.NoError(t, unix.Syncfs(fd))
	require.NoError(t, unix.Close(fd))
	require.NoError(t, m.CommitLive(ctx, stage))
	require.NoError(t, m.SettleLiveCommit(ctx, stage))
	cut, err := client.ObserveLiveCommit(ctx, stage)
	require.NoError(t, err)
	require.Positive(t, cut.Progress.CommittedSequence)
	require.Equal(t, cut.Progress.CommittedSequence, cut.Progress.InstalledSequence)
	// No controller is running. This fsynced file and its directory metadata
	// remain only in the local suffix; recovery must not silently publish them.
	sentinel, err := os.OpenFile(filepath.Join(mount, "unpublished-after-cut.bin"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = sentinel.Write([]byte("local fsync does not imply remote custody\n"))
	require.NoError(t, err)
	require.NoError(t, sentinel.Sync())
	require.NoError(t, sentinel.Close())
	dir, err := os.Open(mount)
	require.NoError(t, err)
	require.NoError(t, dir.Sync())
	require.NoError(t, dir.Close())
	after, err := client.ObserveLiveCommit(ctx, stage)
	require.NoError(t, err)
	require.Equal(t, cut.Progress, after.Progress)
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	require.NoError(t, err)
	receipt := autogrowOwnerLossReceipt{PID: os.Getpid(), Root: root, Bucket: bucket, Namespace: namespace,
		MachineBoot: strings.TrimSpace(string(boot)),
		Stage:       stage.WithoutWriterGrantToken(), CommittedGeneration: cut.Progress.CommittedGenerationID, CommittedSequence: cut.Progress.CommittedSequence}
	payload, err := json.Marshal(receipt)
	require.NoError(t, err)
	out, err := os.OpenFile(path+".new", os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	_, err = out.Write(payload)
	require.NoError(t, err)
	require.NoError(t, out.Sync())
	require.NoError(t, out.Close())
	require.NoError(t, os.Rename(path+".new", path))
	t.Logf("owner_loss_ready=true owner_pid=%d committed_sequence=%d unpublished_local_suffix=true", receipt.PID, receipt.CommittedSequence)
	<-ctx.Done()
	t.Fatal("external owner-loss runner did not kill the source")
}

func TestPrivilegedRemoteAutogrowRecoversAfterOwnerLoss(t *testing.T) {
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_RECOVER") != "1" {
		t.Skip("requires separately SIGKILLed isolated source fixture")
	}
	require.Zero(t, os.Geteuid())
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Minute)
	defer cancel()
	data, err := os.ReadFile(os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_RECEIPT"))
	require.NoError(t, err)
	var receipt autogrowOwnerLossReceipt
	require.NoError(t, json.Unmarshal(data, &receipt))
	require.Greater(t, receipt.PID, 1)
	require.True(t, strings.HasPrefix(receipt.Root, "/tmp/sandbox0-remote-autogrow-"))
	require.Equal(t, filepath.Clean(receipt.Root), receipt.Root)
	t.Cleanup(func() {
		if t.Failed() {
			return
		}
		mounts, err := mountinfo.GetMounts(nil)
		require.NoError(t, err)
		for _, mount := range mounts {
			require.False(t, mount.Mountpoint == receipt.Root || strings.HasPrefix(mount.Mountpoint, receipt.Root+"/"), "retain fixture with mount %s", mount.Mountpoint)
		}
		require.NoError(t, os.RemoveAll(receipt.Root))
	})
	err = unix.Kill(receipt.PID, 0)
	require.ErrorIs(t, err, unix.ESRCH, "source must be absent before any lease or physical fence")
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	require.NoError(t, err)
	require.Equal(t, receipt.Namespace, namespace)
	// Reuse the existing database: the source fixture's destructive migration
	// helper would erase precisely the live progress this test must recover.
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: os.Getenv("INTEGRATION_DATABASE_URL"), Schema: "scheduler", MaxConns: 10})
	require.NoError(t, err)
	defer pool.Close()
	var database string
	require.NoError(t, pool.QueryRow(ctx, `SELECT current_database()`).Scan(&database))
	require.True(t, strings.HasPrefix(database, "s0_rootfs_autogrow_owner_loss_"))
	store := sandboxstore.NewPGSandboxStore(pool)
	objects, err := objectstore.Create(objectstore.Config{Type: objectstore.TypeS3, Bucket: receipt.Bucket, Region: "us-east-1",
		Endpoint: os.Getenv("SANDBOX0_RUSTFS_ENDPOINT"), AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY")})
	require.NoError(t, err)
	conditional, ok := objects.(objectstore.ContextConditionalStore)
	require.True(t, ok)
	t.Cleanup(func() {
		if t.Failed() || os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_EXPORT") == "1" {
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
	authorityRoot := filepath.Join(receipt.Root, "recovery-authority")
	require.NoError(t, os.MkdirAll(authorityRoot, 0700))
	client, _ := autogrowRemoteAuthority(t, authorityRoot, store)
	runscRoot := filepath.Join(receipt.Root, "runsc")
	runner := gvisorcli.New(gvisorcli.Config{Path: os.Getenv("SANDBOX0_TEST_RUNSC"), Root: runscRoot, Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true, RootFSMountRoot: filepath.Join(receipt.Root, "mounts")})
	require.NoError(t, runner.Delete(ctx, "writer", true))
	if err := unix.Unmount(filepath.Join(runscRoot, "null-netns"), 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
		require.NoError(t, err)
	}
	host, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{DevicePaths: []string{os.Getenv("SANDBOX0_PRIVILEGED_NBD_DEVICE")}})
	require.NoError(t, err)
	publisher := rootfsblock.ObjectStorePublisher{Store: conditional}
	m, err := rootfssession.New(rootfssession.Config{StatePath: filepath.Join(receipt.Root, "sessions.db"), BranchRoot: filepath.Join(receipt.Root, "branches"), MountRoot: filepath.Join(receipt.Root, "mounts"),
		MaxDirtyTailBytes: 32 << 20, MaxNodeDirtyTailBytes: 64 << 20, DirtyTailRetirementReserveBytes: 8 << 20,
		ReadCacheBytes: 64 << 20, ReadDiskCache: rootfsblock.DiskCacheConfig{Directory: filepath.Join(receipt.Root, "read-cache"), MaxBytes: 32 << 20},
		Source: conditional, Publisher: publisher, Runtime: host, LiveCommitAuthority: liveCommitAuthorityAdapter{client: client},
		PublisherForOperation: func(s rootfshandoff.StageRequest, op string) rootfsblock.ImmutableObjectPublisher {
			return nodeJournalPublisher{authority: client, stage: s, operation: op, publisher: publisher}
		}})
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()
	// Match production runtime startup: recover freeze intent and restore
	// durable device reservations before attempting any terminal transition.
	require.NoError(t, m.ReconcileFreezes(ctx))
	require.NoError(t, m.ReconcileRunningForkCaptures(ctx))
	require.NoError(t, m.ReconcileRebases(ctx))
	require.NoError(t, m.ReconcileReleases(ctx))
	// Advance only this dedicated fixture's lease after proof of process death.
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET lease_expires_at=NOW()-INTERVAL '1 minute' WHERE grant_id=$1`, receipt.Stage.Identity.WriterGrantID)
	require.NoError(t, err)
	runtime := &rootfsRuntime{sessions: m, authority: client, liveCommits: true, autoGrow: true, logger: newLogger(zap.NewNop())}
	proof, err := runtime.CrashFence(ctx, receipt.Stage, "autogrow-owner-loss-crash", CrashTaskObservation{ActiveKey: receipt.Stage.Identity.AllocationID + "/app", HostMountNamespaceID: namespace,
		ContainerAbsent: true, TaskAbsent: true, FrontendSnapshotAbsent: true, StableMountAbsent: true})
	require.NoError(t, err)
	require.NoError(t, proof.Validate())
	filesystem, err := store.GetRootFSFilesystem(ctx, receipt.Stage.Identity.RootFSID)
	require.NoError(t, err)
	require.Equal(t, receipt.CommittedGeneration, filesystem.HeadGenerationID)
	grant, err := store.GetRootFSWriterGrant(ctx, receipt.Stage.Identity.WriterGrantID)
	require.NoError(t, err)
	require.Equal(t, sandboxstore.RootFSWriterGrantStateRetired, grant.State)
	_, err = client.RenewWriterGrant(ctx, receipt.Stage)
	require.Error(t, err, "retired owner cannot renew")
	require.NoError(t, m.Close())
	for range 5 {
		_, err := store.InventoryRootFSGeneration(ctx, conditional, 100)
		require.NoError(t, err)
	}
	_, err = store.DeleteUnreferencedRootFSGenerations(ctx, "", 100)
	require.NoError(t, err)
	if os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_EXPORT") == "1" {
		t.Logf("source_fenced_for_replacement=true remote_cut_sequence=%d local_state_cleanup=true objects_retained=true", receipt.CommittedSequence)
		return
	}
	autogrowVerifyColdRestore(t, ctx, receipt.Root, conditional, store, client, receipt.Stage)
	t.Logf("source_sigkill=true actual_nbd_crash_fence=true generation_inventory_and_gc=true remote_cut_sequence=%d remote_bytes_verified=%d unpublished_local_suffix_absent=true", receipt.CommittedSequence, 600<<20)
}
