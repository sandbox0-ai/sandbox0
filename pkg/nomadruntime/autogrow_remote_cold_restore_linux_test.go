//go:build linux

package nomadruntime

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/opencontainers/runtime-spec/specs-go"
	managerauthority "github.com/sandbox0-ai/sandbox0/manager/pkg/rootfswriterauthority"
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

// This is a fresh session/cache/device gate on the isolated host, not a full
// Nomad resume or a replacement-machine/memory-checkpoint acceptance claim.
func autogrowVerifyColdRestore(t *testing.T, ctx context.Context, originalRoot string, objects objectstore.ContextConditionalStore,
	store *sandboxstore.PGSandboxStore, client *managerauthority.ManagerClient, original rootfshandoff.StageRequest,
) {
	t.Helper()
	device := os.Getenv("SANDBOX0_PRIVILEGED_NBD_RESTORE_DEVICE")
	require.NotEmpty(t, device)
	require.NotEqual(t, os.Getenv("SANDBOX0_PRIVILEGED_NBD_DEVICE"), device, "cold restore needs a separately reserved device")
	filesystem, err := store.GetRootFSFilesystem(ctx, original.Identity.RootFSID)
	require.NoError(t, err)
	require.NotEqual(t, original.InitialGeneration, filesystem.HeadGenerationID)
	head, err := store.GetRootFSGeneration(ctx, filesystem.HeadGenerationID)
	require.NoError(t, err)
	record, err := store.GetSandbox(ctx, "autogrow-sandbox")
	require.NoError(t, err)
	record.DesiredState, record.RuntimeGeneration = sandboxstore.SandboxDesiredStateActive, 2
	record.RuntimeNamespace, record.RuntimeID = "sandbox0", "autogrow-cold-pod"
	require.NoError(t, store.UpsertSandbox(ctx, record))
	stage := original
	stage.Parent, stage.InitialGeneration = digest.FromString("autogrow-cold-parent").String(), head.ID
	stage.Identity.BootID, stage.Identity.RuntimeGeneration = "autogrow-cold-boot", "2"
	stage.Identity.AllocationID, stage.Identity.WriterGrantID = "autogrow-cold-allocation", "autogrow-cold-grant"
	stage.Identity.ClaimID, stage.Identity.SlotNonce, stage.Identity.LaunchAttempt = "autogrow-cold-claim", "autogrow-cold-slot", "autogrow-cold-attempt"
	stage.Identity.WriterEpoch = filesystem.WriterEpoch + 1
	stage.Identity.WriterGrantToken = digest.FromString("autogrow-cold-token").Encoded()
	stage.Identity.WriterGrantTokenDigest = rootfshandoff.WriterGrantTokenDigest(stage.Identity.WriterGrantToken)
	stage.ExpectedPolicyToken.AllocationID, stage.ExpectedPolicyToken.ClaimID = stage.Identity.AllocationID, stage.Identity.ClaimID
	stage.ExpectedPolicyToken.NetworkEpoch, stage.ExpectedPolicyToken.NetNSIdentity = 2, "autogrow-cold-netns"
	stage.Generation = &rootfshandoff.GenerationDescriptor{Version: rootfshandoff.GenerationDescriptorVersion, GenerationID: head.ID, FilesystemID: head.FilesystemID,
		SourceOCIDigest: head.SourceOCIDigest, BaseArtifactDigest: head.BaseArtifactDigest, BaseBlockRoot: head.BaseBlockRoot,
		CurrentBlockHead: head.CurrentBlockHead, WriterEpoch: head.WriterEpoch, FormatGeneration: head.FormatGeneration,
		DurabilityState: head.DurabilityState, LocatorVersion: head.LocatorVersion, Descriptor: head.Descriptor}
	require.NoError(t, stage.Validate())
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	issued, err := store.IssueRootFSWriterGrant(ctx, &sandboxstore.IssueRootFSWriterGrantRequest{GrantID: stage.Identity.WriterGrantID,
		SandboxID: record.ID, ExpectedFilesystemID: filesystem.ID, ExpectedWriterEpoch: filesystem.WriterEpoch, InitialGenerationID: head.ID,
		ClaimID: stage.Identity.ClaimID, SlotID: stage.Identity.SlotNonce, OperationID: "autogrow-cold-issue", RawToken: stage.Identity.WriterGrantToken,
		BindingVersion: stage.BindingVersion, BindingDigest: binding[:], NodeUID: stage.Identity.NodeUID, NodeBootID: stage.Identity.BootID,
		RuntimeNamespace: record.RuntimeNamespace, RuntimeID: record.RuntimeID, RuntimeIncarnationID: stage.Identity.AllocationID,
		NodeName: "autogrow-node", GateParent: stage.Parent, RuntimeGeneration: strconv.FormatInt(record.RuntimeGeneration, 10), ConsumeExpiresAt: time.Now().Add(time.Minute)})
	require.NoError(t, err)
	require.Equal(t, stage.Identity.WriterEpoch, issued.Grant.WriterEpoch)
	_, err = client.ConsumeWriterGrant(ctx, stage)
	require.NoError(t, err)
	stage = stage.WithoutWriterGrantToken()
	root := filepath.Join(originalRoot, "cold")
	require.NoError(t, os.Mkdir(root, 0700))
	host, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{DevicePaths: []string{device}})
	require.NoError(t, err)
	publisher := rootfsblock.ObjectStorePublisher{Store: objects}
	m, err := rootfssession.New(rootfssession.Config{StatePath: filepath.Join(root, "sessions.db"), BranchRoot: filepath.Join(root, "branches"), MountRoot: filepath.Join(root, "mounts"),
		MaxDirtyTailBytes: 32 << 20, MaxNodeDirtyTailBytes: 64 << 20, DirtyTailRetirementReserveBytes: 8 << 20,
		ReadCacheBytes: 64 << 20, ReadDiskCache: rootfsblock.DiskCacheConfig{Directory: filepath.Join(root, "read-cache"), MaxBytes: 32 << 20},
		Source: objects, Publisher: publisher, Runtime: host, LiveCommitAuthority: liveCommitAuthorityAdapter{client: client},
		PublisherForOperation: func(s rootfshandoff.StageRequest, op string) rootfsblock.ImmutableObjectPublisher {
			return nodeJournalPublisher{authority: client, stage: s, operation: op, publisher: publisher}
		}})
	require.NoError(t, err)
	defer func() { require.NoError(t, m.Close()) }()
	retired := false
	defer func() {
		if !retired {
			cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			operation := rootfshandoff.PlannedRetireOperationID(stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch)
			require.NoError(t, m.BeginRetire(stage.Parent, stage.Identity, operation))
			require.NoError(t, m.Release(cleanup, stage.Identity))
		}
	}()
	mount, err := m.Ensure(ctx, stage)
	require.NoError(t, err)
	usage, err := host.InspectXFS(device, filepath.Dir(mount.Source)+"/xfs")
	require.NoError(t, err)
	require.GreaterOrEqual(t, usage.SizeBytes, int64(1200<<20))
	require.NoError(t, os.Mkdir(filepath.Join(root, "bundle"), 0700))
	runscRoot := filepath.Join(root, "runsc")
	runner := gvisorcli.New(gvisorcli.Config{Path: os.Getenv("SANDBOX0_TEST_RUNSC"), Root: runscRoot, Platform: "systrap", Overlay2: "none", FileAccess: "shared", DirectFS: true, RootFSMountRoot: filepath.Join(root, "mounts")})
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		require.NoError(t, runner.Delete(cleanup, "reader", true))
		if err := unix.Unmount(filepath.Join(runscRoot, "null-netns"), 0); err != nil && !errors.Is(err, unix.EINVAL) && !errors.Is(err, unix.ENOENT) {
			t.Errorf("retain cold network namespace: %v", err)
		}
	}()
	spec := specs.Spec{Version: specs.Version, Root: &specs.Root{Path: mount.Source, Readonly: true},
		Process: &specs.Process{Cwd: "/", Args: []string{"/writer", "verify"}, Env: []string{"PATH=/"}},
		Mounts: []specs.Mount{{Destination: "/proc", Type: "proc", Source: "proc"}, {Destination: "/dev", Type: "tmpfs", Source: "tmpfs", Options: []string{"mode=755", "size=1m"}},
			{Destination: "/evidence", Type: "bind", Source: filepath.Join(originalRoot, "evidence"), Options: []string{"rbind", "rw"}}},
		Linux: &specs.Linux{Namespaces: []specs.LinuxNamespace{{Type: specs.PIDNamespace}, {Type: specs.MountNamespace}, {Type: specs.NetworkNamespace}, {Type: specs.IPCNamespace}, {Type: specs.UTSNamespace}}}}
	config, err := json.Marshal(spec)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, "bundle/config.json"), config, 0600))
	require.NoError(t, runner.Create(ctx, filepath.Join(root, "bundle"), "reader"))
	require.NoError(t, runner.Start(ctx, "reader"))
	result, err := runner.Wait(ctx, "reader")
	require.NoError(t, err)
	require.Zero(t, result.ExitStatus, "cold guest failed remote file verification")
	require.NoError(t, runner.Delete(ctx, "reader", true))
	runtime := &rootfsRuntime{sessions: m, authority: client, liveCommits: true, autoGrow: true, logger: newLogger(zap.NewNop())}
	operation := rootfshandoff.PlannedRetireOperationID(stage.Parent, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch)
	require.NoError(t, runtime.PlanRetire(ctx, stage, operation))
	_, err = runtime.Retire(ctx, stage, operation)
	require.NoError(t, err)
	retired = true
	t.Logf("cold_session_device_cache=true remote_bytes_verified=%d filesystem_bytes=%d writer_epoch=%d retired=true", 600<<20, usage.SizeBytes, stage.Identity.WriterEpoch)
}
