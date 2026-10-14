//go:build linux

package nomadruntime

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
)

// Registry records use real admission/claim transitions. This component gate
// still does not run a Nomad server or assert a real procd readiness handshake.
func autogrowPrepareRegionalSlot(t *testing.T, store *sandboxstore.PGSandboxStore, stage rootfshandoff.StageRequest) {
	t.Helper()
	ctx := t.Context()
	_, err := store.RegisterRuntimeNodeCapacity(ctx, &sandboxstore.RegisterRuntimeNodeCapacityRequest{
		ClusterID: "autogrow-cluster", NodeID: "autogrow-node", NodeUID: stage.Identity.NodeUID, NodeBootID: stage.Identity.BootID,
		CPUMillicores: 4000, MemoryBytes: 8 << 30, CPUSetCPUs: "0-3", CPUSetMems: "0", TTL: 5 * time.Minute})
	require.NoError(t, err)
	compatibility := digest.FromString("autogrow-fixture-compatibility").String()
	_, err = store.RegisterRuntimeSlot(ctx, &sandboxstore.RegisterRuntimeSlotRequest{
		SlotID: stage.Identity.SlotNonce, ClusterID: "autogrow-cluster", AllocationID: stage.Identity.AllocationID,
		AllocationNamespace: "sandbox0", NodeID: "autogrow-node", NodeUID: stage.Identity.NodeUID, NodeBootID: stage.Identity.BootID,
		NetNSIdentity: stage.ExpectedPolicyToken.NetNSIdentity, ControlEndpoint: "unix:///run/sandbox0/autogrow-fixture.sock",
		CompatibilityDigest: compatibility, HeartbeatTTL: 5 * time.Minute})
	require.NoError(t, err)
	proof := bytes.Repeat([]byte{0x91}, 32)
	_, err = store.ReportRuntimeSlotReady(ctx, &sandboxstore.ReportRuntimeSlotReadyRequest{
		SlotID: stage.Identity.SlotNonce, AllocationID: stage.Identity.AllocationID, NodeUID: stage.Identity.NodeUID, NodeBootID: stage.Identity.BootID,
		RuntimeReadyDigest: proof, NetworkReadyDigest: proof, StorageReadyDigest: proof, HeartbeatTTL: 5 * time.Minute})
	require.NoError(t, err)
	_, err = store.AcquireRuntimeSlot(ctx, &sandboxstore.AcquireRuntimeSlotRequest{
		OperationID: "autogrow-claim-operation", ClaimID: stage.Identity.ClaimID, SandboxID: "autogrow-sandbox", ClusterID: "autogrow-cluster",
		FilesystemID: stage.Identity.RootFSID, SourceGenerationID: stage.InitialGeneration, CompatibilityDigest: compatibility,
		RuntimeAssignmentRevision: digest.FromString("autogrow-assignment").Encoded(), NetworkPolicyDigest: stage.ExpectedPolicyToken.PolicyDigest,
		ClaimTTL: time.Minute, Resources: runtimeslot.RuntimeResourceRequest{Version: runtimeslot.RuntimeResourceRequestVersion,
			CPUMillicores: 1000, MemoryBytes: 1 << 30, PIDsLimit: runtimeslot.DefaultRuntimePIDsLimit}})
	require.NoError(t, err)
}

func autogrowActivateRegionalSlot(t *testing.T, store *sandboxstore.PGSandboxStore, stage rootfshandoff.StageRequest) {
	t.Helper()
	ctx := t.Context()
	_, err := store.BindRuntimeSlotWriterGrant(ctx, &sandboxstore.BindRuntimeSlotWriterGrantRequest{
		SlotID: stage.Identity.SlotNonce, OperationID: "autogrow-claim-operation", ClaimID: stage.Identity.ClaimID, GrantID: stage.Identity.WriterGrantID})
	require.NoError(t, err)
	slot, err := store.GetRuntimeSlot(ctx, stage.Identity.SlotNonce)
	require.NoError(t, err)
	binding, err := stage.BindingDigest()
	require.NoError(t, err)
	_, err = store.StartRuntimeSlot(ctx, &sandboxstore.StartRuntimeSlotRequest{
		SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		OperationID: "autogrow-claim-operation", ClaimID: slot.ClaimID, LaunchAttempt: stage.Identity.LaunchAttempt,
		RunscContainerID: "writer", RootFSBindingDigest: binding[:], ClaimNetworkDigest: bytes.Repeat([]byte{0x92}, 32),
		ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest})
	require.NoError(t, err)
	_, err = store.MarkRuntimeSlotCommandReady(ctx, &sandboxstore.MarkRuntimeSlotCommandReadyRequest{
		SlotID: slot.ID, AllocationID: slot.AllocationID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		OperationID: "autogrow-claim-operation", ClaimID: slot.ClaimID, ProcdInstanceID: "autogrow-fixture-procd",
		ProcdAddress: "http://192.0.2.2:49983", CommandReadyDigest: bytes.Repeat([]byte{0x93}, 32)})
	require.NoError(t, err)
	_, err = store.CompleteSandboxClaim(ctx, &sandboxstore.CompleteSandboxClaimRequest{
		SandboxID: "autogrow-sandbox", OperationID: "autogrow-claim-operation", SlotID: slot.ID,
		AllocationID: slot.AllocationID, AllocationNamespace: slot.AllocationNamespace,
		ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest})
	require.NoError(t, err)
}

func autogrowRegionalFork(t *testing.T, ctx context.Context, store *sandboxstore.PGSandboxStore, runtime *rootfsRuntime, stage rootfshandoff.StageRequest) {
	t.Helper()
	source, err := store.GetSandbox(ctx, "autogrow-sandbox")
	require.NoError(t, err)
	target := *source
	target.ID, target.DesiredState = "autogrow-regional-fork-target", sandboxstore.SandboxDesiredStatePaused
	target.RuntimeID, target.RuntimeNamespace, target.RuntimeGeneration, target.LifecycleEpoch = "", "", 0, 0
	target.HotClaimCompletedAt = time.Time{}
	target.CreatedAt, target.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	request := &sandboxstore.NomadSandboxForkRequest{OperationID: "autogrow-regional-fork", SourceSandboxID: source.ID, ExpectedTeamID: source.TeamID, Target: &target}
	candidate, err := store.RequestNomadSandboxRunningFork(ctx, request)
	require.NoError(t, err)
	fork := rootfshandoff.RunningForkCheckpointRequest{OperationID: request.OperationID, SourceSandboxID: source.ID, TargetSandboxID: target.ID, TargetGenerationID: candidate.TargetGenerationID}
	checkpoint, err := runtime.CaptureRunningFork(ctx, stage, fork)
	require.NoError(t, err)
	require.NoError(t, checkpoint.Validate())
	completed, err := store.RequestNomadSandboxRunningFork(ctx, request)
	require.NoError(t, err)
	require.True(t, completed.Completed)
	filesystem, err := store.GetRootFSFilesystem(ctx, target.ID)
	require.NoError(t, err)
	require.Equal(t, candidate.TargetGenerationID, filesystem.HeadGenerationID)
	t.Logf("real_regional_running_fork=true target_generation=%s source_writer_epoch=%d", filesystem.HeadGenerationID, candidate.SourceWriterEpoch)
}

// Verify the committed fork only after the source has finished writing. This
// also checks that later source commits did not change the fork's file prefix.
func autogrowVerifyRegionalFork(t *testing.T, ctx context.Context, root string, store *sandboxstore.PGSandboxStore, objects objectstore.ContextConditionalStore) {
	t.Helper()
	devicePath := os.Getenv("SANDBOX0_PRIVILEGED_NBD_RESTORE_DEVICE")
	require.NotEmpty(t, devicePath)
	require.NotEqual(t, os.Getenv("SANDBOX0_PRIVILEGED_NBD_DEVICE"), devicePath)
	deviceName := filepath.Base(devicePath)
	size, err := os.ReadFile(filepath.Join("/sys/block", deviceName, "size"))
	require.NoError(t, err)
	require.Equal(t, "0\n", string(size), "fork verification needs a separately reserved unused device")
	filesystem, err := store.GetRootFSFilesystem(ctx, "autogrow-regional-fork-target")
	require.NoError(t, err)
	generation, err := store.GetRootFSGeneration(ctx, filesystem.HeadGenerationID)
	require.NoError(t, err)
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	require.NoError(t, err)
	forkRoot := filepath.Join(root, "fork-reader")
	require.NoError(t, os.Mkdir(forkRoot, 0700))
	cache, err := rootfsblock.NewReadCache(32 << 20)
	require.NoError(t, err)
	defer cache.Close()
	reader, err := rootfsblock.NewReaderWithCache(objects, descriptor, cache)
	require.NoError(t, err)
	branch, err := rootfsblock.OpenBranchWithOptions(filepath.Join(forkRoot, "replay.wal"), rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: filesystem.ID, GenerationID: generation.ID,
		WriterEpoch: generation.WriterEpoch, LogicalSizeBytes: descriptor.LogicalSizeBytes, BaseRootDigest: descriptor.MappingRoot.RootDigest}, reader,
		rootfsblock.BranchOptions{MaxDirtyTailBytes: 32 << 20})
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()
	device, err := rootfsblock.StartKernelNBD(ctx, ctx, branch, rootfsblock.KernelNBDOptions{DevicePath: devicePath, RequestTimeout: 30 * time.Second})
	require.NoError(t, err)
	defer func() { require.NoError(t, device.Close()) }()
	host, err := rootfssession.NewLinuxRuntime(rootfssession.LinuxRuntimeConfig{DevicePaths: []string{devicePath}})
	require.NoError(t, err)
	xfsRoot, mergedRoot := filepath.Join(forkRoot, "xfs"), filepath.Join(forkRoot, "merged")
	require.NoError(t, host.MountXFS(devicePath, xfsRoot))
	defer func() { require.NoError(t, host.UnmountXFS(xfsRoot, true)) }()
	require.NoError(t, host.MountOverlay(xfsRoot, mergedRoot))
	defer func() { require.NoError(t, host.UnmountOverlay(mergedRoot, true)) }()
	file, err := os.Open(filepath.Join(mergedRoot, "growing.bin"))
	require.NoError(t, err)
	defer file.Close()
	stat, err := file.Stat()
	require.NoError(t, err)
	require.GreaterOrEqual(t, stat.Size(), int64(100<<20))
	require.Less(t, stat.Size(), int64(600<<20), "fork must retain its earlier boundary after the source completes")
	buffer := make([]byte, 1<<20)
	stress := os.Getenv("SANDBOX0_ROOTFS_AUTOGROW_UNIQUE") == "1"
	for offset := int64(0); offset < stat.Size(); {
		length := min(int64(len(buffer)), stat.Size()-offset)
		_, err := io.ReadFull(file, buffer[:length])
		require.NoError(t, err)
		expected := autogrowChunkPattern(int(offset>>20), stress, false)
		require.True(t, bytes.Equal(expected[:length], buffer[:length]), "fork chunk differs at offset=%d", offset)
		offset += length
	}
	t.Logf("real_remote_fork_prefix_bytes_verified=%d source_final_bytes=%d fresh_cache_device=true", stat.Size(), 600<<20)
}
