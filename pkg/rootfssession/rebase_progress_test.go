package session

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

type rebaseSyncTestHost struct{ *fakeHostRuntime }

func (*rebaseSyncTestHost) SyncXFS(string, string) error { return nil }

type rebaseGrowthTestHost struct {
	*fakeHostRuntime
	usage map[string]XFSUsage
	grown []string
	err   error
}

func (h *rebaseGrowthTestHost) InspectXFS(device, mount string) (XFSUsage, error) {
	u, ok := h.usage[device+":"+mount]
	if !ok {
		return XFSUsage{}, fmt.Errorf("wrong exact mount/device")
	}
	return u, nil
}
func (h *rebaseGrowthTestHost) GrowXFS(_ context.Context, device, mount string, target int64) (XFSUsage, error) {
	h.grown = append(h.grown, device+":"+mount)
	if h.err != nil {
		return XFSUsage{}, h.err
	}
	u, err := h.InspectXFS(device, mount)
	if err != nil {
		return u, err
	}
	u.FreeBytes += target - u.SizeBytes
	u.SizeBytes = target
	h.usage[device+":"+mount] = u
	return u, nil
}

func TestRebaseTargetGrowsBeforeCopyWithoutGrowingSourceOrShrinkingTarget(t *testing.T) {
	objects := newSessionObjectStore()
	host := &rebaseGrowthTestHost{fakeHostRuntime: newFakeHostRuntime(objects), usage: map[string]XFSUsage{
		"source:/source": {SizeBytes: 1200 << 20, FreeBytes: 600 << 20, BlockBytes: 4096},
		"target:/target": {SizeBytes: 300 << 20, FreeBytes: 250 << 20, BlockBytes: 4096},
	}}
	manager := &Manager{runtime: host}
	source, target := &rebaseResource{DevicePath: "source", XFSRoot: "/source"}, &rebaseResource{DevicePath: "target", XFSRoot: "/target"}
	require.NoError(t, manager.prepareRebaseTargetCapacity(t.Context(), source, target, 64<<40))
	require.Equal(t, []string{"target:/target"}, host.grown)
	require.Equal(t, int64(1200<<20), host.usage["target:/target"].SizeBytes)
	require.NoError(t, manager.prepareRebaseTargetCapacity(t.Context(), source, target, 64<<40))
	require.Len(t, host.grown, 1)
	u := host.usage["target:/target"]
	u.SizeBytes = 2400 << 20
	host.usage["target:/target"] = u
	require.NoError(t, manager.prepareRebaseTargetCapacity(t.Context(), source, target, 64<<40))
	require.Len(t, host.grown, 1)
	require.Error(t, manager.prepareRebaseTargetCapacity(t.Context(), source, target, 1<<30))
	require.Len(t, host.grown, 1, "a logical ceiling conflict must not mutate either mount")
}

func TestPrivateRebaseCutsKeepWALBoundedAndChainRemoteBases(t *testing.T) {
	objects, request := testRebaseRequest(t, "private-cuts")
	host := newFakeHostRuntime(objects)
	manager := newRebaseTestManager(t, t.TempDir(), objects, host, &fakeRebaseEngine{})
	defer manager.Close()
	manager.runtime = &rebaseSyncTestHost{host}
	descriptor, err := rootfsblock.DecodeDescriptor(request.TargetBaseDescriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(objects, descriptor, 0)
	require.NoError(t, err)
	digest, err := request.Digest()
	require.NoError(t, err)
	current := newRebaseRecord(manager.branchRoot, manager.mountRoot, request, digest)
	require.NoError(t, manager.saveNewRebase(current))
	target := current.resource("target")
	fsID, genID := rebaseBranchIdentity(request, "target")
	branch, err := rootfsblock.OpenBranchWithOptions(target.BranchPath, rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: fsID, GenerationID: genID, WriterEpoch: request.TargetWriterEpoch,
		LogicalSizeBytes: reader.Size(), BaseRootDigest: descriptor.MappingRoot.RootDigest,
	}, reader, manager.rebaseBranchOptions(request.OperationID))
	require.NoError(t, err)
	defer branch.Close()
	progress := manager.newRebaseProgress(&current, branch, descriptor)
	var written uint64
	for index := 0; index < 100; index++ {
		require.NoError(t, progress.advance(t.Context(), written))
		_, err = branch.WriteAt(bytes.Repeat([]byte{byte(index)}, 4096), 2*4096)
		require.NoError(t, err)
		written += 4096
		require.LessOrEqual(t, branch.DirtyTailUsage().DirtyBytes, manager.maxDirty/4+4096)
	}
	require.NoError(t, progress.advance(t.Context(), written))
	require.NotEmpty(t, current.WorkingTargetDescriptor)
	require.Greater(t, progress.objects, 0)
	stored, err := manager.loadRebase(request.OperationID)
	require.NoError(t, err)
	require.Equal(t, current.WorkingTargetDescriptor, stored.WorkingTargetDescriptor)
	require.Equal(t, target.BranchPath, stored.resource("target").BranchPath)
	tampered := stored
	tampered.WorkingTargetSequence++
	require.Error(t, manager.validateRebaseRecord(tampered, request.OperationID), "rotated WAL path must match its acknowledged sequence")
	tampered = stored
	tampered.Resources = append([]rebaseResource(nil), stored.Resources...)
	tampered.resource("target").ObsoleteBranchPath = filepath.Join(filepath.Dir(target.BranchPath), "..", "foreign.wal")
	require.Error(t, manager.validateRebaseRecord(tampered, request.OperationID), "cleanup cannot delete an unowned obsolete path")
	checkpoint, err := branch.Checkpoint()
	require.NoError(t, err)
	built, err := rootfsblock.BuildIncrementalGenerationFromBlockReader(t.Context(), objects, progress.base, checkpoint, objects, rootfsblock.BuildOptions{})
	require.NoError(t, err)
	require.NoError(t, checkpoint.Close())
	fresh, err := rootfsblock.NewReader(objects, built.Descriptor, 0)
	require.NoError(t, err)
	data := make([]byte, 4096)
	_, err = fresh.ReadAt(data, 2*4096)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{99}, 4096), data)
	original, err := rootfsblock.NewReader(objects, descriptor, 0)
	require.NoError(t, err)
	_, err = original.ReadAt(data, 2*4096)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 4096), data, "private cuts cannot change the requested target base")
	require.NoError(t, branch.Close())
	// Recovery discards the incomplete merge, including its rotated journals,
	// instead of exposing the latest private working descriptor as a head.
	require.NoError(t, manager.ReconcileRebases(t.Context()))
	_, err = os.Stat(filepath.Dir(target.BranchPath))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Zero(t, manager.NodeDirtyTailUsage().Owners)
}

type rejectedRebasePublisher struct{}

func (rejectedRebasePublisher) PutImmutable(context.Context, string, []byte) error {
	return fmt.Errorf("injected custody or PUT failure")
}

func TestPrivateRebaseUploadFailureRetainsOriginalWAL(t *testing.T) {
	objects, request := testRebaseRequest(t, "private-upload-failure")
	host := newFakeHostRuntime(objects)
	manager := newRebaseTestManager(t, t.TempDir(), objects, host, &fakeRebaseEngine{})
	defer manager.Close()
	manager.runtime = &rebaseSyncTestHost{host}
	manager.publisher = rejectedRebasePublisher{}
	descriptor, err := rootfsblock.DecodeDescriptor(request.TargetBaseDescriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(objects, descriptor, 0)
	require.NoError(t, err)
	digest, err := request.Digest()
	require.NoError(t, err)
	current := newRebaseRecord(manager.branchRoot, manager.mountRoot, request, digest)
	require.NoError(t, manager.saveNewRebase(current))
	target := current.resource("target")
	oldPath := target.BranchPath
	fsID, genID := rebaseBranchIdentity(request, "target")
	branch, err := rootfsblock.OpenBranchWithOptions(oldPath, rootfsblock.BranchIdentity{
		Version: rootfsblock.BranchFormatVersion, RootFSID: fsID, GenerationID: genID, WriterEpoch: request.TargetWriterEpoch,
		LogicalSizeBytes: reader.Size(), BaseRootDigest: descriptor.MappingRoot.RootDigest,
	}, reader, manager.rebaseBranchOptions(request.OperationID))
	require.NoError(t, err)
	defer branch.Close()
	for range 4 {
		_, err = branch.WriteAt(bytes.Repeat([]byte{0x77}, 4096), 0)
		require.NoError(t, err)
	}
	progress := manager.newRebaseProgress(&current, branch, descriptor)
	require.ErrorContains(t, progress.advance(t.Context(), 4*4096), "injected custody or PUT failure")
	require.Equal(t, oldPath, target.BranchPath)
	require.Empty(t, current.WorkingTargetDescriptor)
	require.Equal(t, int64(4*4096), branch.DirtyTailUsage().DirtyBytes)
	entries, err := os.ReadDir(filepath.Dir(oldPath))
	require.NoError(t, err)
	require.Len(t, entries, 1)
	data := make([]byte, 4096)
	_, err = branch.ReadAt(data, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x77}, 4096), data)
}
