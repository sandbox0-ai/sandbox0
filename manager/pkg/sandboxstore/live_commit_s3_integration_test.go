package sandboxstore

import (
	"bytes"
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestRootFSLiveCommitInstalledBaseRetentionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	isolated, ok := objectstore.Prefix(objects, prefix).(objectstore.ContextConditionalStore)
	require.True(t, ok)
	f := newLiveCommitFixtureWithObjects(t, isolated)
	verifyLiveCommitProgressAndReceiptReplay(t, f, isolated)
}

func TestRootFSLiveCommitSnapshotForkRestoreGCS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	isolated, ok := objectstore.Prefix(objects, prefix).(objectstore.ContextConditionalStore)
	require.True(t, ok)
	f := newLiveCommitFixtureWithObjects(t, isolated)
	verifyLiveCommitTerminalPublicationRetainsInstalledBase(t, f, isolated)
	ctx := t.Context()
	record, err := f.store.GetSandbox(ctx, f.sandboxID)
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStatePaused, record.DesiredState)
	filesystem, err := f.store.GetRootFSFilesystem(ctx, f.filesystem.ID)
	require.NoError(t, err)
	snapshot, err := f.store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: f.sandboxID, SnapshotID: "snapshot-after-installed-live-base",
	})
	require.NoError(t, err)
	require.Equal(t, filesystem.HeadGenerationID, snapshot.HeadGenerationID)
	child := rootFSTestSandboxRecord("live-s3-fork", record.TeamID)
	child.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, f.store.UpsertSandbox(ctx, child))
	fork, err := f.store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: f.sandboxID, TargetSandboxID: child.ID,
	})
	require.NoError(t, err)
	copyRecord := rootFSTestSandboxRecord("live-s3-restore", record.TeamID)
	copyRecord.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, f.store.UpsertSandbox(ctx, copyRecord))
	restored, err := f.store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: copyRecord.ID, SnapshotID: snapshot.ID, TeamID: record.TeamID,
		OperationID: "live-s3-snapshot-restore",
	})
	require.NoError(t, err)
	require.NotEqual(t, snapshot.HeadGenerationID, restored.HeadGenerationID)
	for range 5 {
		_, err = f.store.InventoryRootFSGeneration(ctx, isolated, 100)
		require.NoError(t, err)
	}
	_, err = f.store.GarbageCollectRootFSFilesystemWithOptions(ctx, isolated, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	for _, head := range []string{filesystem.HeadGenerationID, fork.HeadGenerationID, restored.HeadGenerationID} {
		generation, err := f.store.GetRootFSGeneration(ctx, head)
		require.NoError(t, err)
		assertRetentionContents(t, isolated, generation, 11)
		descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
		require.NoError(t, err)
		reader, err := rootfsblock.NewReader(isolated, descriptor, 0)
		require.NoError(t, err)
		block := make([]byte, rootfsblock.LogicalBlockSize)
		_, err = reader.ReadAt(block, rootfsblock.LogicalBlockSize)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{22}, len(block)), block, "terminal suffix survives snapshot/fork/restore and GC")
	}
	t.Log("installed live base, terminal publication, snapshot, fork and restore survive real S3 inventory/GC")
}
