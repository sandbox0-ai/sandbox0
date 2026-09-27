package sandboxstore

import (
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// Two independent writers deduplicate the same map and pack. The legacy
// writer has no regional references until its bounded inventory completes.
func TestRootFSLegacySharedObjectGCBeforeInventoryS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, fs, base := newRetentionS3Filesystem(t, objects, prefix, "legacy-shared")
	legacy := publishRetentionNodeCheckpoint(t, store, objects, prefix, base, fs.ID, "legacy-shared-cut", 7, false)
	record := rootFSTestSandboxRecord("modern-shared", "team-1")
	record.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(t.Context(), record))
	other, otherBase, err := store.EnsureInitialRootFSGeneration(t.Context(), &EnsureInitialRootFSGenerationRequest{SandboxID: record.ID, TeamID: "team-1", SourceOCIRef: readyRootFSBaseArtifactTestRequest().SourceOCIRef, SourceOCIDigest: base.SourceOCIDigest, BaseArtifactDigest: base.BaseArtifactDigest})
	require.NoError(t, err)
	modern := publishRetentionNodeCheckpoint(t, store, objects, prefix, otherBase, other.ID, "modern-shared-cut", 7, true)
	require.NoError(t, store.MarkSandboxDeleted(t.Context(), other.ID, time.Now()))
	// Include a terminal PUT whose successful acknowledgement was lost.
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_node_uploads SET terminal_at=clock_timestamp()-INTERVAL '3 minutes' WHERE terminal_at IS NOT NULL`)
	require.NoError(t, err)
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	assertRetentionContents(t, objects, legacy, 7)
	var holds int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM manager.rootfs_legacy_object_gc_holds`).Scan(&holds))
	require.Positive(t, holds, "deferred custody must survive the retiring owner")
	// Also cover a deduplicated reservation with a lost successful PUT ack.
	// Its payload is real and immutable, but its catalog upload is unconfirmed.
	descriptor, err := rootfsblock.DecodeDescriptor(legacy.Descriptor)
	require.NoError(t, err)
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_objects SET uploaded_at=NULL WHERE object_key=$1`, descriptor.MappingRoot.Object.Key)
	require.NoError(t, err)
	// Restart before adoption: custody must persist, then eventually disappear.
	restarted := NewPGSandboxStore(store.pool)
	complete := false
	for range 30 {
		complete, err = restarted.InventoryRootFSGeneration(t.Context(), objects, 1)
		require.NoError(t, err)
		if complete {
			break
		}
	}
	require.True(t, complete)
	assertRetentionContents(t, objects, legacy, 7)
	_, err = restarted.GetRootFSGeneration(t.Context(), modern.ID)
	require.Error(t, err)
	require.NoError(t, restarted.MarkSandboxDeleted(t.Context(), fs.ID, time.Now()))
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_object_deletions SET next_attempt_at=NOW()-INTERVAL '1 minute'; UPDATE manager.rootfs_node_uploads SET terminal_at=clock_timestamp()-INTERVAL '3 minutes' WHERE terminal_at IS NOT NULL`)
	require.NoError(t, err)
	for range 10 {
		_, err = restarted.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
	}
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM manager.rootfs_legacy_object_gc_holds`).Scan(&holds))
	require.Zero(t, holds, "bounded revisits must release all temporary holds")
	physical := retentionS3Usage(t, objects, prefix)
	require.Equal(t, int64(1), physical.objects, "only independently retained base mapping remains")
	t.Logf("legacy sharing: authenticated read survives unrelated owner deletion; final objects=%d bytes=%d", physical.objects, physical.bytes)
}
