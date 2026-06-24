package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRootFSFilesystemPersistenceIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-source", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-root", "", 1, "root")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-child", "layer-root", 2, "child")))

	latest, err := store.GetLatestRootFSState(ctx, "sandbox-source")
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, "sandbox-source", latest.SandboxID)
	assert.Equal(t, "layer-child", latest.LayerID)
	require.Len(t, latest.LayerChain, 2)
	assert.Equal(t, "layer-root", latest.LayerChain[0].ID)
	assert.Equal(t, "layer-child", latest.LayerChain[1].ID)

	err = store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-stale", "layer-root", 3, "stale"))
	require.ErrorIs(t, err, ErrRootFSHeadConflict)

	snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID:   "sandbox-source",
		SnapshotID:  "snapshot-child",
		Name:        "child",
		Description: "child head",
	})
	require.NoError(t, err)
	assert.Equal(t, "sandbox-source", snapshot.FilesystemID)
	assert.Equal(t, "layer-child", snapshot.HeadLayerID)

	snapshots, err := store.ListRootFSSnapshots(ctx, &ListRootFSSnapshotsRequest{
		SandboxID: "sandbox-source",
		TeamID:    "team-1",
	})
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	assert.Equal(t, snapshot.ID, snapshots[0].ID)

	loadedSnapshot, err := store.GetRootFSSnapshot(ctx, "snapshot-child", "team-1")
	require.NoError(t, err)
	assert.Equal(t, snapshot.HeadLayerID, loadedSnapshot.HeadLayerID)
	_, err = store.GetRootFSSnapshot(ctx, "snapshot-child", "team-2")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)

	_, err = store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID:  "sandbox-source",
		SnapshotID: "snapshot-delete",
	})
	require.NoError(t, err)
	require.NoError(t, store.DeleteRootFSSnapshot(ctx, "snapshot-delete", "team-1"))
	_, err = store.GetRootFSSnapshot(ctx, "snapshot-delete", "team-1")
	require.ErrorIs(t, err, ErrRootFSSnapshotNotFound)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-fork", "team-1")))
	forked, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: "sandbox-source",
		TargetSandboxID: "sandbox-fork",
	})
	require.NoError(t, err)
	assert.Equal(t, "sandbox-source", forked.SourceFilesystemID)
	assert.Equal(t, "layer-child", forked.HeadLayerID)

	forkLatest, err := store.GetLatestRootFSState(ctx, "sandbox-fork")
	require.NoError(t, err)
	require.NotNil(t, forkLatest)
	assert.Equal(t, "sandbox-fork", forkLatest.SandboxID)
	assert.Equal(t, "layer-child", forkLatest.LayerID)
	require.Len(t, forkLatest.LayerChain, 2)

	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-fork", "team-1", "layer-fork", "layer-child", 4, "fork")))
	forkLatest, err = store.GetLatestRootFSState(ctx, "sandbox-fork")
	require.NoError(t, err)
	require.NotNil(t, forkLatest)
	assert.Equal(t, "layer-fork", forkLatest.LayerID)

	restored, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID:  "sandbox-fork",
		SnapshotID: "snapshot-child",
		TeamID:     "team-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "sandbox-fork", restored.ID)
	assert.Equal(t, "layer-child", restored.HeadLayerID)

	gcResult, err := store.GarbageCollectRootFSFilesystem(ctx, nil, "team-1", 10)
	require.NoError(t, err)
	require.Len(t, gcResult.Layers, 1)
	assert.Equal(t, "layer-fork", gcResult.Layers[0].ID)
	assert.Empty(t, gcResult.DeletedS0FSSegments)
	assert.Empty(t, gcResult.DeletedS0FSManifests)

	require.NoError(t, store.MarkSandboxDeleted(ctx, "sandbox-source", time.Now().UTC()))
	sourceFilesystem, err := store.GetRootFSFilesystem(ctx, "sandbox-source")
	require.NoError(t, err)
	assert.Nil(t, sourceFilesystem)

	var sourceFilesystemStillExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_filesystems
			WHERE filesystem_id = 'sandbox-source'
		)
	`).Scan(&sourceFilesystemStillExists))
	assert.True(t, sourceFilesystemStillExists, "source filesystem is retained by snapshot and fork references")
}

func TestRootFSRootLayerSaveCanCASAgainstExistingHead(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-source", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-root", "", 1, "root")))

	fullLayer := rootFSTestStoreState("sandbox-source", "team-1", "layer-full", "", 2, "full")
	fullLayer.ExpectedHeadLayerID = "layer-root"
	require.NoError(t, store.SaveRootFSState(ctx, fullLayer))

	latest, err := store.GetLatestRootFSState(ctx, "sandbox-source")
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, "layer-full", latest.LayerID)
	assert.Empty(t, latest.ParentLayerID)
	require.Len(t, latest.LayerChain, 1)

	staleLayer := rootFSTestStoreState("sandbox-source", "team-1", "layer-stale", "", 3, "stale")
	staleLayer.ExpectedHeadLayerID = "layer-root"
	err = store.SaveRootFSState(ctx, staleLayer)
	require.ErrorIs(t, err, ErrRootFSHeadConflict)

	var staleExists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_layers
			WHERE layer_id = 'layer-stale'
		)
	`).Scan(&staleExists))
	assert.False(t, staleExists, "failed CAS must roll back the candidate layer")
}

func TestRootFSGCCollectsS0FSLayerMetadataWithoutObjectStore(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	live := rootFSTestStoreState("sandbox-live", "team-1", "layer-live", "", 1, "shared")
	stale := rootFSTestStoreState("sandbox-stale", "team-1", "layer-stale", "", 1, "shared")

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-live", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, live))
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-stale", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, stale))
	require.NoError(t, store.MarkSandboxDeleted(ctx, "sandbox-stale", time.Now().UTC()))

	result, err := store.GarbageCollectRootFSFilesystem(ctx, nil, "team-1", 10)
	require.NoError(t, err)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-stale", result.Layers[0].ID)
	assert.Empty(t, result.DeletedS0FSSegments)
	assert.Empty(t, result.DeletedS0FSManifests)
}

func TestRootFSGCDeletesUnboundAncestorFilesystemAfterChildDeletion(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-source", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-root", "", 1, "root")))
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-child", "team-1")))
	_, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: "sandbox-source",
		TargetSandboxID: "sandbox-child",
	})
	require.NoError(t, err)

	require.NoError(t, store.MarkSandboxDeleted(ctx, "sandbox-source", time.Now().UTC()))
	assert.True(t, rootFSTestFilesystemExists(t, pool, "sandbox-source"))
	require.NoError(t, store.MarkSandboxDeleted(ctx, "sandbox-child", time.Now().UTC()))

	deleted, err := store.DeleteUnreferencedRootFSFilesystems(ctx, "team-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 2, deleted)
	assert.False(t, rootFSTestFilesystemExists(t, pool, "sandbox-source"))
}

func TestRootFSGCSkipsLayerStillReferencedByOrphanFilesystem(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	now := time.Now().UTC()

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-orphan-a", "team-1")))
	stateA := rootFSTestStoreState("sandbox-orphan-a", "team-1", "layer-orphan-a", "", 1, "orphan-a")
	stateA.CreatedAt = now
	require.NoError(t, store.SaveRootFSState(ctx, stateA))
	time.Sleep(10 * time.Millisecond)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-orphan-b", "team-1")))
	stateB := rootFSTestStoreState("sandbox-orphan-b", "team-1", "layer-orphan-b", "", 1, "orphan-b")
	stateB.CreatedAt = now.Add(-time.Hour)
	require.NoError(t, store.SaveRootFSState(ctx, stateB))

	_, err := pool.Exec(ctx, `
		DELETE FROM manager.sandbox_rootfs_bindings
		WHERE sandbox_id IN ('sandbox-orphan-a', 'sandbox-orphan-b')
	`)
	require.NoError(t, err)

	result, err := store.GarbageCollectRootFSFilesystem(ctx, nil, "team-1", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, result.DeletedFilesystems)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-orphan-a", result.Layers[0].ID)
	assert.True(t, rootFSTestFilesystemExists(t, pool, "sandbox-orphan-b"))

	result, err = store.GarbageCollectRootFSFilesystem(ctx, nil, "team-1", 1)
	require.NoError(t, err)
	assert.Equal(t, 1, result.DeletedFilesystems)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-orphan-b", result.Layers[0].ID)
	assert.False(t, rootFSTestFilesystemExists(t, pool, "sandbox-orphan-b"))
}

func TestRootFSGCExpiresSnapshotBeforeLayerCollection(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-source", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-old", "", 1, "old")))
	_, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID:  "sandbox-source",
		SnapshotID: "snapshot-expired",
		ExpiresAt:  time.Now().Add(-time.Hour),
	})
	require.NoError(t, err)
	full := rootFSTestStoreState("sandbox-source", "team-1", "layer-full", "", 2, "full")
	full.ExpectedHeadLayerID = "layer-old"
	require.NoError(t, store.SaveRootFSState(ctx, full))

	result, err := store.GarbageCollectRootFSFilesystem(ctx, nil, "team-1", 10)
	require.NoError(t, err)
	assert.Equal(t, 1, result.ExpiredSnapshots)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-old", result.Layers[0].ID)
	assert.Empty(t, result.DeletedS0FSSegments)
	assert.Empty(t, result.DeletedS0FSManifests)
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_snapshots"))
}

func TestRootFSStorageUsageIgnoresS0FSCheckpoints(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	first := rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "shared")
	second := rootFSTestStoreState("sandbox-b", "team-1", "layer-b", "", 1, "shared")

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, first))
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-b", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, second))

	usages, err := store.ListRootFSStorageUsage(ctx, nil, "team-1")
	require.NoError(t, err)
	assert.Empty(t, usages)

	recorder := &recordingRootFSStorageMeteringRecorder{}
	observedAt := time.Now().UTC()
	usages, err = store.RecordRootFSStorageObservations(ctx, nil, recorder, "team-1", observedAt)
	require.NoError(t, err)
	assert.Empty(t, usages)
	assert.Empty(t, recorder.observations)
}

func TestRootFSS0FSObjectGCAndStorageUsageIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	objectStore := objectstore.NewMemoryStore("rootfs-s0fs-object-gc-" + t.Name())
	teamID := "team-1"
	filesystemID := "sandbox-s0fs-gc"

	oldManifest, currentManifest := rootFSTestMaterializeTwoS0FSHeads(t, ctx, objectStore, teamID, filesystemID)
	prefixed := rootfsstore.S0FSObjectStore(objectStore, teamID, filesystemID)
	require.NoError(t, prefixed.Put("segments/orphan.bin", strings.NewReader("orphan")))

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord(filesystemID, teamID)))
	oldState := rootFSTestStoreState(filesystemID, teamID, "layer-old", "", 1, "old")
	oldState.S0FSManifestKey = rootFSTestS0FSManifestKey(oldManifest.ManifestSeq)
	oldState.S0FSManifestSeq = oldManifest.ManifestSeq
	oldState.S0FSCheckpointSeq = oldManifest.CheckpointSeq
	require.NoError(t, store.SaveRootFSState(ctx, oldState))

	currentState := rootFSTestStoreState(filesystemID, teamID, "layer-current", "layer-old", 2, "current")
	currentState.S0FSManifestKey = rootFSTestS0FSManifestKey(currentManifest.ManifestSeq)
	currentState.S0FSManifestSeq = currentManifest.ManifestSeq
	currentState.S0FSCheckpointSeq = currentManifest.CheckpointSeq
	require.NoError(t, store.SaveRootFSState(ctx, currentState))

	result, err := store.GarbageCollectRootFSFilesystem(ctx, objectStore, teamID, 10)
	require.NoError(t, err)
	assert.Empty(t, result.Layers)
	assert.Contains(t, result.DeletedS0FSSegments, teamID+"/"+filesystemID+"/segments/orphan.bin")
	assert.Contains(t, result.DeletedS0FSSegments, teamID+"/"+filesystemID+"/segments/00000000000000000002-0.bin")
	assert.Contains(t, result.DeletedS0FSManifests, teamID+"/"+filesystemID+"/"+rootFSTestS0FSManifestKey(oldManifest.ManifestSeq))

	_, err = prefixed.Head(rootFSTestS0FSManifestKey(oldManifest.ManifestSeq))
	assert.Error(t, err)
	_, err = prefixed.Head(rootFSTestS0FSManifestKey(currentManifest.ManifestSeq))
	assert.NoError(t, err)
	_, err = prefixed.Head("manifests/latest.json")
	assert.NoError(t, err)

	usages, err := store.ListRootFSStorageUsage(ctx, objectStore, teamID)
	require.NoError(t, err)
	require.Len(t, usages, 1)
	assert.Equal(t, teamID, usages[0].TeamID)
	assert.Equal(t, int64(3), usages[0].ObjectCount)
	assert.Positive(t, usages[0].StorageBytes)

	recorder := &recordingRootFSStorageMeteringRecorder{}
	observedAt := time.Now().UTC()
	usages, err = store.RecordRootFSStorageObservations(ctx, objectStore, recorder, teamID, observedAt)
	require.NoError(t, err)
	require.Len(t, usages, 1)
	require.Len(t, recorder.observations, 1)
	assert.Equal(t, usages[0].StorageBytes, recorder.observations[0].SizeBytes)
}

func newSandboxStoreIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS manager CASCADE")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS manager CASCADE")
	})
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	return pool
}

func rootFSTestMaterializeTwoS0FSHeads(t *testing.T, ctx context.Context, store objectstore.Store, teamID, filesystemID string) (*s0fs.Manifest, *s0fs.Manifest) {
	t.Helper()
	engine, err := s0fs.Open(ctx, s0fs.Config{
		VolumeID:    filesystemID,
		WALPath:     filepath.Join(t.TempDir(), "engine.wal"),
		ObjectStore: rootfsstore.S0FSObjectStore(store, teamID, filesystemID),
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	node, err := engine.CreateFile(s0fs.RootInode, "state.txt", 0o644)
	require.NoError(t, err)
	_, err = engine.Write(node.Inode, 0, []byte("old"))
	require.NoError(t, err)
	oldManifest, err := engine.SyncMaterialize(ctx)
	require.NoError(t, err)

	require.NoError(t, engine.Truncate(node.Inode, 0))
	_, err = engine.Write(node.Inode, 0, []byte("new"))
	require.NoError(t, err)
	currentManifest, err := engine.SyncMaterialize(ctx)
	require.NoError(t, err)
	return oldManifest, currentManifest
}

func rootFSTestS0FSManifestKey(seq uint64) string {
	return fmt.Sprintf("manifests/%020d.json", seq)
}

func rootFSTestSandboxRecord(sandboxID, teamID string) *SandboxRecord {
	return &SandboxRecord{
		ID:                sandboxID,
		TeamID:            teamID,
		UserID:            "user-1",
		TemplateID:        "template-1",
		TemplateName:      "template-1",
		TemplateNamespace: "template-default",
		Status:            SandboxStatusRunning,
		CreatedAt:         time.Now().UTC(),
	}
}

func rootFSTestStoreState(sandboxID, teamID, layerID, parentLayerID string, generation int64, suffix string) *SandboxRootFSState {
	manifestKey := "manifests/" + suffix + ".json"
	return &SandboxRootFSState{
		LayerID:             layerID,
		ParentLayerID:       parentLayerID,
		SandboxID:           sandboxID,
		TeamID:              teamID,
		RuntimeGeneration:   generation,
		Runtime:             "runc",
		RuntimeHandler:      "io.containerd.runc.v2",
		BaseImageRef:        "docker.io/library/busybox:1.36",
		BaseImageDigest:     "sha256:base",
		Snapshotter:         "overlayfs",
		SnapshotParent:      "parent-1",
		SnapshotParentChain: []string{"parent-1", "parent-0"},
		StorageEngine:       ctldapi.RootFSStorageEngineS0FS,
		S0FSVolumeID:        sandboxID,
		S0FSManifestKey:     manifestKey,
		S0FSManifestSeq:     uint64(generation),
		S0FSCheckpointSeq:   uint64(generation),
		CreatedAt:           time.Now().UTC(),
	}
}

func rootFSTestCountRows(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	query := ""
	switch table {
	case "rootfs_snapshots":
		query = "SELECT COUNT(*) FROM manager.rootfs_snapshots"
	default:
		t.Fatalf("unexpected table %q", table)
	}
	var count int64
	require.NoError(t, pool.QueryRow(context.Background(), query).Scan(&count))
	return count
}

func rootFSTestFilesystemExists(t *testing.T, pool *pgxpool.Pool, filesystemID string) bool {
	t.Helper()
	var exists bool
	require.NoError(t, pool.QueryRow(context.Background(), `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_filesystems
			WHERE filesystem_id = $1
		)
	`, filesystemID).Scan(&exists))
	return exists
}

type recordingRootFSStorageMeteringRecorder struct {
	observations []*meteringpkg.StorageObservation
}

func (r *recordingRootFSStorageMeteringRecorder) RecordStorageObservation(_ context.Context, observation *meteringpkg.StorageObservation) error {
	r.observations = append(r.observations, observation)
	return nil
}

type noopSandboxStoreMigrateLogger struct{}

func (noopSandboxStoreMigrateLogger) Printf(string, ...any) {}
func (noopSandboxStoreMigrateLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
