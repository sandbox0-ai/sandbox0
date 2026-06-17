package service

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "sandbox_rootfs_states"))
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "sandbox_rootfs_heads"))

	err = store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-source", "team-1", "layer-stale", "layer-root", 3, "stale"))
	require.ErrorIs(t, err, ErrRootFSHeadConflict)

	require.NoError(t, store.SquashRootFSFilesystem(ctx, &SquashRootFSFilesystemRequest{
		SandboxID:           "sandbox-source",
		ExpectedHeadLayerID: "layer-child",
		SquashedRootFSState: rootFSTestStoreState("sandbox-source", "team-1", "layer-squash", "", 3, "squash"),
	}))
	latest, err = store.GetLatestRootFSState(ctx, "sandbox-source")
	require.NoError(t, err)
	require.NotNil(t, latest)
	assert.Equal(t, "layer-squash", latest.LayerID)
	require.Len(t, latest.LayerChain, 1)

	deleter := &recordingRootFSObjectDeleter{}
	gcResult, err := store.GarbageCollectRootFSFilesystem(ctx, deleter, "team-1", 10)
	require.NoError(t, err)
	require.Len(t, gcResult.Layers, 1)
	assert.Equal(t, "layer-child", gcResult.Layers[0].ID)
	assert.Equal(t, []string{"rootfs/child.tar"}, gcResult.DeletedObjectKeys)
	gcResult, err = store.GarbageCollectRootFSFilesystem(ctx, deleter, "team-1", 10)
	require.NoError(t, err)
	require.Len(t, gcResult.Layers, 1)
	assert.Equal(t, "layer-root", gcResult.Layers[0].ID)
	assert.Equal(t, []string{"rootfs/root.tar"}, gcResult.DeletedObjectKeys)

	snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID:   "sandbox-source",
		SnapshotID:  "snapshot-squash",
		Name:        "squash",
		Description: "squashed head",
	})
	require.NoError(t, err)
	assert.Equal(t, "sandbox-source", snapshot.FilesystemID)
	assert.Equal(t, "layer-squash", snapshot.HeadLayerID)

	snapshots, err := store.ListRootFSSnapshots(ctx, &ListRootFSSnapshotsRequest{
		SandboxID: "sandbox-source",
		TeamID:    "team-1",
	})
	require.NoError(t, err)
	require.Len(t, snapshots, 1)
	assert.Equal(t, snapshot.ID, snapshots[0].ID)

	loadedSnapshot, err := store.GetRootFSSnapshot(ctx, "snapshot-squash", "team-1")
	require.NoError(t, err)
	assert.Equal(t, snapshot.HeadLayerID, loadedSnapshot.HeadLayerID)
	_, err = store.GetRootFSSnapshot(ctx, "snapshot-squash", "team-2")
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
	assert.Equal(t, "layer-squash", forked.HeadLayerID)

	forkLatest, err := store.GetLatestRootFSState(ctx, "sandbox-fork")
	require.NoError(t, err)
	require.NotNil(t, forkLatest)
	assert.Equal(t, "sandbox-fork", forkLatest.SandboxID)
	assert.Equal(t, "layer-squash", forkLatest.LayerID)
	require.Len(t, forkLatest.LayerChain, 1)

	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-fork", "team-1", "layer-fork", "layer-squash", 4, "fork")))
	forkLatest, err = store.GetLatestRootFSState(ctx, "sandbox-fork")
	require.NoError(t, err)
	require.NotNil(t, forkLatest)
	assert.Equal(t, "layer-fork", forkLatest.LayerID)

	restored, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID:  "sandbox-fork",
		SnapshotID: "snapshot-squash",
		TeamID:     "team-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "sandbox-fork", restored.ID)
	assert.Equal(t, "layer-squash", restored.HeadLayerID)

	failDeleter := &recordingRootFSObjectDeleter{failKey: "rootfs/fork.tar", err: assert.AnError}
	gcResult, err = store.GarbageCollectRootFSFilesystem(ctx, failDeleter, "team-1", 10)
	require.ErrorIs(t, err, assert.AnError)
	require.Len(t, gcResult.Layers, 1)
	assert.Equal(t, "layer-fork", gcResult.Layers[0].ID)
	assert.Equal(t, int64(1), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_object_deletions
		SET next_attempt_at = NOW(),
			claimed_by = '',
			claimed_until = NULL
	`)
	require.NoError(t, err)
	deletedObjects, err := store.DeletePendingRootFSObjects(ctx, deleter, 10)
	require.NoError(t, err)
	assert.Equal(t, []string{"rootfs/fork.tar"}, deletedObjects)
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

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

func TestRootFSObjectDeletionQueueClaimsBacksOffAndDeadLetters(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.rootfs_object_deletions (object_key, team_id, next_attempt_at)
		VALUES
			('rootfs/a.tar', 'team-1', NOW()),
			('rootfs/b.tar', 'team-1', NOW())
	`)
	require.NoError(t, err)

	claimed, err := store.claimPendingRootFSObjectDeletions(ctx, DeletePendingRootFSObjectsOptions{
		Limit:     1,
		ClaimedBy: "worker-a",
		ClaimTTL:  time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, claimed, 1)

	claimedAgain, err := store.claimPendingRootFSObjectDeletions(ctx, DeletePendingRootFSObjectsOptions{
		Limit:     2,
		ClaimedBy: "worker-b",
		ClaimTTL:  time.Minute,
	})
	require.NoError(t, err)
	require.Len(t, claimedAgain, 1)
	assert.NotEqual(t, claimed[0].ObjectKey, claimedAgain[0].ObjectKey)

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_object_deletions
		SET claimed_by = '',
			claimed_until = NOW() - INTERVAL '1 second'
	`)
	require.NoError(t, err)

	deleteErr := assert.AnError
	deleter := &recordingRootFSObjectDeleter{failKey: "rootfs/a.tar", err: deleteErr}
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{
		Limit:           2,
		ClaimedBy:       "worker-c",
		ClaimTTL:        time.Minute,
		BackoffBase:     time.Minute,
		BackoffMax:      time.Minute,
		ContinueOnError: true,
	})
	require.ErrorIs(t, err, deleteErr)
	assert.Equal(t, []string{"rootfs/b.tar"}, deleted)
	assert.ElementsMatch(t, []string{"rootfs/a.tar", "rootfs/b.tar"}, deleter.keys)

	var attempts int
	var nextAttemptAt time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT attempts, next_attempt_at
		FROM manager.rootfs_object_deletions
		WHERE object_key = 'rootfs/a.tar'
	`).Scan(&attempts, &nextAttemptAt))
	assert.Equal(t, 1, attempts)
	assert.True(t, nextAttemptAt.After(time.Now().Add(30*time.Second)))
	assert.Equal(t, int64(1), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_object_deletions
		SET next_attempt_at = NOW()
		WHERE object_key = 'rootfs/a.tar'
	`)
	require.NoError(t, err)
	_, err = store.DeletePendingRootFSObjectsWithOptions(ctx, &recordingRootFSObjectDeleter{failKey: "rootfs/a.tar", err: deleteErr}, DeletePendingRootFSObjectsOptions{
		Limit:       1,
		ClaimedBy:   "worker-d",
		ClaimTTL:    time.Minute,
		BackoffBase: time.Minute,
		BackoffMax:  time.Minute,
		MaxAttempts: 2,
	})
	require.ErrorIs(t, err, deleteErr)

	stats, err := store.RootFSObjectDeletionQueueStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(1), stats.DeadLettered)
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
		DiffDigest:          "sha256:" + suffix,
		DiffMediaType:       "application/vnd.oci.image.layer.v1.tar",
		DiffSize:            int64(len(suffix)),
		DiffObjectKey:       "rootfs/" + suffix + ".tar",
		CreatedAt:           time.Now().UTC(),
	}
}

func rootFSTestCountRows(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	query := ""
	switch table {
	case "sandbox_rootfs_states":
		query = "SELECT COUNT(*) FROM manager.sandbox_rootfs_states"
	case "sandbox_rootfs_heads":
		query = "SELECT COUNT(*) FROM manager.sandbox_rootfs_heads"
	case "rootfs_object_deletions":
		query = "SELECT COUNT(*) FROM manager.rootfs_object_deletions"
	default:
		t.Fatalf("unexpected table %q", table)
	}
	var count int64
	require.NoError(t, pool.QueryRow(context.Background(), query).Scan(&count))
	return count
}

type noopSandboxStoreMigrateLogger struct{}

func (noopSandboxStoreMigrateLogger) Printf(string, ...any) {}
func (noopSandboxStoreMigrateLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
