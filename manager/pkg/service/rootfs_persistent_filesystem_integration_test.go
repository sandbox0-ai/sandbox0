package service

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
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
	gcResult, err := store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		deleter,
		"team-1",
		10,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
	require.NoError(t, err)
	require.Len(t, gcResult.Layers, 1)
	assert.Equal(t, "layer-child", gcResult.Layers[0].ID)
	assert.Equal(t, []string{"rootfs/child.tar"}, gcResult.DeletedObjectKeys)
	gcResult, err = store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		deleter,
		"team-1",
		10,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
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
	gcResult, err = store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		failDeleter,
		"team-1",
		10,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
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
	deletedObjects, err := store.DeletePendingRootFSObjectsWithOptions(
		ctx,
		deleter,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{Limit: 10}),
	)
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

	require.NoError(t, store.QueueUncommittedRootFSObjectDeletion(
		ctx,
		rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "a"),
		time.Now(),
	))
	require.NoError(t, store.QueueUncommittedRootFSObjectDeletion(
		ctx,
		rootFSTestStoreState("sandbox-b", "team-1", "layer-b", "", 1, "b"),
		time.Now(),
	))

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
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{
		Limit:           2,
		ClaimedBy:       "worker-c",
		ClaimTTL:        time.Minute,
		BackoffBase:     time.Minute,
		BackoffMax:      time.Minute,
		ContinueOnError: true,
	}))
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
	_, err = store.DeletePendingRootFSObjectsWithOptions(ctx, &recordingRootFSObjectDeleter{failKey: "rootfs/a.tar", err: deleteErr}, rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{
		Limit:       1,
		ClaimedBy:   "worker-d",
		ClaimTTL:    time.Minute,
		BackoffBase: time.Minute,
		BackoffMax:  time.Minute,
		MaxAttempts: 2,
	}))
	require.ErrorIs(t, err, deleteErr)

	stats, err := store.RootFSObjectDeletionQueueStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(0), stats.Pending)
	assert.Equal(t, int64(1), stats.DeadLettered)
}

func TestRootFSObjectDeletionSkipsActiveLifecyclePreparedHead(t *testing.T) {
	for _, kind := range []string{
		SandboxLifecycleKindPause,
		SandboxLifecycleKindSnapshot,
		SandboxLifecycleKindFork,
	} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)

			require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-pending", "team-1")))
			state := rootFSTestStoreState("sandbox-pending", "team-1", "layer-pending", "", 1, "pending")
			require.NoError(t, store.QueueUncommittedRootFSObjectDeletion(ctx, state, time.Now().Add(-time.Minute)))
			require.NoError(t, store.WithSandboxLock(ctx, "sandbox-pending", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
				return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
					ID:                  "txn-pending",
					SandboxID:           "sandbox-pending",
					Kind:                kind,
					Phase:               SandboxLifecyclePhasePublishing,
					Source:              SandboxLifecycleSourceAuto,
					Cancelable:          true,
					FromGeneration:      1,
					PreparedHeadLayerID: "layer-pending",
				})
			}))

			deleter := &recordingRootFSObjectDeleter{}
			deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{
				Limit:     1,
				ClaimedBy: "worker-active-txn",
				ClaimTTL:  time.Minute,
			}))

			require.NoError(t, err)
			assert.Empty(t, deleted)
			assert.Empty(t, deleter.keys)
			assert.Equal(t, int64(1), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

			var claimedBy string
			var nextAttemptAt time.Time
			require.NoError(t, pool.QueryRow(ctx, `
				SELECT claimed_by, next_attempt_at
				FROM manager.rootfs_object_deletions
				WHERE object_key = $1
			`, state.DiffObjectKey).Scan(&claimedBy, &nextAttemptAt))
			assert.Empty(t, claimedBy)
			assert.True(t, nextAttemptAt.After(time.Now().Add(30*time.Second)))

			require.NoError(t, store.WithSandboxLock(ctx, "sandbox-pending", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
				return tx.AbortLifecycleTxn(lockCtx, "txn-pending", "test cleanup")
			}))
			_, err = pool.Exec(ctx, `
				UPDATE manager.rootfs_object_deletions
				SET next_attempt_at = NOW()
				WHERE object_key = $1
			`, state.DiffObjectKey)
			require.NoError(t, err)

			deleted, err = store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{
				Limit:     1,
				ClaimedBy: "worker-aborted-txn",
				ClaimTTL:  time.Minute,
			}))

			require.NoError(t, err)
			assert.Equal(t, []string{state.DiffObjectKey}, deleted)
			assert.Equal(t, []string{state.DiffObjectKey}, deleter.keys)
			assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))
		})
	}
}

func TestRootFSObjectDeletionDoesNotLeakStaleSourceCheckpoint(t *testing.T) {
	for _, kind := range []string{
		SandboxLifecycleKindSnapshot,
		SandboxLifecycleKindFork,
	} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			pool := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(pool)

			require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-stale", "team-1")))
			state := rootFSTestStoreState("sandbox-stale", "team-1", "layer-stale", "", 1, "stale")
			require.NoError(t, store.QueueUncommittedRootFSObjectDeletion(ctx, state, time.Now().Add(-time.Minute)))
			require.NoError(t, store.WithSandboxLock(ctx, "sandbox-stale", func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
				return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
					ID:                  "txn-stale",
					SandboxID:           "sandbox-stale",
					Kind:                kind,
					Phase:               SandboxLifecyclePhasePublishing,
					Source:              SandboxLifecycleSourceManual,
					FromGeneration:      1,
					PreparedHeadLayerID: "layer-stale",
				})
			}))
			_, err := pool.Exec(ctx, `
				ALTER TABLE manager.sandbox_lifecycle_txns
				DISABLE TRIGGER update_sandbox_lifecycle_txns_updated_at
			`)
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `
				UPDATE manager.sandbox_lifecycle_txns
				SET updated_at = NOW() - ($2::int * INTERVAL '1 second')
				WHERE txn_id = $1
			`, "txn-stale", durationSeconds(sandboxRootFSSourceCheckpointLifecycleStaleAfter+time.Minute))
			require.NoError(t, err)
			_, err = pool.Exec(ctx, `
				ALTER TABLE manager.sandbox_lifecycle_txns
				ENABLE TRIGGER update_sandbox_lifecycle_txns_updated_at
			`)
			require.NoError(t, err)

			deleter := &recordingRootFSObjectDeleter{}
			deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{
				Limit:     1,
				ClaimedBy: "worker-stale-txn",
				ClaimTTL:  time.Minute,
			}))

			require.NoError(t, err)
			assert.Equal(t, []string{state.DiffObjectKey}, deleted)
			assert.Equal(t, []string{state.DiffObjectKey}, deleter.keys)
			assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))
		})
	}
}

func TestRootFSObjectDeletionQueueClearedWhenObjectCommits(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-committed", "team-1")))
	state := rootFSTestStoreState("sandbox-committed", "team-1", "layer-committed", "", 1, "committed")
	require.NoError(t, store.QueueUncommittedRootFSObjectDeletion(ctx, state, time.Now().Add(-time.Minute)))
	assert.Equal(t, int64(1), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	require.NoError(t, store.SaveRootFSState(ctx, state))
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	deleter := &recordingRootFSObjectDeleter{}
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(
		ctx,
		deleter,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{Limit: 10}),
	)
	require.NoError(t, err)
	assert.Empty(t, deleted)
	assert.Empty(t, deleter.keys)
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

func TestRootFSGCSkipsObjectDeleteWhenAnotherLayerReferencesSameKey(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	live := rootFSTestStoreState("sandbox-live", "team-1", "layer-live", "", 1, "shared")
	live.DiffObjectKey = "rootfs/shared.tar"
	stale := rootFSTestStoreState("sandbox-stale", "team-1", "layer-stale", "", 1, "shared")
	stale.DiffObjectKey = live.DiffObjectKey

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-live", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, live))
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-stale", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, stale))
	require.NoError(t, store.MarkSandboxDeleted(ctx, "sandbox-stale", time.Now().UTC()))

	deleter := &recordingRootFSObjectDeleter{}
	result, err := store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		deleter,
		"team-1",
		10,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
	require.NoError(t, err)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-stale", result.Layers[0].ID)
	assert.Empty(t, result.DeletedObjectKeys)
	assert.Empty(t, deleter.keys)
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_object_deletions"))

	var deletedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted_at
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/shared.tar'
	`).Scan(&deletedAt))
	assert.Nil(t, deletedAt)
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

	deleter := &recordingRootFSObjectDeleter{}
	result, err := store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		deleter,
		"team-1",
		1,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
	require.NoError(t, err)
	assert.Equal(t, 1, result.DeletedFilesystems)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-orphan-a", result.Layers[0].ID)
	assert.True(t, rootFSTestFilesystemExists(t, pool, "sandbox-orphan-b"))

	result, err = store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		deleter,
		"team-1",
		1,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
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

	deleter := &recordingRootFSObjectDeleter{}
	result, err := store.GarbageCollectRootFSFilesystemWithOptions(
		ctx,
		deleter,
		"team-1",
		10,
		rootFSTestDeletionOptions(pool, DeletePendingRootFSObjectsOptions{}),
	)
	require.NoError(t, err)
	assert.Equal(t, 1, result.ExpiredSnapshots)
	require.Len(t, result.Layers, 1)
	assert.Equal(t, "layer-old", result.Layers[0].ID)
	assert.Equal(t, []string{"rootfs/old.tar"}, result.DeletedObjectKeys)
	assert.Equal(t, int64(0), rootFSTestCountRows(t, pool, "rootfs_snapshots"))
}

func TestRootFSStorageUsageDedupesReachableObjectsAndRecordsMetering(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	first := rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "shared")
	first.DiffObjectKey = "rootfs/shared.tar"
	second := rootFSTestStoreState("sandbox-b", "team-1", "layer-b", "", 1, "shared")
	second.DiffObjectKey = first.DiffObjectKey

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, first))
	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-b", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, second))

	usages, err := store.ListRootFSStorageUsage(ctx, "team-1")
	require.NoError(t, err)
	require.Len(t, usages, 1)
	assert.Equal(t, "team-1", usages[0].TeamID)
	assert.Equal(t, int64(1), usages[0].ObjectCount)
	assert.Equal(t, int64(len("shared")), usages[0].StorageBytes)

	recorder := &recordingRootFSStorageMeteringRecorder{}
	observedAt := time.Now().UTC()
	usages, err = store.RecordRootFSStorageObservations(ctx, recorder, "team-1", observedAt)
	require.NoError(t, err)
	require.Len(t, usages, 1)
	require.Len(t, recorder.observations, 1)
	assert.Equal(t, meteringpkg.SubjectTypeRootFS, recorder.observations[0].SubjectType)
	assert.Equal(t, "team-1", recorder.observations[0].SubjectID)
	assert.Equal(t, int64(len("shared")), recorder.observations[0].SizeBytes)
}

func TestRootFSObjectAuditRecordsAndClearsMissingState(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "audit")))

	inspector := &recordingRootFSObjectInspector{err: assert.AnError}
	result, err := store.AuditRootFSObjects(ctx, inspector, "team-1", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Checked)
	assert.Equal(t, 1, result.Missing)

	var missingAt *time.Time
	var lastError string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT missing_at, last_error
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/audit.tar'
	`).Scan(&missingAt, &lastError))
	require.NotNil(t, missingAt)
	assert.Contains(t, lastError, assert.AnError.Error())

	inspector.err = nil
	inspector.info = RootFSObjectInfo{Key: "rootfs/audit.tar", Size: int64(len("audit"))}
	result, err = store.AuditRootFSObjects(ctx, inspector, "team-1", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Checked)
	assert.Equal(t, 0, result.Missing)

	require.NoError(t, pool.QueryRow(ctx, `
		SELECT missing_at, last_error
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/audit.tar'
	`).Scan(&missingAt, &lastError))
	assert.Nil(t, missingAt)
	assert.Empty(t, lastError)
}

func TestRootFSObjectAuditAcceptsEncryptedPhysicalSizeOverhead(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "audit")))

	_, err := pool.Exec(ctx, `
		UPDATE manager.rootfs_objects
		SET missing_at = NOW(),
			last_error = 'previous transient audit error'
		WHERE object_key = 'rootfs/audit.tar'
	`)
	require.NoError(t, err)

	inspector := &recordingRootFSObjectInspector{
		info: RootFSObjectInfo{Key: "rootfs/audit.tar", Size: int64(len("audit")) + 512},
	}
	result, err := store.AuditRootFSObjects(ctx, inspector, "team-1", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Checked)
	assert.Equal(t, 0, result.SizeMismatched)

	var missingAt *time.Time
	var lastError string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT missing_at, last_error
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/audit.tar'
	`).Scan(&missingAt, &lastError))
	assert.Nil(t, missingAt)
	assert.Empty(t, lastError)
}

func TestRootFSObjectAuditRejectsTruncatedPhysicalSize(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "audit")))

	inspector := &recordingRootFSObjectInspector{
		info: RootFSObjectInfo{Key: "rootfs/audit.tar", Size: int64(len("audit")) - 1},
	}
	result, err := store.AuditRootFSObjects(ctx, inspector, "team-1", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Checked)
	assert.Equal(t, 1, result.SizeMismatched)

	var missingAt *time.Time
	var lastError string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT missing_at, last_error
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/audit.tar'
	`).Scan(&missingAt, &lastError))
	require.NotNil(t, missingAt)
	assert.Contains(t, lastError, "smaller than db diff size")
}

func TestRootFSObjectAuditRejectsLogicalSizeMismatch(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSTestSandboxRecord("sandbox-a", "team-1")))
	require.NoError(t, store.SaveRootFSState(ctx, rootFSTestStoreState("sandbox-a", "team-1", "layer-a", "", 1, "audit")))

	inspector := &recordingRootFSObjectInspector{
		info: RootFSObjectInfo{Key: "rootfs/audit.tar", Size: int64(len("audit")) + 1, SizeIsLogical: true},
	}
	result, err := store.AuditRootFSObjects(ctx, inspector, "team-1", 10)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.Checked)
	assert.Equal(t, 1, result.SizeMismatched)

	var lastError string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT last_error
		FROM manager.rootfs_objects
		WHERE object_key = 'rootfs/audit.tar'
	`).Scan(&lastError))
	assert.Contains(t, lastError, "logical size")
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
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS quota CASCADE")
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS manager CASCADE")
		_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS quota CASCADE")
	})
	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))
	require.NoError(t, teamquota.RunMigrations(ctx, pool, nil))
	require.NoError(t, teamquota.NewRepository(pool).UnsafeReplaceDefaultPoliciesForTest(
		ctx,
		rootFSTestDefaultQuotaPolicies(1<<60),
	))
	return pool
}

func rootFSTestDeletionOptions(
	pool *pgxpool.Pool,
	opts DeletePendingRootFSObjectsOptions,
) DeletePendingRootFSObjectsOptions {
	quotaStore := teamquota.NewRepository(pool)
	opts.ObjectQuotaStore = quotaStore
	opts.SnapshotQuotaStore = quotaStore
	return opts
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

type recordingRootFSObjectInspector struct {
	info RootFSObjectInfo
	err  error
}

func (i *recordingRootFSObjectInspector) StatRootFSObject(string) (RootFSObjectInfo, error) {
	if i.err != nil {
		return RootFSObjectInfo{}, i.err
	}
	return i.info, nil
}

type noopSandboxStoreMigrateLogger struct{}

func (noopSandboxStoreMigrateLogger) Printf(string, ...any) {}
func (noopSandboxStoreMigrateLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
