package sandboxstore

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/managerapi"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfslease"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSandboxDesiredStateMigrationRepairsLegacyObservedStatuses(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPoolThrough(t, "00010")

	updatedAt := time.Date(2026, time.August, 4, 10, 0, 0, 0, time.UTC)
	legacyRows := []struct {
		id        string
		status    string
		deletedAt *time.Time
	}{
		{id: "starting", status: managerapi.SandboxStatusStarting},
		{id: "running", status: managerapi.SandboxStatusRunning},
		{id: "failed", status: managerapi.SandboxStatusFailed},
		{id: "paused", status: managerapi.SandboxStatusPaused},
		{id: "terminating", status: managerapi.SandboxStatusTerminating},
		{id: "deleted", status: managerapi.SandboxStatusRunning, deletedAt: &updatedAt},
		{id: "deleted-status", status: "deleted"},
	}
	for _, row := range legacyRows {
		_, err := pool.Exec(ctx, `
			INSERT INTO manager.sandboxes (
				sandbox_id, team_id, template_id, template_name, template_namespace,
				status, deleted_at, updated_at
			) VALUES ($1, 'team-1', 'template-1', 'template-1', 'template-default', $2, $3, $4)
		`, row.id, row.status, row.deletedAt, updatedAt)
		require.NoError(t, err)
	}

	require.NoError(t, RunSandboxStoreMigrations(ctx, pool, noopSandboxStoreMigrateLogger{}))

	type migratedState struct {
		desiredState string
		completedAt  *time.Time
		deletedAt    *time.Time
	}
	migrated := make(map[string]migratedState, len(legacyRows))
	rows, err := pool.Query(ctx, `
		SELECT sandbox_id, desired_state, hot_claim_completed_at, deleted_at
		FROM manager.sandboxes
	`)
	require.NoError(t, err)
	defer rows.Close()
	for rows.Next() {
		var id string
		var state migratedState
		require.NoError(t, rows.Scan(&id, &state.desiredState, &state.completedAt, &state.deletedAt))
		migrated[id] = state
	}
	require.NoError(t, rows.Err())

	for _, id := range []string{"starting", "running", "failed"} {
		assert.Equal(t, SandboxDesiredStateActive, migrated[id].desiredState)
		assert.Nil(t, migrated[id].completedAt)
	}
	assert.Equal(t, SandboxDesiredStatePaused, migrated["paused"].desiredState)
	assert.Equal(t, SandboxDesiredStateTerminating, migrated["terminating"].desiredState)
	assert.Equal(t, SandboxDesiredStateDeleted, migrated["deleted"].desiredState)
	assert.Equal(t, SandboxDesiredStateDeleted, migrated["deleted-status"].desiredState)
	assert.NotNil(t, migrated["deleted-status"].deletedAt)

	_, err = pool.Exec(ctx, `UPDATE manager.sandboxes SET desired_state = 'running' WHERE sandbox_id = 'running'`)
	require.Error(t, err, "legacy observed status must be rejected after migration")

	active, err := NewPGSandboxStore(pool).CountActiveSandboxes(ctx, "team-1")
	require.NoError(t, err)
	assert.Equal(t, int64(3), active)
}

func TestMixedVersionSchemaAllowsLegacySameSandboxRestore(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)

	_, err := pool.Exec(ctx, `
		INSERT INTO manager.rootfs_filesystems (filesystem_id, team_id)
		VALUES ('legacy-sandbox', 'team-1')
	`)
	require.NoError(t, err)

	// A manager from before the COW v3 rollout can write this transient value
	// when it restores a snapshot into its source sandbox. The additive schema
	// must accept that statement until every legacy writer has been retired.
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET source_filesystem_id = filesystem_id
		WHERE filesystem_id = 'legacy-sandbox'
	`)
	require.NoError(t, err)

	var sourceFilesystemID string
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT source_filesystem_id
		FROM manager.rootfs_filesystems
		WHERE filesystem_id = 'legacy-sandbox'
	`).Scan(&sourceFilesystemID))
	assert.Equal(t, "legacy-sandbox", sourceFilesystemID)
}

func TestRootFSV3PersistenceSnapshotForkRestoreAndCAS(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSV3TestSandboxRecord("sandbox-source", "team-1")))
	root := rootFSV3TestHead(t, "sandbox-source", "sandbox-source", "team-1", "head-root", 1, nil)
	require.NoError(t, store.SaveRootFSHead(ctx, root))
	child := rootFSV3TestHead(t, "sandbox-source", "sandbox-source", "team-1", "head-child", 2, &root.Reference)
	require.NoError(t, store.SaveRootFSHead(ctx, child))
	exportObject := rootFSV3TestObject(t, "team-1", rootfshead.ExportLayerMediaType, "shared template export")
	exportDiffID := digest.FromString("shared template export diff").String()
	require.NoError(t, store.SaveRootFSExport(ctx, &RootFSExport{
		HeadID: root.Reference.HeadID, TeamID: "team-1", Object: exportObject, DiffID: exportDiffID,
	}))
	require.NoError(t, store.SaveRootFSExport(ctx, &RootFSExport{
		HeadID: child.Reference.HeadID, TeamID: "team-1", Object: exportObject, DiffID: exportDiffID,
	}))
	loadedExport, err := store.GetRootFSExport(ctx, child.Reference.HeadID, "team-1")
	require.NoError(t, err)
	require.NotNil(t, loadedExport)
	assert.Equal(t, exportObject, loadedExport.Object)
	assert.Equal(t, exportDiffID, loadedExport.DiffID)

	loaded, err := store.GetRootFSHead(ctx, "sandbox-source")
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, child.Reference.HeadID, loaded.Reference.HeadID)
	assert.Equal(t, "sandbox-source", loaded.SourceSandboxID)

	stale := rootFSV3TestHead(t, "sandbox-source", "sandbox-source", "team-1", "head-stale", 3, &root.Reference)
	require.NoError(t, store.StageRootFSHead(ctx, stale))
	require.ErrorIs(t, store.SaveRootFSHead(ctx, stale), ErrRootFSHeadConflict)

	snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{
		SandboxID: "sandbox-source", SnapshotID: "snapshot-source", Name: "source Head",
	})
	require.NoError(t, err)
	assert.Equal(t, child.Reference.HeadID, snapshot.HeadID)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSV3TestSandboxRecord("sandbox-fork", "team-1")))
	forked, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: "sandbox-source", TargetSandboxID: "sandbox-fork", TargetTeamID: "team-1",
	})
	require.NoError(t, err)
	assert.Equal(t, child.Reference.HeadID, forked.HeadID)
	forkHead, err := store.GetRootFSHead(ctx, "sandbox-fork")
	require.NoError(t, err)
	require.NotNil(t, forkHead)
	assert.Equal(t, "sandbox-source", forkHead.SourceSandboxID)

	forkChild := rootFSV3TestHead(t, "sandbox-fork", "sandbox-fork", "team-1", "head-fork", 3, &child.Reference)
	require.NoError(t, store.SaveRootFSHead(ctx, forkChild))
	forkHead, err = store.GetRootFSHead(ctx, "sandbox-fork")
	require.NoError(t, err)
	assert.Equal(t, "sandbox-fork", forkHead.SourceSandboxID)

	restored, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: "sandbox-fork", SnapshotID: snapshot.ID, TeamID: "team-1",
	})
	require.NoError(t, err)
	assert.Equal(t, child.Reference.HeadID, restored.HeadID)
	assert.Equal(t, "sandbox-source", restored.SourceFilesystemID)
	assert.Equal(t, child.Base.ImageReference, restored.BaseImageRef)
	assert.Equal(t, child.Base.ManifestDigest, restored.BaseImageDigest)

	restoredSource, err := store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: "sandbox-source", SnapshotID: snapshot.ID, TeamID: "team-1",
	})
	require.NoError(t, err)
	assert.Equal(t, child.Reference.HeadID, restoredSource.HeadID)
	assert.Empty(t, restoredSource.SourceFilesystemID)
	var selfReferences int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.rootfs_filesystems
		WHERE source_filesystem_id = filesystem_id
	`).Scan(&selfReferences))
	assert.Zero(t, selfReferences)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSV3TestSandboxRecord("sandbox-other-team", "team-2")))
	_, err = store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: "sandbox-source", TargetSandboxID: "sandbox-other-team", TargetTeamID: "team-2",
	})
	require.Error(t, err)
	_, err = store.RestoreRootFSFromSnapshot(ctx, &RestoreRootFSFromSnapshotRequest{
		SandboxID: "sandbox-other-team", SnapshotID: snapshot.ID, TeamID: "team-2",
	})
	require.Error(t, err)
}

func TestRootFSV3WriteLeasesProtectCASObjects(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	record := rootFSV3TestSandboxRecord("sandbox-lease", "team-1")
	record.RuntimeGeneration = 7
	record.CurrentPodNamespace = "sandbox-default"
	record.CurrentPodName = "sandbox-pod"
	require.NoError(t, store.UpsertSandbox(ctx, record))
	require.NoError(t, store.EnsureRootFSCaptureLease(ctx, record.ID, record.TeamID, record.RuntimeGeneration))
	prefix, err := rootfshead.TeamObjectPrefix(record.TeamID)
	require.NoError(t, err)
	resolvedTeam, err := rootfslease.NewRepository(pool).ResolveRootFSTeam(ctx, prefix)
	require.NoError(t, err)
	assert.Equal(t, record.TeamID, resolvedTeam)

	captureObject := rootFSV3TestObject(t, record.TeamID, rootfshead.ChunkMediaType, "capture orphan")
	require.NoError(t, store.CheckpointRootFSCapture(ctx, record.ID, record.TeamID, record.RuntimeGeneration, []rootfshead.Object{captureObject}))
	prefix, err = rootfshead.TeamPrefixFromObjectKey(captureObject.Key)
	require.NoError(t, err)
	deleter := &recordingRootFSV3ObjectDeleter{}
	deleted, err := store.DeleteUnknownRootFSObject(ctx, captureObject.Key, prefix, deleter)
	require.NoError(t, err)
	assert.False(t, deleted)
	assert.Empty(t, deleter.keys)
	trackedObject := rootFSV3TestObject(t, record.TeamID, rootfshead.ChunkMediaType, "tracked reusable object")
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_objects_v3 (
			object_key, team_id, digest, media_type, size,
			last_referenced_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, NOW(), NOW(), NOW())
	`, trackedObject.Key, record.TeamID, trackedObject.Digest, trackedObject.MediaType, trackedObject.Size)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_object_deletions (object_key, team_id)
		VALUES ($1, $2)
	`, trackedObject.Key, record.TeamID)
	require.NoError(t, err)
	deletedKeys, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{
		Limit: 10, ClaimedBy: "leased-delete-worker",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{trackedObject.Key}, deletedKeys)
	assert.Equal(t, []string{trackedObject.Key}, deleter.keys)

	_, err = pool.Exec(ctx, `UPDATE manager.sandboxes SET desired_state = 'paused' WHERE sandbox_id = $1`, record.ID)
	require.NoError(t, err)
	removed, err := store.CleanupStaleRootFSWriteLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)
	deleted, err = store.DeleteUnknownRootFSObject(ctx, captureObject.Key, prefix, deleter)
	require.NoError(t, err)
	assert.True(t, deleted)
	assert.ElementsMatch(t, []string{captureObject.Key, trackedObject.Key}, deleter.keys)
	resurrectedHead := rootFSV3TestHead(t, record.ID, record.ID, record.TeamID, "head-resurrect", record.RuntimeGeneration, nil)
	require.NoError(t, store.StageRootFSHead(ctx, resurrectedHead))
	jobs, err := store.ClaimRootFSInventoryJobs(ctx, "resurrect-worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	resurrectedObjects := append(rootFSV3TestObjects(t, resurrectedHead), trackedObject)
	require.NoError(t, store.CompleteRootFSInventoryJob(ctx, "resurrect-worker", resurrectedHead.Reference.HeadID, resurrectedHead.TeamID, resurrectedObjects))
	var deletedAt *time.Time
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT deleted_at FROM manager.rootfs_objects_v3 WHERE object_key = $1
	`, trackedObject.Key).Scan(&deletedAt))
	assert.Nil(t, deletedAt)

	writeObject := rootFSV3TestObject(t, record.TeamID, rootfshead.ChunkMediaType, "export orphan")
	writePrefix, err := rootfshead.TeamPrefixFromObjectKey(writeObject.Key)
	require.NoError(t, err)
	require.NoError(t, store.AcquireRootFSWriteLease(ctx, "export:head-1", record.TeamID, time.Hour))
	deleted, err = store.DeleteUnknownRootFSObject(ctx, writeObject.Key, writePrefix, deleter)
	require.NoError(t, err)
	assert.False(t, deleted)
	require.NoError(t, store.ReleaseRootFSWriteLease(ctx, "export:head-1", record.TeamID))
	deleted, err = store.DeleteUnknownRootFSObject(ctx, writeObject.Key, writePrefix, deleter)
	require.NoError(t, err)
	assert.True(t, deleted)
}

func TestRootFSV3CaptureLeaseAllowsPreparedResumeGeneration(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	const sandboxID = "sandbox-resume-lease"
	const teamID = "team-resume-lease"
	record := rootFSV3TestSandboxRecord(sandboxID, teamID)
	record.RuntimeGeneration = 7
	require.NoError(t, store.UpsertSandbox(ctx, record))
	_, err := pool.Exec(ctx, `UPDATE manager.sandboxes SET desired_state = 'paused' WHERE sandbox_id = $1`, sandboxID)
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(ctx, sandboxID, func(lockCtx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		return tx.BeginLifecycleTxn(lockCtx, &SandboxLifecycleTxn{
			ID:             "resume-lease-txn",
			SandboxID:      sandboxID,
			Kind:           SandboxLifecycleKindResume,
			Phase:          SandboxLifecyclePhasePreparing,
			FromGeneration: 7,
			ToGeneration:   8,
		})
	}))

	require.NoError(t, store.EnsureRootFSCaptureLease(ctx, sandboxID, teamID, 8))
	require.NoError(t, store.BeginRootFSCapture(ctx, sandboxID, teamID, 8))
	object := rootFSV3TestObject(t, teamID, rootfshead.ChunkMediaType, "prepared resume capture")
	require.NoError(t, store.CheckpointRootFSCapture(ctx, sandboxID, teamID, 8, []rootfshead.Object{object}))
	removed, err := store.CleanupStaleRootFSWriteLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed)
	var leases, objects int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM manager.rootfs_capture_leases_v3
			 WHERE sandbox_id = $1 AND runtime_generation = 8),
			(SELECT COUNT(*) FROM manager.rootfs_capture_lease_objects_v3
			 WHERE sandbox_id = $1 AND runtime_generation = 8)
	`, sandboxID).Scan(&leases, &objects))
	assert.Equal(t, 1, leases, "stale cleanup must preserve the target generation during resume")
	assert.Equal(t, 1, objects, "resume capture objects must remain GC-protected")
	require.Error(t, store.EnsureRootFSCaptureLease(ctx, sandboxID, teamID, 9))
}

func TestRootFSV3HeadPrefixGuardProtectsObjectsUntilInventory(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	const teamID = "team-prefix-guard"
	const sandboxID = "sandbox-prefix-guard"
	require.NoError(t, store.UpsertSandbox(ctx, rootFSV3TestSandboxRecord(sandboxID, teamID)))
	head := rootFSV3TestHead(t, sandboxID, sandboxID, teamID, "head-prefix-guard", 1, nil)
	require.NoError(t, store.SaveRootFSHead(ctx, head))

	var guards, registered int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM manager.rootfs_head_prefix_guards_v3 WHERE head_id = $1),
			(SELECT COUNT(*) FROM manager.rootfs_objects_v3 WHERE team_id = $2)
	`, head.Reference.HeadID, teamID).Scan(&guards, &registered))
	assert.Equal(t, 1, guards)
	assert.Equal(t, 3, registered, "Head publication must register only bounded descriptors")
	stats, err := store.RootFSInventoryStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(1), stats.Pending)
	assert.Equal(t, int64(1), stats.PrefixGuards)

	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	require.NoError(t, err)
	unknown := rootFSV3TestObject(t, teamID, rootfshead.ChunkMediaType, "unknown guarded object")
	deleter := &recordingRootFSV3ObjectDeleter{}
	deleted, err := store.DeleteUnknownRootFSObject(ctx, unknown.Key, prefix, deleter)
	require.NoError(t, err)
	assert.False(t, deleted)

	known := rootFSV3TestObject(t, teamID, rootfshead.ChunkMediaType, "known guarded object")
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_objects_v3 (
			object_key, team_id, digest, media_type, size,
			last_referenced_at, created_at, updated_at
		) VALUES ($1, $2, $3, $4, $5, NOW(), NOW() - INTERVAL '1 hour', NOW())
	`, known.Key, teamID, known.Digest, known.MediaType, known.Size)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `
		INSERT INTO manager.rootfs_object_deletions (object_key, team_id)
		VALUES ($1, $2)
	`, known.Key, teamID)
	require.NoError(t, err)
	deletedKeys, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{
		Limit: 10, ClaimedBy: "guarded-delete-worker",
	})
	require.NoError(t, err)
	assert.Empty(t, deletedKeys)
	assert.Empty(t, deleter.keys)

	jobs, err := store.ClaimRootFSInventoryJobs(ctx, "guard-inventory-worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, store.CompleteRootFSInventoryJob(
		ctx, "guard-inventory-worker", head.Reference.HeadID, teamID, rootFSV3TestObjects(t, head),
	))
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_head_prefix_guards_v3 WHERE head_id = $1
	`, head.Reference.HeadID).Scan(&guards))
	assert.Equal(t, 0, guards)
	stats, err = store.RootFSInventoryStats(ctx)
	require.NoError(t, err)
	require.NotNil(t, stats)
	assert.Equal(t, int64(1), stats.Complete)
	assert.Equal(t, int64(0), stats.PrefixGuards)

	deleted, err = store.DeleteUnknownRootFSObject(ctx, unknown.Key, prefix, deleter)
	require.NoError(t, err)
	assert.True(t, deleted)
	_, err = pool.Exec(ctx, `
		UPDATE manager.rootfs_object_deletions
		SET next_attempt_at = NOW(), claimed_by = '', claimed_until = NULL
		WHERE object_key = $1
	`, known.Key)
	require.NoError(t, err)
	deletedKeys, err = store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{
		Limit: 10, ClaimedBy: "unguarded-delete-worker",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{known.Key}, deletedKeys)
	assert.ElementsMatch(t, []string{unknown.Key, known.Key}, deleter.keys)
}

func TestRootFSV3CaptureLeaseEpochRotationDefersRowCleanup(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	const teamID = "team-capture-epoch"
	const sandboxID = "sandbox-capture-epoch"
	record := rootFSV3TestSandboxRecord(sandboxID, teamID)
	record.RuntimeGeneration = 7
	require.NoError(t, store.UpsertSandbox(ctx, record))
	require.NoError(t, store.EnsureRootFSCaptureLease(ctx, sandboxID, teamID, record.RuntimeGeneration))
	captureObject := rootFSV3TestObject(t, teamID, rootfshead.ChunkMediaType, "old capture epoch")
	require.NoError(t, store.CheckpointRootFSCapture(
		ctx, sandboxID, teamID, record.RuntimeGeneration, []rootfshead.Object{captureObject},
	))
	head := rootFSV3TestHead(t, sandboxID, sandboxID, teamID, "head-capture-epoch", record.RuntimeGeneration, nil)
	require.NoError(t, store.StageRootFSHead(ctx, head))
	require.NoError(t, store.ResetRootFSCapture(ctx, sandboxID, teamID, record.RuntimeGeneration))

	var active, protectAll bool
	var epoch, historicalObjects int64
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT active, protect_all, object_epoch,
			(SELECT COUNT(*) FROM manager.rootfs_capture_lease_objects_v3
			 WHERE sandbox_id = $1 AND runtime_generation = $2)
		FROM manager.rootfs_capture_leases_v3
		WHERE sandbox_id = $1 AND runtime_generation = $2
	`, sandboxID, record.RuntimeGeneration).Scan(&active, &protectAll, &epoch, &historicalObjects))
	assert.True(t, active)
	assert.False(t, protectAll)
	assert.Equal(t, int64(2), epoch)
	assert.Equal(t, int64(1), historicalObjects, "publication acknowledgement must not delete object rows")

	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	require.NoError(t, err)
	deleter := &recordingRootFSV3ObjectDeleter{}
	deleted, err := store.DeleteUnknownRootFSObject(ctx, captureObject.Key, prefix, deleter)
	require.NoError(t, err)
	assert.False(t, deleted, "the uninventoried Head guard must cover epoch rotation")
	jobs, err := store.ClaimRootFSInventoryJobs(ctx, "epoch-inventory-worker", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	require.NoError(t, store.CompleteRootFSInventoryJob(
		ctx, "epoch-inventory-worker", head.Reference.HeadID, teamID, rootFSV3TestObjects(t, head),
	))
	deleted, err = store.DeleteUnknownRootFSObject(ctx, captureObject.Key, prefix, deleter)
	require.NoError(t, err)
	assert.True(t, deleted)

	removed, err := store.CleanupStaleRootFSWriteLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), removed, "active current-epoch lease must remain")
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM manager.rootfs_capture_lease_objects_v3
		WHERE sandbox_id = $1 AND runtime_generation = $2
	`, sandboxID, record.RuntimeGeneration).Scan(&historicalObjects))
	assert.Equal(t, int64(0), historicalObjects)

	require.NoError(t, store.ReleaseRootFSCaptureLease(ctx, sandboxID, teamID, record.RuntimeGeneration))
	require.NoError(t, store.ReleaseRootFSCaptureLease(ctx, sandboxID, teamID, record.RuntimeGeneration))
	removed, err = store.CleanupStaleRootFSWriteLeases(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(1), removed)
}

func TestRootFSV3InventoryGCAndDurableObjectDeletion(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSV3TestSandboxRecord("sandbox-gc", "team-1")))
	head := rootFSV3TestHead(t, "sandbox-gc", "sandbox-gc", "team-1", "head-gc", 1, nil)
	head.CreatedAt = time.Now().Add(-time.Hour)
	require.NoError(t, store.SaveRootFSHead(ctx, head))
	exportObject := rootFSV3TestObject(t, head.TeamID, rootfshead.ExportLayerMediaType, "gc template export")
	require.NoError(t, store.SaveRootFSExport(ctx, &RootFSExport{
		HeadID: head.Reference.HeadID, TeamID: head.TeamID, Object: exportObject,
		DiffID: digest.FromString("gc template export diff").String(), CreatedAt: head.CreatedAt,
	}))
	jobs, err := store.ClaimRootFSInventoryJobs(ctx, "worker-1", 10, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	renewed, err := store.RenewRootFSInventoryJob(ctx, "worker-1", head.Reference.HeadID, time.Minute)
	require.NoError(t, err)
	assert.True(t, renewed)
	renewed, err = store.RenewRootFSInventoryJob(ctx, "different-worker", head.Reference.HeadID, time.Minute)
	require.NoError(t, err)
	assert.False(t, renewed)
	require.NoError(t, store.CompleteRootFSInventoryJob(ctx, "worker-1", head.Reference.HeadID, head.TeamID, rootFSV3TestObjects(t, head)))

	var complete bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT inventory_complete FROM manager.rootfs_heads_v3 WHERE head_id = $1`, head.Reference.HeadID).Scan(&complete))
	assert.True(t, complete)

	require.NoError(t, store.MarkSandboxDeleted(ctx, head.SandboxID, time.Now().UTC()))
	var bindings, filesystems, snapshots, lifecycleRefs, parentGuards, prefixGuards int
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM manager.sandbox_rootfs_bindings WHERE sandbox_id = $1),
			(SELECT COUNT(*) FROM manager.rootfs_filesystems WHERE head_id_v3 = $2),
			(SELECT COUNT(*) FROM manager.rootfs_snapshots WHERE head_id_v3 = $2),
			(SELECT COUNT(*) FROM manager.sandbox_lifecycle_txns
			 WHERE (expected_head_id_v3 = $2 OR prepared_head_id_v3 = $2)
			   AND phase IN ('preparing', 'barriered', 'publishing', 'committing')),
			(SELECT COUNT(*) FROM manager.rootfs_head_parent_guards_v3
			 WHERE child_head_id = $2 OR parent_head_id = $2),
			(SELECT COUNT(*) FROM manager.rootfs_head_prefix_guards_v3 WHERE head_id = $2)
	`, head.SandboxID, head.Reference.HeadID).Scan(
		&bindings, &filesystems, &snapshots, &lifecycleRefs, &parentGuards, &prefixGuards,
	))
	assert.Zero(t, bindings)
	assert.Zero(t, filesystems)
	assert.Zero(t, snapshots)
	assert.Zero(t, lifecycleRefs)
	assert.Zero(t, parentGuards)
	assert.Zero(t, prefixGuards)
	_, err = pool.Exec(ctx, `UPDATE manager.rootfs_objects_v3 SET created_at = NOW() - INTERVAL '1 hour'`)
	require.NoError(t, err)
	result, err := store.GarbageCollectRootFSV3(ctx, "team-1", time.Millisecond, 100)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, 1, result.DeletedHeads)
	assert.Equal(t, len(rootFSV3TestObjects(t, head))+1, result.QueuedObjects)

	deleter := &recordingRootFSV3ObjectDeleter{}
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{
		Limit: 100, ClaimedBy: "delete-worker",
	})
	require.NoError(t, err)
	wantDeleted := append(rootFSV3TestObjectKeys(t, head), exportObject.Key)
	assert.ElementsMatch(t, wantDeleted, deleted)
	assert.ElementsMatch(t, deleted, deleter.keys)
	assert.Equal(t, int64(0), rootFSV3CountRows(t, pool, "rootfs_object_deletions"))
}

func TestRootFSV3StorageUsageAndObjectAudit(t *testing.T) {
	ctx := context.Background()
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)

	require.NoError(t, store.UpsertSandbox(ctx, rootFSV3TestSandboxRecord("sandbox-audit", "team-1")))
	head := rootFSV3TestHead(t, "sandbox-audit", "sandbox-audit", "team-1", "head-audit", 1, nil)
	require.NoError(t, store.SaveRootFSHead(ctx, head))
	exportObject := rootFSV3TestObject(t, head.TeamID, rootfshead.ExportLayerMediaType, "audit template export")
	require.NoError(t, store.SaveRootFSExport(ctx, &RootFSExport{
		HeadID: head.Reference.HeadID, TeamID: head.TeamID, Object: exportObject,
		DiffID: digest.FromString("audit template export diff").String(),
	}))
	jobs, err := store.ClaimRootFSInventoryJobs(ctx, "worker-audit", 1, time.Minute)
	require.NoError(t, err)
	require.Len(t, jobs, 1)
	objects := rootFSV3TestObjects(t, head)
	require.NoError(t, store.CompleteRootFSInventoryJob(ctx, "worker-audit", head.Reference.HeadID, head.TeamID, objects))

	usage, err := store.ListRootFSStorageUsage(ctx, "team-1")
	require.NoError(t, err)
	require.Len(t, usage, 1)
	var wantBytes int64
	for _, object := range objects {
		wantBytes += object.Size
	}
	wantBytes += exportObject.Size
	assert.Equal(t, int64(len(objects)+1), usage[0].ObjectCount)
	assert.Equal(t, wantBytes, usage[0].StorageBytes)

	inspector := &rootFSV3ObjectInspector{sizes: make(map[string]int64), missing: map[string]bool{objects[0].Key: true}}
	for _, object := range objects {
		inspector.sizes[object.Key] = object.Size
	}
	inspector.sizes[exportObject.Key] = exportObject.Size
	audit, err := store.AuditRootFSObjects(ctx, inspector, "team-1", 100)
	require.NoError(t, err)
	assert.Equal(t, len(objects)+1, audit.Checked)
	assert.Equal(t, 1, audit.Missing)

	delete(inspector.missing, objects[0].Key)
	audit, err = store.AuditRootFSObjects(ctx, inspector, "team-1", 100)
	require.NoError(t, err)
	assert.Equal(t, 0, audit.Missing)
	var errorsRemaining int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM manager.rootfs_objects_v3 WHERE last_error <> '' OR missing_at IS NOT NULL`).Scan(&errorsRemaining))
	assert.Zero(t, errorsRemaining)
}

func newSandboxStoreIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool := newSandboxStoreIntegrationDatabase(t)
	require.NoError(t, RunSandboxStoreMigrations(context.Background(), pool, noopSandboxStoreMigrateLogger{}))
	return pool
}

func newSandboxStoreIntegrationPoolThrough(t *testing.T, maximumPrefix string) *pgxpool.Pool {
	t.Helper()
	pool := newSandboxStoreIntegrationDatabase(t)
	require.NoError(t, migrate.Up(context.Background(), pool, ".",
		migrate.WithBaseFS(sandboxStoreMigrationFilesThrough(t, maximumPrefix)),
		migrate.WithLogger(noopSandboxStoreMigrateLogger{}),
		migrate.WithSchema(sandboxStoreSchemaName),
	))
	return pool
}

func newSandboxStoreIntegrationDatabase(t *testing.T) *pgxpool.Pool {
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
	return pool
}

func sandboxStoreMigrationFilesThrough(t *testing.T, maximumPrefix string) fs.FS {
	t.Helper()
	files, err := fs.Glob(storemigrations.FS, "*.sql")
	require.NoError(t, err)

	selected := fstest.MapFS{}
	for _, name := range files {
		if len(name) < len(maximumPrefix) || name[:len(maximumPrefix)] > maximumPrefix {
			continue
		}
		data, err := fs.ReadFile(storemigrations.FS, name)
		require.NoError(t, err)
		selected[name] = &fstest.MapFile{Data: data, Mode: 0o444}
	}
	return selected
}

func rootFSV3TestSandboxRecord(sandboxID, teamID string) *SandboxRecord {
	return &SandboxRecord{
		ID: sandboxID, TeamID: teamID, UserID: "user-1",
		TemplateID: "template-1", TemplateName: "template-1", TemplateNamespace: "template-default",
		DesiredState: SandboxDesiredStateActive, CreatedAt: time.Now().UTC(),
	}
}

func rootFSTestSandboxRecord(sandboxID, teamID string) *SandboxRecord {
	return rootFSV3TestSandboxRecord(sandboxID, teamID)
}

type noopSandboxStoreMigrateLogger struct{}

func (noopSandboxStoreMigrateLogger) Printf(string, ...any) {}
func (noopSandboxStoreMigrateLogger) Fatalf(string, ...any) {}

func rootFSV3TestHead(t *testing.T, sandboxID, sourceSandboxID, teamID, headID string, generation int64, parent *rootfshead.HeadReference) *SandboxRootFSHead {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	require.NoError(t, err)
	object := func(mediaType, payload string) rootfshead.Object {
		digestValue := digest.FromString(payload)
		key, keyErr := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
		require.NoError(t, keyErr)
		return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
	}
	manifest := object(rootfshead.HeadMediaType, "head:"+headID)
	reference := rootfshead.HeadReference{Version: rootfshead.Version, HeadID: headID, Manifest: manifest}
	base := rootfshead.BaseIdentity{
		ImageReference: "docker.io/library/busybox@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ManifestDigest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		ChainID:        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		OS:             "linux", Architecture: "amd64",
	}
	composed, err := rootfshead.ComposeImage(prefix, reference, []byte(`{"architecture":"amd64","os":"linux","rootfs":{"type":"layers","diff_ids":[]}}`))
	require.NoError(t, err)
	return &SandboxRootFSHead{
		SandboxID: sandboxID, SourceSandboxID: sourceSandboxID, TeamID: teamID,
		RuntimeGeneration: generation, Parent: parent, Reference: reference, Base: base,
		Image:     composed.Reference,
		CreatedAt: time.Now().UTC(),
	}
}

func rootFSV3TestObject(t *testing.T, teamID, mediaType, payload string) rootfshead.Object {
	t.Helper()
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	require.NoError(t, err)
	digestValue := digest.FromString(payload)
	key, err := rootfshead.ObjectKey(prefix, mediaType, digestValue.String())
	require.NoError(t, err)
	return rootfshead.Object{Key: key, Digest: digestValue.String(), Size: int64(len(payload)), MediaType: mediaType}
}

func rootFSV3TestObjects(t *testing.T, head *SandboxRootFSHead) []rootfshead.Object {
	t.Helper()
	return []rootfshead.Object{
		rootFSV3TestObject(t, head.TeamID, rootfshead.DirectoryIndexMediaType, "directory:"+head.Reference.HeadID),
		rootFSV3TestObject(t, head.TeamID, rootfshead.ChunkMediaType, "data:"+head.Reference.HeadID),
		head.Reference.Manifest,
		head.Image.Marker,
		head.Image.Envelope,
	}
}

func rootFSV3TestObjectKeys(t *testing.T, head *SandboxRootFSHead) []string {
	t.Helper()
	objects := rootFSV3TestObjects(t, head)
	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		keys = append(keys, object.Key)
	}
	return keys
}

type recordingRootFSV3ObjectDeleter struct {
	keys []string
}

func (d *recordingRootFSV3ObjectDeleter) Delete(key string) error {
	d.keys = append(d.keys, key)
	return nil
}

type rootFSV3ObjectInspector struct {
	sizes   map[string]int64
	missing map[string]bool
}

func (i *rootFSV3ObjectInspector) StatRootFSObject(key string) (RootFSObjectInfo, error) {
	if i.missing[key] {
		return RootFSObjectInfo{}, fmt.Errorf("object %s is missing", key)
	}
	return RootFSObjectInfo{Key: key, Size: i.sizes[key], SizeIsLogical: true}, nil
}

func rootFSV3CountRows(t *testing.T, pool *pgxpool.Pool, table string) int64 {
	t.Helper()
	query := ""
	switch table {
	case "rootfs_object_deletions":
		query = "SELECT COUNT(*) FROM manager.rootfs_object_deletions"
	default:
		t.Fatalf("unexpected table %q", table)
	}
	var count int64
	require.NoError(t, pool.QueryRow(context.Background(), query).Scan(&count))
	return count
}
