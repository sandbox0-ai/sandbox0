package sandboxstore

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

// This suite exercises real PostgreSQL migrations, durable publication journals,
// immutable S3 PUT/GET and the production object deletion queue. It must run
// against an isolated endpoint; an unset endpoint is a skip, not validation.
func TestRootFSGenerationOverwriteRetentionS3Integration(t *testing.T) {
	for _, versions := range []int{24, 240} {
		t.Run(fmt.Sprint(versions), func(t *testing.T) {
			objects, prefix := generationRetentionS3Store(t)
			store, filesystem, current := newRetentionS3Filesystem(t, objects, prefix, "retention-overwrite")
			pool := store.pool
			options := rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize}
			var err error
			shared := bytes.Repeat([]byte{0xa7}, rootfsblock.LogicalBlockSize)
			for index := 1; index <= versions; index++ {
				updates := []rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{byte(index)}, rootfsblock.LogicalBlockSize)}}
				if index == 1 {
					updates = append(updates, rootfsblock.BlockUpdate{Sequence: 2, Block: 1, Data: shared})
				}
				current = publishRetentionCheckpoint(t, store, objects, options, current, filesystem.ID, fmt.Sprintf("overwrite-%02d", index), updates)
			}
			// Published retry journals retain their exact identities only for the
			// existing bounded terminal-retention window; expire that window in this trace.
			_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
			require.NoError(t, err)
			_, err = store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 1000)
			require.NoError(t, err)
			before := retentionS3Usage(t, objects, prefix)
			usageBefore, err := store.ListRootFSStorageUsage(t.Context(), "team-1")
			require.NoError(t, err)
			_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
			require.NoError(t, err)
			require.Equal(t, before, retentionS3Usage(t, objects, prefix), "incomplete descendant inventories must retain their ancestors")
			for {
				complete, err := store.InventoryRootFSGeneration(t.Context(), objects, 1)
				require.NoError(t, err)
				if !complete {
					break
				}
			}
			for index := 0; index < versions+5; index++ {
				_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 2, DeletePendingRootFSObjectsOptions{Limit: 100})
				require.NoError(t, err)
			}
			var generations int
			require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&generations))
			require.Equal(t, 1, generations, "obsolete generations in a live filesystem must converge to its retained head")
			after := retentionS3Usage(t, objects, prefix)
			require.LessOrEqual(t, after.objects, int64(4), "one base map plus current map/data and one shared data pack")
			require.Less(t, after.bytes, before.bytes)
			usageAfter, err := store.ListRootFSStorageUsage(t.Context(), "team-1")
			require.NoError(t, err)
			require.Len(t, usageBefore, 1)
			require.Len(t, usageAfter, 1)
			require.Less(t, usageAfter[0].StorageBytes, usageBefore[0].StorageBytes)
			decoded, err := rootfsblock.DecodeDescriptor(current.Descriptor)
			require.NoError(t, err)
			reader, err := rootfsblock.NewReader(objects, decoded, 0)
			require.NoError(t, err)
			actual := make([]byte, 4*rootfsblock.LogicalBlockSize)
			_, err = reader.ReadAt(actual, 0)
			require.NoError(t, err)
			require.Equal(t, bytes.Repeat([]byte{byte(versions)}, rootfsblock.LogicalBlockSize), actual[:rootfsblock.LogicalBlockSize])
			require.Equal(t, shared, actual[rootfsblock.LogicalBlockSize:2*rootfsblock.LogicalBlockSize], "an inherited data pack survives its original generation")
			require.Equal(t, make([]byte, 2*rootfsblock.LogicalBlockSize), actual[2*rootfsblock.LogicalBlockSize:])
			t.Logf("%d overwrites: S3 objects %d -> %d, bytes %d -> %d; tenant bytes %d -> %d", versions, before.objects, after.objects, before.bytes, after.bytes, usageBefore[0].StorageBytes, usageAfter[0].StorageBytes)

		})
	}
}

func TestRootFSGenerationSnapshotForkRestoreRetentionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, initial := newRetentionS3Filesystem(t, objects, prefix, "retention-snapshots")
	options := rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize}
	value := func(b byte) []rootfsblock.BlockUpdate {
		return []rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{b}, rootfsblock.LogicalBlockSize)}}
	}
	pinned := publishRetentionCheckpoint(t, store, objects, options, initial, filesystem.ID, "snapshot-pinned", value(7))
	snapshot, err := store.CreateRootFSSnapshot(t.Context(), &CreateRootFSSnapshotRequest{SandboxID: filesystem.ID, SnapshotID: "named-snapshot"})
	require.NoError(t, err)
	child := rootFSTestSandboxRecord("retention-fork", "team-1")
	child.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(t.Context(), child))
	fork, err := store.ForkRootFSFilesystem(t.Context(), &ForkRootFSFilesystemRequest{SourceSandboxID: filesystem.ID, TargetSandboxID: child.ID})
	require.NoError(t, err)
	require.Equal(t, pinned.ID, fork.HeadGenerationID)
	current := publishRetentionCheckpoint(t, store, objects, options, pinned, filesystem.ID, "snapshot-overwritten", value(9))
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 1000)
	require.NoError(t, err)
	for range 10 {
		_, err = store.InventoryRootFSGeneration(t.Context(), objects, 1)
		require.NoError(t, err)
	}
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	assertRetentionContents(t, objects, current, 9)
	assertRetentionContents(t, objects, pinned, 7)
	restored, err := store.RestoreRootFSFromSnapshot(t.Context(), &RestoreRootFSFromSnapshotRequest{SandboxID: child.ID,
		SnapshotID: snapshot.ID, TeamID: "team-1", OperationID: "retention-restore"})
	require.NoError(t, err)
	copy, err := store.GetRootFSGeneration(t.Context(), restored.HeadGenerationID)
	require.NoError(t, err)
	require.NoError(t, store.DeleteRootFSSnapshot(t.Context(), snapshot.ID, "team-1"))
	// The restored clone has not completed its inventory yet. Its temporary
	// dependency protects the original map even after explicit snapshot removal.
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	assertRetentionContents(t, objects, copy, 7)

	for range 10 {
		_, err = store.InventoryRootFSGeneration(t.Context(), objects, 1)
		require.NoError(t, err)
	}
	require.NoError(t, store.MarkSandboxDeleted(t.Context(), filesystem.ID, time.Now().UTC()))
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	assertRetentionContents(t, objects, copy, 7)
	assertRetentionContents(t, objects, pinned, 7)
	usages, err := store.ListRootFSStorageUsage(t.Context(), "team-1")
	require.NoError(t, err)
	require.Len(t, usages, 1)
	require.Positive(t, usages[0].StorageBytes, "restored fork storage remains billable after deleting its snapshot and original sandbox")
}

func assertRetentionContents(t *testing.T, objects rootfsblock.RangeSource, generation *RootFSGeneration, value byte) {
	t.Helper()
	descriptor, err := rootfsblock.DecodeDescriptor(generation.Descriptor)
	require.NoError(t, err)
	reader, err := rootfsblock.NewReader(objects, descriptor, 0)
	require.NoError(t, err)
	actual := make([]byte, rootfsblock.LogicalBlockSize)
	_, err = reader.ReadAt(actual, 0)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{value}, len(actual)), actual)
}

func TestRootFSGenerationBoundedInventoryAndDeletionRetryS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, initial := newRetentionS3Filesystem(t, objects, prefix, "retention-restart")
	options := rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize, PageEntries: 2}
	updates := make([]rootfsblock.BlockUpdate, 8)
	for index := range updates {
		updates[index] = rootfsblock.BlockUpdate{Sequence: uint64(index + 1), Block: uint64(index), Data: bytes.Repeat([]byte{byte(index + 1)}, rootfsblock.LogicalBlockSize)}
	}
	first := publishRetentionCheckpoint(t, store, objects, options, initial, filesystem.ID, "restart-parent", updates)
	current := publishRetentionCheckpoint(t, store, objects, options, first, filesystem.ID, "restart-current", []rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{93}, rootfsblock.LogicalBlockSize)}})
	_, err := store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 1000)
	require.NoError(t, err)
	complete, err := store.InventoryRootFSGeneration(t.Context(), objects, 1)
	require.NoError(t, err)
	require.False(t, complete, "one page cannot complete a multi-level inventory")
	deleted, err := store.DeleteUnreferencedRootFSGenerations(t.Context(), "", 100)
	require.NoError(t, err)
	require.Zero(t, deleted, "partial inventories retain ancestry")
	partialID := current.ID
	current = publishRetentionCheckpoint(t, store, objects, options, current, filesystem.ID, "restart-newer-head",
		[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{94}, rootfsblock.LogicalBlockSize)}})
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 1000)
	require.NoError(t, err)
	// A new worker instance resumes the committed page queue; no in-memory
	// traversal state is needed after process loss or a newer head publication.
	store = NewPGSandboxStore(store.pool)
	for steps := 0; steps < 100; steps++ {
		_, err = store.InventoryRootFSGeneration(t.Context(), objects, 1)
		require.NoError(t, err)
		require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT inventory_complete FROM manager.rootfs_generations WHERE generation_id=$1`, current.ID).Scan(&complete))
		if complete {
			break
		}
	}
	require.True(t, complete)
	var resumed bool
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT inventory_complete FROM manager.rootfs_generations WHERE generation_id=$1`, partialID).Scan(&resumed))
	require.True(t, resumed, "a partially inventoried former head must not lose its persisted work when the head advances")
	failing := &retentionFailingDeleter{Store: objects}
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), failing, "", 100, DeletePendingRootFSObjectsOptions{Limit: 100, ContinueOnError: true})
	require.Error(t, err)
	var pending, attempts int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*),SUM(attempts) FROM manager.rootfs_object_deletions`).Scan(&pending, &attempts))
	require.Positive(t, pending)
	require.Positive(t, attempts)
	assertRetentionContents(t, objects, current, 94)
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_object_deletions SET next_attempt_at=NOW()-INTERVAL '1 minute'`)
	require.NoError(t, err)
	_, err = NewPGSandboxStore(store.pool).GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{Limit: 100})
	require.NoError(t, err)
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_object_deletions`).Scan(&pending))
	require.Zero(t, pending)
	assertRetentionContents(t, objects, current, 94)
}

func TestRootFSGenerationDeletedOriginForkRetentionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, source, initial := newRetentionS3Filesystem(t, objects, prefix, "retention-origin")
	options := rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize}
	first := publishRetentionCheckpoint(t, store, objects, options, initial, source.ID, "origin-first",
		[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{1}, rootfsblock.LogicalBlockSize)}})
	child := rootFSTestSandboxRecord("retention-evolved-fork", "team-1")
	child.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(t.Context(), child))
	fork, err := store.ForkRootFSFilesystem(t.Context(), &ForkRootFSFilesystemRequest{SourceSandboxID: source.ID, TargetSandboxID: child.ID})
	require.NoError(t, err)
	current := publishRetentionCheckpoint(t, store, objects, options, first, fork.ID, "origin-fork-own-head",
		[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{2}, rootfsblock.LogicalBlockSize)}})
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 1000)
	require.NoError(t, err)
	require.NoError(t, store.MarkSandboxDeleted(t.Context(), source.ID, time.Now().UTC()))
	// Model an existing database produced by the previous deletion path: its
	// origin row retained a former head because child provenance blocked removal.
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_filesystems SET head_generation_id=$2 WHERE filesystem_id=$1`, source.ID, first.ID)
	require.NoError(t, err)
	for range 10 {
		_, err = store.InventoryRootFSGeneration(t.Context(), objects, 1)
		require.NoError(t, err)
		_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 100, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
	}
	var generations int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&generations))
	require.Equal(t, 1, generations, "origin metadata must not retain its obsolete generations after a fork owns its complete mapping")
	assertRetentionContents(t, objects, current, 2)
	var originHead string
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COALESCE(head_generation_id,'') FROM manager.rootfs_filesystems WHERE filesystem_id=$1`, source.ID).Scan(&originHead))
	require.Empty(t, originHead)
	loadedFork, err := store.GetRootFSFilesystem(t.Context(), fork.ID)
	require.NoError(t, err)
	require.Equal(t, source.ID, loadedFork.SourceFilesystemID, "immutable fork provenance is preserved")
}

type retentionFailingDeleter struct{ objectstore.Store }

func (*retentionFailingDeleter) Delete(string) error {
	return errors.New("injected S3 deletion outage")
}

func TestRootFSGenerationConcurrentCheckpointSnapshotGCS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, current := newRetentionS3Filesystem(t, objects, prefix, "retention-concurrent")
	options := rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize}
	ctx, stop := context.WithCancel(t.Context())
	var workers sync.WaitGroup
	failures := make(chan error, 2)
	var mu sync.Mutex
	var snapshots []*RootFSSnapshot
	var forks []*RootFSFilesystem
	var published atomic.Int32
	for worker := range 2 {
		target := rootFSTestSandboxRecord(fmt.Sprintf("concurrent-fork-%d", worker), "team-1")
		target.DesiredState = SandboxDesiredStatePaused
		require.NoError(t, store.UpsertSandbox(t.Context(), target))
		workers.Add(1)
		go func() {
			defer workers.Done()
			acknowledged := 0
			forked := false
			for cycle := 0; ctx.Err() == nil; cycle++ {
				_, err := store.ReconcileRootFSGenerationMaterializationGarbage(ctx, time.Hour, time.Hour, 20)
				if err == nil {
					_, err = store.InventoryRootFSGeneration(ctx, objects, 2)
				}
				if err == nil {
					_, err = store.GarbageCollectRootFSFilesystemWithOptions(ctx, objects, "", 2,
						DeletePendingRootFSObjectsOptions{Limit: 10, ClaimedBy: fmt.Sprintf("concurrent-%d", worker)})
				}
				if err != nil && ctx.Err() == nil && !retentionRetryableConflict(err) {
					failures <- err
					return
				}
				if cycle%5 == 0 && acknowledged < 2 {
					snapshot, err := store.CreateRootFSSnapshot(ctx, &CreateRootFSSnapshotRequest{SandboxID: filesystem.ID,
						SnapshotID: fmt.Sprintf("concurrent-snapshot-%d-%d", worker, cycle)})
					if err == nil {
						acknowledged++
						mu.Lock()
						snapshots = append(snapshots, snapshot)
						mu.Unlock()
					} else if ctx.Err() == nil && !errors.Is(err, ErrRootFSFilesystemNotFound) && !retentionRetryableConflict(err) {
						failures <- err
						return
					}
				}
				if !forked && published.Load() >= 4 {
					fork, err := store.ForkRootFSFilesystem(ctx, &ForkRootFSFilesystemRequest{SourceSandboxID: filesystem.ID, TargetSandboxID: target.ID})
					if err == nil {
						forked = true
						mu.Lock()
						forks = append(forks, fork)
						mu.Unlock()
					} else if ctx.Err() == nil && !errors.Is(err, ErrRootFSFilesystemNotFound) && !errors.Is(err, ErrRootFSFilesystemConflict) && !retentionRetryableConflict(err) {
						failures <- err
						return
					}
				}
			}
		}()
	}
	t.Cleanup(func() { stop(); workers.Wait() })
	for index := 1; index <= 12; index++ {
		current = publishRetentionCheckpoint(t, store, objects, options, current, filesystem.ID, fmt.Sprintf("concurrent-%d", index),
			[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{byte(index)}, rootfsblock.LogicalBlockSize)}})
		_, err := store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days' WHERE state='published'`)
		require.NoError(t, err)
		published.Store(int32(index))
		if index == 4 {
			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return len(forks) == 2
			}, 5*time.Second, 10*time.Millisecond, "both concurrent fork acknowledgments must be established before later head replacement")
		}
	}
	stop()
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
	for range 20 {
		_, err := store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 20)
		require.NoError(t, err)
		_, err = store.InventoryRootFSGeneration(t.Context(), objects, 2)
		require.NoError(t, err)
		_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), objects, "", 2, DeletePendingRootFSObjectsOptions{Limit: 20})
		require.NoError(t, err)
	}
	var remaining int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&remaining))
	require.LessOrEqual(t, remaining, 1+len(snapshots)+len(forks), "unacknowledged histories are reclaimed while acknowledged snapshots/forks remain")
	assertRetentionContents(t, objects, current, 12)
	for _, snapshot := range snapshots {
		generation, err := store.GetRootFSGeneration(t.Context(), snapshot.HeadGenerationID)
		require.NoError(t, err, "every acknowledged concurrent snapshot retains its immutable content")
		value := 0
		if strings.HasPrefix(generation.ID, "concurrent-") {
			value, err = strconv.Atoi(strings.TrimPrefix(generation.ID, "concurrent-"))
			require.NoError(t, err)
		}
		assertRetentionContents(t, objects, generation, byte(value))
	}
	for _, fork := range forks {
		generation, err := store.GetRootFSGeneration(t.Context(), fork.HeadGenerationID)
		require.NoError(t, err)
		assertRetentionContents(t, objects, generation, 4)
	}
	t.Logf("12 checkpoint publications with two inventory/GC workers: %d acknowledged snapshots and %d forks remain readable", len(snapshots), len(forks))
}

func retentionRetryableConflict(err error) bool {
	var pgError *pgconn.PgError
	return errors.As(err, &pgError) && (pgError.Code == "40001" || pgError.Code == "40P01" || pgError.Code == "23503")
}

func TestRootFSGenerationObjectRecreationAfterDeletionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, initial := newRetentionS3Filesystem(t, objects, prefix, "retention-object-recreate")
	options := rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize}
	first := publishRetentionCheckpoint(t, store, objects, options, initial, filesystem.ID, "recreate-first",
		[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{1}, rootfsblock.LogicalBlockSize)}})
	var old rootfsblock.ObjectReference
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT object.object_key,object.object_kind,object.object_size,object.checksum
		FROM manager.rootfs_generation_materialization_objects locator JOIN manager.rootfs_materialization_objects object USING(object_key)
		WHERE locator.generation_id=$1 AND object.object_kind='data_pack'`, first.ID).Scan(&old.Key, &old.Kind, &old.Size, &old.Checksum))
	current := publishRetentionCheckpoint(t, store, objects, options, first, filesystem.ID, "recreate-second",
		[]rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{2}, rootfsblock.LogicalBlockSize)}})
	_, err := store.pool.Exec(t.Context(), `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = store.ReconcileRootFSGenerationMaterializationGarbage(t.Context(), time.Hour, time.Hour, 100)
	require.NoError(t, err)
	_, err = store.InventoryRootFSGeneration(t.Context(), objects, 10)
	require.NoError(t, err)
	_, err = store.DeleteUnreferencedRootFSGenerations(t.Context(), "", 100)
	require.NoError(t, err)
	var queued bool
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT EXISTS(SELECT 1 FROM manager.rootfs_object_deletions WHERE object_key=$1)`, old.Key).Scan(&queued))
	require.True(t, queued)
	// Establish a fresh pending materialization whose final block is identical
	// to the collected old block. Its registered-before-PUT protocol must refuse
	// the old key while a physical deletion can still be in flight.
	probe := compositeTestGeneration(t, current, filesystem.ID, "recreate-third", 1)
	probe.WriterEpoch = current.WriterEpoch + 1
	require.NoError(t, insertCompositeTestGeneration(t.Context(), store.pool, probe))
	identities := []RootFSGenerationMaterializationIdentity{{GenerationID: probe.ID, ExpectedLocatorVersion: probe.LocatorVersion, ExpectedDescriptor: probe.Descriptor}}
	lane := RootFSMaterializationPackLane("team-1", probe.FormatGeneration)
	batchID, err := RootFSMaterializationBatchID(lane, identities)
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(t.Context(), &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, PackLane: lane, TeamID: "team-1", FormatGeneration: probe.FormatGeneration, Members: identities})
	require.NoError(t, err)
	err = store.RegisterRootFSGenerationMaterializationBatchObject(t.Context(), batchID, old)
	require.ErrorContains(t, err, "pending deletion")
	_, err = store.DeletePendingRootFSObjectsWithOptions(t.Context(), objects, DeletePendingRootFSObjectsOptions{Limit: 100})
	require.NoError(t, err)
	_, err = objects.Head(old.Key)
	require.Error(t, err, "the old object must really have been removed from S3")
	descriptor, err := rootfsblock.DecodeDescriptor(probe.Descriptor)
	require.NoError(t, err)
	result, err := rootfsblock.BuildIncrementalGeneration(t.Context(), objects, descriptor, nil,
		retentionJournalPublisher{store: store, objects: objects, batchID: batchID}, options)
	require.NoError(t, err)
	require.Contains(t, result.References, old, "the same content-addressed object identity is safely recreated")
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(t.Context(), &PublishRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, Members: []RootFSGenerationMaterializationPublication{{GenerationID: probe.ID, ExpectedLocatorVersion: probe.LocatorVersion,
			ExpectedDescriptor: probe.Descriptor, MaterializedDescriptor: result.Payload, References: result.References}}}))
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_filesystems SET head_generation_id=$2,writer_epoch=$3 WHERE filesystem_id=$1`, filesystem.ID, probe.ID, probe.WriterEpoch)
	require.NoError(t, err)
	loaded, err := store.GetRootFSGeneration(t.Context(), probe.ID)
	require.NoError(t, err)
	assertRetentionContents(t, objects, loaded, 1)
}

// Exercise the real regional pause/resume transactions and writer/slot fences
// with S3-backed disk contents. Node execution receipts are fixture receipts;
// this test does not claim to launch a Nomad allocation or gVisor process.
func TestRootFSGenerationPauseResumeRetentionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	base, err := rootfsblock.BuildMaterializedGeneration(t.Context(), retentionZeroImage{}, 1<<30,
		rootfsblock.ObjectStorePublisher{Store: objects}, rootfsblock.BuildOptions{ObjectPrefix: prefix})
	require.NoError(t, err)
	request := readyRootFSBaseArtifactTestRequest()
	request.Descriptor, request.BaseBlockRoot = base.Payload, base.Descriptor.MappingRoot.RootDigest
	f := newNomadPauseStoreFixtureWithBase(t, "retention-s3-resume", newSandboxStoreIntegrationPool(t), "", request)
	for _, ref := range base.References {
		_, err = f.pool.Exec(f.ctx, `INSERT INTO manager.rootfs_materialization_objects
			(object_key,object_kind,object_size,checksum,uploaded_at) VALUES ($1,$2,$3,$4,clock_timestamp())`, ref.Key, ref.Kind, ref.Size, ref.Checksum)
		require.NoError(t, err)
		_, err = f.pool.Exec(f.ctx, `INSERT INTO manager.rootfs_base_artifact_objects VALUES ($1,$2)`, request.ArtifactDigest, ref.Key)
		require.NoError(t, err)
	}
	sealed, payload, err := rootfsblock.BuildCompositeGeneration(base.Descriptor, []rootfsblock.BlockUpdate{
		{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{71}, rootfsblock.LogicalBlockSize)}})
	require.NoError(t, err)
	next := *f.initial
	next.ID, next.ParentGenerationID, next.CurrentBlockHead = "retention-pause-generation", f.initial.ID, sealed.MappingRoot.RootDigest
	next.WriterEpoch, next.LocatorVersion, next.Descriptor, next.DurabilityState = f.writerEpoch, f.initial.LocatorVersion+1, payload, RootFSGenerationStateCompositeDurable
	pause, err := f.store.RequestNomadSandboxPause(f.ctx, f.sandboxID, SandboxLifecycleSourceManual)
	require.NoError(t, err)
	f.publishPlannedPause(t, pause.OperationID, &next)
	terminalizeNomadPauseSlot(t, f, pause)
	func() {
		// SQL's NULL <> 32 is unknown, not true. A terminal state with no
		// physical receipt must not release the original launch generation.
		tx, err := f.pool.Begin(f.ctx)
		require.NoError(t, err)
		defer func() { _ = tx.Rollback(context.Background()) }()
		_, err = tx.Exec(f.ctx, `UPDATE manager.runtime_slots SET source_generation_ref=NULL,terminal_proof_digest=NULL WHERE slot_id=$1`, f.slotID)
		require.ErrorContains(t, err, "physical terminal proof")
	}()
	identities := []RootFSGenerationMaterializationIdentity{{GenerationID: next.ID, ExpectedLocatorVersion: next.LocatorVersion, ExpectedDescriptor: payload}}
	lane := RootFSMaterializationPackLane("team-slot", next.FormatGeneration)
	batchID, err := RootFSMaterializationBatchID(lane, identities)
	require.NoError(t, err)
	_, err = f.store.BeginRootFSGenerationMaterializationBatch(f.ctx, &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, PackLane: lane, TeamID: "team-slot", FormatGeneration: next.FormatGeneration, Members: identities})
	require.NoError(t, err)
	result, err := rootfsblock.BuildIncrementalGeneration(f.ctx, objects, sealed, nil,
		retentionJournalPublisher{store: f.store, objects: objects, batchID: batchID}, rootfsblock.BuildOptions{ObjectPrefix: prefix})
	require.NoError(t, err)
	require.NoError(t, f.store.PublishRootFSGenerationMaterializationBatch(f.ctx, &PublishRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, Members: []RootFSGenerationMaterializationPublication{{GenerationID: next.ID, ExpectedLocatorVersion: next.LocatorVersion,
			ExpectedDescriptor: payload, MaterializedDescriptor: result.Payload, References: result.References}}}))
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_materialization_batches SET updated_at=NOW()-INTERVAL '2 days'`)
	require.NoError(t, err)
	_, err = f.store.ReconcileRootFSGenerationMaterializationGarbage(f.ctx, time.Hour, time.Hour, 100)
	require.NoError(t, err)
	for range 3 {
		_, err = f.store.InventoryRootFSGeneration(f.ctx, objects, 100)
		require.NoError(t, err)
		_, err = f.store.GarbageCollectRootFSFilesystemWithOptions(f.ctx, objects, "", 100, DeletePendingRootFSObjectsOptions{})
		require.NoError(t, err)
	}
	paused, err := f.store.GetRootFSGeneration(f.ctx, next.ID)
	require.NoError(t, err)
	assertRetentionContents(t, objects, paused, 71)
	_, err = f.store.GetRootFSGeneration(f.ctx, f.initial.ID)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound, "terminal slot launch history must release the old generation")
	slot, err := f.store.GetRuntimeSlot(f.ctx, f.slotID)
	require.NoError(t, err)
	require.Equal(t, f.initial.ID, slot.SourceGenerationID, "exact launch identity remains available after collection")
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{SandboxID: f.sandboxID, ExpectedTeamID: "team-slot"})
	require.NoError(t, err)
	require.Equal(t, next.ID, candidate.SourceGenerationID)
	runtime := prepareNomadResumeRuntime(t, f, candidate, "retention-s3")
	_, err = f.store.MarkRuntimeSlotCommandReady(f.ctx, &MarkRuntimeSlotCommandReadyRequest{
		SlotID: runtime.claimed.ID, AllocationID: runtime.registration.AllocationID,
		NodeUID: runtime.registration.NodeUID, NodeBootID: runtime.registration.NodeBootID,
		OperationID: candidate.OperationID, ClaimID: runtime.acquire.ClaimID,
		ProcdInstanceID: "retention-procd", ProcdAddress: "http://192.0.2.10:49983", CommandReadyDigest: bytes.Repeat([]byte{0xa4}, 32)})
	require.NoError(t, err)
	completed, err := f.store.CompleteNomadSandboxResume(f.ctx, &CompleteNomadSandboxResumeRequest{
		SandboxID: f.sandboxID, OperationID: candidate.OperationID, SlotID: runtime.claimed.ID,
		AllocationID: runtime.registration.AllocationID, AllocationNamespace: runtime.registration.AllocationNamespace,
		ResourceLeaseID: runtime.claimed.ResourceLease.LeaseID, ResourceLeaseDigest: runtime.claimed.ResourceLeaseDigest})
	require.NoError(t, err)
	require.Equal(t, SandboxDesiredStateActive, completed.DesiredState)
	_, err = f.store.GarbageCollectRootFSFilesystemWithOptions(f.ctx, objects, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	resumed, err := f.store.GetRootFSGeneration(f.ctx, candidate.SourceGenerationID)
	require.NoError(t, err)
	assertRetentionContents(t, objects, resumed, 71)
}

func newRetentionS3Filesystem(t *testing.T, objects objectstore.ContextConditionalStore, prefix, sandboxID string) (*PGSandboxStore, *RootFSFilesystem, *RootFSGeneration) {
	t.Helper()
	return newRetentionS3FilesystemWithPool(t, objects, prefix, sandboxID, newSandboxStoreIntegrationPool(t))
}

func newRetentionS3FilesystemWithPool(t *testing.T, objects objectstore.ContextConditionalStore, prefix, sandboxID string, pool *pgxpool.Pool) (*PGSandboxStore, *RootFSFilesystem, *RootFSGeneration) {
	t.Helper()
	store := NewPGSandboxStore(pool)
	publisher := rootfsblock.ObjectStorePublisher{Store: objects}
	base, err := rootfsblock.BuildMaterializedGeneration(t.Context(), retentionZeroImage{}, 1<<30, publisher, rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.DefaultDataRangeBytes})
	require.NoError(t, err)
	request := readyRootFSBaseArtifactTestRequest()
	request.Descriptor, request.LogicalSizeBytes, request.BaseBlockRoot = base.Payload, base.Descriptor.LogicalSizeBytes, base.Descriptor.MappingRoot.RootDigest
	artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), request)
	require.NoError(t, err)
	for _, ref := range base.References {
		_, err = pool.Exec(t.Context(), `INSERT INTO manager.rootfs_materialization_objects
			(object_key,object_kind,object_size,checksum,uploaded_at) VALUES ($1,$2,$3,$4,clock_timestamp())`, ref.Key, ref.Kind, ref.Size, ref.Checksum)
		require.NoError(t, err)
		_, err = pool.Exec(t.Context(), `INSERT INTO manager.rootfs_base_artifact_objects VALUES ($1,$2)`, artifact.ArtifactDigest, ref.Key)
		require.NoError(t, err)
	}
	record := rootFSTestSandboxRecord(sandboxID, "team-1")
	record.DesiredState = SandboxDesiredStatePaused
	require.NoError(t, store.UpsertSandbox(t.Context(), record))
	filesystem, current, err := store.EnsureInitialRootFSGeneration(t.Context(), &EnsureInitialRootFSGenerationRequest{
		SandboxID: record.ID, TeamID: "team-1", SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	})
	require.NoError(t, err)
	return store, filesystem, current
}

func publishRetentionCheckpoint(t *testing.T, store *PGSandboxStore, objects objectstore.ContextConditionalStore, options rootfsblock.BuildOptions, parent *RootFSGeneration, filesystemID, id string, updates []rootfsblock.BlockUpdate) *RootFSGeneration {
	t.Helper()
	descriptor, err := rootfsblock.DecodeDescriptor(parent.Descriptor)
	require.NoError(t, err)
	sealed, payload, err := rootfsblock.BuildCompositeGeneration(descriptor, updates)
	require.NoError(t, err)
	generation := &RootFSGeneration{ID: id, FilesystemID: filesystemID, ParentGenerationID: parent.ID,
		SourceOCIDigest: parent.SourceOCIDigest, BaseArtifactDigest: parent.BaseArtifactDigest, BaseBlockRoot: parent.BaseBlockRoot,
		CurrentBlockHead: sealed.MappingRoot.RootDigest, WriterEpoch: parent.WriterEpoch + 1, FormatGeneration: parent.FormatGeneration,
		DurabilityState: RootFSGenerationStateCompositeDurable, LocatorVersion: parent.LocatorVersion + 1, Descriptor: payload}
	// Exercise the real sealed checkpoint/head publication transaction, rather
	// than synthesizing a head update outside its fenced writer lifecycle.
	binding := sha256.Sum256([]byte(id))
	issue := rootFSWriterGrantTestIssueRequest(filesystemID, id+"-writer", id+"-claim", id+"-slot", binding[:])
	filesystem, err := store.GetRootFSFilesystem(t.Context(), filesystemID)
	require.NoError(t, err)
	issue.ExpectedFilesystemID, issue.InitialGenerationID, issue.ExpectedWriterEpoch = filesystemID, parent.ID, filesystem.WriterEpoch
	issued, err := store.IssueRootFSWriterGrant(t.Context(), issue)
	require.NoError(t, err)
	generation.WriterEpoch = issued.Grant.WriterEpoch
	_, err = store.ConsumeRootFSWriterGrant(t.Context(), &ConsumeRootFSWriterGrantRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, RawToken: issue.RawToken,
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ConsumerNodeUID: "node-a", ConsumerAgentUID: "ctld-a", LeaseTTL: time.Minute})
	require.NoError(t, err)
	_, err = store.BeginRootFSWriterRetire(t.Context(), &BeginRootFSWriterRetireRequest{
		GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: id + "-txn",
		BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ExpectedOldGenerationID: parent.ID})
	require.NoError(t, err)
	require.NoError(t, store.WithSandboxLock(t.Context(), filesystemID, func(ctx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
		if err := tx.BeginLifecycleTxn(ctx, &SandboxLifecycleTxn{ID: id + "-txn", SandboxID: filesystemID,
			Kind: SandboxLifecycleKindPause, Phase: SandboxLifecyclePhasePublishing, ExpectedGenerationID: parent.ID}); err != nil {
			return err
		}
		_, err := tx.(RootFSWriterGrantTx).CompleteRootFSWriterRetireAndPublishGeneration(ctx, &CompleteRootFSWriterRetireAndPublishGenerationRequest{
			LifecycleTxnID: id + "-txn", GrantID: issue.GrantID, WriterEpoch: issued.Grant.WriterEpoch, OperationID: id + "-txn",
			BindingVersion: RootFSWriterBindingVersion, BindingDigest: binding[:], ProofDigest: binding[:], ExpectedOldGenerationID: parent.ID, Generation: generation})
		return err
	}))
	identities := []RootFSGenerationMaterializationIdentity{{GenerationID: id, ExpectedLocatorVersion: generation.LocatorVersion, ExpectedDescriptor: payload}}
	lane := RootFSMaterializationPackLane("team-1", generation.FormatGeneration)
	batchID, err := RootFSMaterializationBatchID(lane, identities)
	require.NoError(t, err)
	_, err = store.BeginRootFSGenerationMaterializationBatch(t.Context(), &BeginRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, PackLane: lane, TeamID: "team-1", FormatGeneration: generation.FormatGeneration, Members: identities})
	require.NoError(t, err)
	result, err := rootfsblock.BuildIncrementalGeneration(t.Context(), objects, descriptor, updates, retentionJournalPublisher{store: store, objects: objects, batchID: batchID}, options)
	require.NoError(t, err)
	for _, ref := range result.References {
		require.NoError(t, store.RegisterRootFSGenerationMaterializationBatchObject(t.Context(), batchID, ref))
		require.NoError(t, store.MarkRootFSGenerationMaterializationBatchObjectUploaded(t.Context(), batchID, ref.Key))
	}
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(t.Context(), &PublishRootFSGenerationMaterializationBatchRequest{
		BatchID: batchID, Members: []RootFSGenerationMaterializationPublication{{GenerationID: id, ExpectedLocatorVersion: generation.LocatorVersion,
			ExpectedDescriptor: payload, MaterializedDescriptor: result.Payload, References: result.References}}}))
	loaded, err := store.GetRootFSGeneration(t.Context(), id)
	require.NoError(t, err)
	return loaded
}

// Respect the production upload protocol: establish catalog/journal custody
// before touching a content-addressed S3 key, then acknowledge the upload.
type retentionJournalPublisher struct {
	store   *PGSandboxStore
	objects objectstore.ContextConditionalStore
	batchID string
}

func (p retentionJournalPublisher) PutImmutable(ctx context.Context, key string, payload []byte) error {
	kind := rootfsblock.ObjectKindMappingPage
	if strings.Contains(key, "/packs/") {
		kind = rootfsblock.ObjectKindDataPack
	}
	ref := rootfsblock.ObjectReference{Key: key, Kind: kind, Size: int64(len(payload)), Checksum: digest.FromBytes(payload).String()}
	if err := p.store.RegisterRootFSGenerationMaterializationBatchObject(ctx, p.batchID, ref); err != nil {
		return err
	}
	if err := (rootfsblock.ObjectStorePublisher{Store: p.objects}).PutImmutable(ctx, key, payload); err != nil {
		return err
	}
	return p.store.MarkRootFSGenerationMaterializationBatchObjectUploaded(ctx, p.batchID, key)
}

func generationRetentionS3Store(t *testing.T) (objectstore.ContextConditionalStore, string) {
	t.Helper()
	endpoint := os.Getenv("SANDBOX0_RUSTFS_ENDPOINT")
	if endpoint == "" {
		t.Skip("requires isolated SANDBOX0_RUSTFS_ENDPOINT")
	}
	objects, err := objectstore.Create(objectstore.Config{Type: objectstore.TypeS3, Bucket: "sandbox0-retention-test", Region: "us-east-1", Endpoint: endpoint,
		AccessKey: os.Getenv("SANDBOX0_RUSTFS_ACCESS_KEY"), SecretKey: os.Getenv("SANDBOX0_RUSTFS_SECRET_KEY")})
	require.NoError(t, err)
	if err := objects.Create(); err != nil {
		require.Contains(t, err.Error(), "Already")
	}
	if os.Getenv("SANDBOX0_RETENTION_ENCRYPTED") == "1" {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		require.NoError(t, err)
		encoded := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		encryptor, err := objectstore.NewKeyEncryptor(string(encoded), "")
		require.NoError(t, err)
		objects = objectstore.EncryptingImmutable(objects, objectstore.EncryptionConfig{
			Enabled: true, Algorithm: objectstore.EncryptionAlgoAES256GCMRSA, KeyEncryptor: encryptor, ChunkSize: 4096,
		}, objectstore.EncryptedHeaderCacheConfig{MaxEntries: 1000, MaxBytes: 8 << 20})
	}
	prefix := fmt.Sprintf("rootfs/retention/%d", time.Now().UnixNano())
	t.Cleanup(func() {
		items, _, _, err := objects.List(prefix, "", "", "", 1000)
		require.NoError(t, err)
		for _, item := range items {
			require.NoError(t, objects.Delete(item.Key))
		}
	})
	return objects.(objectstore.ContextConditionalStore), prefix
}

type retentionObjectUsage struct{ objects, bytes int64 }

type retentionZeroImage struct{}

func (retentionZeroImage) ReadAt(p []byte, _ int64) (int, error) {
	clear(p)
	return len(p), nil
}

func retentionS3Usage(t *testing.T, objects objectstore.Store, prefix string) retentionObjectUsage {
	t.Helper()
	items, truncated, _, err := objects.List(prefix, "", "", "", 1000)
	require.NoError(t, err)
	require.False(t, truncated)
	var usage retentionObjectUsage
	for _, item := range items {
		usage.objects++
		usage.bytes += item.Size
	}
	return usage
}

func TestRootFSGenerationInventoryCanaryScopeS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, filesystem, current := newRetentionS3Filesystem(t, objects, prefix, "retention-canary")
	publishRetentionCheckpoint(t, store, objects, rootfsblock.BuildOptions{ObjectPrefix: prefix, DataRangeBytes: rootfsblock.LogicalBlockSize}, current, filesystem.ID, "canary-checkpoint", []rootfsblock.BlockUpdate{{Sequence: 1, Block: 0, Data: bytes.Repeat([]byte{9}, rootfsblock.LogicalBlockSize)}})
	_, err := store.InventoryRootFSGenerationForTeam(t.Context(), objects, " ", 1)
	require.ErrorContains(t, err, "inventory team is required")
	complete, err := store.InventoryRootFSGenerationForTeam(t.Context(), objects, "other-team", 1)
	require.NoError(t, err)
	require.False(t, complete)
	var attempts int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_generations WHERE inventory_attempted_at IS NOT NULL`).Scan(&attempts))
	require.Zero(t, attempts, "a canary must not mutate another team's inventory")
	complete, err = store.InventoryRootFSGenerationForTeam(t.Context(), objects, "team-1", 1)
	require.NoError(t, err)
	require.True(t, complete)
}
