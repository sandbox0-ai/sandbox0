package sandboxstore

import (
	"context"
	"github.com/sandbox0-ai/sandbox0/pkg/objectstore"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/rootfsblock"
	"github.com/stretchr/testify/require"
)

func TestRootFSLegacyQueuedRetirementAdoptionS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, fs, base := newRetentionS3Filesystem(t, objects, prefix, "legacy-queued")
	current := publishRetentionNodeCheckpoint(t, store, objects, prefix, base, fs.ID, "legacy-queued-cut", 9, false)
	descriptor, err := rootfsblock.DecodeDescriptor(current.Descriptor)
	require.NoError(t, err)
	key := descriptor.MappingRoot.Object.Key
	_, err = store.pool.Exec(t.Context(), `INSERT INTO manager.rootfs_object_deletions(object_key,team_id,next_attempt_at) VALUES($1,'team-1',NOW())`, key)
	require.NoError(t, err)
	deleted, err := store.DeletePendingRootFSObjectsWithOptions(t.Context(), objects, DeletePendingRootFSObjectsOptions{Limit: 1})
	require.NoError(t, err)
	require.Empty(t, deleted, "unknown legacy graph must fence a queue inherited from an older binary")
	assertRetentionContents(t, objects, current, 9)
	// A claim from an older worker cannot be stolen during adoption.
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_object_deletions SET claimed_by='old-worker',claimed_until=NOW()+INTERVAL '1 minute' WHERE object_key=$1`, key)
	require.NoError(t, err)
	_, err = store.InventoryRootFSGeneration(t.Context(), objects, 100)
	require.ErrorIs(t, err, ErrRootFSGenerationConflict)
	_, err = store.pool.Exec(t.Context(), `UPDATE manager.rootfs_object_deletions SET claimed_until=NOW()-INTERVAL '1 minute' WHERE object_key=$1`, key)
	require.NoError(t, err)
	complete, err := NewPGSandboxStore(store.pool).InventoryRootFSGeneration(t.Context(), objects, 100)
	require.NoError(t, err)
	require.True(t, complete)
	var queued int
	require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT count(*) FROM manager.rootfs_object_deletions WHERE object_key=$1`, key).Scan(&queued))
	require.Zero(t, queued)
	assertRetentionContents(t, objects, current, 9)
}

// The final guard must remain effective through the S3 call, not only while
// claiming a queue row. A new unknown graph cannot appear in that interval.
func TestRootFSLegacyPublicationWaitsForPhysicalDeleteS3Integration(t *testing.T) {
	objects, prefix := generationRetentionS3Store(t)
	store, _, base := newRetentionS3Filesystem(t, objects, prefix, "legacy-publication-fence")
	key := prefix + "/unreferenced"
	require.NoError(t, objects.Put(key, strings.NewReader("unreferenced payload")))
	_, err := store.pool.Exec(t.Context(), `INSERT INTO manager.rootfs_object_deletions(object_key,team_id,next_attempt_at) VALUES($1,'team-1',NOW())`, key)
	require.NoError(t, err)
	entered, resume := make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-resume:
		default:
			close(resume)
		}
	}()
	deletion := make(chan error, 1)
	go func() {
		_, err := store.DeletePendingRootFSObjectsWithOptions(t.Context(), retentionGateDeleter{objects, entered, resume}, DeletePendingRootFSObjectsOptions{Limit: 1})
		deletion <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("physical delete did not start")
	}
	conn, err := store.pool.Acquire(t.Context())
	require.NoError(t, err)
	defer conn.Release()
	pid := conn.Conn().PgConn().PID()
	publication := make(chan error, 1)
	go func() {
		_, err := conn.Exec(t.Context(), `UPDATE manager.rootfs_generations SET storage_inventory_required=TRUE WHERE generation_id=$1`, base.ID)
		publication <- err
	}()
	deadline := time.Now().Add(5 * time.Second)
	waiting := false
	for time.Now().Before(deadline) {
		require.NoError(t, store.pool.QueryRow(t.Context(), `SELECT cardinality(pg_blocking_pids($1))>0`, pid).Scan(&waiting))
		if waiting {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	require.True(t, waiting, "new unknown references must wait until the exact external deletion finishes")
	close(resume)
	require.NoError(t, <-deletion)
	require.NoError(t, <-publication)
	_, err = objects.Get(key, 0, -1)
	require.Error(t, err)
}

type retentionGateDeleter struct {
	objectstore.ContextConditionalStore
	entered chan struct{}
	resume  chan struct{}
}

func (d retentionGateDeleter) Delete(key string) error {
	return d.DeleteContext(context.Background(), key)
}
func (d retentionGateDeleter) DeleteContext(ctx context.Context, key string) error {
	close(d.entered)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.resume:
	}
	return d.ContextConditionalStore.(interface {
		DeleteContext(context.Context, string) error
	}).DeleteContext(ctx, key)
}
