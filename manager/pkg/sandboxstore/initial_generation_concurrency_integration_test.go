package sandboxstore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

const initialGenerationGateKey = 6149281

type initialGenerationTestResult struct {
	filesystem *RootFSFilesystem
	generation *RootFSGeneration
	err        error
}

func initialGenerationTestWaitBlockedQuery(t *testing.T, observer *pgxpool.Pool, query string, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting int
		err := observer.QueryRow(t.Context(), `
			SELECT count(*) FROM pg_stat_activity
			WHERE datname=current_database() AND application_name='initial-generation-workers'
				AND wait_event_type='Lock' AND query LIKE $1
		`, "%"+query+"%").Scan(&waiting)
		return err == nil && waiting == count
	}, 5*time.Second, 10*time.Millisecond, "%d workers must be blocked in %s", count, query)
}

func initialGenerationTestAssertRows(t *testing.T, observer *pgxpool.Pool, count int) {
	t.Helper()
	for _, table := range []string{"rootfs_filesystems", "sandbox_rootfs_bindings", "rootfs_generations"} {
		var actual int
		require.NoError(t, observer.QueryRow(t.Context(), "SELECT count(*) FROM manager."+table).Scan(&actual))
		require.Equal(t, count, actual, table)
	}
}

func TestInitialRootFSGenerationConcurrentSameSandboxIntegration(t *testing.T) {
	observer := newSandboxStoreIntegrationPool(t)
	const width = 8
	store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, width))
	artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord("same-sandbox", "team-1")))
	release := initialGenerationTestGate(t, observer)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req := initialGenerationTestRequest("same-sandbox", artifact)
	results := []<-chan initialGenerationTestResult{initialGenerationTestStart(ctx, store, req)}
	initialGenerationTestWaitAtGate(t, observer, 1)
	for range width - 1 {
		results = append(results, initialGenerationTestStart(ctx, store, req))
	}
	initialGenerationTestWaitBlockedQuery(t, observer, "FROM manager.sandboxes", width-1)
	release()
	var expected string
	for _, result := range results {
		value := <-result
		require.NoError(t, value.err)
		if expected == "" {
			expected = value.generation.ID
		}
		require.Equal(t, expected, value.generation.ID)
		require.Equal(t, expected, value.filesystem.HeadGenerationID)
	}
	initialGenerationTestAssertRows(t, observer, 1)
}

func TestInitialRootFSGenerationFencesDeletionIntegration(t *testing.T) {
	for _, deleteFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("delete_first_%t", deleteFirst), func(t *testing.T) {
			observer := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, 2))
			artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
			require.NoError(t, err)
			require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord("delete-race", "team-1")))
			release := initialGenerationTestGate(t, observer)
			if deleteFirst {
				_, err := observer.Exec(t.Context(), `
					CREATE TRIGGER test_initial_deletion_gate
					BEFORE UPDATE ON manager.sandboxes
					FOR EACH ROW WHEN (NEW.deleted_at IS NOT NULL)
					EXECUTE FUNCTION manager.test_initial_generation_gate();
				`)
				require.NoError(t, err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			deleted := make(chan error, 1)
			startDelete := func() { go func() { deleted <- store.MarkSandboxDeleted(ctx, "delete-race", time.Now()) }() }
			var initialized <-chan initialGenerationTestResult
			req := initialGenerationTestRequest("delete-race", artifact)
			if deleteFirst {
				startDelete()
				initialGenerationTestWaitAtGate(t, observer, 1)
				initialized = initialGenerationTestStart(ctx, store, req)
			} else {
				initialized = initialGenerationTestStart(ctx, store, req)
				initialGenerationTestWaitAtGate(t, observer, 1)
				startDelete()
			}
			initialGenerationTestWaitBlockedQuery(t, observer, "FROM manager.sandboxes", 1)
			release()
			value := <-initialized
			if deleteFirst {
				require.ErrorIs(t, value.err, ErrSandboxRecordNotFound)
			} else {
				require.NoError(t, value.err)
			}
			require.NoError(t, <-deleted)
			initialGenerationTestAssertRows(t, observer, 0)
			_, _, err = store.EnsureInitialRootFSGeneration(ctx, req)
			require.ErrorIs(t, err, ErrSandboxRecordNotFound, "a deleted identity cannot acquire another initial binding")
		})
	}
}

func TestInitialRootFSGenerationConcurrentArtifactConflictIntegration(t *testing.T) {
	observer := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, 2))
	first, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	secondSpec := readyRootFSBaseArtifactTestRequest()
	secondSpec.ArtifactDigest = digest.FromString("another-initial-artifact").String()
	second, err := store.PutReadyRootFSBaseArtifact(t.Context(), secondSpec)
	require.NoError(t, err)
	require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord("conflicting-artifact", "team-1")))
	release := initialGenerationTestGate(t, observer)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	owner := initialGenerationTestStart(ctx, store, initialGenerationTestRequest("conflicting-artifact", first))
	initialGenerationTestWaitAtGate(t, observer, 1)
	conflict := initialGenerationTestStart(ctx, store, initialGenerationTestRequest("conflicting-artifact", second))
	initialGenerationTestWaitBlockedQuery(t, observer, "FROM manager.sandboxes", 1)
	release()
	value := <-owner
	require.NoError(t, value.err)
	require.ErrorIs(t, (<-conflict).err, ErrRootFSGenerationConflict)
	initialGenerationTestAssertRows(t, observer, 1)
	var boundArtifact string
	require.NoError(t, observer.QueryRow(ctx, "SELECT base_artifact_digest FROM manager.rootfs_filesystems WHERE filesystem_id=$1", "conflicting-artifact").Scan(&boundArtifact))
	require.Equal(t, first.ArtifactDigest, boundArtifact, "the waiting caller cannot replace the winning binding")
}

func TestInitialRootFSGenerationProtectsArtifactUntilPublicationIntegration(t *testing.T) {
	observer := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, 2))
	artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord("artifact-protected", "team-1")))
	release := initialGenerationTestGate(t, observer)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	initialized := initialGenerationTestStart(ctx, store, initialGenerationTestRequest("artifact-protected", artifact))
	initialGenerationTestWaitAtGate(t, observer, 1)
	deleted := make(chan error, 1)
	go func() {
		_, err := store.pool.Exec(ctx, "DELETE FROM manager.rootfs_base_artifacts WHERE artifact_digest=$1", artifact.ArtifactDigest)
		deleted <- err
	}()
	initialGenerationTestWaitBlockedQuery(t, observer, "DELETE FROM manager.rootfs_base_artifacts", 1)
	release()
	require.NoError(t, (<-initialized).err)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, <-deleted, &pgErr)
	require.Equal(t, "23503", pgErr.Code, "the committed filesystem FK must take over artifact lifetime protection")
	initialGenerationTestAssertRows(t, observer, 1)
}

func TestInitialRootFSGenerationRechecksRetiredArtifactIntegration(t *testing.T) {
	observer := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, 1))
	artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord("artifact-retired", "team-1")))
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	tx, err := observer.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = tx.Exec(ctx, "SELECT artifact_digest FROM manager.rootfs_base_artifacts WHERE artifact_digest=$1 FOR UPDATE", artifact.ArtifactDigest)
	require.NoError(t, err)
	initialized := initialGenerationTestStart(ctx, store, initialGenerationTestRequest("artifact-retired", artifact))
	initialGenerationTestWaitBlockedQuery(t, observer, "FROM manager.rootfs_base_artifacts", 1)
	_, err = tx.Exec(ctx, "UPDATE manager.rootfs_base_artifacts SET state='retired' WHERE artifact_digest=$1", artifact.ArtifactDigest)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	require.ErrorIs(t, (<-initialized).err, ErrRootFSBaseArtifactNotFound)
	initialGenerationTestAssertRows(t, observer, 0)
}

func TestInitialRootFSGenerationCanceledInsertIsAtomicWithGCIntegration(t *testing.T) {
	observer := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, 2))
	artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
	require.NoError(t, err)
	require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord("canceled-insert", "team-1")))
	release := initialGenerationTestGate(t, observer)
	// Hold an actual uncommitted filesystem row before its binding/head exist.
	_, err = observer.Exec(t.Context(), `
		DROP TRIGGER test_initial_generation_gate ON manager.rootfs_filesystems;
		CREATE TRIGGER test_initial_generation_gate
		AFTER INSERT ON manager.rootfs_filesystems
		FOR EACH ROW EXECUTE FUNCTION manager.test_initial_generation_gate();
	`)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	req := initialGenerationTestRequest("canceled-insert", artifact)
	initialized := initialGenerationTestStart(ctx, store, req)
	initialGenerationTestWaitAtGate(t, observer, 1)
	initialGenerationTestAssertRows(t, observer, 0)
	collected, err := NewPGSandboxStore(observer).DeleteUnreferencedRootFSFilesystems(t.Context(), "team-1", 100)
	require.NoError(t, err)
	require.Zero(t, collected, "GC cannot observe a partially published filesystem")
	cancel()
	require.ErrorIs(t, (<-initialized).err, context.Canceled)
	release()
	initialGenerationTestAssertRows(t, observer, 0)
	_, _, err = store.EnsureInitialRootFSGeneration(t.Context(), req)
	require.NoError(t, err, "cancellation must release the identity and artifact locks")
	initialGenerationTestAssertRows(t, observer, 1)
	collected, err = NewPGSandboxStore(observer).DeleteUnreferencedRootFSFilesystems(t.Context(), "team-1", 100)
	require.NoError(t, err)
	require.Zero(t, collected, "the committed binding must retain its filesystem")
}

// The trigger holds each independent filesystem insertion before publication.
// All workers must reach it while the observer holds the advisory gate. This
// proves that a shared base artifact does not serialize unrelated sandboxes;
// a start channel alone cannot distinguish overlap from incidental scheduling.
func initialGenerationTestGate(t *testing.T, pool *pgxpool.Pool) func() {
	t.Helper()
	ctx := t.Context()
	_, err := pool.Exec(ctx, fmt.Sprintf(`
		CREATE FUNCTION manager.test_initial_generation_gate() RETURNS trigger AS $$
		BEGIN
			PERFORM pg_advisory_xact_lock_shared(%d);
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
		CREATE TRIGGER test_initial_generation_gate
		BEFORE INSERT ON manager.rootfs_filesystems
		FOR EACH ROW EXECUTE FUNCTION manager.test_initial_generation_gate();
	`, initialGenerationGateKey))
	require.NoError(t, err)
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "SELECT pg_advisory_lock($1)", initialGenerationGateKey)
	require.NoError(t, err)
	released := false
	release := func() {
		if released {
			return
		}
		released = true
		// Release even after the test context is canceled, so failed assertions
		// cannot leave workers blocked while their pool is being closed.
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := conn.Exec(cleanup, "SELECT pg_advisory_unlock($1)", initialGenerationGateKey)
		conn.Release()
		require.NoError(t, err)
	}
	t.Cleanup(release)
	return release
}

func initialGenerationTestWaitAtGate(t *testing.T, pool *pgxpool.Pool, count int) {
	t.Helper()
	require.Eventually(t, func() bool {
		var waiting int
		err := pool.QueryRow(t.Context(), `
			SELECT count(*) FROM pg_locks
			WHERE locktype='advisory' AND mode='ShareLock' AND NOT granted
				AND objid=$1::oid AND classid=0 AND objsubid=1
				AND database=(SELECT oid FROM pg_database WHERE datname=current_database())
		`, initialGenerationGateKey).Scan(&waiting)
		return err == nil && waiting == count
	}, 5*time.Second, 10*time.Millisecond, "%d independent initializations must reach their own INSERT concurrently", count)
}

func initialGenerationTestWorkerPool(t *testing.T, observer *pgxpool.Pool, width int) *pgxpool.Pool {
	t.Helper()
	config := observer.Config()
	config.MaxConns = int32(width)
	config.ConnConfig.RuntimeParams["application_name"] = "initial-generation-workers"
	pool, err := pgxpool.NewWithConfig(t.Context(), config)
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func initialGenerationTestStart(ctx context.Context, store *PGSandboxStore, req *EnsureInitialRootFSGenerationRequest) <-chan initialGenerationTestResult {
	result := make(chan initialGenerationTestResult, 1)
	go func() {
		filesystem, generation, err := store.EnsureInitialRootFSGeneration(ctx, req)
		result <- initialGenerationTestResult{filesystem: filesystem, generation: generation, err: err}
	}()
	return result
}

func initialGenerationTestRequest(sandboxID string, artifact *RootFSBaseArtifact) *EnsureInitialRootFSGenerationRequest {
	return &EnsureInitialRootFSGenerationRequest{
		SandboxID: sandboxID, TeamID: "team-1", SourceOCIRef: artifact.SourceOCIRef,
		SourceOCIDigest: artifact.SourceOCIDigest, BaseArtifactDigest: artifact.ArtifactDigest,
	}
}

func TestInitialRootFSGenerationIndependentSandboxesOverlapIntegration(t *testing.T) {
	for _, width := range []int{2, 8, 64} {
		t.Run(fmt.Sprintf("width_%d", width), func(t *testing.T) {
			observer := newSandboxStoreIntegrationPool(t)
			store := NewPGSandboxStore(initialGenerationTestWorkerPool(t, observer, width))
			artifact, err := store.PutReadyRootFSBaseArtifact(t.Context(), readyRootFSBaseArtifactTestRequest())
			require.NoError(t, err)
			for i := range width {
				require.NoError(t, store.UpsertSandbox(t.Context(), rootFSTestSandboxRecord(fmt.Sprintf("overlap-%d", i), "team-1")))
			}
			release := initialGenerationTestGate(t, observer)
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			results := make([]<-chan initialGenerationTestResult, width)
			for i := range width {
				results[i] = initialGenerationTestStart(ctx, store, initialGenerationTestRequest(fmt.Sprintf("overlap-%d", i), artifact))
			}
			initialGenerationTestWaitAtGate(t, observer, width)
			release()
			for i, result := range results {
				value := <-result
				require.NoError(t, value.err, "sandbox %d", i)
				require.Equal(t, fmt.Sprintf("overlap-%d", i), value.filesystem.ID)
				require.Equal(t, value.filesystem.ID, value.generation.FilesystemID)
				require.Equal(t, value.generation.ID, value.filesystem.HeadGenerationID)
			}
			for _, table := range []string{"rootfs_filesystems", "sandbox_rootfs_bindings", "rootfs_generations"} {
				var count int
				require.NoError(t, observer.QueryRow(t.Context(), "SELECT count(*) FROM manager."+table).Scan(&count))
				require.Equal(t, width, count)
			}
		})
	}
}
