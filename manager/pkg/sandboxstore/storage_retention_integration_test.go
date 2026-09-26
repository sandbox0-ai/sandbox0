package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	meteringclickhouse "github.com/sandbox0-ai/sandbox0/pkg/metering/clickhouse"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func storageRetentionMeteringRepository(t *testing.T, pool *pgxpool.Pool) *meteringoutbox.Repository {
	t.Helper()
	_, err := pool.Exec(t.Context(), `DROP SCHEMA IF EXISTS metering CASCADE`)
	require.NoError(t, err)
	require.NoError(t, meteringoutbox.RunMigrations(t.Context(), pool, noopSandboxStoreMigrateLogger{}))
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), `DROP SCHEMA IF EXISTS metering CASCADE`) })
	return meteringoutbox.NewRepository(pool)
}

func TestRootFSStorageDeletionRecreationGapIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	seedStorageRetentionObjects(t, store, "storage-gap-old")
	end := time.Now().UTC().Truncate(time.Microsecond)
	start := end.Add(-31 * 24 * time.Hour)
	_, err := pool.Exec(t.Context(), `UPDATE manager.rootfs_storage_transitions SET changed_at=$1`, start)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_storage_usage SET changed_at=$1`, start)
	require.NoError(t, err)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", start)
	require.NoError(t, err)
	before, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.NoError(t, store.MarkSandboxDeleted(t.Context(), "sandbox-storage-gap-old", time.Now().UTC()))
	seedStorageRetentionObjects(t, store, "storage-gap-new")
	// The deletion and recreation both occur before the next metering pass.
	// Move their committed journal boundaries to a deterministic 31-day trace.
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_storage_transitions SET changed_at=
		CASE WHEN storage_bytes=0 THEN $1::TIMESTAMPTZ ELSE $2::TIMESTAMPTZ END`, start.Add(time.Hour), end.Add(-time.Hour))
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `UPDATE manager.rootfs_storage_usage SET changed_at=$1`, end.Add(-time.Hour))
	require.NoError(t, err)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", end)
	require.NoError(t, err)
	var charged int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COALESCE(SUM((payload->>'value')::BIGINT),0)
		FROM metering.projection_outbox WHERE operation_type='window'`).Scan(&charged))
	require.Equal(t, 2*before.SizeBytes, charged, "only one retained hour before deletion and one after recreation may be charged")
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", end)
	require.NoError(t, err)
	var retried int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COALESCE(SUM((payload->>'value')::BIGINT),0)
		FROM metering.projection_outbox WHERE operation_type='window'`).Scan(&retried))
	require.Equal(t, charged, retried, "retries must not duplicate byte-time windows")
	after, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.Equal(t, before.SizeBytes, after.SizeBytes)
	require.True(t, end.Equal(after.ObservedAt))
	if dsn := os.Getenv("SANDBOX0_CLICKHOUSE_INTEGRATION_DSN"); dsn != "" {
		exportStorageRetentionEvidence(t, repo, dsn, charged)
	}
}

func exportStorageRetentionEvidence(t *testing.T, repo *meteringoutbox.Repository, dsn string, expected int64) {
	t.Helper()
	name := fmt.Sprintf("storage_retention_%d", time.Now().UnixNano())
	db, sink, err := meteringclickhouse.Open(t.Context(), meteringclickhouse.OpenConfig{DSN: dsn, Schema: meteringclickhouse.Config{Database: name}, Migrate: true})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, err := db.ExecContext(context.Background(), "DROP DATABASE "+name)
		require.NoError(t, err)
		require.NoError(t, db.Close())
	})
	projector := meteringoutbox.NewProjector(repo, sink, meteringoutbox.ProjectorConfig{WorkerID: "storage-regression"}, zap.NewNop())
	for {
		processed, err := projector.ProjectOnce(t.Context())
		require.NoError(t, err)
		if !processed {
			break
		}
	}
	windows, _, err := sink.ListWindows(t.Context(), "", 1000)
	require.NoError(t, err)
	var exported int64
	for _, window := range windows {
		exported += window.Value
	}
	require.Equal(t, expected, exported, "real ClickHouse projection/export preserves the zero-gap byte-time result")
	if path := os.Getenv("SANDBOX0_STORAGE_WINDOW_EVIDENCE_PATH"); path != "" {
		payload, err := json.MarshalIndent(windows, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(path, payload, 0o600))
	}
	t.Logf("PostgreSQL outbox -> real ClickHouse export: %d windows, %d byte-hours", len(windows), exported)
}

func TestRootFSStorageLegacyOrphanReconciliationIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	start := time.Now().UTC().Add(-30 * 24 * time.Hour).Truncate(time.Microsecond)
	require.NoError(t, repo.RecordStorageObservation(t.Context(), rootFSStorageObservation("orphan", 1<<30, start)))
	_, err := store.RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	state, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "orphan")
	require.NoError(t, err)
	require.Zero(t, state.SizeBytes)
	var windows int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM metering.projection_outbox WHERE operation_type='window'`).Scan(&windows))
	require.Zero(t, windows, "unknown deletion time cannot establish 30 days of chargeable storage")
	var raw []byte
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT previous_state FROM manager.rootfs_storage_reconciliation_audit WHERE team_id='orphan'`).Scan(&raw))
	var previous meteringpkg.StorageProjectionState
	require.NoError(t, json.Unmarshal(raw, &previous))
	require.Equal(t, int64(1<<30), previous.SizeBytes)
	require.True(t, start.Equal(previous.ObservedAt))
}

func TestRootFSStorageTransitionRollbackAndWorkersIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	seedStorageRetentionObjects(t, store, "storage-rollback")
	failure := errors.New("simulate worker crash before journal acknowledgment")
	_, err := store.RecordRootFSStorageObservations(t.Context(), &failingStorageTransactionRecorder{Repository: repo, failure: failure}, "", time.Now().UTC())
	require.ErrorIs(t, err, failure)
	var pending, emitted int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_storage_transitions`).Scan(&pending))
	require.Positive(t, pending)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM metering.projection_outbox`).Scan(&emitted))
	require.Zero(t, emitted, "journal acknowledgment and metering projection roll back together")
	state, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.Nil(t, state)
	// A second worker must skip an account whose reference mutation or first
	// worker is still open, rather than using a stale capacity from its scan.
	locked, err := pool.Begin(t.Context())
	require.NoError(t, err)
	_, err = locked.Exec(t.Context(), `SELECT 1 FROM manager.rootfs_storage_usage WHERE team_id='team-1' FOR UPDATE`)
	require.NoError(t, err)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	state, err = repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.Nil(t, state)
	require.NoError(t, locked.Rollback(t.Context()))
	_, err = NewPGSandboxStore(pool).RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_storage_transitions`).Scan(&pending))
	require.Zero(t, pending)
	state, err = repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.Positive(t, state.SizeBytes)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", state.ObservedAt.Add(-time.Hour))
	require.NoError(t, err)
	after, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.Equal(t, state.SizeBytes, after.SizeBytes)
	require.True(t, state.ObservedAt.Equal(after.ObservedAt), "late scan does not move the metering boundary backwards")
}

type failingStorageTransactionRecorder struct {
	*meteringoutbox.Repository
	failure error
}

func (f *failingStorageTransactionRecorder) RecordStorageObservationTx(ctx context.Context, tx pgx.Tx, observation *meteringpkg.StorageObservation) error {
	if err := f.Repository.RecordStorageObservationTx(ctx, tx, observation); err != nil {
		return err
	}
	return f.failure
}

func TestRootFSStorageMigrationBackfillIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPoolAt(t, 108)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	seedStorageRetentionObjects(t, store, "storage-before-migration")
	previous := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	require.NoError(t, repo.RecordStorageObservation(t.Context(), rootFSStorageObservation("legacy-orphan", 12345, previous)))
	var expected int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT SUM(object_size) FROM manager.rootfs_materialization_objects
		WHERE object_key IN (SELECT object_key FROM manager.rootfs_generation_materialization_objects)`).Scan(&expected))
	require.NoError(t, RunSandboxStoreMigrations(t.Context(), pool, noopSandboxStoreMigrateLogger{}))
	usage, err := store.ListRootFSStorageUsage(t.Context(), "team-1")
	require.NoError(t, err)
	require.Len(t, usage, 1)
	require.Equal(t, expected, usage[0].StorageBytes)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	state, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "legacy-orphan")
	require.NoError(t, err)
	require.Zero(t, state.SizeBytes)
	require.NoError(t, store.MarkSandboxDeleted(t.Context(), "sandbox-storage-before-migration", time.Now().UTC()))
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	state, err = repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.Zero(t, state.SizeBytes)
}

func seedStorageRetentionObjects(t *testing.T, store *PGSandboxStore, sandboxID string) {
	t.Helper()
	filesystem, initial := seedCompositeTestFilesystem(t, t.Context(), store, sandboxID)
	composite := compositeTestGeneration(t, initial, filesystem.ID, sandboxID+"-checkpoint", 1)
	require.NoError(t, insertCompositeTestGeneration(t.Context(), store.pool, composite))
	request := beginRootFSMaterializationIntegrationBatch(t, t.Context(), store, composite, initial.Descriptor)
	require.NoError(t, store.PublishRootFSGenerationMaterializationBatch(t.Context(), request))
	_, err := store.pool.Exec(t.Context(), `UPDATE manager.rootfs_filesystems SET head_generation_id=$2 WHERE filesystem_id=$1`, filesystem.ID, composite.ID)
	require.NoError(t, err)
}

func TestRootFSStorageLastFilesystemZeroIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	seedStorageRetentionObjects(t, store, "storage-zero")
	_, err := store.RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	before, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.NotNil(t, before)
	require.Positive(t, before.SizeBytes)

	require.NoError(t, store.MarkSandboxDeleted(t.Context(), "sandbox-storage-zero", time.Now().UTC()))
	_, err = store.GarbageCollectRootFSFilesystemWithOptions(t.Context(), storageRetentionDeleter{}, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	var filesystems int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_filesystems`).Scan(&filesystems))
	require.Zero(t, filesystems)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", time.Now().UTC())
	require.NoError(t, err)
	after, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.NotNil(t, after)
	require.Zero(t, after.SizeBytes, "deleting the last filesystem must end the previous byte-time interval")
}

func TestRootFSMemoryForkTerminalStorageRetentionIntegration(t *testing.T) {
	f := completedCheckpointStoreFixture(t, "retention-memory-fork")
	request := memoryForkRequest(t, f, f.sandboxID, "retention-memory-child", "retention-memory-operation")
	child, err := f.store.ForkNomadPausedSandbox(f.ctx, request)
	require.NoError(t, err)
	_, err = f.store.DeleteUnreferencedRootFSGenerations(f.ctx, "", 100)
	require.NoError(t, err)
	candidate, err := f.store.RequestNomadSandboxResume(f.ctx, &RequestNomadSandboxResumeRequest{
		SandboxID: child.ID, ExpectedTeamID: child.TeamID, Memory: true})
	require.NoError(t, err, "a live child keeps both its disk and memory custody during GC")
	_, err = f.store.AbortNomadSandboxResume(f.ctx, child.ID, candidate.OperationID, "test ends child ownership")
	require.NoError(t, err)
	require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, child.ID, time.Now().UTC()))
	require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now().UTC()))
	_, err = f.store.GarbageCollectRootFSFilesystemWithOptions(f.ctx, storageRetentionDeleter{}, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	var generations, history int
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&generations))
	require.Zero(t, generations, "terminal memory fork history must not pin obsolete disk data")
	require.NoError(t, f.pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.sandbox_runtime_checkpoint_forks WHERE operation_id=$1`, request.OperationID).Scan(&history))
	require.Equal(t, 1, history, "immutable fork mode and retry identity survive collection")
}

func TestRootFSStorageBoundedTransitionBacklogIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	seedStorageRetentionObjects(t, store, "storage-backlog")
	start := time.Now().UTC().Add(-1100 * time.Hour).Truncate(time.Microsecond)
	_, err := pool.Exec(t.Context(), `WITH transitions AS (UPDATE manager.rootfs_storage_transitions SET changed_at=$1 RETURNING 1)
		UPDATE manager.rootfs_storage_usage SET changed_at=$1`, start)
	require.NoError(t, err)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", start)
	require.NoError(t, err)
	initial, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	var revision int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT revision FROM manager.rootfs_storage_usage WHERE team_id='team-1'`).Scan(&revision))
	// Exercise the real reference-count triggers, including repeated zero and
	// recreation boundaries. The worker must not jump over a bounded backlog.
	_, err = pool.Exec(t.Context(), `DO $$ DECLARE refs RECORD; step INTEGER; BEGIN
		SELECT * INTO STRICT refs FROM manager.rootfs_generation_materialization_objects LIMIT 1;
		FOR step IN 1..1002 LOOP
			IF step%2=1 THEN DELETE FROM manager.rootfs_generation_materialization_objects WHERE generation_id=refs.generation_id AND object_key=refs.object_key;
			ELSE INSERT INTO manager.rootfs_generation_materialization_objects (generation_id,locator_version,object_key)
				VALUES (refs.generation_id,refs.locator_version,refs.object_key); END IF;
		END LOOP;
	END $$`)
	require.NoError(t, err)
	_, err = pool.Exec(t.Context(), `WITH transitions AS (UPDATE manager.rootfs_storage_transitions SET changed_at=$1::TIMESTAMPTZ+(revision-$2)*INTERVAL '1 hour' RETURNING 1)
		UPDATE manager.rootfs_storage_usage SET changed_at=$1::TIMESTAMPTZ+INTERVAL '1002 hours'`, start, revision)
	require.NoError(t, err)
	end := start.Add(1003 * time.Hour)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", end)
	require.NoError(t, err)
	state, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "team-1")
	require.NoError(t, err)
	require.True(t, start.Add(1000*time.Hour).Equal(state.ObservedAt), "first pass stops at the last acknowledged transition")
	var pending int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_storage_transitions`).Scan(&pending))
	require.Equal(t, 2, pending)
	_, err = NewPGSandboxStore(pool).RecordRootFSStorageObservations(t.Context(), repo, "", end)
	require.NoError(t, err)
	var charged int64
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COALESCE(SUM((payload->>'value')::BIGINT),0) FROM metering.projection_outbox WHERE operation_type='window'`).Scan(&charged))
	require.Equal(t, 502*initial.SizeBytes, charged, "each empty interval is excluded even across worker batches/restart")
}

func TestRootFSStorageLateBootstrapAndQuiescentZeroIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPool(t)
	store := NewPGSandboxStore(pool)
	repo := storageRetentionMeteringRepository(t, pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	require.NoError(t, repo.RecordStorageObservation(t.Context(), rootFSStorageObservation("late-bootstrap", 12345, now.Add(-30*24*time.Hour))))
	_, err := store.RecordRootFSStorageObservations(t.Context(), repo, "", now)
	require.NoError(t, err)
	var before, after int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM metering.projection_outbox`).Scan(&before))
	usages, err := store.RecordRootFSStorageObservations(t.Context(), repo, "", now.Add(time.Minute))
	require.NoError(t, err)
	require.Empty(t, usages, "idle zero accounts must not generate recurring observations")
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM metering.projection_outbox`).Scan(&after))
	require.Equal(t, before, after)
	_, err = pool.Exec(t.Context(), `UPDATE metering.storage_projection_state SET size_bytes=12345,observed_at=$1 WHERE subject_id='late-bootstrap'`, now.Add(-30*24*time.Hour))
	require.NoError(t, err)
	_, err = store.RecordRootFSStorageObservations(t.Context(), repo, "", now.Add(2*time.Minute))
	require.NoError(t, err)
	state, err := repo.GetStorageProjectionState(t.Context(), meteringpkg.SubjectTypeRootFS, "late-bootstrap")
	require.NoError(t, err)
	require.Zero(t, state.SizeBytes)
	var windows, audits int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM metering.projection_outbox WHERE operation_type='window'`).Scan(&windows))
	require.Zero(t, windows, "restored legacy state cannot invent an empty month's usage")
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_storage_reconciliation_audit WHERE team_id='late-bootstrap'`).Scan(&audits))
	require.Equal(t, 2, audits)
}

func TestRootFSStorageMigrationPreservesCapacityHistoryIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPoolAt(t, 109)
	seedStorageRetentionObjects(t, NewPGSandboxStore(pool), "storage-forward-only")
	err := migrate.Down(t.Context(), pool, ".", migrate.WithBaseFS(storemigrations.FS), migrate.WithSchema(sandboxStoreSchemaName), migrate.WithLogger(noopSandboxStoreMigrateLogger{}))
	require.ErrorContains(t, err, "RootFS capacity history cannot be discarded")
	var pending int
	require.NoError(t, pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM manager.rootfs_storage_transitions`).Scan(&pending))
	require.Positive(t, pending, "a rejected rollback leaves committed capacity authority intact")
}

func TestRootFSStorageMigrationBackfillsLiveMemoryForkIntegration(t *testing.T) {
	pool := newSandboxStoreIntegrationPoolAt(t, 110)
	f := newNomadPauseStoreFixtureOnNode(t, "retention-legacy-memory-fork", pool, "")
	assignment := migrationAssignmentFixture(t, f, "retention-legacy-memory-fork")
	source := assignment.Target
	source.RuntimeGeneration = assignment.SourceGeneration
	c, prepare := prepareCheckpointStoreFixture(t, f, source, "retention-legacy-checkpoint", migrationSourcePolicy(source.SandboxID, source.TeamID))
	capture, err := f.store.AuthorizeNomadCheckpointCapture(f.ctx, *prepare, checkpointPreparedResponse(t, *prepare))
	require.NoError(t, err)
	publication := checkpointWorkerPublication(t, f, c, source, *capture)
	_, err = f.store.AuthorizeNomadCheckpointPublication(f.ctx, publication.Capture)
	require.NoError(t, err)
	receipt := migrationPublicationReceipt(t, publication)
	_, err = f.store.CommitNomadCheckpointPublication(f.ctx, publication, receipt)
	require.NoError(t, err)
	fence := protocol.MigrationSourceFenceRequest{PublicationRequest: publication, Publication: receipt}
	_, err = f.store.AuthorizeNomadCheckpointSourceFence(f.ctx, fence)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFence(f.ctx, fence, migrationSourceFenceStoreProof(t, f, fence)))
	finalization, err := f.store.AuthorizeNomadCheckpointSourceFinalization(f.ctx, publication.Capture.Request.OperationID)
	require.NoError(t, err)
	require.NoError(t, f.store.CommitNomadCheckpointSourceFinalization(f.ctx, *finalization, migrationFinalizationStoreProof(t, *finalization)))
	markCheckpointAllocationMissing(t, f)
	_, err = f.store.CompleteNomadSandboxMemoryPause(f.ctx, publication.Capture.Request.OperationID)
	require.NoError(t, err)
	request := memoryForkRequest(t, f, f.sandboxID, "retention-legacy-memory-child", "retention-legacy-memory-fork")
	child, err := f.store.ForkNomadPausedSandbox(f.ctx, request)
	require.NoError(t, err)
	require.NoError(t, RunSandboxStoreMigrations(f.ctx, pool, noopSandboxStoreMigrateLogger{}))
	var retained bool
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT generation_ref=generation_id AND storage_released_at IS NULL
		FROM manager.sandbox_runtime_checkpoint_forks WHERE operation_id=$1`, request.OperationID).Scan(&retained))
	require.True(t, retained)
	_, err = pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoint_forks SET generation_ref=NULL,storage_released_at=clock_timestamp() WHERE operation_id=$1`, request.OperationID)
	require.ErrorContains(t, err, "terminal child custody", "migration must protect an existing live memory fork")
	_, err = pool.Exec(f.ctx, `UPDATE manager.sandbox_runtime_checkpoint_forks SET generation_id='tampered' WHERE operation_id=$1`, request.OperationID)
	require.Error(t, err, "historical fork identity remains immutable")
	// This is a SQL-only historical custody test; its checkpoint descriptors
	// have no S3 payloads. Supply the completion boundary explicitly. Real
	// legacy object authentication/migration is tested against S3 separately.
	_, err = pool.Exec(f.ctx, `UPDATE manager.rootfs_generations SET inventory_complete=TRUE,storage_inventory_required=FALSE`)
	require.NoError(t, err)
	require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, child.ID, time.Now().UTC()))
	require.NoError(t, f.store.MarkSandboxDeleted(f.ctx, f.sandboxID, time.Now().UTC()))
	_, err = f.store.GarbageCollectRootFSFilesystemWithOptions(f.ctx, storageRetentionDeleter{}, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	var generations int
	require.NoError(t, pool.QueryRow(f.ctx, `SELECT COUNT(*) FROM manager.rootfs_generations`).Scan(&generations))
	require.Zero(t, generations)
}

func TestRootFSWriterTerminalRetryAfterParentGCIntegration(t *testing.T) {
	f := newNomadPauseStoreFixture(t, "retention-terminal-retry")
	pause := terminalizeNomadPauseFixture(t, f)
	filesystem, err := f.store.GetRootFSFilesystem(f.ctx, f.filesystem.ID)
	require.NoError(t, err)
	current, err := f.store.GetRootFSGeneration(f.ctx, filesystem.HeadGenerationID)
	require.NoError(t, err)
	// This SQL-only identity test supplies the inventory completion boundary.
	// The separate S3 tests verify the authenticated traversal establishing it.
	_, err = f.pool.Exec(f.ctx, `UPDATE manager.rootfs_generations SET inventory_complete=TRUE WHERE generation_id=$1`, current.ID)
	require.NoError(t, err)
	_, err = f.store.GarbageCollectRootFSFilesystemWithOptions(f.ctx, storageRetentionDeleter{}, "", 100, DeletePendingRootFSObjectsOptions{})
	require.NoError(t, err)
	_, err = f.store.GetRootFSGeneration(f.ctx, f.initial.ID)
	require.ErrorIs(t, err, ErrRootFSFilesystemNotFound)
	proof := sha256.Sum256([]byte("nomad-pause-proof-" + f.sandboxID))
	request := &CompleteRootFSWriterRetireAndPublishGenerationRequest{
		LifecycleTxnID: pause.OperationID, GrantID: f.issue.GrantID, WriterEpoch: f.writerEpoch,
		OperationID: pause.OperationID, BindingVersion: RootFSWriterBindingVersion,
		BindingDigest: f.issue.BindingDigest, ProofDigest: proof[:], ExpectedOldGenerationID: f.initial.ID, Generation: current}
	retry := func() error {
		return f.store.WithSandboxLock(f.ctx, f.sandboxID, func(ctx context.Context, tx SandboxStoreTx, _ *SandboxRecord) error {
			_, err := tx.(RootFSWriterGrantTx).CompleteRootFSWriterRetireAndPublishGeneration(ctx, request)
			return err
		})
	}
	require.NoError(t, retry(), "an exact committed retry does not require collected parent metadata")
	request.ProofDigest = make([]byte, 32)
	require.Error(t, retry(), "collection does not weaken exact retry proof validation")
}

type storageRetentionDeleter struct{}

func (storageRetentionDeleter) Delete(string) error { return nil }
