package outbox

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

func TestRepositoryCapturesBusinessTransactionAndProjectionBatch(t *testing.T) {
	pool := newMeteringTestDatabase(t)
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE business_resource (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create business table: %v", err)
	}

	repo := NewRepository(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	event := &metering.Event{
		EventID:     "event-1",
		Producer:    "test",
		EventType:   metering.EventTypeSandboxClaimed,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		OccurredAt:  now,
	}
	state := &metering.SandboxProjectionState{
		SandboxID:      "sandbox-1",
		Namespace:      "default",
		LastObservedAt: now,
	}
	wantRollback := errors.New("rollback business transaction")
	err := repo.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO business_resource (id) VALUES ('resource-1')`); err != nil {
			return err
		}
		if err := repo.AppendEventTx(ctx, tx, event); err != nil {
			return err
		}
		if err := repo.UpsertSandboxProjectionStateTx(ctx, tx, state); err != nil {
			return err
		}
		return wantRollback
	})
	if !errors.Is(err, wantRollback) {
		t.Fatalf("rolled back transaction error = %v", err)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM business_resource`, 0)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.manager_sandbox_projection_state`, 0)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.projection_outbox`, 0)

	window := &metering.Window{
		WindowID:    "window-1",
		Producer:    "test",
		WindowType:  metering.WindowTypeSandboxEgressBytes,
		SubjectType: metering.SubjectTypeSandbox,
		SubjectID:   "sandbox-1",
		WindowStart: now,
		WindowEnd:   now.Add(time.Second),
		Value:       10,
		Unit:        metering.WindowUnitBytes,
	}
	if err := repo.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO business_resource (id) VALUES ('resource-1')`); err != nil {
			return err
		}
		if err := repo.AppendEventTx(ctx, tx, event); err != nil {
			return err
		}
		if err := repo.AppendWindowTx(ctx, tx, window); err != nil {
			return err
		}
		if err := repo.UpsertSandboxProjectionStateTx(ctx, tx, state); err != nil {
			return err
		}
		return repo.UpsertProducerWatermarkTx(ctx, tx, "test", "region-1", now)
	}); err != nil {
		t.Fatalf("commit business transaction: %v", err)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM business_resource`, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.manager_sandbox_projection_state`, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.projection_outbox`, 4)
	assertCount(t, pool, `SELECT COUNT(DISTINCT batch_id) FROM metering.projection_outbox`, 1)
	if event.RecordedAt.IsZero() || window.RecordedAt.IsZero() {
		t.Fatal("captured payloads must receive stable recorded_at values")
	}

	repo.now = func() time.Time { return now.Add(time.Minute) }
	first, err := repo.ClaimNextBatch(ctx, "worker-1", 30*time.Second)
	if err != nil || first == nil || len(first.Operations) != 4 {
		t.Fatalf("first ClaimNextBatch = (%#v, %v)", first, err)
	}
	if err := repo.MarkFailed(ctx, first.ID, "worker-1", "clickhouse unavailable", repo.timestamp()); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}
	second, err := repo.ClaimNextBatch(ctx, "worker-1", 30*time.Second)
	if err != nil || second == nil || len(second.Operations) != len(first.Operations) {
		t.Fatalf("second ClaimNextBatch = (%#v, %v)", second, err)
	}
	for i := range first.Operations {
		if first.Operations[i].Sequence != second.Operations[i].Sequence || string(first.Operations[i].Payload) != string(second.Operations[i].Payload) {
			t.Fatalf("operation %d changed across retry: %#v != %#v", i, first.Operations[i], second.Operations[i])
		}
	}
	if err := repo.MarkDelivered(ctx, second.ID, "worker-1"); err != nil {
		t.Fatalf("MarkDelivered: %v", err)
	}
	stats, err := repo.Stats(ctx)
	if err != nil || stats.Pending != 0 || stats.OldestPending != nil {
		t.Fatalf("Stats after delivery = (%#v, %v)", stats, err)
	}
}

func TestRepositoryStorageProjectionIsAtomicWithCallerTransaction(t *testing.T) {
	pool := newMeteringTestDatabase(t)
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `CREATE TABLE business_volume (id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create business table: %v", err)
	}
	repo := NewRepository(pool)
	start := time.Now().UTC().Truncate(time.Microsecond).Add(-time.Hour)
	observation := &metering.StorageObservation{
		SubjectType: metering.SubjectTypeVolume,
		SubjectID:   "volume-1",
		VolumeID:    "volume-1",
		TeamID:      "team-1",
		SizeBytes:   1024,
		ObservedAt:  start,
	}
	rollback := errors.New("rollback volume")
	err := repo.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO business_volume (id) VALUES ('volume-1')`); err != nil {
			return err
		}
		if err := repo.RecordStorageObservationTx(ctx, tx, observation); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("rollback error = %v", err)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM business_volume`, 0)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.storage_projection_state`, 0)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.projection_outbox`, 0)

	if err := repo.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO business_volume (id) VALUES ('volume-1')`); err != nil {
			return err
		}
		return repo.RecordStorageObservationTx(ctx, tx, observation)
	}); err != nil {
		t.Fatalf("record first storage observation: %v", err)
	}
	observation.ObservedAt = start.Add(time.Hour)
	if err := repo.RecordStorageObservation(ctx, observation); err != nil {
		t.Fatalf("record second storage observation: %v", err)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.storage_projection_state`, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.projection_outbox WHERE operation_type = 'window'`, 1)
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.projection_outbox WHERE operation_type = 'storage_state'`, 2)

	state, err := repo.GetStorageProjectionState(ctx, metering.SubjectTypeVolume, "volume-1")
	if err != nil {
		t.Fatalf("get storage projection state: %v", err)
	}
	if state == nil || state.SizeBytes != 1024 || !state.ObservedAt.Equal(start.Add(time.Hour)) {
		t.Fatalf("storage projection state = %+v, want size 1024 observed at %s", state, start.Add(time.Hour))
	}
	states, err := repo.ListStorageProjectionStatesByTeam(ctx, metering.SubjectTypeVolume, "team-1")
	if err != nil {
		t.Fatalf("list storage projection states: %v", err)
	}
	if len(states) != 1 || states[0].SubjectID != "volume-1" {
		t.Fatalf("storage projection states = %+v, want volume-1", states)
	}
	otherTeamStates, err := repo.ListStorageProjectionStatesByTeam(ctx, metering.SubjectTypeVolume, "team-2")
	if err != nil {
		t.Fatalf("list other team storage projection states: %v", err)
	}
	if len(otherTeamStates) != 0 {
		t.Fatalf("other team storage projection states = %+v, want empty", otherTeamStates)
	}
}

func TestMigrationsUpgradeLegacyVersionFiveSchema(t *testing.T) {
	pool := newMeteringTestDatabase(t)
	ctx := context.Background()
	_, err := pool.Exec(ctx, `
		CREATE SCHEMA metering;
		CREATE TABLE metering.goose_db_version (
			id SERIAL PRIMARY KEY,
			version_id BIGINT NOT NULL,
			is_applied BOOLEAN NOT NULL,
			tstamp TIMESTAMP NOT NULL DEFAULT NOW()
		);
		INSERT INTO metering.goose_db_version (version_id, is_applied) VALUES (0, TRUE), (5, TRUE);
		CREATE TABLE metering.manager_sandbox_projection_state (
			sandbox_id TEXT PRIMARY KEY,
			namespace TEXT NOT NULL,
			team_id TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL DEFAULT '',
			template_id TEXT NOT NULL DEFAULT '', cluster_id TEXT NOT NULL DEFAULT '',
			owner_kind TEXT NOT NULL DEFAULT '', resource_millicpu BIGINT NOT NULL DEFAULT 0,
			resource_memory_mib BIGINT NOT NULL DEFAULT 0, claimed_at TIMESTAMPTZ,
			active_since TIMESTAMPTZ, paused BOOLEAN NOT NULL DEFAULT FALSE,
			paused_at TIMESTAMPTZ, terminated_at TIMESTAMPTZ,
			last_observed_at TIMESTAMPTZ NOT NULL, last_resource_version TEXT NOT NULL DEFAULT ''
		);
		CREATE TABLE metering.storage_projection_state (
			subject_type TEXT NOT NULL, subject_id TEXT NOT NULL,
			product TEXT NOT NULL DEFAULT 'sandbox', owner_kind TEXT NOT NULL DEFAULT '',
			team_id TEXT NOT NULL DEFAULT '', user_id TEXT NOT NULL DEFAULT '',
			sandbox_id TEXT, volume_id TEXT, snapshot_id TEXT, cluster_id TEXT,
			region_id TEXT NOT NULL DEFAULT '', size_bytes BIGINT NOT NULL DEFAULT 0,
			observed_at TIMESTAMPTZ NOT NULL, unbilled_byte_nanoseconds BIGINT NOT NULL DEFAULT 0,
			updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (subject_type, subject_id)
		);
	`)
	if err != nil {
		t.Fatalf("create legacy schema: %v", err)
	}
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("upgrade legacy schema: %v", err)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.goose_db_version WHERE version_id = 6 AND is_applied`, 1)
	var tableName string
	if err := pool.QueryRow(ctx, `SELECT to_regclass('metering.projection_outbox')::text`).Scan(&tableName); err != nil || tableName == "" {
		t.Fatalf("projection_outbox after migration = %q, %v", tableName, err)
	}
}

type fakeProjectionStateSource struct {
	sandboxes []*metering.SandboxProjectionState
	storage   []*metering.StorageProjectionState
}

func (f *fakeProjectionStateSource) ListActiveSandboxProjectionStates(context.Context) ([]*metering.SandboxProjectionState, error) {
	return f.sandboxes, nil
}

func (f *fakeProjectionStateSource) ListActiveStorageProjectionStates(context.Context) ([]*metering.StorageProjectionState, error) {
	return f.storage, nil
}

func TestBootstrapProjectionStatesKeepsNewerPostgresStateAndCopiesNoHistory(t *testing.T) {
	pool := newMeteringTestDatabase(t)
	ctx := context.Background()
	if err := RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	repo := NewRepository(pool)
	now := time.Now().UTC().Truncate(time.Microsecond)
	source := &fakeProjectionStateSource{
		sandboxes: []*metering.SandboxProjectionState{{
			SandboxID:      "sandbox-1",
			Namespace:      "default",
			TeamID:         "team-current",
			LastObservedAt: now,
		}},
		storage: []*metering.StorageProjectionState{{
			SubjectType: metering.SubjectTypeVolume,
			SubjectID:   "volume-1",
			TeamID:      "team-current",
			SizeBytes:   1024,
			ObservedAt:  now,
		}},
	}
	result, err := repo.BootstrapProjectionStates(ctx, source)
	if err != nil || result.SandboxStates != 1 || result.StorageStates != 1 {
		t.Fatalf("first BootstrapProjectionStates = (%#v, %v)", result, err)
	}
	completed, err := repo.ProjectionBootstrapCompleted(ctx)
	if err != nil || !completed {
		t.Fatalf("ProjectionBootstrapCompleted = (%v, %v), want true", completed, err)
	}
	assertCount(t, pool, `SELECT COUNT(*) FROM metering.projection_outbox`, 0)

	source.sandboxes[0].TeamID = "team-stale"
	source.sandboxes[0].LastObservedAt = now.Add(-time.Minute)
	source.storage[0].TeamID = "team-stale"
	source.storage[0].ObservedAt = now.Add(-time.Minute)
	result, err = repo.BootstrapProjectionStates(ctx, source)
	if err != nil || result.SandboxStates != 0 || result.StorageStates != 0 {
		t.Fatalf("stale BootstrapProjectionStates = (%#v, %v)", result, err)
	}
	var sandboxTeam, storageTeam string
	if err := pool.QueryRow(ctx, `SELECT team_id FROM metering.manager_sandbox_projection_state WHERE sandbox_id = 'sandbox-1'`).Scan(&sandboxTeam); err != nil {
		t.Fatalf("query sandbox bootstrap state: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT team_id FROM metering.storage_projection_state WHERE subject_id = 'volume-1'`).Scan(&storageTeam); err != nil {
		t.Fatalf("query storage bootstrap state: %v", err)
	}
	if sandboxTeam != "team-current" || storageTeam != "team-current" {
		t.Fatalf("stale bootstrap overwrote state: sandbox=%q storage=%q", sandboxTeam, storageTeam)
	}
}

func newMeteringTestDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	adminConfig, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminConfig)
	if err != nil {
		t.Fatalf("connect test database admin: %v", err)
	}
	databaseName := "metering_outbox_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	quotedName := `"` + strings.ReplaceAll(databaseName, `"`, `""`) + `"`
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+quotedName); err != nil {
		admin.Close()
		t.Fatalf("create test database: %v", err)
	}
	testConfig := adminConfig.Copy()
	testConfig.ConnConfig.Database = databaseName
	pool, err := pgxpool.NewWithConfig(ctx, testConfig)
	if err != nil {
		_, _ = admin.Exec(ctx, "DROP DATABASE "+quotedName)
		admin.Close()
		t.Fatalf("connect isolated test database: %v", err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(context.Background(), "DROP DATABASE "+quotedName); err != nil {
			t.Errorf("drop test database: %v", err)
		}
		admin.Close()
	})
	return pool
}

func assertCount(t *testing.T, pool *pgxpool.Pool, query string, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(context.Background(), query).Scan(&got); err != nil {
		t.Fatalf("query count %q: %v", query, err)
	}
	if got != want {
		t.Fatalf("count for %q = %d, want %d", query, got, want)
	}
}
