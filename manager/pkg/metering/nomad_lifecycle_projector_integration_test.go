package metering

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/egressauthstore"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	meteringoutbox "github.com/sandbox0-ai/sandbox0/pkg/metering/outbox"
)

func TestNomadLifecycleProjectorCommitsDurableHistoryAndResumesAfterRestartIntegration(t *testing.T) {
	ctx := context.Background()
	pool := newNomadMeteringIntegrationPool(t)
	store := sandboxstore.NewPGSandboxStore(pool)
	claimedAt := time.Now().UTC().Add(-time.Minute).Truncate(time.Microsecond)
	activeAt := claimedAt.Add(time.Second)
	record := &sandboxstore.SandboxRecord{
		ID: "sandbox-nomad-metering", TeamID: "team-1", UserID: "user-1",
		TemplateID: "template-1", TemplateName: "template-1", TemplateNamespace: "default",
		ClusterID: "cluster-1", DesiredState: sandboxstore.SandboxDesiredStateActive,
		RuntimeID: "allocation-1", RuntimeNamespace: "default", RuntimeGeneration: 1,
		ResourceMillicpu: 1000, ResourceMemoryMiB: 1024,
		ClaimedAt: claimedAt, CreatedAt: claimedAt,
	}
	if err := store.UpsertSandbox(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO manager.sandbox_runtime_claims (
			sandbox_id, operation_id, phase, completed_at
		) VALUES ($1, 'initial-claim', $2, $3)
	`, record.ID, sandboxstore.SandboxRuntimeClaimPhaseReady, activeAt); err != nil {
		t.Fatal(err)
	}

	repo := meteringoutbox.NewRepository(pool)
	projector, err := NewNomadLifecycleProjector(repo, "region-1", "cluster-1", NomadLifecycleProjectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	processed, err := projector.RunOnce(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("initial RunOnce = (%d, %v)", processed, err)
	}
	state, err := repo.GetSandboxProjectionState(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || state.ActiveSince == nil || !state.ActiveSince.Equal(activeAt) || state.Paused {
		t.Fatalf("initial projection state = %#v", state)
	}

	time.Sleep(2 * time.Millisecond)
	commitNomadMeteringPause(t, ctx, store, record.ID, "pause-1", 1)
	time.Sleep(2 * time.Millisecond)
	commitNomadMeteringResume(t, ctx, store, record.ID, "resume-1", 2)
	time.Sleep(2 * time.Millisecond)
	commitNomadMeteringPause(t, ctx, store, record.ID, "pause-2", 2)

	processed, err = projector.RunOnce(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("transition RunOnce = (%d, %v)", processed, err)
	}
	state, err = repo.GetSandboxProjectionState(ctx, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if state == nil || !state.Paused || state.ActiveSince != nil || state.SourceLifecycleEpoch != 3 {
		t.Fatalf("transition projection state = %#v", state)
	}
	assertNomadMeteringOutboxCount(t, ctx, pool, "event", meteringpkg.EventTypeSandboxClaimed, 1)
	assertNomadMeteringOutboxCount(t, ctx, pool, "event", meteringpkg.EventTypeSandboxPaused, 2)
	assertNomadMeteringOutboxCount(t, ctx, pool, "event", meteringpkg.EventTypeSandboxResumed, 1)
	assertNomadMeteringOutboxCount(t, ctx, pool, "window", meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds, 2)

	restarted, err := NewNomadLifecycleProjector(repo, "region-1", "cluster-1", NomadLifecycleProjectorConfig{})
	if err != nil {
		t.Fatal(err)
	}
	processed, err = restarted.RunOnce(ctx)
	if err != nil || processed != 0 {
		t.Fatalf("restart RunOnce = (%d, %v), want drained", processed, err)
	}

	deletedAt := time.Now().UTC().Add(time.Millisecond)
	if err := store.MarkSandboxDeleted(ctx, record.ID, deletedAt); err != nil {
		t.Fatal(err)
	}
	processed, err = restarted.RunOnce(ctx)
	if err != nil || processed != 1 {
		t.Fatalf("termination RunOnce = (%d, %v)", processed, err)
	}
	assertNomadMeteringOutboxCount(t, ctx, pool, "event", meteringpkg.EventTypeSandboxTerminated, 1)
	assertNomadMeteringOutboxCount(t, ctx, pool, "window", meteringpkg.WindowTypeSandboxRuntimeMiBMilliseconds, 2)
}

func commitNomadMeteringPause(
	t *testing.T,
	ctx context.Context,
	store *sandboxstore.PGSandboxStore,
	sandboxID, operationID string,
	generation int64,
) {
	t.Helper()
	if err := store.WithSandboxLock(ctx, sandboxID, func(
		lockCtx context.Context,
		tx sandboxstore.SandboxStoreTx,
		record *sandboxstore.SandboxRecord,
	) error {
		if err := tx.BeginLifecycleTxn(lockCtx, &sandboxstore.SandboxLifecycleTxn{
			ID: operationID, SandboxID: sandboxID, Kind: sandboxstore.SandboxLifecycleKindPause,
			Phase:          sandboxstore.SandboxLifecyclePhaseCommitting,
			FromGeneration: generation, FromRuntimeNamespace: record.RuntimeNamespace, FromRuntimeID: record.RuntimeID,
		}); err != nil {
			return err
		}
		if err := tx.MarkRuntimePaused(lockCtx, sandboxID, generation, time.Time{}); err != nil {
			return err
		}
		return tx.CommitLifecycleTxn(lockCtx, operationID, "")
	}); err != nil {
		t.Fatal(err)
	}
}

func commitNomadMeteringResume(
	t *testing.T,
	ctx context.Context,
	store *sandboxstore.PGSandboxStore,
	sandboxID, operationID string,
	generation int64,
) {
	t.Helper()
	if err := store.WithSandboxLock(ctx, sandboxID, func(
		lockCtx context.Context,
		tx sandboxstore.SandboxStoreTx,
		record *sandboxstore.SandboxRecord,
	) error {
		if err := tx.BeginLifecycleTxn(lockCtx, &sandboxstore.SandboxLifecycleTxn{
			ID: operationID, SandboxID: sandboxID, Kind: sandboxstore.SandboxLifecycleKindResume,
			Phase:          sandboxstore.SandboxLifecyclePhaseCommitting,
			FromGeneration: record.RuntimeGeneration, ToGeneration: generation,
		}); err != nil {
			return err
		}
		if err := tx.SaveRuntime(
			lockCtx, sandboxID, "default", "allocation-2", generation,
			time.Time{}, time.Time{}, "",
		); err != nil {
			return err
		}
		return tx.CommitLifecycleTxn(lockCtx, operationID, "")
	}); err != nil {
		t.Fatal(err)
	}
}

func assertNomadMeteringOutboxCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	operationType, value string,
	want int64,
) {
	t.Helper()
	field := "event_type"
	if operationType == "window" {
		field = "window_type"
	}
	var count int64
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM metering.projection_outbox
		WHERE operation_type = $1 AND payload->>$2 = $3
	`, operationType, field, value).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox %s/%s count = %d, want %d", operationType, value, count, want)
	}
}

func newNomadMeteringIntegrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}
	ctx := context.Background()
	pool, err := dbpool.New(ctx, dbpool.Options{DatabaseURL: databaseURL, Schema: "scheduler", MaxConns: 10})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if _, err := pool.Exec(ctx, `
		DROP SCHEMA IF EXISTS manager CASCADE;
		DROP SCHEMA IF EXISTS metering CASCADE;
		DROP SCHEMA IF EXISTS scheduler CASCADE;
		CREATE SCHEMA scheduler;
		CREATE OR REPLACE FUNCTION scheduler.update_updated_at_column()
		RETURNS TRIGGER AS $$
		BEGIN
			NEW.updated_at = NOW();
			RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS manager CASCADE")
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS metering CASCADE")
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS scheduler CASCADE")
	})
	logger := nomadMeteringMigrationLogger{}
	if err := egressauthstore.RunMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	if err := sandboxstore.RunSandboxStoreMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	if err := meteringoutbox.RunMigrations(ctx, pool, logger); err != nil {
		t.Fatal(err)
	}
	return pool
}

type nomadMeteringMigrationLogger struct{}

func (nomadMeteringMigrationLogger) Printf(string, ...any) {}
func (nomadMeteringMigrationLogger) Fatalf(string, ...any) {}
