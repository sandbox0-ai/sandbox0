package admission

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

func TestRepositoryPostgresIntegration(t *testing.T) {
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	schema := "gateway_admission_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	adminPool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect admin pool: %v", err)
	}
	t.Cleanup(adminPool.Close)
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: databaseURL,
		MaxConns:    20,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect schema pool: %v", err)
	}
	t.Cleanup(pool.Close)

	migrationOptions := []migrate.Option{
		migrate.WithBaseFS(gatewaymigrations.FS),
		migrate.WithSchema(schema),
	}
	if err := migrate.Up(ctx, pool, ".", migrationOptions...); err != nil {
		t.Fatalf("migrate up: %v", err)
	}
	if err := migrate.Down(ctx, pool, ".", migrationOptions...); err != nil {
		t.Fatalf("migrate admission decoupling down: %v", err)
	}
	var admissionForeignKey *string
	if err := adminPool.QueryRow(ctx, `
		SELECT conname
		FROM pg_constraint
		WHERE conrelid = to_regclass($1)
		  AND conname = 'team_admission_states_team_id_fkey'
	`, schema+".team_admission_states").Scan(&admissionForeignKey); err != nil {
		t.Fatalf("look up restored admission foreign key: %v", err)
	}
	if admissionForeignKey == nil || *admissionForeignKey != "team_admission_states_team_id_fkey" {
		t.Fatalf("restored admission foreign key = %v", admissionForeignKey)
	}
	if err := migrate.Down(ctx, pool, ".", migrationOptions...); err != nil {
		t.Fatalf("migrate admission table down: %v", err)
	}
	var admissionTable *string
	if err := adminPool.QueryRow(ctx, "SELECT to_regclass($1)::text", schema+".team_admission_states").Scan(&admissionTable); err != nil {
		t.Fatalf("look up rolled back table: %v", err)
	}
	if admissionTable != nil {
		t.Fatalf("team_admission_states still exists after down migration: %s", *admissionTable)
	}
	if err := migrate.Up(ctx, pool, ".", migrationOptions...); err != nil {
		t.Fatalf("migrate back up: %v", err)
	}

	repository := NewRepository(pool)
	teamID := uuid.NewString()
	if _, found, err := repository.Get(ctx, teamID); err != nil || found {
		t.Fatalf("default Get() found = %v, error = %v", found, err)
	}

	const versions = 24
	var waitGroup sync.WaitGroup
	errs := make(chan error, versions)
	for version := int64(1); version <= versions; version++ {
		waitGroup.Add(1)
		go func(version int64) {
			defer waitGroup.Done()
			_, err := repository.Put(ctx, teamID, Update{
				Version: version,
				State:   StateRestricted,
				Source:  "integration",
				Reason:  fmt.Sprintf("version-%d", version),
			})
			errs <- err
		}(version)
	}
	waitGroup.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Put() error = %v", err)
		}
	}
	record, found, err := repository.Get(ctx, teamID)
	if err != nil || !found {
		t.Fatalf("final Get() found = %v, error = %v", found, err)
	}
	if record.Version != versions || record.Reason != fmt.Sprintf("version-%d", versions) {
		t.Fatalf("final admission = %#v", record)
	}

	conflictTeamID := uuid.NewString()
	conflictUpdates := []Update{
		{Version: 1, State: StateAllowed, Source: "integration", Reason: "a"},
		{Version: 1, State: StateRestricted, Source: "integration", Reason: "b"},
	}
	results := make(chan error, len(conflictUpdates))
	for _, update := range conflictUpdates {
		waitGroup.Add(1)
		go func(update Update) {
			defer waitGroup.Done()
			_, err := repository.Put(ctx, conflictTeamID, update)
			results <- err
		}(update)
	}
	waitGroup.Wait()
	close(results)
	successes := 0
	conflicts := 0
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrVersionConflict):
			conflicts++
		default:
			t.Fatalf("conflicting Put() error = %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("conflicting results: successes = %d, conflicts = %d", successes, conflicts)
	}

	replayTeamID := uuid.NewString()
	replay := Update{Version: 1, State: StateRestricted, Source: "integration", Reason: "same"}
	replayApplied := make(chan bool, 2)
	replayErrors := make(chan error, 2)
	for range 2 {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			result, err := repository.Put(ctx, replayTeamID, replay)
			replayApplied <- result.Applied
			replayErrors <- err
		}()
	}
	waitGroup.Wait()
	close(replayApplied)
	close(replayErrors)
	appliedCount := 0
	for applied := range replayApplied {
		if applied {
			appliedCount++
		}
	}
	for err := range replayErrors {
		if err != nil {
			t.Fatalf("replayed Put() error = %v", err)
		}
	}
	if appliedCount != 1 {
		t.Fatalf("replayed Put() applied count = %d, want 1", appliedCount)
	}

	localTeamID := createAdmissionTestTeam(t, ctx, pool, "cleanup")
	if _, err := repository.Put(ctx, localTeamID, Update{
		Version: 1,
		State:   StateAllowed,
		Source:  "integration",
	}); err != nil {
		t.Fatalf("Put() local team admission state: %v", err)
	}
	if _, err := pool.Exec(ctx, "DELETE FROM teams WHERE id = $1", localTeamID); err != nil {
		t.Fatalf("delete team: %v", err)
	}
	if _, found, err := repository.Get(ctx, localTeamID); err != nil || found {
		t.Fatalf("Get() after team delete found = %v, error = %v", found, err)
	}
}

func createAdmissionTestTeam(t *testing.T, ctx context.Context, pool *pgxpool.Pool, suffix string) string {
	t.Helper()
	teamID := uuid.NewString()
	if _, err := pool.Exec(
		ctx,
		"INSERT INTO teams (id, name, slug) VALUES ($1, $2, $3)",
		teamID,
		"Admission "+suffix,
		"admission-"+suffix+"-"+uuid.NewString(),
	); err != nil {
		t.Fatalf("create team: %v", err)
	}
	return teamID
}
