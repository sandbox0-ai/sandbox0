package functions

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

func TestNewRevisionCompilesSandboxServiceIntoRevisionSpec(t *testing.T) {
	rev, err := NewRevision("team-1", "source-sandbox", "api", "tmpl-1", map[string]any{
		"id":   "api",
		"port": 3000,
	}, []RestoreMount{{
		SandboxVolumeID:       "revision-volume",
		SourceSandboxVolumeID: "source-volume",
		SnapshotID:            "snapshot-1",
		MountPoint:            "/workspace/data",
	}}, "user-1")
	if err != nil {
		t.Fatalf("NewRevision() error = %v", err)
	}
	if rev.SourceType != RevisionSourceTypeSandboxService {
		t.Fatalf("source_type = %q, want %q", rev.SourceType, RevisionSourceTypeSandboxService)
	}
	if rev.Spec.TemplateID != "tmpl-1" {
		t.Fatalf("spec.template_id = %q, want tmpl-1", rev.Spec.TemplateID)
	}
	if len(rev.Spec.Mounts) != 1 {
		t.Fatalf("spec mounts = %d, want 1", len(rev.Spec.Mounts))
	}
	mount := rev.Spec.Mounts[0]
	if mount.MountPoint != "/workspace/data" || mount.Source.SandboxVolumeID != "revision-volume" {
		t.Fatalf("spec mount = %+v, want prepared sandbox volume mount", mount)
	}
	var provenance RevisionProvenance
	if err := json.Unmarshal(rev.Provenance, &provenance); err != nil {
		t.Fatalf("unmarshal provenance: %v", err)
	}
	if provenance.Type != RevisionSourceTypeSandboxService || provenance.SandboxService == nil || provenance.SandboxService.SandboxID != "source-sandbox" {
		t.Fatalf("provenance = %+v, want sandbox service provenance", provenance)
	}
}

func TestListRuntimeCleanupCandidatesIncludesInactiveAndFailedRuntimes(t *testing.T) {
	ctx := context.Background()
	pool, _ := newGatewayFunctionsTestPool(t)
	repo := NewRepository(pool)

	teamID := uuid.NewString()
	userID := uuid.NewString()
	fn := NewFunction(teamID, "runtime cleanup", userID)
	rev1, err := NewRevision(teamID, "source-sandbox-1", "svc", "tmpl", map[string]any{"id": "svc"}, nil, userID)
	if err != nil {
		t.Fatalf("NewRevision() error = %v", err)
	}
	fn, rev1, err = repo.CreateFunctionWithRevision(ctx, fn, rev1, userID)
	if err != nil {
		t.Fatalf("CreateFunctionWithRevision() error = %v", err)
	}
	rev2, err := NewRevision(teamID, "source-sandbox-2", "svc", "tmpl", map[string]any{"id": "svc"}, nil, userID)
	if err != nil {
		t.Fatalf("NewRevision() error = %v", err)
	}
	rev2, err = repo.CreateRevision(ctx, teamID, fn.ID, rev2, true, userID)
	if err != nil {
		t.Fatalf("CreateRevision() error = %v", err)
	}

	activeReady := createRuntimeInstanceForTest(t, ctx, repo, teamID, fn.ID, rev2.ID, "sb-active-ready", RuntimeInstanceStateReady)
	inactiveReady := createRuntimeInstanceForTest(t, ctx, repo, teamID, fn.ID, rev1.ID, "sb-inactive-ready", RuntimeInstanceStateReady)
	failedActive := createRuntimeInstanceForTest(t, ctx, repo, teamID, fn.ID, rev2.ID, "sb-failed-active", RuntimeInstanceStateFailed)

	if _, err := pool.Exec(ctx, `
		UPDATE function_runtime_instances
		SET ready_at = NOW() - INTERVAL '10 minutes',
			last_used_at = NOW() - INTERVAL '10 minutes',
			updated_at = NOW() - INTERVAL '10 minutes'
		WHERE id = $1
	`, inactiveReady.ID); err != nil {
		t.Fatalf("age inactive runtime: %v", err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE function_runtime_instances
		SET failed_at = NOW() - INTERVAL '10 minutes',
			updated_at = NOW() - INTERVAL '10 minutes'
		WHERE id = $1
	`, failedActive.ID); err != nil {
		t.Fatalf("age failed runtime: %v", err)
	}

	candidates, err := repo.ListRuntimeCleanupCandidates(ctx, 100)
	if err != nil {
		t.Fatalf("ListRuntimeCleanupCandidates() error = %v", err)
	}
	got := runtimeInstanceIDSet(candidates)
	if !got[inactiveReady.ID] {
		t.Fatalf("inactive ready runtime was not selected for cleanup: %#v", got)
	}
	if !got[failedActive.ID] {
		t.Fatalf("failed active runtime was not selected for cleanup: %#v", got)
	}
	if got[activeReady.ID] {
		t.Fatalf("active ready runtime was selected for cleanup: %#v", got)
	}
}

func TestListRuntimeCleanupCandidatesIncludesDisabledFunctionRuntimeImmediately(t *testing.T) {
	ctx := context.Background()
	pool, _ := newGatewayFunctionsTestPool(t)
	repo := NewRepository(pool)

	teamID := uuid.NewString()
	userID := uuid.NewString()
	fn := NewFunction(teamID, "disabled runtime cleanup", userID)
	rev, err := NewRevision(teamID, "source-sandbox", "svc", "tmpl", map[string]any{"id": "svc"}, nil, userID)
	if err != nil {
		t.Fatalf("NewRevision() error = %v", err)
	}
	fn, rev, err = repo.CreateFunctionWithRevision(ctx, fn, rev, userID)
	if err != nil {
		t.Fatalf("CreateFunctionWithRevision() error = %v", err)
	}
	inst := createRuntimeInstanceForTest(t, ctx, repo, teamID, fn.ID, rev.ID, "sb-disabled-ready", RuntimeInstanceStateReady)
	disabled := false
	if _, err := repo.UpdateFunction(ctx, teamID, fn.ID, nil, &disabled, nil); err != nil {
		t.Fatalf("UpdateFunction() error = %v", err)
	}

	candidates, err := repo.ListRuntimeCleanupCandidates(ctx, 100)
	if err != nil {
		t.Fatalf("ListRuntimeCleanupCandidates() error = %v", err)
	}
	if got := runtimeInstanceIDSet(candidates); !got[inst.ID] {
		t.Fatalf("disabled function runtime was not selected for cleanup: %#v", got)
	}
}

func createRuntimeInstanceForTest(t *testing.T, ctx context.Context, repo *Repository, teamID, functionID, revisionID, sandboxID string, state RuntimeInstanceState) *RuntimeInstance {
	t.Helper()
	now := time.Now().UTC()
	inst, err := repo.CreateRuntimeInstance(ctx, &RuntimeInstance{
		TeamID:     teamID,
		FunctionID: functionID,
		RevisionID: revisionID,
		SandboxID:  sandboxID,
		State:      state,
		ReadyAt:    &now,
		LastUsedAt: &now,
	})
	if err != nil {
		t.Fatalf("CreateRuntimeInstance(%s) error = %v", sandboxID, err)
	}
	return inst
}

func runtimeInstanceIDSet(instances []*RuntimeInstance) map[string]bool {
	out := make(map[string]bool, len(instances))
	for _, inst := range instances {
		if inst != nil {
			out[inst.ID] = true
		}
	}
	return out
}

func newGatewayFunctionsTestPool(t *testing.T) (*pgxpool.Pool, string) {
	t.Helper()

	ctx := context.Background()
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil, ""
	}

	schema := fmt.Sprintf("gateway_functions_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	adminPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      schema,
	})
	if err != nil {
		t.Fatalf("connect schema-scoped pool: %v", err)
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(migrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate gateway schema: %v", err)
	}

	return pool, schema
}
