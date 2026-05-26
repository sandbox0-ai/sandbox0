package functions

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

func TestRepositoryDeployRevisionLifecycle(t *testing.T) {
	pool, _ := newFunctionRepositoryTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool)
	teamID := uuid.NewString()
	userID := uuid.NewString()
	spec := testRevisionSpec("echo", []string{"python", "-m", "http.server", "3000"})

	result, err := repo.DeployRevision(ctx, DeployInput{
		TeamID:   teamID,
		UserID:   userID,
		Name:     "Echo",
		Slug:     "Echo API",
		Source:   FunctionSource{Type: RevisionSourceSnapshot, Snapshot: &SnapshotRevisionSource{}},
		Spec:     spec,
		Activate: true,
	})
	if err != nil {
		t.Fatalf("DeployRevision: %v", err)
	}
	if result.Function.Slug != "echo-api" {
		t.Fatalf("slug = %q, want echo-api", result.Function.Slug)
	}
	if result.Function.ActiveRevisionID != result.Revision.ID {
		t.Fatalf("active revision = %q, want %q", result.Function.ActiveRevisionID, result.Revision.ID)
	}
	if result.Revision.Number != 1 {
		t.Fatalf("revision number = %d, want 1", result.Revision.Number)
	}
	if result.Revision.Status != RevisionStatusActive {
		t.Fatalf("revision status = %q, want active", result.Revision.Status)
	}

	second, err := repo.DeployRevision(ctx, DeployInput{
		TeamID:   teamID,
		UserID:   userID,
		Name:     "Echo v2",
		Slug:     "echo-api",
		Source:   FunctionSource{Type: RevisionSourceSnapshot, Snapshot: &SnapshotRevisionSource{}},
		Spec:     testRevisionSpec("echo", []string{"python", "-m", "http.server", "3001"}),
		Activate: false,
	})
	if err != nil {
		t.Fatalf("DeployRevision second: %v", err)
	}
	if second.Revision.Number != 2 {
		t.Fatalf("second revision number = %d, want 2", second.Revision.Number)
	}
	if second.Function.ActiveRevisionID != result.Revision.ID {
		t.Fatalf("active revision changed to %q, want %q", second.Function.ActiveRevisionID, result.Revision.ID)
	}

	functions, err := repo.ListFunctions(ctx, teamID)
	if err != nil {
		t.Fatalf("ListFunctions: %v", err)
	}
	if len(functions) != 1 {
		t.Fatalf("functions len = %d, want 1", len(functions))
	}

	revisions, err := repo.ListRevisions(ctx, teamID, "echo-api")
	if err != nil {
		t.Fatalf("ListRevisions: %v", err)
	}
	if len(revisions) != 2 || revisions[0].Number != 2 || revisions[1].Number != 1 {
		t.Fatalf("unexpected revisions: %+v", revisions)
	}

	activated, err := repo.ActivateRevision(ctx, teamID, "echo-api", second.Revision.ID)
	if err != nil {
		t.Fatalf("ActivateRevision: %v", err)
	}
	if activated.Function.ActiveRevisionID != second.Revision.ID {
		t.Fatalf("active revision = %q, want %q", activated.Function.ActiveRevisionID, second.Revision.ID)
	}

	if err := repo.SetRevisionRuntime(ctx, second.Revision.ID, "sb-runtime", "cluster-a", "ctx-1"); err != nil {
		t.Fatalf("SetRevisionRuntime: %v", err)
	}
	active, err := repo.GetActiveRevisionByDomainLabel(ctx, result.Function.DomainLabel)
	if err != nil {
		t.Fatalf("GetActiveRevisionByDomainLabel: %v", err)
	}
	if active.Revision.RuntimeSandboxID != "sb-runtime" {
		t.Fatalf("runtime sandbox = %q, want sb-runtime", active.Revision.RuntimeSandboxID)
	}
	if err := repo.ClearRevisionRuntime(ctx, second.Revision.ID); err != nil {
		t.Fatalf("ClearRevisionRuntime: %v", err)
	}

	if err := repo.DeleteFunction(ctx, teamID, "echo-api"); err != nil {
		t.Fatalf("DeleteFunction: %v", err)
	}
	if _, err := repo.GetFunction(ctx, teamID, "echo-api"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetFunction after delete err = %v, want %v", err, ErrNotFound)
	}
}

func testRevisionSpec(serviceID string, command []string) FunctionRevisionSpec {
	return FunctionRevisionSpec{
		Template: "python",
		Service: mgr.SandboxAppService{
			ID:   serviceID,
			Port: 3000,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type:    mgr.SandboxAppServiceRuntimeCMD,
				Command: command,
			},
		},
	}
}

func newFunctionRepositoryTestPool(t *testing.T) (*pgxpool.Pool, string) {
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
