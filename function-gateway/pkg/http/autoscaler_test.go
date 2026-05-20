package http

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"go.uber.org/zap"
)

func TestFunctionAutoscalerReserveReadyHonorsTargetConcurrency(t *testing.T) {
	autoscaler := newFunctionAutoscaler(&Server{})
	instances := []*functions.RuntimeInstance{
		{
			ID:        "inst-a",
			SandboxID: "sandbox-a",
			State:     functions.RuntimeInstanceStateReady,
		},
		{
			ID:        "inst-b",
			SandboxID: "sandbox-b",
			State:     functions.RuntimeInstanceStateReady,
		},
	}
	cfg := functions.Autoscaling{MaxActive: 2, TargetConcurrency: 1, ScaleDownAfterSeconds: 300}

	first, firstRelease := autoscaler.reserveReady(instances, cfg, false)
	if first == nil {
		t.Fatal("first reservation is nil")
	}
	defer firstRelease()
	second, secondRelease := autoscaler.reserveReady(instances, cfg, false)
	if second == nil {
		t.Fatal("second reservation is nil")
	}
	defer secondRelease()
	if first.SandboxID == second.SandboxID {
		t.Fatalf("second reservation used %q, want the other ready runtime", second.SandboxID)
	}
	third, thirdRelease := autoscaler.reserveReady(instances, cfg, false)
	if third != nil {
		thirdRelease()
		t.Fatalf("third reservation = %+v, want nil when all local runtimes hit target", third)
	}
}

func TestFunctionAutoscalerReserveReadyCanOverflowSoftTarget(t *testing.T) {
	autoscaler := newFunctionAutoscaler(&Server{})
	instances := []*functions.RuntimeInstance{{
		ID:        "inst-a",
		SandboxID: "sandbox-a",
		State:     functions.RuntimeInstanceStateReady,
	}}
	cfg := functions.Autoscaling{MaxActive: 1, TargetConcurrency: 1, ScaleDownAfterSeconds: 300}

	first, firstRelease := autoscaler.reserveReady(instances, cfg, false)
	if first == nil {
		t.Fatal("first reservation is nil")
	}
	defer firstRelease()
	second, secondRelease := autoscaler.reserveReady(instances, cfg, true)
	if second == nil {
		t.Fatal("second reservation is nil with allowOverTarget")
	}
	defer secondRelease()
	if second.SandboxID != "sandbox-a" {
		t.Fatalf("second sandbox = %q, want sandbox-a", second.SandboxID)
	}
}

func TestFunctionAutoscalerTracksLocalInflight(t *testing.T) {
	autoscaler := newFunctionAutoscaler(&Server{})
	release := autoscaler.reserveSandbox("sandbox-a")
	if !autoscaler.hasLocalInflight("sandbox-a") {
		t.Fatal("hasLocalInflight() = false, want true")
	}
	release()
	if autoscaler.hasLocalInflight("sandbox-a") {
		t.Fatal("hasLocalInflight() = true after release, want false")
	}
}

func TestActiveRuntimeInstanceCountIncludesAllocatedCapacity(t *testing.T) {
	instances := []*functions.RuntimeInstance{
		{State: functions.RuntimeInstanceStateStarting},
		{State: functions.RuntimeInstanceStateReady},
		{State: functions.RuntimeInstanceStateDraining},
		{State: functions.RuntimeInstanceStateFailed},
	}
	if got := activeRuntimeInstanceCount(instances); got != 3 {
		t.Fatalf("activeRuntimeInstanceCount() = %d, want 3", got)
	}
}

func TestCleanupRuntimeInstanceRecordsDeletedEventBeforeDeletingInstance(t *testing.T) {
	ctx := context.Background()
	pool := newFunctionAutoscalerTestPool(t)
	repo := functions.NewRepository(pool)

	teamID := uuid.NewString()
	userID := uuid.NewString()
	fn := functions.NewFunction(teamID, "runtime gc event", userID)
	rev, err := functions.NewRevision(teamID, "source-sandbox", "api", "default", map[string]any{"id": "api"}, nil, userID)
	if err != nil {
		t.Fatalf("NewRevision() error = %v", err)
	}
	fn, rev, err = repo.CreateFunctionWithRevision(ctx, fn, rev, userID)
	if err != nil {
		t.Fatalf("CreateFunctionWithRevision() error = %v", err)
	}
	failedAt := time.Now().UTC().Add(-10 * time.Minute)
	inst, err := repo.CreateRuntimeInstance(ctx, &functions.RuntimeInstance{
		TeamID:         teamID,
		FunctionID:     fn.ID,
		RevisionID:     rev.ID,
		SandboxID:      "runtime-sandbox",
		State:          functions.RuntimeInstanceStateFailed,
		ReadinessState: functions.RuntimeReadinessStateFailed,
		FailedAt:       &failedAt,
	})
	if err != nil {
		t.Fatalf("CreateRuntimeInstance() error = %v", err)
	}

	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete {
			t.Errorf("method = %s, want DELETE", r.Method)
		}
		if r.URL.Path != "/api/v1/sandboxes/runtime-sandbox" {
			t.Errorf("path = %s, want runtime sandbox delete path", r.URL.Path)
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Error("internal auth header is empty")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer clusterGateway.Close()

	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		functionRepo: repo,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}

	newFunctionAutoscaler(server).cleanupRuntimeInstance(ctx, inst)

	var instanceCount int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM function_runtime_instances WHERE id = $1`, inst.ID).Scan(&instanceCount); err != nil {
		t.Fatalf("count runtime instances: %v", err)
	}
	if instanceCount != 0 {
		t.Fatalf("runtime instance count = %d, want 0", instanceCount)
	}

	var eventCount int
	var runtimeInstanceID *string
	var runtimeSandboxID string
	if err := pool.QueryRow(ctx, `
		SELECT COUNT(*), MAX(runtime_instance_id::text), MAX(runtime_sandbox_id)
		FROM function_runtime_events
		WHERE function_id = $1 AND revision_id = $2 AND reason = 'runtime_gc_deleted'
	`, fn.ID, rev.ID).Scan(&eventCount, &runtimeInstanceID, &runtimeSandboxID); err != nil {
		t.Fatalf("query runtime_gc_deleted event: %v", err)
	}
	if eventCount != 1 {
		t.Fatalf("runtime_gc_deleted event count = %d, want 1", eventCount)
	}
	if runtimeInstanceID != nil {
		t.Fatalf("runtime_gc_deleted runtime_instance_id = %q, want null after instance deletion", *runtimeInstanceID)
	}
	if runtimeSandboxID != inst.SandboxID {
		t.Fatalf("runtime_gc_deleted runtime_sandbox_id = %q, want %q", runtimeSandboxID, inst.SandboxID)
	}
}

func newFunctionAutoscalerTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	schema := fmt.Sprintf("function_autoscaler_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
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
	return pool
}
