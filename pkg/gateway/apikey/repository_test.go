package apikey

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	gatewaymigrations "github.com/sandbox0-ai/sandbox0/pkg/gateway/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	teamquotatestutil "github.com/sandbox0-ai/sandbox0/pkg/teamquota/testutil"
)

func TestValidateAPIKeyRequiresExistingTeam(t *testing.T) {
	pool := newGatewayAPIKeyTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	identityRepo := identity.NewRepository(pool)
	apiKeyRepo := NewRepository(
		pool,
		WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()),
		WithAuthenticationCacheConfig(AuthenticationCacheConfig{
			PositiveTTL: 5 * time.Millisecond,
			NegativeTTL: time.Millisecond,
			MaxEntries:  10,
		}),
	)
	t.Cleanup(func() { _ = apiKeyRepo.Close() })

	user := &identity.User{
		Email: "api-key-owner@example.com",
		Name:  "API Key Owner",
	}
	if err := identityRepo.CreateUser(ctx, user); err != nil {
		t.Fatalf("create user: %v", err)
	}

	ownerID := user.ID
	team := &identity.Team{
		Name:    "API Key Team",
		Slug:    "api-key-team",
		OwnerID: &ownerID,
	}
	if _, err := identityRepo.CreateTeamWithOwner(ctx, team); err != nil {
		t.Fatalf("create team: %v", err)
	}

	_, keyValue, err := apiKeyRepo.CreateAPIKey(
		ctx,
		team.ID,
		"aws-us-east-1",
		user.ID,
		"build key",
		ScopeTeam,
		[]string{"viewer"},
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create api key: %v", err)
	}

	if _, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue); err != nil {
		t.Fatalf("validate api key before team deletion: %v", err)
	}

	if err := identityRepo.FenceTeamDeletionOwnedBy(ctx, team.ID, user.ID); err != nil {
		t.Fatalf("fence team deletion: %v", err)
	}
	if err := identityRepo.DeleteTeamOwnedBy(ctx, team.ID, user.ID); err != nil {
		t.Fatalf("delete fenced team: %v", err)
	}
	time.Sleep(10 * time.Millisecond)

	if _, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("validate api key after team deletion error = %v, want %v", err, ErrInvalidKey)
	}
}

func TestAPIKeyAuthenticationInvalidationAndUsageIntegration(t *testing.T) {
	pool := newGatewayAPIKeyTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	identityRepo := identity.NewRepository(pool)
	apiKeyRepo := NewRepository(
		pool,
		WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()),
		WithAuthenticationCacheConfig(AuthenticationCacheConfig{
			PositiveTTL: time.Hour,
			NegativeTTL: time.Hour,
			MaxEntries:  100,
		}),
		WithUsageRecorderConfig(UsageRecorderConfig{
			FlushInterval: time.Hour,
			FlushTimeout:  2 * time.Second,
			CloseTimeout:  4 * time.Second,
			QueueSize:     10_001,
			MaxPending:    100,
		}),
	)
	t.Cleanup(func() { _ = apiKeyRepo.Close() })

	user := &identity.User{
		Email: "api-key-hot-path@example.com",
		Name:  "API Key Hot Path",
	}
	if err := identityRepo.CreateUser(ctx, user); err != nil {
		t.Fatalf("create user: %v", err)
	}
	ownerID := user.ID
	team := &identity.Team{
		Name:    "API Key Hot Path",
		Slug:    "api-key-hot-path",
		OwnerID: &ownerID,
	}
	if _, err := identityRepo.CreateTeamWithOwner(ctx, team); err != nil {
		t.Fatalf("create team: %v", err)
	}
	key, keyValue, err := apiKeyRepo.CreateAPIKey(
		ctx,
		team.ID,
		"aws-us-east-1",
		user.ID,
		"hot-path",
		ScopeTeam,
		[]string{"viewer"},
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create API key: %v", err)
	}

	for index := 0; index < 10_000; index++ {
		if _, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue); err != nil {
			t.Fatalf("ValidateAPIKey(%d) error = %v", index, err)
		}
	}
	if err := apiKeyRepo.DeactivateAPIKey(ctx, key.ID); err != nil {
		t.Fatalf("deactivate API key: %v", err)
	}
	if _, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue); !errors.Is(err, ErrInactiveKey) {
		t.Fatalf("validate deactivated key error = %v, want ErrInactiveKey", err)
	}
	if err := apiKeyRepo.Close(); err != nil {
		t.Fatalf("close API key repository: %v", err)
	}

	var usageCount int64
	var lastUsed *time.Time
	if err := pool.QueryRow(ctx, `
		SELECT usage_count, last_used_at
		FROM api_keys
		WHERE id = $1
	`, key.ID).Scan(&usageCount, &lastUsed); err != nil {
		t.Fatalf("query recorded API key usage: %v", err)
	}
	if usageCount != 10_000 {
		t.Fatalf("usage_count = %d, want 10000", usageCount)
	}
	if lastUsed == nil {
		t.Fatal("last_used_at is nil")
	}

	if err := apiKeyRepo.DeleteAPIKey(ctx, key.ID); err != nil {
		t.Fatalf("delete API key: %v", err)
	}
	if _, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("validate deleted key error = %v, want ErrInvalidKey", err)
	}
	if err := apiKeyRepo.Close(); err != nil {
		t.Fatalf("second close API key repository: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("repository Close closed external pool: %v", err)
	}
}

func TestAPIKeyControlPlaneQuotaReleasesOnDelete(t *testing.T) {
	pool := newGatewayAPIKeyTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	identityRepo := identity.NewRepository(pool)
	apiKeyRepo := NewRepository(pool)
	quotaRepo := coreteamquota.NewRepository(pool)

	user := &identity.User{
		Email: "api-key-quota-owner@example.com",
		Name:  "API Key Quota Owner",
	}
	if err := identityRepo.CreateUser(ctx, user); err != nil {
		t.Fatalf("create user: %v", err)
	}
	ownerID := user.ID
	team := &identity.Team{
		Name:    "API Key Quota Team",
		Slug:    "api-key-quota-team",
		OwnerID: &ownerID,
	}
	if _, err := identityRepo.CreateTeamWithOwner(ctx, team); err != nil {
		t.Fatalf("create team: %v", err)
	}
	if err := quotaRepo.UnsafePutTeamPolicyForTest(ctx, team.ID, coreteamquota.Policy{
		Key:   coreteamquota.KeyControlPlaneObjectCount,
		Kind:  coreteamquota.KindCapacity,
		Limit: 1,
	}); err != nil {
		t.Fatalf("set team quota: %v", err)
	}

	first, _, err := apiKeyRepo.CreateAPIKey(
		ctx,
		team.ID,
		"aws-us-east-1",
		user.ID,
		"first key",
		ScopeTeam,
		[]string{"viewer"},
		time.Now().Add(time.Hour),
	)
	if err != nil {
		t.Fatalf("create first api key: %v", err)
	}
	if _, _, err := apiKeyRepo.CreateAPIKey(
		ctx,
		team.ID,
		"aws-us-east-1",
		user.ID,
		"blocked key",
		ScopeTeam,
		[]string{"viewer"},
		time.Now().Add(time.Hour),
	); !coreteamquota.IsExceeded(err) {
		t.Fatalf("create second api key error = %v, want quota exceeded", err)
	}
	if err := apiKeyRepo.DeleteAPIKey(ctx, first.ID); err != nil {
		t.Fatalf("delete first api key: %v", err)
	}
	if _, _, err := apiKeyRepo.CreateAPIKey(
		ctx,
		team.ID,
		"aws-us-east-1",
		user.ID,
		"replacement key",
		ScopeTeam,
		[]string{"viewer"},
		time.Now().Add(time.Hour),
	); err != nil {
		t.Fatalf("create replacement api key: %v", err)
	}
}

func TestCreateAPIKeyRejectsOversizedNameBeforePostgresWrite(t *testing.T) {
	pool := newGatewayAPIKeyTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	repo := NewRepository(pool, WithTeamQuotaStore(teamquotatestutil.NewPermissiveCapacityStore()))
	var before int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM api_keys`).Scan(&before); err != nil {
		t.Fatalf("count API keys before rejected create: %v", err)
	}

	_, _, err := repo.CreateAPIKey(
		ctx,
		uuid.NewString(),
		"aws-us-east-1",
		uuid.NewString(),
		strings.Repeat("n", int(MaxNameBytes)+1),
		ScopeTeam,
		[]string{"viewer"},
		time.Now().Add(time.Hour),
	)
	if !resourceguard.IsTooLarge(err) {
		t.Fatalf("CreateAPIKey() error = %v, want TooLargeError", err)
	}

	var after int
	if err := pool.QueryRow(ctx, `SELECT COUNT(*) FROM api_keys`).Scan(&after); err != nil {
		t.Fatalf("count API keys after rejected create: %v", err)
	}
	if after != before {
		t.Fatalf("API key count = %d, want unchanged %d", after, before)
	}
}

func newGatewayAPIKeyTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	ctx := context.Background()
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
		return nil
	}

	schema := fmt.Sprintf("gateway_apikey_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))
	adminPool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL:     dbURL,
		Schema:          schema,
		DefaultMaxConns: 10,
		DefaultMinConns: 1,
	})
	if err != nil {
		t.Fatalf("connect schema-scoped pool: %v", err)
	}
	t.Cleanup(pool.Close)
	t.Cleanup(func() {
		_, _ = adminPool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
	})

	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(gatewaymigrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate gateway schema: %v", err)
	}
	if err := coreteamquota.RunMigrations(ctx, pool, nil); err != nil {
		t.Fatalf("migrate team quota schema: %v", err)
	}
	if err := coreteamquota.NewRepository(pool).UnsafeReplaceDefaultPoliciesForTest(
		ctx,
		teamquotatestutil.CompleteDefaultPolicies(),
	); err != nil {
		t.Fatalf("configure team quota defaults: %v", err)
	}

	return pool
}
