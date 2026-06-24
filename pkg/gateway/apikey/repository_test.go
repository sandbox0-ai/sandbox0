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
)

func TestValidateAPIKeyRequiresExistingTeam(t *testing.T) {
	pool := newGatewayAPIKeyTestPool(t)
	if pool == nil {
		return
	}

	ctx := context.Background()
	identityRepo := identity.NewRepository(pool)
	apiKeyRepo := NewRepository(pool)

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
	if err := identityRepo.CreateTeam(ctx, team); err != nil {
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

	if err := identityRepo.DeleteTeam(ctx, team.ID); err != nil {
		t.Fatalf("delete team: %v", err)
	}

	if _, err := apiKeyRepo.ValidateAPIKey(ctx, keyValue); !errors.Is(err, ErrInvalidKey) {
		t.Fatalf("validate api key after team deletion error = %v, want %v", err, ErrInvalidKey)
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

	if err := migrate.Up(ctx, pool, ".", migrate.WithBaseFS(gatewaymigrations.FS), migrate.WithSchema(schema)); err != nil {
		t.Fatalf("migrate gateway schema: %v", err)
	}

	return pool
}
