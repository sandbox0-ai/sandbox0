package migrate_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/dbpool"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

type noopLogger struct{}

func (noopLogger) Printf(string, ...any) {}
func (noopLogger) Fatalf(string, ...any) {}

func TestUpWithSchemaRestoresPoolSearchPath(t *testing.T) {
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	appSchema := fmt.Sprintf("migrate_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      appSchema,
	})
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	defer pool.Close()

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", appSchema))
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", metering.SchemaName))
	})

	migrationsDir := t.TempDir()
	migration := `-- +goose Up
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email TEXT NOT NULL
);

-- +goose Down
DROP TABLE IF EXISTS users;
`
	if err := os.WriteFile(filepath.Join(migrationsDir, "00001_create_users.sql"), []byte(migration), 0o644); err != nil {
		t.Fatalf("write migration: %v", err)
	}

	if err := migrate.Up(ctx, pool, migrationsDir, migrate.WithSchema(appSchema)); err != nil {
		t.Fatalf("run app migrations: %v", err)
	}
	if err := metering.RunMigrations(ctx, pool, noopLogger{}); err != nil {
		t.Fatalf("run metering migrations: %v", err)
	}

	var count int
	if err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM users").Scan(&count); err != nil {
		t.Fatalf("query users with restored search_path: %v", err)
	}
}

func TestUpWithDistinctTableNamesAvoidsVersionCollisions(t *testing.T) {
	dbURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if dbURL == "" {
		dbURL = os.Getenv("TEST_DATABASE_URL")
	}
	if dbURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	appSchema := fmt.Sprintf("migrate_collision_test_%s", strings.ReplaceAll(uuid.NewString(), "-", ""))

	pool, err := dbpool.New(ctx, dbpool.Options{
		DatabaseURL: dbURL,
		Schema:      appSchema,
	})
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	defer pool.Close()

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, fmt.Sprintf("DROP SCHEMA IF EXISTS %s CASCADE", appSchema))
	})

	writeMigration := func(dir, filename, table string) {
		t.Helper()
		migration := fmt.Sprintf(`-- +goose Up
CREATE TABLE IF NOT EXISTS %s (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid()
);

-- +goose Down
DROP TABLE IF EXISTS %s;
`, table, table)
		if err := os.WriteFile(filepath.Join(dir, filename), []byte(migration), 0o644); err != nil {
			t.Fatalf("write migration %s: %v", filename, err)
		}
	}

	firstDir := t.TempDir()
	secondDir := t.TempDir()
	writeMigration(firstDir, "00001_create_alpha.sql", "alpha_records")
	writeMigration(secondDir, "00001_create_beta.sql", "beta_records")

	if err := migrate.Up(ctx, pool, firstDir,
		migrate.WithSchema(appSchema),
		migrate.WithTableName("goose_alpha"),
	); err != nil {
		t.Fatalf("run first migration set: %v", err)
	}
	if err := migrate.Up(ctx, pool, secondDir,
		migrate.WithSchema(appSchema),
		migrate.WithTableName("goose_beta"),
	); err != nil {
		t.Fatalf("run second migration set: %v", err)
	}

	for _, table := range []string{"alpha_records", "beta_records"} {
		var exists string
		if err := pool.QueryRow(ctx, "SELECT to_regclass($1)", table).Scan(&exists); err != nil {
			t.Fatalf("query table %s existence: %v", table, err)
		}
		if exists == "" {
			t.Fatalf("expected table %s to exist", table)
		}
	}
}
