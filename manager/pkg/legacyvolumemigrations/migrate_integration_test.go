package legacyvolumemigrations

import (
	"context"
	"io/fs"
	"os"
	"testing"
	"testing/fstest"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

type testLogger struct{}

func (testLogger) Printf(string, ...any) {}
func (testLogger) Fatalf(string, ...any) {}

func TestTerminalMigrationDropsLegacyVolumeSchema(t *testing.T) {
	databaseURL := os.Getenv("INTEGRATION_DATABASE_URL")
	if databaseURL == "" {
		databaseURL = os.Getenv("TEST_DATABASE_URL")
	}
	if databaseURL == "" {
		t.Skip("missing INTEGRATION_DATABASE_URL or TEST_DATABASE_URL")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Fatalf("connect test database: %v", err)
	}
	t.Cleanup(pool.Close)
	_, _ = pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP SCHEMA IF EXISTS "+schemaName+" CASCADE")
	})

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrationsThrough(t, "00016")),
		migrate.WithLogger(testLogger{}),
		migrate.WithSchema(schemaName),
	); err != nil {
		t.Fatalf("run historical volume migrations: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO storage_proxy.sandbox_volumes (
    id, team_id, user_id, access_mode
) VALUES ('volume-test', 'team-test', 'user-test', 'RWO')
`); err != nil {
		t.Fatalf("seed legacy volume: %v", err)
	}

	if err := Run(ctx, pool, testLogger{}); err != nil {
		t.Fatalf("run terminal volume migration: %v", err)
	}
	if err := Run(ctx, pool, testLogger{}); err != nil {
		t.Fatalf("rerun terminal volume migration: %v", err)
	}

	var volumeTableExists bool
	if err := pool.QueryRow(ctx, `
SELECT to_regclass('storage_proxy.sandbox_volumes') IS NOT NULL
`).Scan(&volumeTableExists); err != nil {
		t.Fatalf("inspect legacy volume schema: %v", err)
	}
	if volumeTableExists {
		t.Fatal("legacy volume table still exists after terminal migration")
	}
}

func migrationsThrough(t *testing.T, maximumPrefix string) fs.FS {
	t.Helper()
	files, err := fs.Glob(FS, "*.sql")
	if err != nil {
		t.Fatalf("list legacy volume migrations: %v", err)
	}
	selected := fstest.MapFS{}
	for _, name := range files {
		if len(name) < len(maximumPrefix) || name[:len(maximumPrefix)] > maximumPrefix {
			continue
		}
		data, err := fs.ReadFile(FS, name)
		if err != nil {
			t.Fatalf("read legacy volume migration %s: %v", name, err)
		}
		selected[name] = &fstest.MapFile{Data: data, Mode: 0o444}
	}
	return selected
}
