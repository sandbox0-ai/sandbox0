package egressauth

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

const migrationTableName = "goose_db_version_egressauth"

type Logger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// RunMigrations ensures the auth-store schema exists.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, logger Logger) error {
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithLogger(logger),
		migrate.WithTableName(migrationTableName),
		migrate.WithSchema("sched"),
	); err != nil {
		return fmt.Errorf("run egress auth migrations: %w", err)
	}
	return nil
}
