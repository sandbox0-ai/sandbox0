package migrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

// Run applies the shared identity and quota migrations used by regional and cluster gateways.
func Run(ctx context.Context, pool *pgxpool.Pool, logger migrate.Logger) error {
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(FS),
		migrate.WithLogger(logger),
		migrate.WithSchema("shared_gateway"),
	); err != nil {
		return fmt.Errorf("migrate up: %w", err)
	}
	if err := quota.RunMigrations(ctx, pool, logger); err != nil {
		return fmt.Errorf("quota migrations: %w", err)
	}

	return nil
}
