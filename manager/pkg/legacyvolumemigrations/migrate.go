package legacyvolumemigrations

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

const schemaName = "storage_proxy"

type logger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// Run applies the historical volume migrations through the terminal cleanup
// migration. Keeping the original sequence lets upgraded and fresh databases
// converge without reusing migration numbers that previously rolled back.
func Run(ctx context.Context, pool *pgxpool.Pool, migrationLogger logger) error {
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(FS),
		migrate.WithLogger(migrationLogger),
		migrate.WithSchema(schemaName),
	); err != nil {
		return fmt.Errorf("run legacy volume migrations: %w", err)
	}
	return nil
}
