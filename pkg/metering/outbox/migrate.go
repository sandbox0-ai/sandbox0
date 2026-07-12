package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/metering/outbox/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

const SchemaName = "metering"

type migrationLogger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// RunMigrations creates the compact producer state and durable projection
// outbox used to deliver metering records to ClickHouse.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, logger migrationLogger) error {
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithLogger(logger),
		migrate.WithSchema(SchemaName),
	); err != nil {
		return fmt.Errorf("run metering outbox migrations: %w", err)
	}
	return nil
}
