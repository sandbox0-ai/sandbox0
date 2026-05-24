package quota

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/quota/migrations"
)

const SchemaName = "quota"

type Logger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func RunMigrations(ctx context.Context, pool *pgxpool.Pool, logger Logger) error {
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithLogger(logger),
		migrate.WithSchema(SchemaName),
	); err != nil {
		return fmt.Errorf("run quota migrations: %w", err)
	}
	return nil
}
