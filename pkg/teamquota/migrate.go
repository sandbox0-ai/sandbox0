package teamquota

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/pglock"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/migrations"
)

const SchemaName = "quota"

const (
	migrationLockResource  = "sandbox0:team-quota:migrations"
	migrationUnlockTimeout = 5 * time.Second
)

// Logger is the migration logger contract used by service runtimes.
type Logger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

// RunMigrations upgrades the region quota schema.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, logger Logger) error {
	if pool == nil {
		return fmt.Errorf("team quota database pool is required")
	}

	// Every regional consumer starts independently and may observe the same
	// pending schema version during a rolling deployment. Use a standalone
	// connection so the migration lock does not consume pool capacity needed
	// by goose, including when the configured pool has only one connection.
	lockConn, err := pgx.ConnectConfig(ctx, pool.Config().ConnConfig.Copy())
	if err != nil {
		return fmt.Errorf("connect team quota migration lock: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), migrationUnlockTimeout)
		defer cancel()
		_ = lockConn.Close(closeCtx)
	}()

	lockKey := pglock.Key(migrationLockResource)
	if _, err := lockConn.Exec(ctx, "SELECT pg_advisory_lock($1)", lockKey); err != nil {
		return fmt.Errorf("acquire team quota migration lock: %w", err)
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), migrationUnlockTimeout)
		defer cancel()
		_, _ = lockConn.Exec(unlockCtx, "SELECT pg_advisory_unlock($1)", lockKey)
	}()

	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(migrations.FS),
		migrate.WithLogger(logger),
		migrate.WithSchema(SchemaName),
	); err != nil {
		return fmt.Errorf("run team quota migrations: %w", err)
	}
	return nil
}
