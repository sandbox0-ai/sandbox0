package sandboxstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// releaseDeletedSandboxMigrationStorage runs under the sandbox row lock before
// deletion mutates lifecycle or filesystem state. Retain every immutable command
// and receipt; release only the writer FK after both carriers and staging have
// completed cleanup. A terminal lifecycle phase alone is not physical evidence.
func releaseDeletedSandboxMigrationStorage(ctx context.Context, tx pgx.Tx, sandbox string) error {
	var blocked bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
        SELECT 1 FROM manager.sandbox_runtime_migrations m
        JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        WHERE l.sandbox_id=$1 AND m.storage_released_at IS NULL
            AND NOT manager.runtime_migration_storage_releasable(m.operation_id)
    )`, sandbox).Scan(&blocked); err != nil {
		return fmt.Errorf("check migration storage retention: %w", err)
	}
	if blocked {
		return fmt.Errorf("%w: migration physical or staging cleanup is pending", ErrSandboxClaimCleanupPending)
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations m
        SET source_writer_grant_ref=NULL,storage_released_at=clock_timestamp()
        FROM manager.sandbox_lifecycle_txns l
        WHERE l.txn_id=m.operation_id AND l.sandbox_id=$1 AND m.storage_released_at IS NULL`, sandbox); err != nil {
		return fmt.Errorf("release terminal migration storage retention: %w", err)
	}
	return nil
}
