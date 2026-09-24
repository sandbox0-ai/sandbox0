package sandboxstore

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// releaseDeletedSandboxCheckpointStorage preserves checkpoint audit evidence
// while releasing its live writer FK only after physical source cleanup and
// the last retained image owner have been proven absent.
func releaseDeletedSandboxCheckpointStorage(ctx context.Context, tx pgx.Tx, sandbox string) error {
	var blocked bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM manager.sandbox_runtime_checkpoints c
		JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
		WHERE l.sandbox_id=$1 AND c.storage_released_at IS NULL
			AND NOT manager.runtime_checkpoint_storage_releasable(c.operation_id)
	)`, sandbox).Scan(&blocked); err != nil {
		return fmt.Errorf("check checkpoint storage retention: %w", err)
	}
	if blocked {
		return fmt.Errorf("%w: checkpoint source cleanup or retained image ownership is pending", ErrSandboxClaimCleanupPending)
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints c
		SET source_writer_grant_ref=NULL,storage_released_at=clock_timestamp()
		FROM manager.sandbox_lifecycle_txns l
		WHERE l.txn_id=c.operation_id AND l.sandbox_id=$1 AND c.storage_released_at IS NULL`, sandbox); err != nil {
		return fmt.Errorf("release terminal checkpoint storage retention: %w", err)
	}
	return nil
}
