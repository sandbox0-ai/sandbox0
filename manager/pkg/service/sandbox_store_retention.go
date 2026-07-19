package service

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

func pruneSandboxLifecycleHistory(
	ctx context.Context,
	tx pgx.Tx,
	sandboxID string,
) error {
	if tx == nil {
		return fmt.Errorf("sandbox store transaction is required")
	}
	if _, err := tx.Exec(ctx, `
		WITH ranked AS (
			SELECT txn_id,
				COALESCE(committed_at, aborted_at, updated_at) AS completed_at,
				ROW_NUMBER() OVER (
					ORDER BY
						COALESCE(committed_at, aborted_at, updated_at) DESC,
						txn_id
				) AS retention_rank
			FROM manager.sandbox_lifecycle_txns
			WHERE sandbox_id = $1
				AND phase IN ('committed', 'aborted')
		),
		victims AS (
			SELECT txn_id
			FROM ranked
			WHERE completed_at < NOW() - $2::interval
				OR retention_rank > $3
			ORDER BY completed_at, txn_id
			LIMIT $4
		)
		DELETE FROM manager.sandbox_lifecycle_txns t
		USING victims v
		WHERE t.txn_id = v.txn_id
	`, sandboxID, postgresDuration(sandboxHistoryRetention),
		maxTerminalLifecycleTxns, lifecycleHistoryPruneBatchSize); err != nil {
		return fmt.Errorf("prune sandbox lifecycle history: %w", err)
	}
	return nil
}

func pruneDeletedSandboxHistory(
	ctx context.Context,
	tx pgx.Tx,
	teamID string,
) error {
	if tx == nil {
		return fmt.Errorf("sandbox store transaction is required")
	}
	var quotaAllocationsAvailable bool
	if err := tx.QueryRow(ctx, `
		SELECT to_regclass('quota.allocations') IS NOT NULL
	`).Scan(&quotaAllocationsAvailable); err != nil {
		return fmt.Errorf("inspect team quota allocation catalog: %w", err)
	}
	// The quota allocation is recovery evidence owned by another subsystem.
	// Fail closed when that catalog is unavailable instead of deleting the
	// manager identity while its capacity release may still be recoverable.
	if !quotaAllocationsAvailable {
		return nil
	}

	if _, err := tx.Exec(ctx, `
		WITH eligible AS (
			SELECT s.sandbox_id,
				GREATEST(s.deleted_at, s.cleanup_completed_at) AS completed_at
			FROM manager.sandboxes s
			WHERE s.team_id = $1
				AND s.status = $2
				AND s.deleted_at IS NOT NULL
				AND s.cleanup_completed_at IS NOT NULL
				AND s.current_pod_namespace = ''
				AND s.current_pod_name = ''
				AND NOT EXISTS (
					SELECT 1
					FROM manager.sandbox_lifecycle_txns t
					WHERE t.sandbox_id = s.sandbox_id
						AND t.phase IN (
							'preparing',
							'barriered',
							'publishing',
							'committing'
						)
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.sandbox_rootfs_bindings b
					WHERE b.sandbox_id = s.sandbox_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.sandbox_rootfs_states r
					WHERE r.sandbox_id = s.sandbox_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.sandbox_rootfs_heads h
					WHERE h.sandbox_id = s.sandbox_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_filesystems f
					WHERE f.filesystem_id = s.sandbox_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_layers l
					WHERE l.source_sandbox_id = s.sandbox_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM quota.allocations a
					WHERE a.team_id = s.team_id
						AND a.owner_kind = 'sandbox'
						AND a.owner_id = s.sandbox_id
				)
		),
		ranked AS (
			SELECT sandbox_id, completed_at,
				ROW_NUMBER() OVER (
					ORDER BY completed_at DESC, sandbox_id
				) AS retention_rank
			FROM eligible
		),
		victims AS (
			SELECT sandbox_id
			FROM ranked
			WHERE completed_at < NOW() - $3::interval
				OR retention_rank > $4
			ORDER BY completed_at, sandbox_id
			LIMIT $5
		)
		DELETE FROM manager.sandboxes s
		USING victims v
		WHERE s.sandbox_id = v.sandbox_id
	`, teamID, SandboxStatusDeleted, postgresDuration(sandboxHistoryRetention),
		maxDeletedSandboxesPerTeam, deletedSandboxPruneBatchSize); err != nil {
		return fmt.Errorf("prune deleted sandbox history: %w", err)
	}
	return nil
}

func postgresDuration(duration time.Duration) string {
	return fmt.Sprintf("%d milliseconds", duration.Milliseconds())
}
