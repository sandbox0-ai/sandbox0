package teamquota

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	defaultCapacityHistoryRetention = 24 * time.Hour
	maxTerminalOperationsPerOwner   = 32
	maxTerminalTransfersPerTeam     = 4096
	maxReleasedAllocationsPerTeam   = 4096
	capacityHistoryPruneBatchSize   = 256
	capacityHistoryPruneRevisionGap = 64
)

// pruneTeamCapacityHistory bounds terminal bookkeeping for one team while the
// caller holds that team's quota row lock. Prepared operations and allocations
// that can participate in recovery are never eligible for deletion.
func pruneTeamCapacityHistory(ctx context.Context, tx pgx.Tx, teamID string) error {
	if tx == nil {
		return fmt.Errorf("team quota transaction is required")
	}
	retention := postgresInterval(defaultCapacityHistoryRetention)

	// Transfer rows must be removed first because they reference allocations.
	// A transfer that is the compact last-operation record of a live allocation
	// remains retryable without keeping every older terminal transfer.
	if _, err := tx.Exec(ctx, `
		WITH candidates AS (
			SELECT o.team_id, o.operation_id, o.completed_at,
				EXISTS (
					SELECT 1
					FROM quota.allocations a
					WHERE a.allocation_id IN (
						o.source_allocation_id,
						o.destination_allocation_id
					)
						AND a.state <> 'released'
						AND a.last_operation_id = o.operation_id
						AND a.last_operation_generation =
							o.operation_generation
				) AS protected
			FROM quota.transfer_operations o
			WHERE o.team_id = $1
				AND o.state IN ('committed', 'aborted')
		),
		ranked AS (
			SELECT team_id, operation_id, completed_at, protected,
				ROW_NUMBER() OVER (
					ORDER BY protected DESC, completed_at DESC, operation_id
				) AS retention_rank
			FROM candidates
		),
		victims AS (
			SELECT team_id, operation_id
			FROM ranked
			WHERE NOT protected
				AND (
					completed_at < NOW() - $2::interval
					OR retention_rank > $3
				)
			ORDER BY completed_at, operation_id
			LIMIT $4
		)
		DELETE FROM quota.transfer_operations o
		USING victims v
		WHERE o.team_id = v.team_id
			AND o.operation_id = v.operation_id
	`, teamID, retention, maxTerminalTransfersPerTeam,
		capacityHistoryPruneBatchSize); err != nil {
		return &UnavailableError{
			Operation: "prune terminal team quota transfers",
			Err:       err,
		}
	}

	// A released allocation is safe to remove only after its operation has
	// completed, every ledger item is exactly zero, and no transfer references
	// it. Allocation items and operation history then cascade atomically.
	if _, err := tx.Exec(ctx, `
		WITH candidates AS (
			SELECT a.allocation_id, a.updated_at,
				ROW_NUMBER() OVER (
					ORDER BY a.updated_at DESC, a.allocation_id
				) AS retention_rank
			FROM quota.allocations a
			WHERE a.team_id = $1
				AND a.state = 'released'
				AND a.operation_id IS NULL
				AND a.reconcile_after IS NULL
				AND NOT EXISTS (
					SELECT 1
					FROM quota.allocation_items i
					WHERE i.allocation_id = a.allocation_id
						AND (
							i.committed_value <> 0
							OR i.pending_value IS NOT NULL
						)
				)
				AND NOT EXISTS (
					SELECT 1
					FROM quota.transfer_operations o
					WHERE o.source_allocation_id = a.allocation_id
						OR o.destination_allocation_id = a.allocation_id
				)
		),
		victims AS (
			SELECT allocation_id
			FROM candidates
			WHERE updated_at < NOW() - $2::interval
				OR retention_rank > $3
			ORDER BY updated_at, allocation_id
			LIMIT $4
		)
		DELETE FROM quota.allocations a
		USING victims v
		WHERE a.allocation_id = v.allocation_id
	`, teamID, retention, maxReleasedAllocationsPerTeam,
		capacityHistoryPruneBatchSize); err != nil {
		return &UnavailableError{
			Operation: "prune released team quota allocations",
			Err:       err,
		}
	}

	// Keep the latest terminal record as the compact idempotency record for a
	// live owner. Older terminal operation IDs have a bounded retry window.
	if _, err := tx.Exec(ctx, `
		WITH candidates AS (
			SELECT h.allocation_id, h.operation_id, h.completed_at,
				(
					a.state <> 'released'
					AND a.last_operation_id = h.operation_id
					AND a.last_operation_generation =
						h.operation_generation
				) AS protected
			FROM quota.allocation_operations h
			JOIN quota.allocations a
				ON a.allocation_id = h.allocation_id
			WHERE a.team_id = $1
				AND h.state IN ('committed', 'aborted')
		),
		ranked AS (
			SELECT allocation_id, operation_id, completed_at, protected,
				ROW_NUMBER() OVER (
					PARTITION BY allocation_id
					ORDER BY protected DESC, completed_at DESC, operation_id
				) AS retention_rank
			FROM candidates
		),
		victims AS (
			SELECT allocation_id, operation_id
			FROM ranked
			WHERE NOT protected
				AND (
					completed_at < NOW() - $2::interval
					OR retention_rank > $3
				)
			ORDER BY completed_at, allocation_id, operation_id
			LIMIT $4
		)
		DELETE FROM quota.allocation_operations h
		USING victims v
		WHERE h.allocation_id = v.allocation_id
			AND h.operation_id = v.operation_id
	`, teamID, retention, maxTerminalOperationsPerOwner,
		capacityHistoryPruneBatchSize); err != nil {
		return &UnavailableError{
			Operation: "prune terminal team quota allocation operations",
			Err:       err,
		}
	}
	return nil
}
