package sandboxstore

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

// MaxSnapshotsPerSandbox reports occupancy of the fullest sandbox, not a
// team-wide sum: the retention policy applies independently to each sandbox.
func (s *PGSandboxStore) MaxSnapshotsPerSandbox(ctx context.Context, teamID string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, fmt.Errorf("sandbox store is not configured")
	}
	var current int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(retained), 0) FROM (
			SELECT COUNT(*) AS retained FROM manager.rootfs_snapshots
			WHERE team_id=$1 AND source_sandbox_id <> ''
				AND snapshot_id NOT LIKE 'template-build-%'
				AND (expires_at IS NULL OR expires_at > NOW())
			GROUP BY source_sandbox_id
		) usage
	`, strings.TrimSpace(teamID)).Scan(&current)
	return current, err
}

// Caller holds the source sandbox row lock, shared by paused snapshot creation,
// running capture publication and background reconciliation. Only snapshot pins
// are removed; derived filesystems and other owners retain their generations.
func pruneRootFSSnapshots(ctx context.Context, db rootFSStoreDB, teamID, sandboxID, keepSnapshotID string, batchSize int) (int, error) {
	policy, err := quota.NewRepository(db).GetLimit(ctx, teamID, quota.DimensionSnapshotsPerSandbox)
	if err != nil {
		return 0, fmt.Errorf("load snapshot retention quota: %w", err)
	}
	retained := quota.DefaultSnapshotsPerSandbox
	if policy != nil {
		retained = policy.LimitValue
	}
	var deleted int
	err = db.QueryRow(ctx, `
		WITH excess AS (
			SELECT snapshot_id, created_at FROM manager.rootfs_snapshots
			WHERE team_id=$1 AND source_sandbox_id=$2 AND source_sandbox_id <> ''
				AND snapshot_id NOT LIKE 'template-build-%'
				AND (expires_at IS NULL OR expires_at > NOW())
			ORDER BY (snapshot_id=$3) DESC, created_at DESC, snapshot_id DESC
			OFFSET $4
		), oldest AS (
			SELECT snapshot_id FROM excess
			ORDER BY created_at, snapshot_id
			LIMIT NULLIF($5, 0)
		), deleted AS (
			DELETE FROM manager.rootfs_snapshots snapshot USING oldest
			WHERE snapshot.snapshot_id=oldest.snapshot_id
			RETURNING snapshot.snapshot_id
		), released AS (
			UPDATE manager.rootfs_running_template_captures capture
			SET cancel_reason='snapshot retention quota', updated_at=NOW()
			FROM deleted WHERE capture.snapshot_id=deleted.snapshot_id
				AND capture.state='published' AND capture.cancel_reason=''
			RETURNING capture.operation_id
		)
		SELECT COUNT(*) FROM deleted
	`, teamID, sandboxID, keepSnapshotID, retained, batchSize).Scan(&deleted)
	if err != nil {
		return 0, fmt.Errorf("prune sandbox snapshots: %w", err)
	}
	return deleted, nil
}

// PruneExcessRootFSSnapshots also applies policy reductions and bounds existing
// snapshots without requiring the sandbox to create another snapshot.
func (s *PGSandboxStore) PruneExcessRootFSSnapshots(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	limit = normalizeRootFSObjectLimit(limit)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT sandbox.sandbox_id, sandbox.team_id
		FROM manager.sandboxes sandbox
		JOIN (
			SELECT source_sandbox_id, team_id, COUNT(*) retained
			FROM manager.rootfs_snapshots
			WHERE source_sandbox_id <> '' AND snapshot_id NOT LIKE 'template-build-%'
				AND (expires_at IS NULL OR expires_at > NOW())
				AND ($1='' OR team_id=$1)
			GROUP BY source_sandbox_id, team_id
		) usage ON usage.source_sandbox_id=sandbox.sandbox_id AND usage.team_id=sandbox.team_id
		LEFT JOIN quota.team_quota_limits override
			ON override.team_id=sandbox.team_id AND override.dimension='snapshots_per_sandbox'
		LEFT JOIN quota.region_quota_limits regional ON regional.dimension='snapshots_per_sandbox'
		WHERE usage.retained > COALESCE(override.limit_value, regional.limit_value, $2)
		ORDER BY sandbox.sandbox_id
		LIMIT $3 FOR UPDATE OF sandbox SKIP LOCKED
	`, strings.TrimSpace(teamID), quota.DefaultSnapshotsPerSandbox, limit)
	if err != nil {
		return 0, fmt.Errorf("list excess sandbox snapshots: %w", err)
	}
	type source struct{ sandboxID, teamID string }
	var sources []source
	for rows.Next() {
		var item source
		if err := rows.Scan(&item.sandboxID, &item.teamID); err != nil {
			rows.Close()
			return 0, err
		}
		sources = append(sources, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, item := range sources {
		n, err := pruneRootFSSnapshots(ctx, tx, item.teamID, item.sandboxID, "", limit-deleted)
		if err != nil {
			return 0, err
		}
		deleted += n
		if deleted >= limit {
			break
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return deleted, nil
}
