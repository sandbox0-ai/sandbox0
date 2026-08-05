// Package rootfslease coordinates rootfs CAS writers with orphan collection.
package rootfslease

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const advisoryLockSalt int64 = 0x7330726f6f746673

// Repository persists team-prefix write leases in the manager schema. The
// prefix advisory lock is shared with orphan deletion so a writer cannot race
// an unknown-object delete.
type Repository struct {
	pool *pgxpool.Pool
}

type ObjectDeleter interface {
	Delete(string) error
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// ResolveRootFSTeam maps an opaque v3 CAS prefix to its retained metering
// owner. The mapping is deliberately independent from team lifecycle rows so
// late request windows remain attributable after team deletion.
func (r *Repository) ResolveRootFSTeam(ctx context.Context, prefix string) (string, error) {
	if r == nil || r.pool == nil {
		return "", fmt.Errorf("rootfs team prefix resolver is not configured")
	}
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	if prefix == "" {
		return "", fmt.Errorf("rootfs team prefix is required")
	}
	var teamID string
	if err := r.pool.QueryRow(ctx, `
		SELECT team_id FROM manager.rootfs_team_prefixes_v3 WHERE object_prefix = $1
	`, prefix).Scan(&teamID); err != nil {
		return "", fmt.Errorf("resolve rootfs team prefix: %w", err)
	}
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return "", fmt.Errorf("rootfs team prefix has no owner")
	}
	return teamID, nil
}

func (r *Repository) EnsureCapture(ctx context.Context, sandboxID, teamID string, generation int64) (string, error) {
	if r == nil || r.pool == nil {
		return "", fmt.Errorf("rootfs capture lease database is not configured")
	}
	sandboxID = strings.TrimSpace(sandboxID)
	teamID = strings.TrimSpace(teamID)
	if sandboxID == "" || teamID == "" || generation <= 0 {
		return "", fmt.Errorf("rootfs capture lease sandbox_id, team_id, and runtime_generation are required")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return "", err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", fmt.Errorf("begin rootfs capture lease: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return "", err
	}
	if err := ensureTeamPrefix(ctx, tx, teamID, prefix); err != nil {
		return "", err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_capture_leases_v3 (
			sandbox_id, runtime_generation, team_id, object_prefix, created_at, updated_at
		)
		SELECT s.sandbox_id, $3, s.team_id, $4, NOW(), NOW()
		FROM manager.sandboxes s
		WHERE s.sandbox_id = $1
			AND s.team_id = $2
			AND s.deleted_at IS NULL
			AND (
				(s.runtime_generation = $3 AND s.desired_state = 'active')
				OR (
					s.desired_state = 'paused'
					AND EXISTS (
						SELECT 1 FROM manager.sandbox_lifecycle_txns t
						WHERE t.sandbox_id = s.sandbox_id
							AND t.kind = 'resume'
							AND t.phase IN ('preparing', 'barriered', 'publishing', 'committing')
							AND t.from_generation = s.runtime_generation
							AND t.to_generation = $3
					)
				)
			)
		ON CONFLICT (sandbox_id, runtime_generation) DO UPDATE SET
			active = TRUE,
			protect_all = TRUE,
			object_epoch = CASE
				WHEN manager.rootfs_capture_leases_v3.active THEN manager.rootfs_capture_leases_v3.object_epoch
				ELSE manager.rootfs_capture_leases_v3.object_epoch + 1
			END,
			updated_at = NOW()
		WHERE manager.rootfs_capture_leases_v3.team_id = EXCLUDED.team_id
			AND manager.rootfs_capture_leases_v3.object_prefix = EXCLUDED.object_prefix
	`, sandboxID, teamID, generation, prefix)
	if err != nil {
		return "", fmt.Errorf("ensure rootfs capture lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", fmt.Errorf("rootfs capture generation is not the active sandbox runtime")
	}
	if err := tx.Commit(ctx); err != nil {
		return "", fmt.Errorf("commit rootfs capture lease: %w", err)
	}
	return prefix, nil
}

// BeginCapture enables coarse prefix protection before a generation writes or
// reuses CAS objects. CheckpointCapture narrows it back to exact object keys.
func (r *Repository) BeginCapture(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("rootfs capture lease database is not configured")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_capture_leases_v3 l
		SET protect_all = TRUE, updated_at = NOW()
		WHERE l.sandbox_id = $1 AND l.runtime_generation = $2
			AND l.team_id = $3 AND l.object_prefix = $4
			AND l.active
			AND EXISTS (
				SELECT 1 FROM manager.sandboxes s
				WHERE s.sandbox_id = l.sandbox_id
					AND s.team_id = l.team_id
					AND s.deleted_at IS NULL
					AND (
						(s.runtime_generation = l.runtime_generation AND s.desired_state = 'active')
						OR (
							s.desired_state = 'paused'
							AND EXISTS (
								SELECT 1 FROM manager.sandbox_lifecycle_txns t
								WHERE t.sandbox_id = s.sandbox_id
									AND t.kind = 'resume'
									AND t.phase IN ('preparing', 'barriered', 'publishing', 'committing')
									AND t.from_generation = s.runtime_generation
									AND t.to_generation = l.runtime_generation
							)
						)
					)
			)
	`, strings.TrimSpace(sandboxID), generation, strings.TrimSpace(teamID), prefix)
	if err != nil {
		return fmt.Errorf("begin rootfs capture protection: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs capture generation is not the active sandbox runtime")
	}
	return tx.Commit(ctx)
}

// CheckpointCapture records newly referenced objects and atomically narrows a
// generation from prefix-wide protection to its exact conservative set.
func (r *Repository) CheckpointCapture(ctx context.Context, sandboxID, teamID string, generation int64, objects []rootfshead.Object) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("rootfs capture lease database is not configured")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	keys := make([]string, 0, len(objects))
	seen := make(map[string]struct{}, len(objects))
	for _, object := range objects {
		if err := rootfshead.ValidateObjectScope(prefix, object); err != nil {
			return err
		}
		if _, ok := seen[object.Key]; ok {
			continue
		}
		seen[object.Key] = struct{}{}
		keys = append(keys, object.Key)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return err
	}
	const batchSize = 5000
	for start := 0; start < len(keys); start += batchSize {
		end := min(start+batchSize, len(keys))
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.rootfs_capture_lease_objects_v3 (
				sandbox_id, runtime_generation, object_epoch, object_key, created_at
			)
			SELECT l.sandbox_id, l.runtime_generation, l.object_epoch, keys.object_key, NOW()
			FROM manager.rootfs_capture_leases_v3 l
			CROSS JOIN unnest($3::text[]) AS keys(object_key)
			WHERE l.sandbox_id = $1 AND l.runtime_generation = $2
				AND l.team_id = $4 AND l.object_prefix = $5 AND l.active
			ON CONFLICT (sandbox_id, runtime_generation, object_epoch, object_key) DO NOTHING
		`, strings.TrimSpace(sandboxID), generation, keys[start:end], strings.TrimSpace(teamID), prefix); err != nil {
			return fmt.Errorf("checkpoint rootfs capture objects: %w", err)
		}
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_capture_leases_v3
		SET protect_all = FALSE, updated_at = NOW()
		WHERE sandbox_id = $1 AND runtime_generation = $2
			AND team_id = $3 AND object_prefix = $4
			AND active
	`, strings.TrimSpace(sandboxID), generation, strings.TrimSpace(teamID), prefix)
	if err != nil {
		return fmt.Errorf("checkpoint rootfs capture lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs capture generation lease was lost")
	}
	return tx.Commit(ctx)
}

// ResetCapture rotates generation-local protection after a published Head has
// durably taken ownership. Historical rows are reclaimed asynchronously so
// publication acknowledgement remains independent of object count.
func (r *Repository) ResetCapture(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("rootfs capture lease database is not configured")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_capture_leases_v3
		SET protect_all = FALSE, object_epoch = object_epoch + 1, updated_at = NOW()
		WHERE sandbox_id = $1 AND runtime_generation = $2
			AND team_id = $3 AND object_prefix = $4
			AND active
	`, strings.TrimSpace(sandboxID), generation, strings.TrimSpace(teamID), prefix)
	if err != nil {
		return fmt.Errorf("reset rootfs capture lease: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs capture generation lease was lost")
	}
	return tx.Commit(ctx)
}

func (r *Repository) ReleaseCapture(ctx context.Context, sandboxID, teamID string, generation int64) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("rootfs capture lease database is not configured")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_capture_leases_v3
		SET active = FALSE, protect_all = FALSE,
			object_epoch = CASE WHEN active THEN object_epoch + 1 ELSE object_epoch END,
			updated_at = NOW()
		WHERE sandbox_id = $1 AND runtime_generation = $2
			AND team_id = $3 AND object_prefix = $4
	`, strings.TrimSpace(sandboxID), generation, strings.TrimSpace(teamID), prefix)
	if err != nil {
		return fmt.Errorf("release rootfs capture lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("rootfs capture generation lease was lost")
	}
	return tx.Commit(ctx)
}

func (r *Repository) AcquireWrite(ctx context.Context, leaseID, teamID string, ttl time.Duration) (string, error) {
	if r == nil || r.pool == nil {
		return "", fmt.Errorf("rootfs write lease database is not configured")
	}
	leaseID = strings.TrimSpace(leaseID)
	teamID = strings.TrimSpace(teamID)
	if leaseID == "" || teamID == "" || ttl <= 0 {
		return "", fmt.Errorf("rootfs write lease id, team_id, and positive ttl are required")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return "", err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return "", err
	}
	if err := ensureTeamPrefix(ctx, tx, teamID, prefix); err != nil {
		return "", err
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_write_leases_v3 (
			lease_id, team_id, object_prefix, expires_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, NOW() + ($4::bigint * INTERVAL '1 millisecond'), NOW(), NOW())
		ON CONFLICT (lease_id) DO UPDATE SET
			expires_at = EXCLUDED.expires_at,
			updated_at = NOW()
		WHERE manager.rootfs_write_leases_v3.team_id = EXCLUDED.team_id
			AND manager.rootfs_write_leases_v3.object_prefix = EXCLUDED.object_prefix
	`, leaseID, teamID, prefix, ttl.Milliseconds())
	if err != nil {
		return "", fmt.Errorf("acquire rootfs write lease: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return "", fmt.Errorf("rootfs write lease %q conflicts with another team", leaseID)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", err
	}
	return prefix, nil
}

func (r *Repository) ReleaseWrite(ctx context.Context, leaseID, teamID string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("rootfs write lease database is not configured")
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_write_leases_v3
		WHERE lease_id = $1 AND team_id = $2 AND object_prefix = $3
	`, strings.TrimSpace(leaseID), strings.TrimSpace(teamID), prefix); err != nil {
		return fmt.Errorf("release rootfs write lease: %w", err)
	}
	return tx.Commit(ctx)
}

func (r *Repository) CleanupStale(ctx context.Context) (int64, error) {
	if r == nil || r.pool == nil {
		return 0, fmt.Errorf("rootfs lease database is not configured")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `
		WITH obsolete AS (
			SELECT o.ctid
			FROM manager.rootfs_capture_lease_objects_v3 o
			JOIN manager.rootfs_capture_leases_v3 l
				ON l.sandbox_id = o.sandbox_id
				AND l.runtime_generation = o.runtime_generation
			WHERE o.object_epoch <> l.object_epoch
				OR NOT l.active
				OR NOT EXISTS (
					SELECT 1 FROM manager.sandboxes s
					WHERE s.sandbox_id = l.sandbox_id
						AND s.team_id = l.team_id
						AND s.deleted_at IS NULL
						AND (
							(s.runtime_generation = l.runtime_generation AND s.desired_state = 'active')
							OR (
								s.desired_state = 'paused'
								AND EXISTS (
									SELECT 1 FROM manager.sandbox_lifecycle_txns t
									WHERE t.sandbox_id = s.sandbox_id
										AND t.kind = 'resume'
										AND t.phase IN ('preparing', 'barriered', 'publishing', 'committing')
										AND t.from_generation = s.runtime_generation
										AND t.to_generation = l.runtime_generation
								)
							)
						)
				)
			LIMIT 5000
		)
		DELETE FROM manager.rootfs_capture_lease_objects_v3 o
		USING obsolete
		WHERE o.ctid = obsolete.ctid
	`); err != nil {
		return 0, fmt.Errorf("clean obsolete rootfs capture objects: %w", err)
	}
	var removed int64
	if err := tx.QueryRow(ctx, `
		WITH stale_captures AS (
			DELETE FROM manager.rootfs_capture_leases_v3 l
			WHERE (NOT l.active OR NOT EXISTS (
				SELECT 1 FROM manager.sandboxes s
				WHERE s.sandbox_id = l.sandbox_id
					AND s.team_id = l.team_id
					AND s.deleted_at IS NULL
					AND (
						(s.runtime_generation = l.runtime_generation AND s.desired_state = 'active')
						OR (
							s.desired_state = 'paused'
							AND EXISTS (
								SELECT 1 FROM manager.sandbox_lifecycle_txns t
								WHERE t.sandbox_id = s.sandbox_id
									AND t.kind = 'resume'
									AND t.phase IN ('preparing', 'barriered', 'publishing', 'committing')
									AND t.from_generation = s.runtime_generation
									AND t.to_generation = l.runtime_generation
							)
						)
					)
			))
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_capture_lease_objects_v3 o
				WHERE o.sandbox_id = l.sandbox_id
					AND o.runtime_generation = l.runtime_generation
			)
			RETURNING 1
		), expired_writes AS (
			DELETE FROM manager.rootfs_write_leases_v3
			WHERE expires_at <= NOW()
			RETURNING 1
		)
		SELECT (SELECT COUNT(*) FROM stale_captures) + (SELECT COUNT(*) FROM expired_writes)
	`).Scan(&removed); err != nil {
		return 0, fmt.Errorf("clean stale rootfs write leases: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return removed, nil
}

// DeleteUnknown serializes one object-store delete against every writer for
// the same team prefix and rechecks PostgreSQL truth while holding that lock.
func (r *Repository) DeleteUnknown(ctx context.Context, key, prefix string, deleter ObjectDeleter) (bool, error) {
	if r == nil || r.pool == nil || deleter == nil {
		return false, fmt.Errorf("rootfs unknown-object deletion is not configured")
	}
	key = strings.TrimSpace(key)
	prefix = strings.Trim(strings.TrimSpace(prefix), "/")
	parsedPrefix, err := rootfshead.TeamPrefixFromObjectKey(key)
	if err != nil {
		return false, err
	}
	if parsedPrefix != prefix {
		return false, fmt.Errorf("rootfs object %s does not belong to prefix %s", key, prefix)
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := LockPrefix(ctx, tx, prefix); err != nil {
		return false, err
	}
	var protected bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_objects_v3 WHERE object_key = $1)
			OR EXISTS (
				SELECT 1 FROM manager.rootfs_capture_leases_v3
				WHERE object_prefix = $2 AND active AND protect_all
			)
			OR EXISTS (
				SELECT 1
				FROM manager.rootfs_capture_lease_objects_v3 o
				JOIN manager.rootfs_capture_leases_v3 l
					ON l.sandbox_id = o.sandbox_id
					AND l.runtime_generation = o.runtime_generation
					AND l.object_epoch = o.object_epoch
				WHERE o.object_key = $1 AND l.active
			)
			OR EXISTS (
				SELECT 1 FROM manager.rootfs_write_leases_v3
				WHERE object_prefix = $2 AND expires_at > NOW()
			)
			OR EXISTS (
				SELECT 1 FROM manager.rootfs_head_prefix_guards_v3
				WHERE object_prefix = $2
			)
	`, key, prefix).Scan(&protected); err != nil {
		return false, fmt.Errorf("classify unknown rootfs object: %w", err)
	}
	if protected {
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return false, nil
	}
	if err := deleter.Delete(key); err != nil {
		return false, fmt.Errorf("delete unknown rootfs object %s: %w", key, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

type PrefixLocker interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func ensureTeamPrefix(ctx context.Context, db PrefixLocker, teamID, prefix string) error {
	tag, err := db.Exec(ctx, `
		INSERT INTO manager.rootfs_team_prefixes_v3 (team_id, object_prefix, created_at, updated_at)
		VALUES ($1, $2, NOW(), NOW())
		ON CONFLICT (team_id) DO UPDATE SET updated_at = NOW()
		WHERE manager.rootfs_team_prefixes_v3.object_prefix = EXCLUDED.object_prefix
	`, strings.TrimSpace(teamID), strings.Trim(strings.TrimSpace(prefix), "/"))
	if err != nil {
		return fmt.Errorf("register rootfs team prefix: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs team prefix conflicts with existing mapping")
	}
	return nil
}

// LockPrefix serializes a rootfs team CAS mutation in the current database
// transaction. Callers must keep the transaction open through external object
// deletion and the corresponding PostgreSQL state update.
func LockPrefix(ctx context.Context, db PrefixLocker, prefix string) error {
	if _, err := db.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, $2))`, prefix, advisoryLockSalt); err != nil {
		return fmt.Errorf("lock rootfs object prefix: %w", err)
	}
	return nil
}
