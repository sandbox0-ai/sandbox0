package sandboxstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

type RootFSInventoryJob struct {
	HeadID    string
	TeamID    string
	Reference rootfshead.HeadReference
	Image     rootfshead.ImageReference
}

type RootFSV3GarbageCollectionResult struct {
	DeletedHeads  int
	QueuedObjects int
}

type RootFSInventoryStats struct {
	Pending      int64
	Running      int64
	Complete     int64
	Dead         int64
	PrefixGuards int64
}

func (s *PGSandboxStore) RootFSInventoryStats(ctx context.Context) (*RootFSInventoryStats, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	stats := &RootFSInventoryStats{}
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE state = 'pending'),
			COUNT(*) FILTER (WHERE state = 'running'),
			COUNT(*) FILTER (WHERE state = 'complete'),
			COUNT(*) FILTER (WHERE state = 'dead'),
			(SELECT COUNT(*) FROM manager.rootfs_head_prefix_guards_v3)
		FROM manager.rootfs_inventory_jobs_v3
	`).Scan(&stats.Pending, &stats.Running, &stats.Complete, &stats.Dead, &stats.PrefixGuards); err != nil {
		return nil, fmt.Errorf("collect rootfs v3 inventory stats: %w", err)
	}
	return stats, nil
}

func (s *PGSandboxStore) RenewRootFSInventoryJob(ctx context.Context, worker, headID string, ttl time.Duration) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	worker = strings.TrimSpace(worker)
	headID = strings.TrimSpace(headID)
	if worker == "" || headID == "" {
		return false, fmt.Errorf("rootfs inventory worker and head_id are required")
	}
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_inventory_jobs_v3
		SET claimed_until = NOW() + ($3::bigint * INTERVAL '1 millisecond'),
			updated_at = NOW()
		WHERE head_id = $1 AND state = 'running' AND claimed_by = $2
	`, headID, worker, ttl.Milliseconds())
	if err != nil {
		return false, fmt.Errorf("renew rootfs v3 inventory claim: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

func (s *PGSandboxStore) ClaimRootFSInventoryJobs(ctx context.Context, worker string, limit int, ttl time.Duration) ([]RootFSInventoryJob, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	worker = strings.TrimSpace(worker)
	if worker == "" {
		return nil, fmt.Errorf("rootfs inventory worker is required")
	}
	if limit <= 0 {
		limit = 1
	}
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT head_id
			FROM manager.rootfs_inventory_jobs_v3
			WHERE state IN ('pending', 'running')
				AND next_attempt_at <= NOW()
				AND (claimed_until IS NULL OR claimed_until <= NOW())
			ORDER BY next_attempt_at ASC, created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		), claimed AS (
			UPDATE manager.rootfs_inventory_jobs_v3 j
			SET state = 'running',
				claimed_by = $2,
				claimed_until = NOW() + ($3::bigint * INTERVAL '1 millisecond'),
				updated_at = NOW()
			FROM due
			WHERE j.head_id = due.head_id
			RETURNING j.head_id
		)
		SELECT h.head_id, h.team_id,
			h.manifest_key, h.manifest_digest, h.manifest_media_type, h.manifest_size,
			h.image_name, h.image_manifest_digest,
			h.platform_os, h.platform_architecture, h.platform_variant,
			h.marker_key, h.marker_digest, h.marker_media_type, h.marker_size,
			h.envelope_key, h.envelope_digest, h.envelope_media_type, h.envelope_size
		FROM claimed c
		JOIN manager.rootfs_heads_v3 h ON h.head_id = c.head_id
		ORDER BY h.created_at ASC
	`, limit, worker, ttl.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("claim rootfs v3 inventory jobs: %w", err)
	}
	defer rows.Close()
	var jobs []RootFSInventoryJob
	for rows.Next() {
		var job RootFSInventoryJob
		job.Reference.Version = rootfshead.Version
		if err := rows.Scan(
			&job.HeadID, &job.TeamID,
			&job.Reference.Manifest.Key, &job.Reference.Manifest.Digest, &job.Reference.Manifest.MediaType, &job.Reference.Manifest.Size,
			&job.Image.Name, &job.Image.ManifestDigest,
			&job.Image.Platform.OS, &job.Image.Platform.Architecture, &job.Image.Platform.Variant,
			&job.Image.Marker.Key, &job.Image.Marker.Digest, &job.Image.Marker.MediaType, &job.Image.Marker.Size,
			&job.Image.Envelope.Key, &job.Image.Envelope.Digest, &job.Image.Envelope.MediaType, &job.Image.Envelope.Size,
		); err != nil {
			return nil, err
		}
		job.Reference.HeadID = job.HeadID
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs v3 inventory jobs: %w", err)
	}
	return jobs, nil
}

func (s *PGSandboxStore) CompleteRootFSInventoryJob(ctx context.Context, worker, headID, teamID string, objects []rootfshead.Object) error {
	if s == nil || s.pool == nil {
		return nil
	}
	prefix, err := rootfshead.TeamObjectPrefix(teamID)
	if err != nil {
		return err
	}
	publicPrefix, err := rootfshead.TeamObjectPrefix(rootfshead.PublicImageFSTeamID)
	if err != nil {
		return err
	}
	objectOwners := make(map[string]string, len(objects))
	for _, object := range objects {
		if err := object.Validate(""); err != nil {
			return err
		}
		if err := rootfshead.ValidateReadableObjectScope(prefix, object); err != nil {
			return err
		}
		owner := teamID
		if rootfshead.ValidateObjectScope(prefix, object) != nil {
			if err := rootfshead.ValidateObjectScope(publicPrefix, object); err != nil {
				return err
			}
			owner = rootfshead.PublicImageFSTeamID
		}
		objectOwners[object.Key] = owner
	}
	sort.Slice(objects, func(i, j int) bool { return objects[i].Key < objects[j].Key })
	unique := objects[:0]
	for _, object := range objects {
		if len(unique) > 0 && unique[len(unique)-1].Key == object.Key {
			if unique[len(unique)-1] != object {
				return fmt.Errorf("rootfs inventory object %s has conflicting descriptors", object.Key)
			}
			continue
		}
		unique = append(unique, object)
	}
	objects = unique
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var state, claimedBy, storedTeamID string
	if err := tx.QueryRow(ctx, `
		SELECT j.state, j.claimed_by, h.team_id
		FROM manager.rootfs_inventory_jobs_v3 j
		JOIN manager.rootfs_heads_v3 h ON h.head_id = j.head_id
		WHERE j.head_id = $1
		FOR UPDATE OF j, h
	`, headID).Scan(&state, &claimedBy, &storedTeamID); err != nil {
		if err == pgx.ErrNoRows {
			return fmt.Errorf("rootfs v3 inventory claim for Head %s was lost", headID)
		}
		return err
	}
	if state != "running" || claimedBy != worker {
		return fmt.Errorf("rootfs v3 inventory claim for Head %s was lost", headID)
	}
	if storedTeamID != teamID {
		return fmt.Errorf("%w: rootfs v3 Head %s belongs to team %s", ErrRootFSHeadConflict, headID, storedTeamID)
	}
	if _, err := tx.Exec(ctx, `
		CREATE TEMP TABLE rootfs_inventory_stage_v3 (
			object_key TEXT PRIMARY KEY,
			team_id TEXT NOT NULL,
			digest TEXT NOT NULL,
			media_type TEXT NOT NULL,
			size BIGINT NOT NULL
		) ON COMMIT DROP
	`); err != nil {
		return fmt.Errorf("create rootfs v3 inventory staging table: %w", err)
	}
	copied, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"rootfs_inventory_stage_v3"},
		[]string{"object_key", "team_id", "digest", "media_type", "size"},
		pgx.CopyFromSlice(len(objects), func(index int) ([]any, error) {
			object := objects[index]
			return []any{object.Key, objectOwners[object.Key], object.Digest, object.MediaType, object.Size}, nil
		}),
	)
	if err != nil {
		return fmt.Errorf("stage rootfs v3 inventory: %w", err)
	}
	if copied != int64(len(objects)) {
		return fmt.Errorf("stage rootfs v3 inventory copied %d objects, expected %d", copied, len(objects))
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_objects_v3 (
			object_key, team_id, digest, media_type, size,
			last_referenced_at, created_at, updated_at
		)
		SELECT object_key, team_id, digest, media_type, size, NOW(), NOW(), NOW()
		FROM rootfs_inventory_stage_v3
		ON CONFLICT (object_key) DO UPDATE SET
			last_referenced_at = NOW(),
			missing_at = NULL,
			deleted_at = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE manager.rootfs_objects_v3.team_id = EXCLUDED.team_id
			AND manager.rootfs_objects_v3.digest = EXCLUDED.digest
			AND manager.rootfs_objects_v3.media_type = EXCLUDED.media_type
			AND manager.rootfs_objects_v3.size = EXCLUDED.size
	`)
	if err != nil {
		return fmt.Errorf("register rootfs v3 inventory objects: %w", err)
	}
	if tag.RowsAffected() != int64(len(objects)) {
		return fmt.Errorf("%w: rootfs v3 inventory contains a conflicting object", ErrRootFSObjectConflict)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_head_objects_v3 (head_id, object_key, conservative, created_at)
		SELECT $1, object_key, FALSE, NOW()
		FROM rootfs_inventory_stage_v3
		ON CONFLICT (head_id, object_key) DO UPDATE SET conservative = FALSE
	`, headID); err != nil {
		return fmt.Errorf("link rootfs v3 inventory objects: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_head_objects_v3 r
		WHERE r.head_id = $1
			AND NOT EXISTS (
				SELECT 1 FROM rootfs_inventory_stage_v3 staged
				WHERE staged.object_key = r.object_key
			)
	`, headID); err != nil {
		return err
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.rootfs_heads_v3 h
		SET inventory_complete = TRUE
		WHERE h.head_id = $1
			AND h.team_id = $2
	`, headID, teamID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("rootfs v3 inventory claim for Head %s was lost", headID)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM manager.rootfs_head_parent_guards_v3 WHERE child_head_id = $1`, headID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM manager.rootfs_head_prefix_guards_v3 WHERE head_id = $1`, headID); err != nil {
		return err
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.rootfs_inventory_jobs_v3
		SET state = 'complete', claimed_by = '', claimed_until = NULL,
			last_error = '', updated_at = NOW()
		WHERE head_id = $1 AND state = 'running' AND claimed_by = $2
	`, headID, worker)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs v3 inventory claim for Head %s was lost", headID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs v3 inventory: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) FailRootFSInventoryJob(ctx context.Context, worker, headID string, jobErr error) error {
	if s == nil || s.pool == nil {
		return nil
	}
	message := "rootfs inventory failed"
	if jobErr != nil {
		message = truncateRootFSError(jobErr.Error())
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_inventory_jobs_v3
		SET attempts = attempts + 1,
			state = CASE WHEN attempts + 1 >= 20 THEN 'dead' ELSE 'pending' END,
			next_attempt_at = NOW() + (LEAST(600, 1 << LEAST(attempts, 9)) * INTERVAL '1 second'),
			claimed_by = '', claimed_until = NULL,
			last_error = $3, updated_at = NOW()
		WHERE head_id = $1 AND claimed_by = $2
	`, headID, worker, message)
	if err != nil {
		return fmt.Errorf("fail rootfs v3 inventory job: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) GarbageCollectRootFSV3(ctx context.Context, teamID string, grace time.Duration, limit int) (*RootFSV3GarbageCollectionResult, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if grace <= 0 {
		grace = 30 * time.Minute
	}
	if limit <= 0 {
		limit = defaultRootFSObjectDeleteLimit
	}
	if limit > MaxRootFSObjectDeleteLimit {
		limit = MaxRootFSObjectDeleteLimit
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var deletedHeads int
	if err := tx.QueryRow(ctx, `
		WITH candidates AS (
			SELECT h.head_id
			FROM manager.rootfs_heads_v3 h
			WHERE ($1 = '' OR h.team_id = $1)
				AND h.created_at < NOW() - ($2::bigint * INTERVAL '1 millisecond')
				AND NOT EXISTS (SELECT 1 FROM manager.rootfs_filesystems f WHERE f.head_id_v3 = h.head_id)
				AND NOT EXISTS (
					SELECT 1 FROM scheduler_template_image_revisions r
					WHERE r.image_fs_head_id = h.head_id AND r.state = 'ready'
				)
				AND NOT EXISTS (SELECT 1 FROM manager.rootfs_snapshots s WHERE s.head_id_v3 = h.head_id AND (s.expires_at IS NULL OR s.expires_at > NOW()))
				AND NOT EXISTS (
					SELECT 1 FROM manager.sandbox_lifecycle_txns t
					WHERE (t.expected_head_id_v3 = h.head_id OR t.prepared_head_id_v3 = h.head_id)
						AND t.phase IN ('preparing', 'barriered', 'publishing', 'committing')
				)
				AND NOT EXISTS (
					SELECT 1 FROM manager.rootfs_head_parent_guards_v3 g
					WHERE g.child_head_id = h.head_id OR g.parent_head_id = h.head_id
				)
			ORDER BY h.created_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		), deleted AS (
			DELETE FROM manager.rootfs_heads_v3 h
			USING candidates c
			WHERE h.head_id = c.head_id
			RETURNING h.head_id
		)
		SELECT COUNT(*) FROM deleted
	`, strings.TrimSpace(teamID), grace.Milliseconds(), limit).Scan(&deletedHeads); err != nil {
		return nil, fmt.Errorf("collect unreferenced rootfs v3 Heads: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_object_deletions (
			object_key, team_id, attempts, last_error,
			next_attempt_at, claimed_by, claimed_until, dead_lettered_at,
			created_at, updated_at
		)
		SELECT o.object_key, o.team_id, 0, '', NOW(), '', NULL, NULL, NOW(), NOW()
		FROM manager.rootfs_objects_v3 o
		WHERE ($1 = '' OR o.team_id = $1)
			AND o.deleted_at IS NULL
			AND o.created_at < NOW() - ($2::bigint * INTERVAL '1 millisecond')
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_head_prefix_guards_v3 g
				WHERE g.team_id = o.team_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_head_objects_v3 r WHERE r.object_key = o.object_key
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_head_exports_v3 e WHERE e.object_key = o.object_key
			)
		ORDER BY o.created_at ASC
		LIMIT $3
		ON CONFLICT (object_key) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			next_attempt_at = NOW(), claimed_by = '', claimed_until = NULL,
			dead_lettered_at = NULL, updated_at = NOW()
	`, strings.TrimSpace(teamID), grace.Milliseconds(), limit)
	if err != nil {
		return nil, fmt.Errorf("queue unreferenced rootfs v3 objects: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &RootFSV3GarbageCollectionResult{DeletedHeads: deletedHeads, QueuedObjects: int(tag.RowsAffected())}, nil
}
