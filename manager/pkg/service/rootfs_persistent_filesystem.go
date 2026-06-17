package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrRootFSHeadConflict = errors.New("rootfs filesystem head conflict")
var ErrRootFSFilesystemNotFound = errors.New("rootfs filesystem not found")
var ErrRootFSFilesystemConflict = errors.New("rootfs filesystem conflict")
var ErrRootFSSnapshotNotFound = errors.New("rootfs snapshot not found")

// RootFSFilesystem is the canonical persistent filesystem object backing a
// sandbox writable rootfs.
type RootFSFilesystem struct {
	ID                 string
	TeamID             string
	SourceFilesystemID string
	HeadLayerID        string
	BaseImageRef       string
	BaseImageDigest    string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// RootFSSnapshot is an immutable pointer to one rootfs filesystem head.
type RootFSSnapshot struct {
	ID              string
	FilesystemID    string
	TeamID          string
	SourceSandboxID string
	HeadLayerID     string
	Name            string
	Description     string
	CreatedAt       time.Time
	ExpiresAt       time.Time
}

type CreateRootFSSnapshotRequest struct {
	SandboxID   string
	SnapshotID  string
	Name        string
	Description string
	ExpiresAt   time.Time
}

type ForkRootFSFilesystemRequest struct {
	SourceSandboxID string
	TargetSandboxID string
	TargetTeamID    string
}

type RestoreRootFSFromSnapshotRequest struct {
	SandboxID  string
	SnapshotID string
	TeamID     string
}

type SquashRootFSFilesystemRequest struct {
	SandboxID           string
	ExpectedHeadLayerID string
	SquashedRootFSState *SandboxRootFSState
}

type RootFSGarbageCollectionResult struct {
	Layers            []*SandboxRootFSLayer
	DeletedObjectKeys []string
}

type DeletePendingRootFSObjectsOptions struct {
	Limit           int
	ClaimedBy       string
	ClaimTTL        time.Duration
	BackoffBase     time.Duration
	BackoffMax      time.Duration
	MaxAttempts     int
	ContinueOnError bool
}

type RootFSObjectDeletionQueueStats struct {
	Pending      int64
	Due          int64
	Claimed      int64
	DeadLettered int64
	OldestQueued time.Time
}

// RootFSObjectDeleter deletes rootfs diff objects from durable object storage.
type RootFSObjectDeleter interface {
	Delete(key string) error
}

const (
	defaultRootFSObjectDeleteLimit       = 100
	maxRootFSObjectDeleteLimit           = 1000
	defaultRootFSObjectDeleteClaimTTL    = 2 * time.Minute
	defaultRootFSObjectDeleteBackoffBase = 5 * time.Second
	defaultRootFSObjectDeleteBackoffMax  = 10 * time.Minute
)

func (s *PGSandboxStore) GetRootFSFilesystem(ctx context.Context, sandboxID string) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(sandboxID) == "" {
		return nil, nil
	}
	filesystem, err := scanRootFSFilesystem(s.pool.QueryRow(ctx, `
		SELECT f.filesystem_id, f.team_id, f.source_filesystem_id, f.head_layer_id,
			f.base_image_ref, f.base_image_digest, f.created_at, f.updated_at
		FROM manager.sandbox_rootfs_bindings b
		JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
		WHERE b.sandbox_id = $1
	`, sandboxID))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("get rootfs filesystem: %w", err)
	}
	return filesystem, nil
}

func (s *PGSandboxStore) CreateRootFSSnapshot(ctx context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	if strings.TrimSpace(req.SandboxID) == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(req.SnapshotID) == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	snapshot, err := scanRootFSSnapshot(s.pool.QueryRow(ctx, `
		WITH source AS (
			SELECT b.filesystem_id, b.team_id, f.head_layer_id
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			WHERE b.sandbox_id = $1
				AND f.head_layer_id IS NOT NULL
		)
		INSERT INTO manager.rootfs_snapshots (
			snapshot_id, filesystem_id, team_id, source_sandbox_id, head_layer_id,
			name, description, created_at, expires_at
		)
		SELECT $2, filesystem_id, team_id, $1, head_layer_id, $3, $4, NOW(), $5
		FROM source
		RETURNING snapshot_id, filesystem_id, team_id, source_sandbox_id,
			head_layer_id, name, description, created_at, expires_at
	`, req.SandboxID, req.SnapshotID, req.Name, req.Description, nullableTime(req.ExpiresAt)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: sandbox %s", ErrRootFSFilesystemNotFound, req.SandboxID)
		}
		return nil, fmt.Errorf("create rootfs snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *PGSandboxStore) ForkRootFSFilesystem(ctx context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	if strings.TrimSpace(req.SourceSandboxID) == "" {
		return nil, fmt.Errorf("source_sandbox_id is required")
	}
	if strings.TrimSpace(req.TargetSandboxID) == "" {
		return nil, fmt.Errorf("target_sandbox_id is required")
	}
	if strings.TrimSpace(req.SourceSandboxID) == strings.TrimSpace(req.TargetSandboxID) {
		return nil, fmt.Errorf("%w: source and target sandbox are the same", ErrRootFSFilesystemConflict)
	}
	filesystem, err := scanRootFSFilesystem(s.pool.QueryRow(ctx, `
		WITH source AS (
			SELECT f.filesystem_id, f.team_id, f.head_layer_id, f.base_image_ref, f.base_image_digest
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			WHERE b.sandbox_id = $1
				AND f.head_layer_id IS NOT NULL
		),
		target_sandbox AS (
			SELECT sandbox_id, COALESCE(NULLIF($3, ''), team_id) AS team_id
			FROM manager.sandboxes
			WHERE sandbox_id = $2
		),
		created AS (
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, source_filesystem_id, head_layer_id,
				base_image_ref, base_image_digest, created_at, updated_at
			)
			SELECT
				$2,
				target_sandbox.team_id,
				source.filesystem_id,
				source.head_layer_id,
				source.base_image_ref,
				source.base_image_digest,
				NOW(),
				NOW()
			FROM source
			CROSS JOIN target_sandbox
			ON CONFLICT (filesystem_id) DO NOTHING
			RETURNING filesystem_id, team_id, source_filesystem_id, head_layer_id,
				base_image_ref, base_image_digest, created_at, updated_at
		),
		bound AS (
			INSERT INTO manager.sandbox_rootfs_bindings (
				sandbox_id, filesystem_id, team_id, created_at, updated_at
			)
			SELECT $2, filesystem_id, team_id, NOW(), NOW()
			FROM created
			ON CONFLICT (sandbox_id) DO NOTHING
			RETURNING filesystem_id
		)
		SELECT created.filesystem_id, created.team_id, created.source_filesystem_id,
			created.head_layer_id, created.base_image_ref, created.base_image_digest,
			created.created_at, created.updated_at
		FROM created
		JOIN bound ON bound.filesystem_id = created.filesystem_id
	`, req.SourceSandboxID, req.TargetSandboxID, strings.TrimSpace(req.TargetTeamID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, s.rootFSForkNoRowsError(ctx, req)
		}
		return nil, fmt.Errorf("fork rootfs filesystem: %w", err)
	}
	return filesystem, nil
}

func (s *PGSandboxStore) RestoreRootFSFromSnapshot(ctx context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	if strings.TrimSpace(req.SandboxID) == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(req.SnapshotID) == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	filesystem, err := scanRootFSFilesystem(s.pool.QueryRow(ctx, `
		WITH snapshot AS (
			SELECT s.snapshot_id, s.filesystem_id, s.team_id, s.head_layer_id,
				l.base_image_ref, l.base_image_digest
			FROM manager.rootfs_snapshots s
			JOIN manager.rootfs_layers l ON l.layer_id = s.head_layer_id
			WHERE s.snapshot_id = $2
				AND ($3 = '' OR s.team_id = $3)
		),
		target_sandbox AS (
			SELECT sandbox_id, COALESCE(NULLIF($3, ''), team_id) AS team_id
			FROM manager.sandboxes
			WHERE sandbox_id = $1
		),
		binding AS (
			SELECT filesystem_id
			FROM manager.sandbox_rootfs_bindings
			WHERE sandbox_id = $1
			UNION ALL
			SELECT $1
			WHERE NOT EXISTS (
				SELECT 1
				FROM manager.sandbox_rootfs_bindings
				WHERE sandbox_id = $1
			)
			LIMIT 1
		),
		restored AS (
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, source_filesystem_id, head_layer_id,
				base_image_ref, base_image_digest, created_at, updated_at
			)
			SELECT
				binding.filesystem_id,
				target_sandbox.team_id,
				snapshot.filesystem_id,
				snapshot.head_layer_id,
				snapshot.base_image_ref,
				snapshot.base_image_digest,
				NOW(),
				NOW()
			FROM snapshot
			CROSS JOIN target_sandbox
			CROSS JOIN binding
			ON CONFLICT (filesystem_id) DO UPDATE SET
				team_id = EXCLUDED.team_id,
				source_filesystem_id = COALESCE(
					manager.rootfs_filesystems.source_filesystem_id,
					EXCLUDED.source_filesystem_id
				),
				head_layer_id = EXCLUDED.head_layer_id,
				base_image_ref = EXCLUDED.base_image_ref,
				base_image_digest = EXCLUDED.base_image_digest,
				updated_at = NOW()
			RETURNING filesystem_id, team_id, source_filesystem_id, head_layer_id,
				base_image_ref, base_image_digest, created_at, updated_at
		),
		bound AS (
			INSERT INTO manager.sandbox_rootfs_bindings (
				sandbox_id, filesystem_id, team_id, created_at, updated_at
			)
			SELECT $1, filesystem_id, team_id, NOW(), NOW()
			FROM restored
			ON CONFLICT (sandbox_id) DO UPDATE SET
				team_id = EXCLUDED.team_id
			RETURNING filesystem_id
		)
		SELECT restored.filesystem_id, restored.team_id, restored.source_filesystem_id,
			restored.head_layer_id, restored.base_image_ref, restored.base_image_digest,
			restored.created_at, restored.updated_at
		FROM restored
		JOIN bound ON bound.filesystem_id = restored.filesystem_id
	`, req.SandboxID, req.SnapshotID, strings.TrimSpace(req.TeamID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, s.rootFSRestoreNoRowsError(ctx, req)
		}
		return nil, fmt.Errorf("restore rootfs filesystem from snapshot: %w", err)
	}
	return filesystem, nil
}

// SquashRootFSFilesystem replaces a filesystem layer chain with a single
// precomputed layer. The caller is responsible for creating the squashed diff
// object before advancing the filesystem head.
func (s *PGSandboxStore) SquashRootFSFilesystem(ctx context.Context, req *SquashRootFSFilesystemRequest) error {
	if s == nil || s.pool == nil || req == nil || req.SquashedRootFSState == nil {
		return nil
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	if sandboxID == "" {
		sandboxID = strings.TrimSpace(req.SquashedRootFSState.SandboxID)
	}
	if sandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(req.ExpectedHeadLayerID) == "" {
		return fmt.Errorf("expected_head_layer_id is required")
	}
	state := *req.SquashedRootFSState
	state.SandboxID = sandboxID
	if strings.TrimSpace(state.ParentLayerID) != "" {
		return fmt.Errorf("squashed rootfs layer cannot have a parent layer")
	}
	if err := validateRootFSState(&state); err != nil {
		return err
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs squash tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := saveRootFSLayer(ctx, tx, &state); err != nil {
		return err
	}
	if err := advanceRootFSFilesystemHead(ctx, tx, &state, nullableText(req.ExpectedHeadLayerID)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs squash tx: %w", err)
	}
	return nil
}

// GarbageCollectRootFSFilesystem removes unreferenced leaf layer metadata and
// deletes the corresponding diff objects from durable object storage. Object
// keys are durably queued before layer metadata is removed, so failed object
// deletes can be retried without losing the key.
func (s *PGSandboxStore) GarbageCollectRootFSFilesystem(ctx context.Context, deleter RootFSObjectDeleter, teamID string, limit int) (*RootFSGarbageCollectionResult, error) {
	return s.GarbageCollectRootFSFilesystemWithOptions(ctx, deleter, teamID, limit, DeletePendingRootFSObjectsOptions{})
}

func (s *PGSandboxStore) GarbageCollectRootFSFilesystemWithOptions(ctx context.Context, deleter RootFSObjectDeleter, teamID string, limit int, opts DeletePendingRootFSObjectsOptions) (*RootFSGarbageCollectionResult, error) {
	if deleter == nil {
		return nil, fmt.Errorf("rootfs object deleter is required")
	}
	layers, err := s.collectUnreferencedRootFSLayers(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	opts.Limit = limit
	deletedObjectKeys, err := s.DeletePendingRootFSObjectsWithOptions(ctx, deleter, opts)
	result := &RootFSGarbageCollectionResult{
		Layers:            layers,
		DeletedObjectKeys: deletedObjectKeys,
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func (s *PGSandboxStore) collectUnreferencedRootFSLayers(ctx context.Context, teamID string, limit int) ([]*SandboxRootFSLayer, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = defaultRootFSObjectDeleteLimit
	}
	if limit > maxRootFSObjectDeleteLimit {
		limit = maxRootFSObjectDeleteLimit
	}
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE roots AS (
			SELECT head_layer_id AS layer_id
			FROM manager.rootfs_filesystems
			WHERE head_layer_id IS NOT NULL
			UNION
			SELECT head_layer_id AS layer_id
			FROM manager.rootfs_snapshots
			WHERE head_layer_id IS NOT NULL
		),
		reachable AS (
			SELECT l.layer_id, l.parent_layer_id
			FROM manager.rootfs_layers l
			JOIN roots r ON r.layer_id = l.layer_id
			UNION
			SELECT parent.layer_id, parent.parent_layer_id
			FROM manager.rootfs_layers parent
			JOIN reachable child ON child.parent_layer_id = parent.layer_id
		),
		candidates AS (
			SELECT l.layer_id
			FROM manager.rootfs_layers l
			WHERE ($1 = '' OR l.team_id = $1)
				AND NOT EXISTS (
					SELECT 1
					FROM reachable r
					WHERE r.layer_id = l.layer_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_layers child
					WHERE child.parent_layer_id = l.layer_id
				)
			ORDER BY l.created_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		),
		queued_objects AS (
			INSERT INTO manager.rootfs_object_deletions (
				object_key, team_id, created_at, updated_at
			)
			SELECT DISTINCT l.diff_object_key, l.team_id, NOW(), NOW()
			FROM manager.rootfs_layers l
			JOIN candidates c ON c.layer_id = l.layer_id
			WHERE l.diff_object_key <> ''
			ON CONFLICT (object_key) DO UPDATE SET
				team_id = EXCLUDED.team_id,
				next_attempt_at = NOW(),
				claimed_by = '',
				claimed_until = NULL,
				dead_lettered_at = NULL,
				updated_at = NOW()
			RETURNING object_key
		),
		deleted AS (
			DELETE FROM manager.rootfs_layers l
			USING candidates c
			WHERE l.layer_id = c.layer_id
				AND (
					l.diff_object_key = ''
					OR EXISTS (
						SELECT 1
						FROM queued_objects q
						WHERE q.object_key = l.diff_object_key
					)
				)
			RETURNING l.layer_id, l.parent_layer_id, l.source_sandbox_id, l.team_id,
				l.runtime_generation, l.runtime, l.runtime_handler, l.base_image_ref,
				l.base_image_digest, l.snapshotter, l.snapshot_parent,
				l.snapshot_parent_chain, l.diff_digest, l.diff_id, l.diff_media_type,
				l.diff_size, l.diff_object_key, l.created_at
		)
		SELECT layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
			runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
			snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
			diff_size, diff_object_key, created_at
		FROM deleted
		ORDER BY created_at ASC
	`, strings.TrimSpace(teamID), limit)
	if err != nil {
		return nil, fmt.Errorf("collect unreferenced rootfs layers: %w", err)
	}
	defer rows.Close()

	var layers []*SandboxRootFSLayer
	for rows.Next() {
		layer, err := scanRootFSLayerRows(rows)
		if err != nil {
			return nil, err
		}
		layers = append(layers, layer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate collected rootfs layers: %w", err)
	}
	return layers, nil
}

func (s *PGSandboxStore) DeletePendingRootFSObjects(ctx context.Context, deleter RootFSObjectDeleter, limit int) ([]string, error) {
	return s.DeletePendingRootFSObjectsWithOptions(ctx, deleter, DeletePendingRootFSObjectsOptions{Limit: limit})
}

func (s *PGSandboxStore) DeletePendingRootFSObjectsWithOptions(ctx context.Context, deleter RootFSObjectDeleter, opts DeletePendingRootFSObjectsOptions) ([]string, error) {
	if s == nil || s.pool == nil || deleter == nil {
		return nil, nil
	}
	opts = normalizeRootFSObjectDeletionOptions(opts)
	claimed, err := s.claimPendingRootFSObjectDeletions(ctx, opts)
	if err != nil {
		return nil, err
	}

	deleted := make([]string, 0, len(claimed))
	var errs []error
	for _, item := range claimed {
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if err := deleter.Delete(item.ObjectKey); err != nil {
			if updateErr := s.recordRootFSObjectDeleteFailure(ctx, item, opts, err); updateErr != nil {
				return deleted, updateErr
			}
			errs = append(errs, fmt.Errorf("delete rootfs object %q: %w", item.ObjectKey, err))
			if !opts.ContinueOnError {
				return deleted, errors.Join(errs...)
			}
			continue
		}
		if _, err := s.pool.Exec(ctx, `
			DELETE FROM manager.rootfs_object_deletions
			WHERE object_key = $1
				AND claimed_by = $2
		`, item.ObjectKey, opts.ClaimedBy); err != nil {
			return deleted, fmt.Errorf("clear rootfs object deletion %q: %w", item.ObjectKey, err)
		}
		deleted = append(deleted, item.ObjectKey)
	}
	return deleted, errors.Join(errs...)
}

func (s *PGSandboxStore) RootFSObjectDeletionQueueStats(ctx context.Context) (*RootFSObjectDeletionQueueStats, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	var stats RootFSObjectDeletionQueueStats
	var oldestQueued *time.Time
	if err := s.pool.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE dead_lettered_at IS NULL) AS pending,
			COUNT(*) FILTER (
				WHERE dead_lettered_at IS NULL
					AND next_attempt_at <= NOW()
					AND (claimed_until IS NULL OR claimed_until <= NOW())
			) AS due,
			COUNT(*) FILTER (
				WHERE dead_lettered_at IS NULL
					AND claimed_until IS NOT NULL
					AND claimed_until > NOW()
			) AS claimed,
			COUNT(*) FILTER (WHERE dead_lettered_at IS NOT NULL) AS dead_lettered,
			MIN(created_at) FILTER (WHERE dead_lettered_at IS NULL) AS oldest_queued
		FROM manager.rootfs_object_deletions
	`).Scan(&stats.Pending, &stats.Due, &stats.Claimed, &stats.DeadLettered, &oldestQueued); err != nil {
		return nil, fmt.Errorf("load rootfs object deletion queue stats: %w", err)
	}
	if oldestQueued != nil {
		stats.OldestQueued = *oldestQueued
	}
	return &stats, nil
}

type claimedRootFSObjectDeletion struct {
	ObjectKey string
	Attempts  int
}

func (s *PGSandboxStore) claimPendingRootFSObjectDeletions(ctx context.Context, opts DeletePendingRootFSObjectsOptions) ([]claimedRootFSObjectDeletion, error) {
	claimTTLSeconds := durationSeconds(opts.ClaimTTL)
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT object_key
			FROM manager.rootfs_object_deletions
			WHERE dead_lettered_at IS NULL
				AND next_attempt_at <= NOW()
				AND (claimed_until IS NULL OR claimed_until <= NOW())
			ORDER BY next_attempt_at ASC, updated_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE manager.rootfs_object_deletions q
			SET claimed_by = $2,
				claimed_until = NOW() + ($3::int * INTERVAL '1 second'),
				updated_at = NOW()
			FROM due
			WHERE q.object_key = due.object_key
			RETURNING q.object_key, q.attempts
		)
		SELECT object_key, attempts
		FROM claimed
		ORDER BY object_key ASC
	`, opts.Limit, opts.ClaimedBy, claimTTLSeconds)
	if err != nil {
		return nil, fmt.Errorf("claim pending rootfs object deletions: %w", err)
	}
	defer rows.Close()

	var claimed []claimedRootFSObjectDeletion
	for rows.Next() {
		var item claimedRootFSObjectDeletion
		if err := rows.Scan(&item.ObjectKey, &item.Attempts); err != nil {
			return nil, err
		}
		claimed = append(claimed, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed rootfs object deletions: %w", err)
	}
	return claimed, nil
}

func (s *PGSandboxStore) recordRootFSObjectDeleteFailure(ctx context.Context, item claimedRootFSObjectDeletion, opts DeletePendingRootFSObjectsOptions, deleteErr error) error {
	nextAttempts := item.Attempts + 1
	delay := rootFSObjectDeleteBackoff(nextAttempts, opts.BackoffBase, opts.BackoffMax)
	deadLetter := opts.MaxAttempts > 0 && nextAttempts >= opts.MaxAttempts
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_object_deletions
		SET attempts = attempts + 1,
			last_error = $3,
			last_attempt_at = NOW(),
			next_attempt_at = NOW() + ($4::int * INTERVAL '1 second'),
			claimed_by = '',
			claimed_until = NULL,
			dead_lettered_at = CASE
				WHEN $5 THEN NOW()
				ELSE NULL
			END,
			updated_at = NOW()
		WHERE object_key = $1
			AND claimed_by = $2
	`, item.ObjectKey, opts.ClaimedBy, truncateRootFSError(deleteErr.Error()), durationSeconds(delay), deadLetter)
	if err != nil {
		return fmt.Errorf("record rootfs object delete failure for %q: %w", item.ObjectKey, err)
	}
	return nil
}

func normalizeRootFSObjectDeletionOptions(opts DeletePendingRootFSObjectsOptions) DeletePendingRootFSObjectsOptions {
	if opts.Limit <= 0 {
		opts.Limit = defaultRootFSObjectDeleteLimit
	}
	if opts.Limit > maxRootFSObjectDeleteLimit {
		opts.Limit = maxRootFSObjectDeleteLimit
	}
	opts.ClaimedBy = strings.TrimSpace(opts.ClaimedBy)
	if opts.ClaimedBy == "" {
		opts.ClaimedBy = fmt.Sprintf("rootfs-gc-%d", time.Now().UnixNano())
	}
	if opts.ClaimTTL <= 0 {
		opts.ClaimTTL = defaultRootFSObjectDeleteClaimTTL
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = defaultRootFSObjectDeleteBackoffBase
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = defaultRootFSObjectDeleteBackoffMax
	}
	if opts.BackoffMax < opts.BackoffBase {
		opts.BackoffMax = opts.BackoffBase
	}
	return opts
}

func rootFSObjectDeleteBackoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = defaultRootFSObjectDeleteBackoffBase
	}
	if max <= 0 {
		max = defaultRootFSObjectDeleteBackoffMax
	}
	if max < base {
		max = base
	}
	if attempt <= 1 {
		return base
	}
	delay := base
	for i := 1; i < attempt; i++ {
		if delay >= max/2 {
			return max
		}
		delay *= 2
	}
	if delay > max {
		return max
	}
	return delay
}

func durationSeconds(d time.Duration) int {
	if d <= 0 {
		return 1
	}
	seconds := int(d.Round(time.Second) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func truncateRootFSError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= 2048 {
		return message
	}
	return message[:2048]
}

func DeleteRootFSObjects(ctx context.Context, deleter RootFSObjectDeleter, layers []*SandboxRootFSLayer) ([]string, error) {
	if deleter == nil || len(layers) == 0 {
		return nil, nil
	}
	seen := make(map[string]struct{}, len(layers))
	deleted := make([]string, 0, len(layers))
	for _, layer := range layers {
		if layer == nil {
			continue
		}
		key := strings.TrimSpace(layer.DiffObjectKey)
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		if err := ctx.Err(); err != nil {
			return deleted, err
		}
		if err := deleter.Delete(key); err != nil {
			return deleted, fmt.Errorf("delete rootfs object %q: %w", key, err)
		}
		deleted = append(deleted, key)
	}
	return deleted, nil
}

func (s *PGSandboxStore) rootFSForkNoRowsError(ctx context.Context, req *ForkRootFSFilesystemRequest) error {
	source, err := s.GetRootFSFilesystem(ctx, req.SourceSandboxID)
	if err != nil {
		return err
	}
	if source == nil || strings.TrimSpace(source.HeadLayerID) == "" {
		return fmt.Errorf("%w: sandbox %s", ErrRootFSFilesystemNotFound, req.SourceSandboxID)
	}
	if ok, err := s.sandboxExists(ctx, req.TargetSandboxID); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, req.TargetSandboxID)
	}
	return fmt.Errorf("%w: target sandbox %s", ErrRootFSFilesystemConflict, req.TargetSandboxID)
}

func (s *PGSandboxStore) rootFSRestoreNoRowsError(ctx context.Context, req *RestoreRootFSFromSnapshotRequest) error {
	if ok, err := s.rootFSSnapshotExists(ctx, req.SnapshotID, strings.TrimSpace(req.TeamID)); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, req.SnapshotID)
	}
	if ok, err := s.sandboxExists(ctx, req.SandboxID); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, req.SandboxID)
	}
	return fmt.Errorf("%w: restore target %s", ErrRootFSFilesystemConflict, req.SandboxID)
}

func (s *PGSandboxStore) sandboxExists(ctx context.Context, sandboxID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.sandboxes
			WHERE sandbox_id = $1
		)
	`, sandboxID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check sandbox exists: %w", err)
	}
	return exists, nil
}

func (s *PGSandboxStore) rootFSSnapshotExists(ctx context.Context, snapshotID, teamID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_snapshots
			WHERE snapshot_id = $1
				AND ($2 = '' OR team_id = $2)
		)
	`, snapshotID, teamID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check rootfs snapshot exists: %w", err)
	}
	return exists, nil
}

func scanRootFSFilesystem(row sandboxRecordScanner) (*RootFSFilesystem, error) {
	var filesystem RootFSFilesystem
	var sourceFilesystemID, headLayerID *string
	if err := row.Scan(
		&filesystem.ID, &filesystem.TeamID, &sourceFilesystemID, &headLayerID,
		&filesystem.BaseImageRef, &filesystem.BaseImageDigest,
		&filesystem.CreatedAt, &filesystem.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if sourceFilesystemID != nil {
		filesystem.SourceFilesystemID = *sourceFilesystemID
	}
	if headLayerID != nil {
		filesystem.HeadLayerID = *headLayerID
	}
	return &filesystem, nil
}

func scanRootFSSnapshot(row sandboxRecordScanner) (*RootFSSnapshot, error) {
	var snapshot RootFSSnapshot
	var filesystemID *string
	var expiresAt *time.Time
	if err := row.Scan(
		&snapshot.ID, &filesystemID, &snapshot.TeamID, &snapshot.SourceSandboxID,
		&snapshot.HeadLayerID, &snapshot.Name, &snapshot.Description,
		&snapshot.CreatedAt, &expiresAt,
	); err != nil {
		return nil, err
	}
	if filesystemID != nil {
		snapshot.FilesystemID = *filesystemID
	}
	if expiresAt != nil {
		snapshot.ExpiresAt = *expiresAt
	}
	return &snapshot, nil
}
