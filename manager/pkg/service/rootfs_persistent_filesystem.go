package service

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

var ErrRootFSHeadConflict = errors.New("rootfs filesystem head conflict")
var ErrRootFSFilesystemNotFound = errors.New("rootfs filesystem not found")
var ErrRootFSFilesystemConflict = errors.New("rootfs filesystem conflict")
var ErrRootFSSnapshotNotFound = errors.New("rootfs snapshot not found")
var ErrRootFSObjectConflict = errors.New("rootfs object metadata conflict")

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

type ListRootFSSnapshotsRequest struct {
	SandboxID string
	TeamID    string
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
	Layers             []*SandboxRootFSLayer
	DeletedObjectKeys  []string
	ExpiredSnapshots   int
	DeletedFilesystems int
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

// RootFSStorageUsage is the current COW physical storage usage for one team.
// It counts distinct reachable rootfs diff objects, not sandbox layer-chain sums.
type RootFSStorageUsage struct {
	TeamID       string
	ObjectCount  int64
	StorageBytes int64
	ObservedAt   time.Time
}

type RootFSObjectInfo struct {
	Key      string
	Size     int64
	Modified time.Time
}

type RootFSObjectAuditResult struct {
	Checked        int
	Missing        int
	SizeMismatched int
}

// RootFSObjectDeleter deletes rootfs diff objects from durable object storage.
type RootFSObjectDeleter interface {
	Delete(key string) error
}

type RootFSObjectInspector interface {
	StatRootFSObject(key string) (RootFSObjectInfo, error)
}

type RootFSStorageMeteringRecorder interface {
	RecordStorageObservation(context.Context, *meteringpkg.StorageObservation) error
}

type rootFSStoreDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
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
	return createRootFSSnapshot(ctx, s.pool, req)
}

func (t sandboxStoreTx) CreateRootFSSnapshot(ctx context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	return createRootFSSnapshot(ctx, t.tx, req)
}

func createRootFSSnapshot(ctx context.Context, db rootFSStoreDB, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	if db == nil || req == nil {
		return nil, nil
	}
	if strings.TrimSpace(req.SandboxID) == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(req.SnapshotID) == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	snapshot, err := scanRootFSSnapshot(db.QueryRow(ctx, `
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

func (s *PGSandboxStore) ListRootFSSnapshots(ctx context.Context, req *ListRootFSSnapshotsRequest) ([]*RootFSSnapshot, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT snapshot_id, filesystem_id, team_id, source_sandbox_id,
			head_layer_id, name, description, created_at, expires_at
		FROM manager.rootfs_snapshots
		WHERE source_sandbox_id = $1
			AND ($2 = '' OR team_id = $2)
			AND (expires_at IS NULL OR expires_at > NOW())
		ORDER BY created_at DESC
	`, strings.TrimSpace(req.SandboxID), strings.TrimSpace(req.TeamID))
	if err != nil {
		return nil, fmt.Errorf("list rootfs snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []*RootFSSnapshot
	for rows.Next() {
		snapshot, err := scanRootFSSnapshot(rows)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, snapshot)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs snapshots: %w", err)
	}
	return snapshots, nil
}

func (s *PGSandboxStore) GetRootFSSnapshot(ctx context.Context, snapshotID, teamID string) (*RootFSSnapshot, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(snapshotID) == "" {
		return nil, nil
	}
	snapshot, err := scanRootFSSnapshot(s.pool.QueryRow(ctx, `
		SELECT snapshot_id, filesystem_id, team_id, source_sandbox_id,
			head_layer_id, name, description, created_at, expires_at
		FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1
			AND ($2 = '' OR team_id = $2)
			AND (expires_at IS NULL OR expires_at > NOW())
	`, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID)))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, snapshotID)
		}
		return nil, fmt.Errorf("get rootfs snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *PGSandboxStore) DeleteRootFSSnapshot(ctx context.Context, snapshotID, teamID string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1
			AND ($2 = '' OR team_id = $2)
	`, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID))
	if err != nil {
		return fmt.Errorf("delete rootfs snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, snapshotID)
	}
	return nil
}

func (s *PGSandboxStore) ForkRootFSFilesystem(ctx context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	filesystem, err := forkRootFSFilesystem(ctx, s.pool, req)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, s.rootFSForkNoRowsError(ctx, req)
		}
		return nil, err
	}
	return filesystem, nil
}

func (t sandboxStoreTx) ForkRootFSFilesystem(ctx context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	filesystem, err := forkRootFSFilesystem(ctx, t.tx, req)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: source sandbox %s or target sandbox %s", ErrRootFSFilesystemNotFound, req.SourceSandboxID, req.TargetSandboxID)
	}
	return filesystem, err
}

func forkRootFSFilesystem(ctx context.Context, db rootFSStoreDB, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	if db == nil || req == nil {
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
	filesystem, err := scanRootFSFilesystem(db.QueryRow(ctx, `
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
		return nil, fmt.Errorf("fork rootfs filesystem: %w", err)
	}
	return filesystem, nil
}

func (s *PGSandboxStore) RestoreRootFSFromSnapshot(ctx context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	filesystem, err := restoreRootFSFromSnapshot(ctx, s.pool, req)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, s.rootFSRestoreNoRowsError(ctx, req)
		}
		return nil, err
	}
	return filesystem, nil
}

func (t sandboxStoreTx) RestoreRootFSFromSnapshot(ctx context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	filesystem, err := restoreRootFSFromSnapshot(ctx, t.tx, req)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: snapshot %s or sandbox %s", ErrRootFSFilesystemConflict, req.SnapshotID, req.SandboxID)
	}
	return filesystem, err
}

func restoreRootFSFromSnapshot(ctx context.Context, db rootFSStoreDB, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	if db == nil || req == nil {
		return nil, nil
	}
	if strings.TrimSpace(req.SandboxID) == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(req.SnapshotID) == "" {
		return nil, fmt.Errorf("snapshot_id is required")
	}
	filesystem, err := scanRootFSFilesystem(db.QueryRow(ctx, `
		WITH snapshot AS (
			SELECT s.snapshot_id, s.filesystem_id, s.team_id, s.head_layer_id,
				l.base_image_ref, l.base_image_digest
			FROM manager.rootfs_snapshots s
			JOIN manager.rootfs_layers l ON l.layer_id = s.head_layer_id
			WHERE s.snapshot_id = $2
				AND ($3 = '' OR s.team_id = $3)
				AND (s.expires_at IS NULL OR s.expires_at > NOW())
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
	expiredSnapshots, err := s.DeleteExpiredRootFSSnapshots(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	deletedFilesystems, err := s.DeleteUnreferencedRootFSFilesystems(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	layers, err := s.collectUnreferencedRootFSLayers(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	opts.Limit = limit
	deletedObjectKeys, err := s.DeletePendingRootFSObjectsWithOptions(ctx, deleter, opts)
	result := &RootFSGarbageCollectionResult{
		Layers:             layers,
		DeletedObjectKeys:  deletedObjectKeys,
		ExpiredSnapshots:   expiredSnapshots,
		DeletedFilesystems: deletedFilesystems,
	}
	if err != nil {
		return result, err
	}
	return result, nil
}

func (s *PGSandboxStore) DeleteExpiredRootFSSnapshots(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = defaultRootFSObjectDeleteLimit
	}
	if limit > maxRootFSObjectDeleteLimit {
		limit = maxRootFSObjectDeleteLimit
	}
	tag, err := s.pool.Exec(ctx, `
		WITH expired AS (
			SELECT snapshot_id
			FROM manager.rootfs_snapshots
			WHERE expires_at IS NOT NULL
				AND expires_at <= NOW()
				AND ($1 = '' OR team_id = $1)
			ORDER BY expires_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM manager.rootfs_snapshots s
		USING expired e
		WHERE s.snapshot_id = e.snapshot_id
	`, strings.TrimSpace(teamID), limit)
	if err != nil {
		return 0, fmt.Errorf("delete expired rootfs snapshots: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PGSandboxStore) DeleteUnreferencedRootFSFilesystems(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = defaultRootFSObjectDeleteLimit
	}
	if limit > maxRootFSObjectDeleteLimit {
		limit = maxRootFSObjectDeleteLimit
	}
	total := 0
	for total < limit {
		deleted, err := s.deleteUnreferencedRootFSFilesystemLeaves(ctx, teamID, limit-total)
		if err != nil {
			return total, err
		}
		total += deleted
		if deleted == 0 {
			break
		}
	}
	return total, nil
}

func (s *PGSandboxStore) deleteUnreferencedRootFSFilesystemLeaves(ctx context.Context, teamID string, limit int) (int, error) {
	tag, err := s.pool.Exec(ctx, `
		WITH RECURSIVE protected AS (
			SELECT f.filesystem_id, f.source_filesystem_id
			FROM manager.rootfs_filesystems f
			WHERE EXISTS (
				SELECT 1
				FROM manager.sandbox_rootfs_bindings b
				WHERE b.filesystem_id = f.filesystem_id
			)
			OR EXISTS (
				SELECT 1
				FROM manager.rootfs_snapshots s
				WHERE s.filesystem_id = f.filesystem_id
					AND (s.expires_at IS NULL OR s.expires_at > NOW())
			)
		),
		protected_tree AS (
			SELECT filesystem_id, source_filesystem_id
			FROM protected
			UNION
			SELECT parent.filesystem_id, parent.source_filesystem_id
			FROM manager.rootfs_filesystems parent
			JOIN protected_tree child ON child.source_filesystem_id = parent.filesystem_id
		),
		candidates AS (
			SELECT f.filesystem_id
			FROM manager.rootfs_filesystems f
			WHERE ($1 = '' OR f.team_id = $1)
				AND NOT EXISTS (
					SELECT 1
					FROM protected_tree p
					WHERE p.filesystem_id = f.filesystem_id
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_filesystems child
					WHERE child.source_filesystem_id = f.filesystem_id
				)
			ORDER BY f.updated_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		DELETE FROM manager.rootfs_filesystems f
		USING candidates c
		WHERE f.filesystem_id = c.filesystem_id
	`, strings.TrimSpace(teamID), limit)
	if err != nil {
		return 0, fmt.Errorf("delete unreferenced rootfs filesystems: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PGSandboxStore) ListRootFSStorageUsage(ctx context.Context, teamID string) ([]RootFSStorageUsage, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	observedAt := time.Now().UTC()
	rows, err := s.pool.Query(ctx, `
		WITH RECURSIVE roots AS (
			SELECT f.head_layer_id AS layer_id
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			WHERE f.head_layer_id IS NOT NULL
			UNION
			SELECT head_layer_id AS layer_id
			FROM manager.rootfs_snapshots
			WHERE head_layer_id IS NOT NULL
				AND (expires_at IS NULL OR expires_at > NOW())
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
		reachable_objects AS (
			SELECT l.team_id, l.diff_object_key, MAX(l.diff_size) AS diff_size
			FROM manager.rootfs_layers l
			JOIN reachable r ON r.layer_id = l.layer_id
			WHERE l.diff_object_key <> ''
				AND ($1 = '' OR l.team_id = $1)
			GROUP BY l.team_id, l.diff_object_key
		),
		known_teams AS (
			SELECT DISTINCT team_id
			FROM manager.rootfs_objects
			WHERE team_id <> ''
				AND ($1 = '' OR team_id = $1)
			UNION
			SELECT DISTINCT team_id
			FROM reachable_objects
			WHERE team_id <> ''
			UNION
			SELECT $1
			WHERE $1 <> ''
		)
		SELECT
			known_teams.team_id,
			COUNT(reachable_objects.diff_object_key) AS object_count,
			COALESCE(SUM(reachable_objects.diff_size), 0) AS storage_bytes
		FROM known_teams
		LEFT JOIN reachable_objects ON reachable_objects.team_id = known_teams.team_id
		GROUP BY known_teams.team_id
		ORDER BY known_teams.team_id ASC
	`, strings.TrimSpace(teamID))
	if err != nil {
		return nil, fmt.Errorf("list rootfs storage usage: %w", err)
	}
	defer rows.Close()

	var usages []RootFSStorageUsage
	for rows.Next() {
		var usage RootFSStorageUsage
		if err := rows.Scan(&usage.TeamID, &usage.ObjectCount, &usage.StorageBytes); err != nil {
			return nil, err
		}
		usage.ObservedAt = observedAt
		usages = append(usages, usage)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs storage usage: %w", err)
	}
	return usages, nil
}

func (s *PGSandboxStore) RecordRootFSStorageObservations(ctx context.Context, recorder RootFSStorageMeteringRecorder, teamID string, observedAt time.Time) ([]RootFSStorageUsage, error) {
	if recorder == nil {
		return nil, nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	} else {
		observedAt = observedAt.UTC()
	}
	usages, err := s.ListRootFSStorageUsage(ctx, teamID)
	if err != nil {
		return nil, err
	}
	for i := range usages {
		usages[i].ObservedAt = observedAt
		if err := recorder.RecordStorageObservation(ctx, &meteringpkg.StorageObservation{
			SubjectType: meteringpkg.SubjectTypeRootFS,
			SubjectID:   usages[i].TeamID,
			Product:     meteringpkg.ProductSandbox,
			TeamID:      usages[i].TeamID,
			SizeBytes:   usages[i].StorageBytes,
			ObservedAt:  observedAt,
		}); err != nil {
			return usages, fmt.Errorf("record rootfs storage observation for team %q: %w", usages[i].TeamID, err)
		}
	}
	return usages, nil
}

func (s *PGSandboxStore) AuditRootFSObjects(ctx context.Context, inspector RootFSObjectInspector, teamID string, limit int) (*RootFSObjectAuditResult, error) {
	if s == nil || s.pool == nil || inspector == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = defaultRootFSObjectDeleteLimit
	}
	if limit > maxRootFSObjectDeleteLimit {
		limit = maxRootFSObjectDeleteLimit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT object_key, diff_size
		FROM manager.rootfs_objects
		WHERE deleted_at IS NULL
			AND ($1 = '' OR team_id = $1)
		ORDER BY last_referenced_at ASC, object_key ASC
		LIMIT $2
	`, strings.TrimSpace(teamID), limit)
	if err != nil {
		return nil, fmt.Errorf("list rootfs objects for audit: %w", err)
	}
	defer rows.Close()

	type auditCandidate struct {
		objectKey string
		diffSize  int64
	}
	var candidates []auditCandidate
	for rows.Next() {
		var candidate auditCandidate
		if err := rows.Scan(&candidate.objectKey, &candidate.diffSize); err != nil {
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs object audit candidates: %w", err)
	}

	result := &RootFSObjectAuditResult{}
	for _, candidate := range candidates {
		result.Checked++
		info, err := inspector.StatRootFSObject(candidate.objectKey)
		if err != nil {
			result.Missing++
			if updateErr := s.recordRootFSObjectAuditError(ctx, candidate.objectKey, err); updateErr != nil {
				return result, updateErr
			}
			continue
		}
		if candidate.diffSize > 0 && info.Size > 0 && info.Size != candidate.diffSize {
			result.SizeMismatched++
			if updateErr := s.recordRootFSObjectAuditError(ctx, candidate.objectKey, fmt.Errorf("object size %d does not match db size %d", info.Size, candidate.diffSize)); updateErr != nil {
				return result, updateErr
			}
			continue
		}
		if err := s.clearRootFSObjectAuditError(ctx, candidate.objectKey); err != nil {
			return result, err
		}
	}
	return result, nil
}

func (s *PGSandboxStore) recordRootFSObjectAuditError(ctx context.Context, objectKey string, auditErr error) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_objects
		SET missing_at = COALESCE(missing_at, NOW()),
			last_error = $2,
			updated_at = NOW()
		WHERE object_key = $1
	`, objectKey, truncateRootFSError(auditErr.Error()))
	if err != nil {
		return fmt.Errorf("record rootfs object audit error for %q: %w", objectKey, err)
	}
	return nil
}

func (s *PGSandboxStore) clearRootFSObjectAuditError(ctx context.Context, objectKey string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_objects
		SET missing_at = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE object_key = $1
	`, objectKey)
	if err != nil {
		return fmt.Errorf("clear rootfs object audit error for %q: %w", objectKey, err)
	}
	return nil
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
			SELECT f.head_layer_id AS layer_id
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			WHERE f.head_layer_id IS NOT NULL
			UNION
			SELECT head_layer_id AS layer_id
			FROM manager.rootfs_snapshots
			WHERE head_layer_id IS NOT NULL
				AND (expires_at IS NULL OR expires_at > NOW())
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
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_layers ref
					WHERE ref.diff_object_key = l.diff_object_key
						AND NOT EXISTS (
							SELECT 1
							FROM candidates candidate_ref
							WHERE candidate_ref.layer_id = ref.layer_id
						)
				)
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
		referenced, err := s.rootFSObjectHasLayerReferences(ctx, item.ObjectKey)
		if err != nil {
			return deleted, err
		}
		if referenced {
			if err := s.clearRootFSObjectDeletion(ctx, item.ObjectKey, opts.ClaimedBy); err != nil {
				return deleted, err
			}
			continue
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
		if err := s.markRootFSObjectDeleted(ctx, item.ObjectKey); err != nil {
			return deleted, err
		}
		if err := s.clearRootFSObjectDeletion(ctx, item.ObjectKey, opts.ClaimedBy); err != nil {
			return deleted, err
		}
		deleted = append(deleted, item.ObjectKey)
	}
	return deleted, errors.Join(errs...)
}

func (s *PGSandboxStore) rootFSObjectHasLayerReferences(ctx context.Context, objectKey string) (bool, error) {
	var referenced bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.rootfs_layers
			WHERE diff_object_key = $1
		)
	`, objectKey).Scan(&referenced); err != nil {
		return false, fmt.Errorf("check rootfs object references for %q: %w", objectKey, err)
	}
	return referenced, nil
}

func (s *PGSandboxStore) markRootFSObjectDeleted(ctx context.Context, objectKey string) error {
	if _, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_objects
		SET deleted_at = NOW(),
			missing_at = NULL,
			last_error = '',
			updated_at = NOW()
		WHERE object_key = $1
			AND NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_layers
				WHERE diff_object_key = $1
			)
	`, objectKey); err != nil {
		return fmt.Errorf("mark rootfs object deleted %q: %w", objectKey, err)
	}
	return nil
}

func (s *PGSandboxStore) clearRootFSObjectDeletion(ctx context.Context, objectKey, claimedBy string) error {
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM manager.rootfs_object_deletions
		WHERE object_key = $1
			AND claimed_by = $2
	`, objectKey, claimedBy); err != nil {
		return fmt.Errorf("clear rootfs object deletion %q: %w", objectKey, err)
	}
	return nil
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
				AND (expires_at IS NULL OR expires_at > NOW())
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
