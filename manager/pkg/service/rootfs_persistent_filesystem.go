package service

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfsstore"
	"github.com/sandbox0-ai/sandbox0/pkg/s0fs"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/objectstore"
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

type RootFSGarbageCollectionResult struct {
	Layers               []*SandboxRootFSLayer
	DeletedS0FSSegments  []string
	DeletedS0FSManifests []string
	ExpiredSnapshots     int
	DeletedFilesystems   int
}

// RootFSStorageUsage is the current COW physical storage usage for one team.
// It counts distinct reachable rootfs objects, not sandbox checkpoint-chain sums.
type RootFSStorageUsage struct {
	TeamID       string
	ObjectCount  int64
	StorageBytes int64
	ObservedAt   time.Time
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
	defaultRootFSGCLimit = 100
	maxRootFSGCLimit     = 1000
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

// GarbageCollectRootFSFilesystem removes unreferenced metadata and, when an
// object store is configured, sweeps s0fs objects not referenced by active
// filesystem or snapshot heads.
func (s *PGSandboxStore) GarbageCollectRootFSFilesystem(ctx context.Context, store objectstore.Store, teamID string, limit int) (*RootFSGarbageCollectionResult, error) {
	limit = normalizeRootFSGCLimit(limit)
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
	result := &RootFSGarbageCollectionResult{
		Layers:             layers,
		ExpiredSnapshots:   expiredSnapshots,
		DeletedFilesystems: deletedFilesystems,
	}
	if store == nil {
		return result, nil
	}
	s0fsResult, err := s.GarbageCollectRootFSS0FSObjects(ctx, store, teamID, limit, layers)
	if s0fsResult != nil {
		result.DeletedS0FSSegments = append(result.DeletedS0FSSegments, s0fsResult.DeletedSegments...)
		result.DeletedS0FSManifests = append(result.DeletedS0FSManifests, s0fsResult.DeletedManifests...)
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
	limit = normalizeRootFSGCLimit(limit)
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
	limit = normalizeRootFSGCLimit(limit)
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

func (s *PGSandboxStore) ListRootFSStorageUsage(ctx context.Context, store objectstore.Store, teamID string) ([]RootFSStorageUsage, error) {
	if s == nil || s.pool == nil || store == nil {
		return nil, nil
	}
	observedAt := time.Now().UTC()
	heads, err := s.listRetainedRootFSS0FSHeads(ctx, teamID)
	if err != nil {
		return nil, err
	}
	objectRefs, err := s.rootFSS0FSLiveObjectRefs(ctx, store, heads)
	if err != nil {
		return nil, err
	}
	byTeam := make(map[string]*RootFSStorageUsage)
	for ref := range objectRefs {
		info, err := rootfsstore.S0FSObjectStore(store, ref.TeamID, ref.VolumeID).Head(ref.Key)
		if err != nil {
			return nil, fmt.Errorf("stat rootfs s0fs object %s/%s/%s: %w", ref.TeamID, ref.VolumeID, ref.Key, err)
		}
		usage := byTeam[ref.TeamID]
		if usage == nil {
			usage = &RootFSStorageUsage{TeamID: ref.TeamID, ObservedAt: observedAt}
			byTeam[ref.TeamID] = usage
		}
		usage.ObjectCount++
		usage.StorageBytes += info.Size
	}
	if strings.TrimSpace(teamID) != "" && byTeam[strings.TrimSpace(teamID)] == nil {
		byTeam[strings.TrimSpace(teamID)] = &RootFSStorageUsage{TeamID: strings.TrimSpace(teamID), ObservedAt: observedAt}
	}
	usages := make([]RootFSStorageUsage, 0, len(byTeam))
	for _, usage := range byTeam {
		usages = append(usages, *usage)
	}
	sort.Slice(usages, func(i, j int) bool { return usages[i].TeamID < usages[j].TeamID })
	return usages, nil
}

func (s *PGSandboxStore) RecordRootFSStorageObservations(ctx context.Context, store objectstore.Store, recorder RootFSStorageMeteringRecorder, teamID string, observedAt time.Time) ([]RootFSStorageUsage, error) {
	if recorder == nil {
		return nil, nil
	}
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	} else {
		observedAt = observedAt.UTC()
	}
	usages, err := s.ListRootFSStorageUsage(ctx, store, teamID)
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

func (s *PGSandboxStore) collectUnreferencedRootFSLayers(ctx context.Context, teamID string, limit int) ([]*SandboxRootFSLayer, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	limit = normalizeRootFSGCLimit(limit)
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
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_filesystems f
					WHERE f.head_layer_id = l.layer_id
				)
			ORDER BY l.created_at ASC
				LIMIT $2
				FOR UPDATE SKIP LOCKED
			),
			deleted AS (
				DELETE FROM manager.rootfs_layers l
				USING candidates c
				WHERE l.layer_id = c.layer_id
				RETURNING l.layer_id, l.parent_layer_id, l.source_sandbox_id, l.team_id,
					l.runtime_generation, l.runtime, l.runtime_handler, l.base_image_ref,
					l.base_image_digest, l.snapshotter, l.snapshot_parent,
					l.snapshot_parent_chain, l.storage_engine, l.s0fs_volume_id,
					l.s0fs_manifest_key, l.s0fs_manifest_seq, l.s0fs_checkpoint_seq,
					l.created_at
			)
			SELECT layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
				runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
				snapshot_parent, snapshot_parent_chain, storage_engine, s0fs_volume_id, s0fs_manifest_key,
				s0fs_manifest_seq, s0fs_checkpoint_seq, created_at
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

type rootFSS0FSVolumeRef struct {
	TeamID   string
	VolumeID string
}

type rootFSS0FSHeadRef struct {
	rootFSS0FSVolumeRef
	ManifestKey string
}

type rootFSS0FSObjectRef struct {
	rootFSS0FSVolumeRef
	Key string
}

func (s *PGSandboxStore) GarbageCollectRootFSS0FSObjects(ctx context.Context, store objectstore.Store, teamID string, limit int, touchedLayers []*SandboxRootFSLayer) (*s0fs.GarbageCollectionResult, error) {
	result := &s0fs.GarbageCollectionResult{}
	if s == nil || s.pool == nil || store == nil {
		return result, nil
	}
	limit = normalizeRootFSGCLimit(limit)
	retainedHeads, err := s.listRetainedRootFSS0FSHeads(ctx, teamID)
	if err != nil {
		return result, err
	}
	headsByVolume := make(map[rootFSS0FSVolumeRef][]rootFSS0FSHeadRef)
	volumes := make(map[rootFSS0FSVolumeRef]struct{})
	for _, head := range retainedHeads {
		if head.TeamID == "" || head.VolumeID == "" || head.ManifestKey == "" {
			continue
		}
		volume := head.rootFSS0FSVolumeRef
		volumes[volume] = struct{}{}
		headsByVolume[volume] = append(headsByVolume[volume], head)
	}
	for _, layer := range touchedLayers {
		volume := rootFSS0FSVolumeFromLayer(layer)
		if volume.TeamID == "" || volume.VolumeID == "" {
			continue
		}
		if strings.TrimSpace(teamID) != "" && volume.TeamID != strings.TrimSpace(teamID) {
			continue
		}
		volumes[volume] = struct{}{}
	}
	ordered := rootFSSortedVolumes(volumes)
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	for _, volume := range ordered {
		states, retainedManifests, err := rootFSS0FSRetainedStates(ctx, store, volume, headsByVolume[volume])
		if err != nil {
			return result, err
		}
		materializer := rootFSS0FSMaterializer(store, volume.TeamID, volume.VolumeID)
		plan, err := materializer.PlanGarbageCollection(ctx, states, retainedManifests)
		if err != nil {
			return result, err
		}
		remaining := limit - len(result.DeletedSegments) - len(result.DeletedManifests)
		if remaining <= 0 {
			break
		}
		trimRootFSS0FSGarbageCollectionPlan(plan, remaining)
		if len(plan.Segments) == 0 && len(plan.Manifests) == 0 {
			continue
		}
		applied, err := plan.Apply(ctx)
		if applied != nil {
			for _, key := range applied.DeletedSegments {
				result.DeletedSegments = append(result.DeletedSegments, rootFSS0FSQualifiedKey(volume, key))
			}
			for _, key := range applied.DeletedManifests {
				result.DeletedManifests = append(result.DeletedManifests, rootFSS0FSQualifiedKey(volume, key))
			}
		}
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func trimRootFSS0FSGarbageCollectionPlan(plan *s0fs.GarbageCollectionPlan, limit int) {
	if plan == nil || limit < 0 {
		return
	}
	if limit == 0 {
		plan.Segments = nil
		plan.Manifests = nil
		return
	}
	if len(plan.Segments) >= limit {
		plan.Segments = plan.Segments[:limit]
		plan.Manifests = nil
		return
	}
	remaining := limit - len(plan.Segments)
	if len(plan.Manifests) > remaining {
		plan.Manifests = plan.Manifests[:remaining]
	}
}

func (s *PGSandboxStore) listRetainedRootFSS0FSHeads(ctx context.Context, teamID string) ([]rootFSS0FSHeadRef, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		WITH refs AS (
			SELECT l.team_id, l.s0fs_volume_id, l.s0fs_manifest_key
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			JOIN manager.rootfs_layers l ON l.layer_id = f.head_layer_id
			WHERE f.head_layer_id IS NOT NULL
			UNION
			SELECT l.team_id, l.s0fs_volume_id, l.s0fs_manifest_key
			FROM manager.rootfs_snapshots s
			JOIN manager.rootfs_layers l ON l.layer_id = s.head_layer_id
			WHERE s.head_layer_id IS NOT NULL
				AND (s.expires_at IS NULL OR s.expires_at > NOW())
		)
		SELECT DISTINCT team_id, s0fs_volume_id, s0fs_manifest_key
		FROM refs
		WHERE team_id <> ''
			AND s0fs_volume_id <> ''
			AND s0fs_manifest_key <> ''
			AND ($1 = '' OR team_id = $1)
		ORDER BY team_id ASC, s0fs_volume_id ASC, s0fs_manifest_key ASC
	`, strings.TrimSpace(teamID))
	if err != nil {
		return nil, fmt.Errorf("list retained rootfs s0fs heads: %w", err)
	}
	defer rows.Close()

	var refs []rootFSS0FSHeadRef
	for rows.Next() {
		var ref rootFSS0FSHeadRef
		if err := rows.Scan(&ref.TeamID, &ref.VolumeID, &ref.ManifestKey); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate retained rootfs s0fs heads: %w", err)
	}
	return refs, nil
}

func (s *PGSandboxStore) rootFSS0FSLiveObjectRefs(ctx context.Context, store objectstore.Store, heads []rootFSS0FSHeadRef) (map[rootFSS0FSObjectRef]struct{}, error) {
	refs := make(map[rootFSS0FSObjectRef]struct{})
	for _, head := range heads {
		if head.TeamID == "" || head.VolumeID == "" || head.ManifestKey == "" {
			continue
		}
		manifest, err := rootFSS0FSMaterializer(store, head.TeamID, head.VolumeID).LoadManifestByKey(ctx, head.ManifestKey)
		if err != nil {
			return nil, fmt.Errorf("load retained rootfs s0fs manifest %s/%s/%s: %w", head.TeamID, head.VolumeID, head.ManifestKey, err)
		}
		refs[rootFSS0FSObjectRef{rootFSS0FSVolumeRef: head.rootFSS0FSVolumeRef, Key: head.ManifestKey}] = struct{}{}
		refs[rootFSS0FSObjectRef{rootFSS0FSVolumeRef: head.rootFSS0FSVolumeRef, Key: "manifests/latest.json"}] = struct{}{}
		for ref := range rootFSS0FSSegmentObjectRefs(head.rootFSS0FSVolumeRef, manifest.State) {
			refs[ref] = struct{}{}
		}
	}
	return refs, nil
}

func rootFSS0FSRetainedStates(ctx context.Context, store objectstore.Store, volume rootFSS0FSVolumeRef, heads []rootFSS0FSHeadRef) ([]*s0fs.SnapshotState, map[string]struct{}, error) {
	retainedManifests := make(map[string]struct{}, len(heads)+1)
	states := make([]*s0fs.SnapshotState, 0, len(heads))
	materializer := rootFSS0FSMaterializer(store, volume.TeamID, volume.VolumeID)
	for _, head := range heads {
		manifest, err := materializer.LoadManifestByKey(ctx, head.ManifestKey)
		if err != nil {
			return nil, nil, fmt.Errorf("load retained rootfs s0fs manifest %s/%s/%s: %w", head.TeamID, head.VolumeID, head.ManifestKey, err)
		}
		states = append(states, manifest.State)
		retainedManifests[head.ManifestKey] = struct{}{}
	}
	if len(states) > 0 {
		retainedManifests["manifests/latest.json"] = struct{}{}
	}
	return states, retainedManifests, nil
}

func rootFSS0FSSegmentObjectRefs(volume rootFSS0FSVolumeRef, state *s0fs.SnapshotState) map[rootFSS0FSObjectRef]struct{} {
	refs := make(map[rootFSS0FSObjectRef]struct{})
	if state == nil {
		return refs
	}
	for _, extents := range state.ColdFiles {
		for _, extent := range extents {
			if strings.TrimSpace(extent.SegmentID) == "" {
				continue
			}
			segment := state.Segments[extent.SegmentID]
			if segment == nil || strings.TrimSpace(segment.Key) == "" {
				continue
			}
			segmentVolume := strings.TrimSpace(segment.VolumeID)
			if segmentVolume == "" {
				segmentVolume = volume.VolumeID
			}
			refs[rootFSS0FSObjectRef{
				rootFSS0FSVolumeRef: rootFSS0FSVolumeRef{TeamID: volume.TeamID, VolumeID: segmentVolume},
				Key:                 strings.TrimSpace(segment.Key),
			}] = struct{}{}
		}
	}
	return refs
}

func rootFSS0FSMaterializer(store objectstore.Store, teamID, volumeID string) *s0fs.Materializer {
	return s0fs.NewMaterializer(
		volumeID,
		rootfsstore.S0FSObjectStore(store, teamID, volumeID),
		nil,
		func(otherVolumeID string) (objectstore.Store, error) {
			return rootfsstore.S0FSObjectStore(store, teamID, otherVolumeID), nil
		},
	)
}

func rootFSS0FSVolumeFromLayer(layer *SandboxRootFSLayer) rootFSS0FSVolumeRef {
	if layer == nil || rootFSStorageEngine(layer.StorageEngine) != "s0fs" {
		return rootFSS0FSVolumeRef{}
	}
	return rootFSS0FSVolumeRef{
		TeamID:   strings.TrimSpace(layer.TeamID),
		VolumeID: strings.TrimSpace(layer.S0FSVolumeID),
	}
}

func rootFSSortedVolumes(volumes map[rootFSS0FSVolumeRef]struct{}) []rootFSS0FSVolumeRef {
	out := make([]rootFSS0FSVolumeRef, 0, len(volumes))
	for volume := range volumes {
		out = append(out, volume)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].TeamID != out[j].TeamID {
			return out[i].TeamID < out[j].TeamID
		}
		return out[i].VolumeID < out[j].VolumeID
	})
	return out
}

func rootFSS0FSQualifiedKey(volume rootFSS0FSVolumeRef, key string) string {
	return strings.Trim(volume.TeamID+"/"+volume.VolumeID+"/"+strings.TrimLeft(key, "/"), "/")
}

func normalizeRootFSGCLimit(limit int) int {
	if limit <= 0 {
		return defaultRootFSGCLimit
	}
	if limit > maxRootFSGCLimit {
		return maxRootFSGCLimit
	}
	return limit
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
