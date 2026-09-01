package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/retryqueue"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

var ErrRootFSHeadConflict = errors.New("rootfs filesystem head conflict")
var ErrRootFSFilesystemNotFound = errors.New("rootfs filesystem not found")
var ErrRootFSFilesystemConflict = errors.New("rootfs filesystem conflict")
var ErrRootFSSnapshotNotFound = errors.New("rootfs snapshot not found")

// RootFSFilesystem is the canonical block-COW filesystem backing one sandbox.
// Source image fields are derived from its immutable artifact and generation;
// PostgreSQL does not maintain duplicate OCI-layer state.
type RootFSFilesystem struct {
	ID                 string
	TeamID             string
	SourceFilesystemID string
	HeadGenerationID   string
	WriterEpoch        int64
	BaseArtifactDigest string
	FormatGeneration   int
	BaseImageRef       string
	BaseImageDigest    string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// RootFSSnapshot is an immutable pointer to one durable block-COW generation.
type RootFSSnapshot struct {
	ID                 string
	FilesystemID       string
	TeamID             string
	SourceSandboxID    string
	HeadGenerationID   string
	BaseArtifactDigest string
	FormatGeneration   int
	SourceOCIDigest    string
	Name               string
	Description        string
	CreatedAt          time.Time
	ExpiresAt          time.Time
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
	SandboxID         string
	SnapshotID        string
	TeamID            string
	OperationID       string
	RollbackExpiresAt time.Time
}

// RootFSHeadRollback retains the old immutable generation for a bounded
// restore or rebase rollback. It never grants write authority by itself.
type RootFSHeadRollback struct {
	OperationID       string
	FilesystemID      string
	SandboxID         string
	TeamID            string
	OperationKind     string
	OldGenerationID   string
	NewGenerationID   string
	HealthCheckDigest []byte
	State             string
	CreatedAt         time.Time
	ExpiresAt         time.Time
	RolledBackAt      time.Time
}

type RollbackRootFSHeadRequest struct {
	SandboxID   string
	OperationID string
	TeamID      string
}

type RootFSGarbageCollectionResult struct {
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

// RootFSStorageUsage is current materialized block-COW usage attributed to one
// team. Shared objects are deduplicated within a team.
type RootFSStorageUsage struct {
	TeamID       string
	ObjectCount  int64
	StorageBytes int64
	ObservedAt   time.Time
}

type RootFSObjectInfo struct {
	Key           string
	Size          int64
	SizeIsLogical bool
	Modified      time.Time
}

type RootFSObjectAuditResult struct {
	Checked        int
	Missing        int
	SizeMismatched int
}

// RootFSObjectDeleter deletes immutable block packs and mapping pages.
type RootFSObjectDeleter interface {
	Delete(key string) error
}

type RootFSObjectInspector interface {
	StatRootFSObject(key string) (RootFSObjectInfo, error)
}

type RootFSStorageMeteringRecorder interface {
	RecordStorageObservation(context.Context, *meteringpkg.StorageObservation) error
}

// ConfiguredRootFSStorageMeteringRecorder rejects nil and typed-nil recorders.
func ConfiguredRootFSStorageMeteringRecorder(recorder RootFSStorageMeteringRecorder) (RootFSStorageMeteringRecorder, bool) {
	if recorder == nil {
		return nil, false
	}
	value := reflect.ValueOf(recorder)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if value.IsNil() {
			return nil, false
		}
	}
	return recorder, true
}

type rootFSStoreDB interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

const (
	defaultRootFSObjectDeleteLimit       = 100
	MaxRootFSObjectDeleteLimit           = 1000
	DefaultRootFSObjectDeleteClaimTTL    = 2 * time.Minute
	DefaultRootFSObjectDeleteBackoffBase = 5 * time.Second
	DefaultRootFSObjectDeleteBackoffMax  = 10 * time.Minute
)

func (s *PGSandboxStore) GetRootFSFilesystem(ctx context.Context, sandboxID string) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(sandboxID) == "" {
		return nil, nil
	}
	filesystem, err := scanRootFSFilesystem(s.pool.QueryRow(ctx, rootFSFilesystemSelectSQL()+`
		WHERE binding.sandbox_id = $1
	`, strings.TrimSpace(sandboxID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs filesystem: %w", err)
	}
	return filesystem, nil
}

func (s *PGSandboxStore) CreateRootFSSnapshot(ctx context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var snapshot *RootFSSnapshot
	err := s.WithSandboxLock(ctx, strings.TrimSpace(req.SandboxID), func(lockCtx context.Context, locked SandboxStoreTx, _ *SandboxRecord) error {
		txStore, ok := locked.(sandboxStoreTx)
		if !ok {
			return fmt.Errorf("rootfs snapshot requires a PostgreSQL transaction")
		}
		var createErr error
		snapshot, createErr = createRootFSSnapshot(lockCtx, txStore.tx, req)
		return createErr
	})
	return snapshot, err
}

func (t sandboxStoreTx) CreateRootFSSnapshot(ctx context.Context, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	return createRootFSSnapshot(ctx, t.tx, req)
}

func createRootFSSnapshot(ctx context.Context, db rootFSStoreDB, req *CreateRootFSSnapshotRequest) (*RootFSSnapshot, error) {
	if db == nil || req == nil {
		return nil, nil
	}
	sandboxID := strings.TrimSpace(req.SandboxID)
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if sandboxID == "" || snapshotID == "" {
		return nil, fmt.Errorf("sandbox_id and snapshot_id are required")
	}
	snapshot, err := scanRootFSSnapshot(db.QueryRow(ctx, `
		WITH source AS (
			SELECT binding.filesystem_id, binding.team_id, filesystem.head_generation_id,
				generation.base_artifact_digest, generation.format_generation,
				generation.source_oci_digest
			FROM manager.sandbox_rootfs_bindings binding
			JOIN manager.rootfs_filesystems filesystem
				ON filesystem.filesystem_id = binding.filesystem_id
			JOIN manager.sandboxes sandbox ON sandbox.sandbox_id = binding.sandbox_id
			JOIN manager.rootfs_generations generation
				ON generation.generation_id = filesystem.head_generation_id
			WHERE binding.sandbox_id = $1
				AND sandbox.desired_state = 'paused'
				AND sandbox.deleted_at IS NULL
				AND generation.durability_state IN ('composite_durable', 's3_materialized')
				AND NOT EXISTS (
					SELECT 1 FROM manager.rootfs_writer_grants writer
					WHERE writer.filesystem_id = filesystem.filesystem_id
						AND writer.state IN ('issued', 'consumed', 'retiring')
				)
				AND NOT EXISTS (
					SELECT 1 FROM manager.sandbox_lifecycle_txns lifecycle
					WHERE lifecycle.sandbox_id = binding.sandbox_id
						AND lifecycle.phase NOT IN ('committed', 'aborted')
				)
		),
		inserted AS (
			INSERT INTO manager.rootfs_snapshots (
				snapshot_id, filesystem_id, team_id, source_sandbox_id,
				head_generation_id, name, description, created_at, expires_at
			)
			SELECT $2, filesystem_id, team_id, $1, head_generation_id,
				$3, $4, NOW(), $5
			FROM source
			RETURNING snapshot_id, filesystem_id, team_id, source_sandbox_id,
				head_generation_id, name, description, created_at, expires_at
		)
		SELECT inserted.snapshot_id, inserted.filesystem_id, inserted.team_id,
			inserted.source_sandbox_id, inserted.head_generation_id,
			source.base_artifact_digest, source.format_generation,
			source.source_oci_digest, inserted.name, inserted.description,
			inserted.created_at, inserted.expires_at
		FROM inserted
		JOIN source ON source.filesystem_id = inserted.filesystem_id
	`, sandboxID, snapshotID, req.Name, req.Description, nullableTime(req.ExpiresAt)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: sandbox %s is not paused at a durable generation", ErrRootFSFilesystemNotFound, sandboxID)
	}
	if err != nil {
		return nil, fmt.Errorf("create rootfs snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *PGSandboxStore) ListRootFSSnapshots(ctx context.Context, req *ListRootFSSnapshotsRequest) ([]*RootFSSnapshot, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, rootFSSnapshotSelectSQL()+`
		WHERE snapshot.source_sandbox_id = $1
			AND ($2 = '' OR snapshot.team_id = $2)
			AND (snapshot.expires_at IS NULL OR snapshot.expires_at > NOW())
		ORDER BY snapshot.created_at DESC
	`, strings.TrimSpace(req.SandboxID), strings.TrimSpace(req.TeamID))
	if err != nil {
		return nil, fmt.Errorf("list rootfs snapshots: %w", err)
	}
	defer rows.Close()
	var snapshots []*RootFSSnapshot
	for rows.Next() {
		snapshot, scanErr := scanRootFSSnapshot(rows)
		if scanErr != nil {
			return nil, scanErr
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
	snapshot, err := scanRootFSSnapshot(s.pool.QueryRow(ctx, rootFSSnapshotSelectSQL()+`
		WHERE snapshot.snapshot_id = $1
			AND ($2 = '' OR snapshot.team_id = $2)
			AND (snapshot.expires_at IS NULL OR snapshot.expires_at > NOW())
	`, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, snapshotID)
	}
	if err != nil {
		return nil, fmt.Errorf("get rootfs snapshot: %w", err)
	}
	return snapshot, nil
}

func (s *PGSandboxStore) DeleteRootFSSnapshot(ctx context.Context, snapshotID, teamID string) error {
	if s == nil || s.pool == nil {
		return nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs snapshot deletion: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1 AND ($2 = '' OR team_id = $2)
	`, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID))
	if err != nil {
		return fmt.Errorf("delete rootfs snapshot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, snapshotID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_running_template_captures
		SET cancel_reason = 'snapshot deleted', updated_at = NOW()
		WHERE snapshot_id = $1 AND ($2 = '' OR team_id = $2)
			AND state = 'published' AND cancel_reason = ''
	`, strings.TrimSpace(snapshotID), strings.TrimSpace(teamID)); err != nil {
		return fmt.Errorf("release running rootfs snapshot capture: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs snapshot deletion: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) ForkRootFSFilesystem(ctx context.Context, req *ForkRootFSFilesystemRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var filesystem *RootFSFilesystem
	err := s.WithSandboxLock(ctx, strings.TrimSpace(req.SourceSandboxID), func(lockCtx context.Context, locked SandboxStoreTx, _ *SandboxRecord) error {
		txStore, ok := locked.(sandboxStoreTx)
		if !ok {
			return fmt.Errorf("rootfs fork requires a PostgreSQL transaction")
		}
		var forkErr error
		filesystem, forkErr = forkRootFSFilesystem(lockCtx, txStore.tx, req)
		return forkErr
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, s.rootFSForkNoRowsError(ctx, req)
	}
	return filesystem, err
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
	sourceSandboxID := strings.TrimSpace(req.SourceSandboxID)
	targetSandboxID := strings.TrimSpace(req.TargetSandboxID)
	if sourceSandboxID == "" || targetSandboxID == "" {
		return nil, fmt.Errorf("source_sandbox_id and target_sandbox_id are required")
	}
	if sourceSandboxID == targetSandboxID {
		return nil, fmt.Errorf("%w: source and target sandbox are the same", ErrRootFSFilesystemConflict)
	}
	filesystem, err := scanRootFSFilesystem(db.QueryRow(ctx, `
		WITH source AS (
			SELECT filesystem.filesystem_id, filesystem.team_id,
				filesystem.head_generation_id, filesystem.base_artifact_digest,
				filesystem.format_generation, generation.writer_epoch
			FROM manager.sandbox_rootfs_bindings binding
			JOIN manager.rootfs_filesystems filesystem
				ON filesystem.filesystem_id = binding.filesystem_id
			JOIN manager.sandboxes sandbox ON sandbox.sandbox_id = binding.sandbox_id
			JOIN manager.rootfs_generations generation
				ON generation.generation_id = filesystem.head_generation_id
			WHERE binding.sandbox_id = $1
				AND sandbox.desired_state = 'paused'
				AND sandbox.deleted_at IS NULL
				AND generation.durability_state IN ('composite_durable', 's3_materialized')
				AND NOT EXISTS (
					SELECT 1 FROM manager.rootfs_writer_grants writer
					WHERE writer.filesystem_id = filesystem.filesystem_id
						AND writer.state IN ('issued', 'consumed', 'retiring')
				)
				AND NOT EXISTS (
					SELECT 1 FROM manager.sandbox_lifecycle_txns lifecycle
					WHERE lifecycle.sandbox_id = binding.sandbox_id
						AND lifecycle.phase NOT IN ('committed', 'aborted')
				)
		),
		target AS (
			SELECT sandbox_id, team_id
			FROM manager.sandboxes
			WHERE sandbox_id = $2 AND desired_state = 'paused' AND deleted_at IS NULL
				AND ($3 = '' OR team_id = $3)
		),
		created AS (
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, source_filesystem_id, head_generation_id,
				writer_epoch, base_artifact_digest, format_generation,
				created_at, updated_at
			)
			SELECT $2, target.team_id, source.filesystem_id, source.head_generation_id,
				source.writer_epoch, source.base_artifact_digest, source.format_generation,
				NOW(), NOW()
			FROM source CROSS JOIN target
			WHERE source.team_id = target.team_id
			ON CONFLICT (filesystem_id) DO NOTHING
			RETURNING *
		),
		bound AS (
			INSERT INTO manager.sandbox_rootfs_bindings (
				sandbox_id, filesystem_id, team_id, created_at, updated_at
			)
			SELECT $2, filesystem_id, team_id, NOW(), NOW() FROM created
			ON CONFLICT (sandbox_id) DO NOTHING
			RETURNING filesystem_id
		)
		SELECT created.filesystem_id, created.team_id, created.source_filesystem_id,
			created.writer_epoch, created.head_generation_id,
			created.base_artifact_digest, created.format_generation,
			artifact.source_oci_ref, generation.source_oci_digest,
			created.created_at, created.updated_at
		FROM created
		JOIN bound ON bound.filesystem_id = created.filesystem_id
		JOIN manager.rootfs_generations generation
			ON generation.generation_id = created.head_generation_id
		JOIN manager.rootfs_base_artifacts artifact
			ON artifact.artifact_digest = created.base_artifact_digest
	`, sourceSandboxID, targetSandboxID, strings.TrimSpace(req.TargetTeamID)))
	if err != nil {
		return nil, fmt.Errorf("fork rootfs filesystem: %w", err)
	}
	return filesystem, nil
}

func (s *PGSandboxStore) RestoreRootFSFromSnapshot(ctx context.Context, req *RestoreRootFSFromSnapshotRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	var filesystem *RootFSFilesystem
	err := s.WithSandboxLock(ctx, strings.TrimSpace(req.SandboxID), func(lockCtx context.Context, locked SandboxStoreTx, _ *SandboxRecord) error {
		txStore, ok := locked.(sandboxStoreTx)
		if !ok {
			return fmt.Errorf("rootfs restore requires a PostgreSQL transaction")
		}
		var restoreErr error
		filesystem, restoreErr = restoreRootFSFromSnapshot(lockCtx, txStore.tx, req)
		return restoreErr
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, s.rootFSRestoreNoRowsError(ctx, req)
	}
	return filesystem, err
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
	sandboxID := strings.TrimSpace(req.SandboxID)
	snapshotID := strings.TrimSpace(req.SnapshotID)
	if sandboxID == "" || snapshotID == "" {
		return nil, fmt.Errorf("sandbox_id and snapshot_id are required")
	}
	filesystem, err := scanRootFSFilesystem(db.QueryRow(ctx, `
		WITH snapshot AS (
			SELECT snapshot.snapshot_id, snapshot.filesystem_id, snapshot.team_id,
				snapshot.head_generation_id, generation.base_artifact_digest,
				generation.format_generation, generation.writer_epoch
			FROM manager.rootfs_snapshots snapshot
			JOIN manager.rootfs_generations generation
				ON generation.generation_id = snapshot.head_generation_id
			WHERE snapshot.snapshot_id = $2
				AND ($3 = '' OR snapshot.team_id = $3)
				AND (snapshot.expires_at IS NULL OR snapshot.expires_at > NOW())
				AND generation.durability_state IN ('composite_durable', 's3_materialized')
		),
		target AS (
			SELECT sandbox_id, team_id
			FROM manager.sandboxes
			WHERE sandbox_id = $1 AND desired_state = 'paused' AND deleted_at IS NULL
				AND ($3 = '' OR team_id = $3)
		),
		binding AS (
			SELECT filesystem_id FROM manager.sandbox_rootfs_bindings WHERE sandbox_id = $1
			UNION ALL
			SELECT $1 WHERE NOT EXISTS (
				SELECT 1 FROM manager.sandbox_rootfs_bindings WHERE sandbox_id = $1
			)
			LIMIT 1
		),
		previous AS (
			SELECT filesystem.filesystem_id, filesystem.head_generation_id
			FROM binding
			JOIN manager.rootfs_filesystems filesystem
				ON filesystem.filesystem_id = binding.filesystem_id
		),
		restored AS (
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, source_filesystem_id, head_generation_id,
				writer_epoch, base_artifact_digest, format_generation,
				created_at, updated_at
			)
			SELECT binding.filesystem_id, target.team_id, snapshot.filesystem_id,
				snapshot.head_generation_id, snapshot.writer_epoch,
				snapshot.base_artifact_digest, snapshot.format_generation,
				NOW(), NOW()
			FROM snapshot CROSS JOIN target CROSS JOIN binding
			WHERE snapshot.team_id = target.team_id
				AND NOT EXISTS (
					SELECT 1 FROM manager.rootfs_writer_grants writer
					WHERE writer.filesystem_id = binding.filesystem_id
						AND writer.state IN ('issued', 'consumed', 'retiring')
				)
				AND NOT EXISTS (
					SELECT 1 FROM manager.sandbox_lifecycle_txns lifecycle
					WHERE lifecycle.sandbox_id = target.sandbox_id
						AND lifecycle.phase NOT IN ('committed', 'aborted')
				)
			ON CONFLICT (filesystem_id) DO UPDATE SET
				team_id = EXCLUDED.team_id,
				source_filesystem_id = COALESCE(
					manager.rootfs_filesystems.source_filesystem_id,
					EXCLUDED.source_filesystem_id
				),
				head_generation_id = EXCLUDED.head_generation_id,
				base_artifact_digest = EXCLUDED.base_artifact_digest,
				format_generation = EXCLUDED.format_generation,
				updated_at = NOW()
			WHERE manager.rootfs_filesystems.team_id = EXCLUDED.team_id
			RETURNING *
		),
		bound AS (
			INSERT INTO manager.sandbox_rootfs_bindings (
				sandbox_id, filesystem_id, team_id, created_at, updated_at
			)
			SELECT $1, filesystem_id, team_id, NOW(), NOW() FROM restored
			ON CONFLICT (sandbox_id) DO UPDATE SET team_id = EXCLUDED.team_id
			WHERE manager.sandbox_rootfs_bindings.filesystem_id = EXCLUDED.filesystem_id
			RETURNING filesystem_id
		),
		rollback_pin AS (
			INSERT INTO manager.rootfs_head_rollbacks (
				operation_id, filesystem_id, sandbox_id, team_id, operation_kind,
				old_generation_id, new_generation_id, state, created_at, expires_at
			)
			SELECT $4, restored.filesystem_id, $1, restored.team_id, 'restore',
				previous.head_generation_id, restored.head_generation_id,
				'available', NOW(), $5
			FROM restored JOIN previous USING (filesystem_id)
			WHERE $4 <> '' AND previous.head_generation_id IS NOT NULL
				AND previous.head_generation_id <> restored.head_generation_id
			ON CONFLICT (operation_id) DO UPDATE SET operation_id = EXCLUDED.operation_id
			WHERE manager.rootfs_head_rollbacks.filesystem_id = EXCLUDED.filesystem_id
				AND manager.rootfs_head_rollbacks.sandbox_id = EXCLUDED.sandbox_id
				AND manager.rootfs_head_rollbacks.team_id = EXCLUDED.team_id
				AND manager.rootfs_head_rollbacks.operation_kind = EXCLUDED.operation_kind
				AND manager.rootfs_head_rollbacks.old_generation_id = EXCLUDED.old_generation_id
				AND manager.rootfs_head_rollbacks.new_generation_id = EXCLUDED.new_generation_id
				AND manager.rootfs_head_rollbacks.state = 'available'
			RETURNING operation_id
		)
		SELECT restored.filesystem_id, restored.team_id, restored.source_filesystem_id,
			restored.writer_epoch, restored.head_generation_id,
			restored.base_artifact_digest, restored.format_generation,
			artifact.source_oci_ref, generation.source_oci_digest,
			restored.created_at, restored.updated_at
		FROM restored
		JOIN bound ON bound.filesystem_id = restored.filesystem_id
		JOIN manager.rootfs_generations generation
			ON generation.generation_id = restored.head_generation_id
		JOIN manager.rootfs_base_artifacts artifact
			ON artifact.artifact_digest = restored.base_artifact_digest
		LEFT JOIN previous ON previous.filesystem_id = restored.filesystem_id
		LEFT JOIN rollback_pin ON rollback_pin.operation_id = $4
		WHERE $4 = '' OR previous.head_generation_id IS NULL
			OR previous.head_generation_id = restored.head_generation_id
			OR rollback_pin.operation_id = $4
	`, sandboxID, snapshotID, strings.TrimSpace(req.TeamID), strings.TrimSpace(req.OperationID), nullableTime(req.RollbackExpiresAt)))
	if err != nil {
		return nil, fmt.Errorf("restore rootfs filesystem from snapshot: %w", err)
	}
	return filesystem, nil
}

// RollbackRootFSHead atomically restores a retained generation. The sandbox
// must remain paused and writer-free, and the current head must still match the
// operation's published generation.
func (s *PGSandboxStore) RollbackRootFSHead(ctx context.Context, req *RollbackRootFSHeadRequest) (*RootFSFilesystem, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	if strings.TrimSpace(req.SandboxID) == "" || strings.TrimSpace(req.OperationID) == "" {
		return nil, fmt.Errorf("sandbox_id and operation_id are required")
	}
	var filesystem *RootFSFilesystem
	err := s.WithSandboxLock(ctx, strings.TrimSpace(req.SandboxID), func(lockCtx context.Context, locked SandboxStoreTx, _ *SandboxRecord) error {
		txStore, ok := locked.(sandboxStoreTx)
		if !ok {
			return fmt.Errorf("rootfs rollback requires a PostgreSQL transaction")
		}
		var rollbackErr error
		filesystem, rollbackErr = rollbackRootFSHead(lockCtx, txStore.tx, req)
		return rollbackErr
	})
	return filesystem, err
}

func rollbackRootFSHead(ctx context.Context, db rootFSStoreDB, req *RollbackRootFSHeadRequest) (*RootFSFilesystem, error) {
	filesystem, err := scanRootFSFilesystem(db.QueryRow(ctx, `
		WITH pin AS (
			SELECT operation_id, filesystem_id, sandbox_id, team_id,
				old_generation_id, new_generation_id
			FROM manager.rootfs_head_rollbacks
			WHERE operation_id = $2 AND sandbox_id = $1
				AND ($3 = '' OR team_id = $3)
				AND state = 'available'
				AND (expires_at IS NULL OR expires_at > NOW())
			FOR UPDATE
		),
		updated AS (
			UPDATE manager.rootfs_filesystems filesystem
			SET head_generation_id = pin.old_generation_id,
				base_artifact_digest = generation.base_artifact_digest,
				format_generation = generation.format_generation,
				updated_at = NOW()
			FROM pin
			JOIN manager.rootfs_generations generation
				ON generation.generation_id = pin.old_generation_id
			JOIN manager.sandbox_rootfs_bindings binding
				ON binding.filesystem_id = pin.filesystem_id
				AND binding.sandbox_id = pin.sandbox_id
			JOIN manager.sandboxes sandbox ON sandbox.sandbox_id = binding.sandbox_id
			WHERE filesystem.filesystem_id = pin.filesystem_id
				AND filesystem.head_generation_id = pin.new_generation_id
				AND sandbox.desired_state = 'paused' AND sandbox.deleted_at IS NULL
				AND NOT EXISTS (
					SELECT 1 FROM manager.rootfs_writer_grants writer
					WHERE writer.filesystem_id = filesystem.filesystem_id
						AND writer.state IN ('issued', 'consumed', 'retiring')
				)
				AND NOT EXISTS (
					SELECT 1 FROM manager.sandbox_lifecycle_txns lifecycle
					WHERE lifecycle.sandbox_id = pin.sandbox_id
						AND lifecycle.phase NOT IN ('committed', 'aborted')
				)
			RETURNING filesystem.*
		),
		consumed AS (
			UPDATE manager.rootfs_head_rollbacks rollback
			SET state = 'rolled_back', rolled_back_at = NOW()
			FROM updated
			WHERE rollback.operation_id = $2
				AND rollback.filesystem_id = updated.filesystem_id
			RETURNING rollback.operation_id
		)
		SELECT updated.filesystem_id, updated.team_id, updated.source_filesystem_id,
			updated.writer_epoch, updated.head_generation_id,
			updated.base_artifact_digest, updated.format_generation,
			artifact.source_oci_ref, generation.source_oci_digest,
			updated.created_at, updated.updated_at
		FROM updated
		JOIN consumed ON consumed.operation_id = $2
		JOIN manager.rootfs_generations generation
			ON generation.generation_id = updated.head_generation_id
		JOIN manager.rootfs_base_artifacts artifact
			ON artifact.artifact_digest = updated.base_artifact_digest
	`, strings.TrimSpace(req.SandboxID), strings.TrimSpace(req.OperationID), strings.TrimSpace(req.TeamID)))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: rollback operation %s", ErrRootFSHeadConflict, req.OperationID)
	}
	if err != nil {
		return nil, fmt.Errorf("rollback rootfs head: %w", err)
	}
	return filesystem, nil
}

// GarbageCollectRootFSFilesystemWithOptions reconciles block-COW metadata and
// drains the durable object-deletion queue.
func (s *PGSandboxStore) GarbageCollectRootFSFilesystemWithOptions(ctx context.Context, deleter RootFSObjectDeleter, teamID string, limit int, opts DeletePendingRootFSObjectsOptions) (*RootFSGarbageCollectionResult, error) {
	if deleter == nil {
		return nil, fmt.Errorf("rootfs object deleter is required")
	}
	expiredSnapshots, err := s.DeleteExpiredRootFSSnapshots(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	if _, err := s.deleteTerminalRootFSHeadRollbacks(ctx, teamID, limit); err != nil {
		return nil, err
	}
	deletedRunningCaptures, err := s.DeleteReleasedNomadRunningRootFSCaptures(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	deletedFilesystems, err := s.DeleteUnreferencedRootFSFilesystems(ctx, teamID, limit)
	if err != nil {
		return nil, err
	}
	opts.Limit = limit
	deletedObjectKeys, deleteErr := s.DeletePendingRootFSObjectsWithOptions(ctx, deleter, opts)
	result := &RootFSGarbageCollectionResult{
		DeletedObjectKeys:  deletedObjectKeys,
		ExpiredSnapshots:   expiredSnapshots,
		DeletedFilesystems: deletedRunningCaptures + deletedFilesystems,
	}
	return result, deleteErr
}

func (s *PGSandboxStore) DeleteExpiredRootFSSnapshots(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	limit = normalizeRootFSObjectLimit(limit)
	var deleted int
	err := s.pool.QueryRow(ctx, `
		WITH expired AS (
			SELECT snapshot_id FROM manager.rootfs_snapshots
			WHERE expires_at IS NOT NULL AND expires_at <= NOW()
				AND ($1 = '' OR team_id = $1)
			ORDER BY expires_at, snapshot_id
			LIMIT $2 FOR UPDATE SKIP LOCKED
		), deleted AS (
			DELETE FROM manager.rootfs_snapshots snapshot
			USING expired
			WHERE snapshot.snapshot_id = expired.snapshot_id
			RETURNING snapshot.snapshot_id
		), released AS (
			UPDATE manager.rootfs_running_template_captures capture
			SET cancel_reason = 'snapshot expired', updated_at = NOW()
			FROM deleted
			WHERE capture.snapshot_id = deleted.snapshot_id
				AND capture.state = 'published' AND capture.cancel_reason = ''
			RETURNING capture.operation_id
		)
		SELECT COUNT(*) FROM deleted
	`, strings.TrimSpace(teamID), limit).Scan(&deleted)
	if err != nil {
		return 0, fmt.Errorf("delete expired rootfs snapshots: %w", err)
	}
	return deleted, nil
}

func (s *PGSandboxStore) deleteTerminalRootFSHeadRollbacks(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	tag, err := s.pool.Exec(ctx, `
		WITH terminal AS (
			SELECT operation_id FROM manager.rootfs_head_rollbacks
			WHERE ($1 = '' OR team_id = $1)
				AND (state = 'rolled_back' OR (state = 'available' AND expires_at <= NOW()))
			ORDER BY created_at, operation_id
			LIMIT $2 FOR UPDATE SKIP LOCKED
		)
		DELETE FROM manager.rootfs_head_rollbacks rollback
		USING terminal
		WHERE rollback.operation_id = terminal.operation_id
	`, strings.TrimSpace(teamID), normalizeRootFSObjectLimit(limit))
	if err != nil {
		return 0, fmt.Errorf("delete terminal rootfs rollback pins: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PGSandboxStore) DeleteUnreferencedRootFSFilesystems(ctx context.Context, teamID string, limit int) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	limit = normalizeRootFSObjectLimit(limit)
	total := 0
	for total < limit {
		deleted, err := s.deleteUnreferencedRootFSFilesystemLeaves(ctx, strings.TrimSpace(teamID), limit-total)
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
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin unreferenced rootfs filesystem cleanup: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT filesystem.filesystem_id
		FROM manager.rootfs_filesystems filesystem
		WHERE ($1 = '' OR filesystem.team_id = $1)
			AND NOT EXISTS (
				SELECT 1 FROM manager.sandbox_rootfs_bindings binding
				WHERE binding.filesystem_id = filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_snapshots snapshot
				WHERE snapshot.filesystem_id = filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_filesystems child
				WHERE child.source_filesystem_id = filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_generations parent_generation
				JOIN manager.rootfs_generations child_generation
					ON child_generation.parent_generation_id = parent_generation.generation_id
				WHERE parent_generation.filesystem_id = filesystem.filesystem_id
					AND child_generation.filesystem_id <> filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_head_rollbacks rollback
				WHERE rollback.filesystem_id = filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_running_forks fork
				WHERE fork.source_filesystem_id = filesystem.filesystem_id
					OR fork.target_filesystem_id = filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.rootfs_running_template_captures capture
				WHERE capture.source_filesystem_id = filesystem.filesystem_id
					OR capture.target_filesystem_id = filesystem.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1 FROM manager.runtime_slots slot
				WHERE slot.filesystem_id = filesystem.filesystem_id
					OR EXISTS (
						SELECT 1 FROM manager.rootfs_generations generation
						WHERE generation.filesystem_id = filesystem.filesystem_id
							AND generation.generation_id = slot.source_generation_id
					)
			)
		ORDER BY filesystem.updated_at, filesystem.filesystem_id
		LIMIT $2 FOR UPDATE OF filesystem SKIP LOCKED
	`, teamID, limit)
	if err != nil {
		return 0, fmt.Errorf("select unreferenced rootfs filesystems: %w", err)
	}
	var filesystemIDs []string
	for rows.Next() {
		var filesystemID string
		if err := rows.Scan(&filesystemID); err != nil {
			rows.Close()
			return 0, err
		}
		filesystemIDs = append(filesystemIDs, filesystemID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, err
	}
	rows.Close()
	if len(filesystemIDs) == 0 {
		if err := tx.Commit(ctx); err != nil {
			return 0, err
		}
		return 0, nil
	}
	objectRows, err := tx.Query(ctx, `
		SELECT DISTINCT locator.object_key
		FROM manager.rootfs_generation_materialization_objects locator
		JOIN manager.rootfs_generations generation USING (generation_id)
		WHERE generation.filesystem_id = ANY($1::text[])
		ORDER BY locator.object_key
	`, filesystemIDs)
	if err != nil {
		return 0, fmt.Errorf("list unreferenced rootfs materialization objects: %w", err)
	}
	var objectKeys []string
	for objectRows.Next() {
		var objectKey string
		if err := objectRows.Scan(&objectKey); err != nil {
			objectRows.Close()
			return 0, err
		}
		objectKeys = append(objectKeys, objectKey)
	}
	if err := objectRows.Err(); err != nil {
		objectRows.Close()
		return 0, err
	}
	objectRows.Close()
	if _, err := tx.Exec(ctx, `
		UPDATE manager.rootfs_filesystems
		SET head_generation_id = NULL, updated_at = NOW()
		WHERE filesystem_id = ANY($1::text[])
	`, filesystemIDs); err != nil {
		return 0, fmt.Errorf("clear unreferenced rootfs heads: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_generations
		WHERE filesystem_id = ANY($1::text[])
	`, filesystemIDs); err != nil {
		return 0, fmt.Errorf("delete unreferenced rootfs generations: %w", err)
	}
	for _, objectKey := range objectKeys {
		if _, err := releaseUnreferencedRootFSMaterializationObject(ctx, tx, objectKey, teamID); err != nil {
			return 0, err
		}
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_filesystems
		WHERE filesystem_id = ANY($1::text[])
	`, filesystemIDs)
	if err != nil {
		return 0, fmt.Errorf("delete unreferenced rootfs filesystems: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit unreferenced rootfs filesystem cleanup: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

func (s *PGSandboxStore) ListRootFSStorageUsage(ctx context.Context, teamID string) ([]RootFSStorageUsage, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	observedAt := time.Now().UTC()
	rows, err := s.pool.Query(ctx, `
		WITH attributed AS (
			SELECT DISTINCT filesystem.team_id, object.object_key, object.object_size
			FROM manager.rootfs_generation_materialization_objects locator
			JOIN manager.rootfs_generations generation USING (generation_id)
			JOIN manager.rootfs_filesystems filesystem
				ON filesystem.filesystem_id = generation.filesystem_id
			JOIN manager.rootfs_materialization_objects object USING (object_key)
			WHERE object.uploaded_at IS NOT NULL
				AND ($1 = '' OR filesystem.team_id = $1)
		),
		known_teams AS (
			SELECT DISTINCT team_id FROM manager.rootfs_filesystems
			WHERE team_id <> '' AND ($1 = '' OR team_id = $1)
			UNION SELECT $1 WHERE $1 <> ''
		)
		SELECT known_teams.team_id, COUNT(attributed.object_key),
			COALESCE(SUM(attributed.object_size), 0)
		FROM known_teams
		LEFT JOIN attributed ON attributed.team_id = known_teams.team_id
		GROUP BY known_teams.team_id
		ORDER BY known_teams.team_id
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
	recorder, ok := ConfiguredRootFSStorageMeteringRecorder(recorder)
	if !ok {
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
	rows, err := s.pool.Query(ctx, `
		SELECT object.object_key, object.object_size
		FROM manager.rootfs_materialization_objects object
		WHERE object.uploaded_at IS NOT NULL
			AND ($1 = '' OR EXISTS (
				SELECT 1
				FROM manager.rootfs_generation_materialization_objects locator
				JOIN manager.rootfs_generations generation USING (generation_id)
				JOIN manager.rootfs_filesystems filesystem
					ON filesystem.filesystem_id = generation.filesystem_id
				WHERE locator.object_key = object.object_key AND filesystem.team_id = $1
			))
		ORDER BY COALESCE(object.last_audited_at, object.created_at), object.object_key
		LIMIT $2
	`, strings.TrimSpace(teamID), normalizeRootFSObjectLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("list rootfs objects for audit: %w", err)
	}
	type auditCandidate struct {
		objectKey string
		size      int64
	}
	var candidates []auditCandidate
	for rows.Next() {
		var candidate auditCandidate
		if err := rows.Scan(&candidate.objectKey, &candidate.size); err != nil {
			rows.Close()
			return nil, err
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	result := &RootFSObjectAuditResult{}
	for _, candidate := range candidates {
		result.Checked++
		info, statErr := inspector.StatRootFSObject(candidate.objectKey)
		if statErr != nil {
			result.Missing++
			if err := s.recordRootFSObjectAuditError(ctx, candidate.objectKey, statErr); err != nil {
				return result, err
			}
			continue
		}
		if info.Size != candidate.size {
			result.SizeMismatched++
			sizeErr := fmt.Errorf("object size %d does not match catalog size %d", info.Size, candidate.size)
			if err := s.recordRootFSObjectAuditError(ctx, candidate.objectKey, sizeErr); err != nil {
				return result, err
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
		UPDATE manager.rootfs_materialization_objects
		SET missing_at = COALESCE(missing_at, NOW()), last_error = $2,
			last_audited_at = NOW(), updated_at = NOW()
		WHERE object_key = $1
	`, objectKey, truncateRootFSError(auditErr.Error()))
	if err != nil {
		return fmt.Errorf("record rootfs object audit error for %q: %w", objectKey, err)
	}
	return nil
}

func (s *PGSandboxStore) clearRootFSObjectAuditError(ctx context.Context, objectKey string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.rootfs_materialization_objects
		SET missing_at = NULL, last_error = '', last_audited_at = NOW(), updated_at = NOW()
		WHERE object_key = $1
	`, objectKey)
	if err != nil {
		return fmt.Errorf("clear rootfs object audit error for %q: %w", objectKey, err)
	}
	return nil
}

// DeletePendingRootFSObjectsWithOptions deletes due immutable objects. The
// inventory row is removed transactionally before enqueue; if an object is
// registered again before deletion, the queue entry is canceled.
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
		var registered bool
		if err := s.pool.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM manager.rootfs_materialization_objects WHERE object_key = $1
			)
		`, item.ObjectKey).Scan(&registered); err != nil {
			return deleted, fmt.Errorf("check rootfs object registration for %q: %w", item.ObjectKey, err)
		}
		if registered {
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
		if err := s.clearRootFSObjectDeletion(ctx, item.ObjectKey, opts.ClaimedBy); err != nil {
			return deleted, err
		}
		deleted = append(deleted, item.ObjectKey)
	}
	return deleted, errors.Join(errs...)
}

func (s *PGSandboxStore) clearRootFSObjectDeletion(ctx context.Context, objectKey, claimedBy string) error {
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM manager.rootfs_object_deletions
		WHERE object_key = $1 AND claimed_by = $2
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
			COUNT(*) FILTER (WHERE dead_lettered_at IS NULL),
			COUNT(*) FILTER (WHERE dead_lettered_at IS NULL AND next_attempt_at <= NOW()
				AND (claimed_until IS NULL OR claimed_until <= NOW())),
			COUNT(*) FILTER (WHERE dead_lettered_at IS NULL AND claimed_until > NOW()),
			COUNT(*) FILTER (WHERE dead_lettered_at IS NOT NULL),
			MIN(created_at) FILTER (WHERE dead_lettered_at IS NULL)
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
	rows, err := s.pool.Query(ctx, `
		WITH due AS (
			SELECT object_key FROM manager.rootfs_object_deletions
			WHERE dead_lettered_at IS NULL AND next_attempt_at <= NOW()
				AND (claimed_until IS NULL OR claimed_until <= NOW())
			ORDER BY next_attempt_at, updated_at, object_key
			LIMIT $1 FOR UPDATE SKIP LOCKED
		),
		claimed AS (
			UPDATE manager.rootfs_object_deletions queue
			SET claimed_by = $2,
				claimed_until = NOW() + ($3::int * INTERVAL '1 second'),
				updated_at = NOW()
			FROM due WHERE queue.object_key = due.object_key
			RETURNING queue.object_key, queue.attempts
		)
		SELECT object_key, attempts FROM claimed ORDER BY object_key
	`, opts.Limit, opts.ClaimedBy, retryqueue.DurationSeconds(opts.ClaimTTL))
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
		SET attempts = attempts + 1, last_error = $3, last_attempt_at = NOW(),
			next_attempt_at = NOW() + ($4::int * INTERVAL '1 second'),
			claimed_by = '', claimed_until = NULL,
			dead_lettered_at = CASE WHEN $5 THEN NOW() ELSE NULL END,
			updated_at = NOW()
		WHERE object_key = $1 AND claimed_by = $2
	`, item.ObjectKey, opts.ClaimedBy, truncateRootFSError(deleteErr.Error()), retryqueue.DurationSeconds(delay), deadLetter)
	if err != nil {
		return fmt.Errorf("record rootfs object delete failure for %q: %w", item.ObjectKey, err)
	}
	return nil
}

func normalizeRootFSObjectDeletionOptions(opts DeletePendingRootFSObjectsOptions) DeletePendingRootFSObjectsOptions {
	opts.Limit = normalizeRootFSObjectLimit(opts.Limit)
	opts.ClaimedBy = strings.TrimSpace(opts.ClaimedBy)
	if opts.ClaimedBy == "" {
		opts.ClaimedBy = fmt.Sprintf("rootfs-gc-%d", time.Now().UnixNano())
	}
	if opts.ClaimTTL <= 0 {
		opts.ClaimTTL = DefaultRootFSObjectDeleteClaimTTL
	}
	if opts.BackoffBase <= 0 {
		opts.BackoffBase = DefaultRootFSObjectDeleteBackoffBase
	}
	if opts.BackoffMax <= 0 {
		opts.BackoffMax = DefaultRootFSObjectDeleteBackoffMax
	}
	if opts.BackoffMax < opts.BackoffBase {
		opts.BackoffMax = opts.BackoffBase
	}
	return opts
}

func normalizeRootFSObjectLimit(limit int) int {
	if limit <= 0 {
		return defaultRootFSObjectDeleteLimit
	}
	if limit > MaxRootFSObjectDeleteLimit {
		return MaxRootFSObjectDeleteLimit
	}
	return limit
}

func rootFSObjectDeleteBackoff(attempt int, base, max time.Duration) time.Duration {
	if base <= 0 {
		base = DefaultRootFSObjectDeleteBackoffBase
	}
	if max <= 0 {
		max = DefaultRootFSObjectDeleteBackoffMax
	}
	if max < base {
		max = base
	}
	return retryqueue.ExponentialBackoff(attempt, base, max)
}

func truncateRootFSError(message string) string {
	message = strings.TrimSpace(message)
	if len(message) <= 2048 {
		return message
	}
	return message[:2048]
}

func (s *PGSandboxStore) rootFSForkNoRowsError(ctx context.Context, req *ForkRootFSFilesystemRequest) error {
	source, err := s.GetRootFSFilesystem(ctx, req.SourceSandboxID)
	if err != nil {
		return err
	}
	if source == nil || strings.TrimSpace(source.HeadGenerationID) == "" {
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
		SELECT EXISTS (SELECT 1 FROM manager.sandboxes WHERE sandbox_id = $1)
	`, sandboxID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check sandbox exists: %w", err)
	}
	return exists, nil
}

func (s *PGSandboxStore) rootFSSnapshotExists(ctx context.Context, snapshotID, teamID string) (bool, error) {
	var exists bool
	if err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM manager.rootfs_snapshots
			WHERE snapshot_id = $1 AND ($2 = '' OR team_id = $2)
				AND (expires_at IS NULL OR expires_at > NOW())
		)
	`, snapshotID, teamID).Scan(&exists); err != nil {
		return false, fmt.Errorf("check rootfs snapshot exists: %w", err)
	}
	return exists, nil
}

func rootFSFilesystemSelectSQL() string {
	return `
		SELECT filesystem.filesystem_id, filesystem.team_id,
			filesystem.source_filesystem_id, filesystem.writer_epoch,
			filesystem.head_generation_id, filesystem.base_artifact_digest,
			filesystem.format_generation, artifact.source_oci_ref,
			generation.source_oci_digest, filesystem.created_at, filesystem.updated_at
		FROM manager.sandbox_rootfs_bindings binding
		JOIN manager.rootfs_filesystems filesystem
			ON filesystem.filesystem_id = binding.filesystem_id
		LEFT JOIN manager.rootfs_generations generation
			ON generation.generation_id = filesystem.head_generation_id
		JOIN manager.rootfs_base_artifacts artifact
			ON artifact.artifact_digest = filesystem.base_artifact_digest `
}

func rootFSSnapshotSelectSQL() string {
	return `
		SELECT snapshot.snapshot_id, snapshot.filesystem_id, snapshot.team_id,
			snapshot.source_sandbox_id, snapshot.head_generation_id,
			generation.base_artifact_digest, generation.format_generation,
			generation.source_oci_digest, snapshot.name, snapshot.description,
			snapshot.created_at, snapshot.expires_at
		FROM manager.rootfs_snapshots snapshot
		JOIN manager.rootfs_generations generation
			ON generation.generation_id = snapshot.head_generation_id `
}

func scanRootFSFilesystem(row sandboxRecordScanner) (*RootFSFilesystem, error) {
	var filesystem RootFSFilesystem
	var sourceFilesystemID, headGenerationID *string
	if err := row.Scan(
		&filesystem.ID, &filesystem.TeamID, &sourceFilesystemID,
		&filesystem.WriterEpoch, &headGenerationID,
		&filesystem.BaseArtifactDigest, &filesystem.FormatGeneration,
		&filesystem.BaseImageRef, &filesystem.BaseImageDigest,
		&filesystem.CreatedAt, &filesystem.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if sourceFilesystemID != nil {
		filesystem.SourceFilesystemID = *sourceFilesystemID
	}
	if headGenerationID != nil {
		filesystem.HeadGenerationID = *headGenerationID
	}
	return &filesystem, nil
}

func scanRootFSSnapshot(row sandboxRecordScanner) (*RootFSSnapshot, error) {
	var snapshot RootFSSnapshot
	var expiresAt *time.Time
	if err := row.Scan(
		&snapshot.ID, &snapshot.FilesystemID, &snapshot.TeamID,
		&snapshot.SourceSandboxID, &snapshot.HeadGenerationID,
		&snapshot.BaseArtifactDigest, &snapshot.FormatGeneration,
		&snapshot.SourceOCIDigest, &snapshot.Name, &snapshot.Description,
		&snapshot.CreatedAt, &expiresAt,
	); err != nil {
		return nil, err
	}
	if expiresAt != nil {
		snapshot.ExpiresAt = *expiresAt
	}
	return &snapshot, nil
}
