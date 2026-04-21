package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("conflict")
)

// CoordinatorRepository defines the database operations needed by coordinator
// This interface is implemented by *Repository
type CoordinatorRepository interface {
	// Mount operations
	AcquireMount(ctx context.Context, mount *VolumeMount, heartbeatTimeout int) error
	CreateMount(ctx context.Context, mount *VolumeMount) error
	UpdateMountHeartbeat(ctx context.Context, volumeID, clusterID, podID string) error
	DeleteMount(ctx context.Context, volumeID, clusterID, podID string) error
	DeleteMountByPodID(ctx context.Context, clusterID, podID string) error
	GetActiveMounts(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*VolumeMount, error)
	GetAllMounts(ctx context.Context) ([]*VolumeMount, error)
	DeleteStaleMounts(ctx context.Context, heartbeatTimeout int) (int64, error)

	// Coordination operations
	CreateCoordination(ctx context.Context, coord *SnapshotCoordination) error
	GetCoordination(ctx context.Context, id string) (*SnapshotCoordination, error)
	UpdateCoordinationStatus(ctx context.Context, id, status string) error
	CreateFlushResponse(ctx context.Context, resp *FlushResponse) error
	CountCompletedFlushes(ctx context.Context, coordID string) (int, error)
	GetFlushResponses(ctx context.Context, coordID string) ([]*FlushResponse, error)
}

type mountOptionsEnvelope struct {
	AccessMode string `json:"access_mode"`
}

func normalizeMountAccessMode(value string) string {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "ROX":
		return "ROX"
	case "RWX":
		return "RWX"
	default:
		return "RWO"
	}
}

func decodeMountAccessMode(raw *json.RawMessage) string {
	if raw == nil || len(*raw) == 0 {
		return "RWO"
	}
	var opts mountOptionsEnvelope
	if err := json.Unmarshal(*raw, &opts); err != nil {
		return "RWO"
	}
	return normalizeMountAccessMode(opts.AccessMode)
}

// Repository provides database access for storage-proxy
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new database repository
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Pool returns the underlying connection pool
func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

// DB interface for query execution (supports both pool and transaction)
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// BeginTx starts a new transaction
func (r *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}

// WithTx executes a function within a transaction
// If the function returns an error, the transaction is rolled back
// Otherwise, the transaction is committed
// Note: This function does not propagate panics to maintain service stability.
// Panics are logged and converted to errors. Caller code should not panic.
func (r *Repository) WithTx(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	// Ensure transaction is always finalized
	committed := false
	defer func() {
		if !committed {
			// Rollback on error or panic, ignore rollback errors in defer
			_ = tx.Rollback(ctx)
		}
	}()

	if err := fn(tx); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	committed = true
	return nil
}

// CreateSandboxVolume creates a new sandbox volume record
func (r *Repository) CreateSandboxVolume(ctx context.Context, volume *SandboxVolume) error {
	return r.createSandboxVolume(ctx, r.pool, volume)
}

// CreateSandboxVolumeTx creates a new sandbox volume record within a transaction.
func (r *Repository) CreateSandboxVolumeTx(ctx context.Context, tx pgx.Tx, volume *SandboxVolume) error {
	return r.createSandboxVolume(ctx, tx, volume)
}

func (r *Repository) createSandboxVolume(ctx context.Context, db DB, volume *SandboxVolume) error {
	_, err := db.Exec(ctx, `
		INSERT INTO sandbox_volumes (
			id, team_id, user_id,
			source_volume_id,
			default_posix_uid, default_posix_gid,
			access_mode,
			created_at, updated_at
		) VALUES (
			$1, $2, $3,
			$4,
			$5, $6,
			$7,
			$8, $9
		)
	`,
		volume.ID, volume.TeamID, volume.UserID,
		volume.SourceVolumeID,
		volume.DefaultPosixUID, volume.DefaultPosixGID,
		volume.AccessMode,
		volume.CreatedAt, volume.UpdatedAt,
	)

	if err != nil {
		return fmt.Errorf("create sandbox volume: %w", err)
	}

	return nil
}

// GetSandboxVolume retrieves a sandbox volume by ID
func (r *Repository) GetSandboxVolume(ctx context.Context, id string) (*SandboxVolume, error) {
	return r.getSandboxVolume(ctx, r.pool, id, false)
}

// GetSandboxVolumeForUpdate retrieves a sandbox volume with FOR UPDATE NOWAIT lock
// This prevents deadlocks by failing immediately if the row is already locked
// Use this within a transaction when you need to ensure exclusive access
func (r *Repository) GetSandboxVolumeForUpdate(ctx context.Context, tx pgx.Tx, id string) (*SandboxVolume, error) {
	return r.getSandboxVolume(ctx, tx, id, true)
}

// getSandboxVolume internal implementation supporting both locked and unlocked reads
func (r *Repository) getSandboxVolume(ctx context.Context, db DB, id string, forUpdate bool) (*SandboxVolume, error) {
	var v SandboxVolume

	query := `
		SELECT
			id, team_id, user_id,
			source_volume_id,
			default_posix_uid, default_posix_gid,
			access_mode,
			created_at, updated_at
		FROM sandbox_volumes
		WHERE id = $1
	`

	// Add FOR UPDATE NOWAIT to prevent blocking and detect conflicts immediately
	if forUpdate {
		query += " FOR UPDATE NOWAIT"
	}

	err := db.QueryRow(ctx, query, id).Scan(
		&v.ID, &v.TeamID, &v.UserID,
		&v.SourceVolumeID,
		&v.DefaultPosixUID, &v.DefaultPosixGID,
		&v.AccessMode,
		&v.CreatedAt, &v.UpdatedAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query sandbox volume: %w", err)
	}

	return &v, nil
}

// UpdateSandboxVolume updates an existing sandbox volume
func (r *Repository) UpdateSandboxVolume(ctx context.Context, volume *SandboxVolume) error {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE sandbox_volumes SET
			default_posix_uid = $2,
			default_posix_gid = $3,
			access_mode = $4,
			updated_at = NOW()
		WHERE id = $1
	`,
		volume.ID,
		volume.DefaultPosixUID, volume.DefaultPosixGID,
		volume.AccessMode,
	)

	if err != nil {
		return fmt.Errorf("update sandbox volume: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// ListSandboxVolumesByTeam retrieves all volumes for a team
func (r *Repository) ListSandboxVolumesByTeam(ctx context.Context, teamID string) ([]*SandboxVolume, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			id, team_id, user_id,
			source_volume_id,
			default_posix_uid, default_posix_gid,
			access_mode,
			created_at, updated_at
		FROM sandbox_volumes
		WHERE team_id = $1
			AND NOT EXISTS (
				SELECT 1 FROM sandbox_volume_owners
				WHERE sandbox_volume_owners.volume_id = sandbox_volumes.id
			)
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("query sandbox volumes: %w", err)
	}
	defer rows.Close()

	var volumes []*SandboxVolume
	for rows.Next() {
		var v SandboxVolume
		err := rows.Scan(
			&v.ID, &v.TeamID, &v.UserID,
			&v.SourceVolumeID,
			&v.DefaultPosixUID, &v.DefaultPosixGID,
			&v.AccessMode,
			&v.CreatedAt, &v.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan sandbox volume: %w", err)
		}
		volumes = append(volumes, &v)
	}

	return volumes, nil
}

// DeleteSandboxVolume deletes a sandbox volume record
func (r *Repository) DeleteSandboxVolume(ctx context.Context, id string) error {
	return r.deleteSandboxVolume(ctx, r.pool, id)
}

// DeleteSandboxVolumeTx deletes a sandbox volume record within a transaction.
func (r *Repository) DeleteSandboxVolumeTx(ctx context.Context, tx pgx.Tx, id string) error {
	return r.deleteSandboxVolume(ctx, tx, id)
}

func (r *Repository) deleteSandboxVolume(ctx context.Context, db DB, id string) error {
	cmdTag, err := db.Exec(ctx, `
		DELETE FROM sandbox_volumes WHERE id = $1
	`, id)

	if err != nil {
		return fmt.Errorf("delete sandbox volume: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// CreateSandboxVolumeOwnerTx creates durable ownership metadata for a system volume.
func (r *Repository) CreateSandboxVolumeOwnerTx(ctx context.Context, tx pgx.Tx, owner *SandboxVolumeOwner) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO sandbox_volume_owners (
			volume_id, owner_kind, owner_sandbox_id, owner_cluster_id, purpose,
			created_at, cleanup_requested_at, cleanup_reason,
			last_cleanup_attempt_at, last_cleanup_error, updated_at
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8,
			$9, $10, $11
		)
	`,
		owner.VolumeID, owner.OwnerKind, owner.OwnerSandboxID, owner.OwnerClusterID, owner.Purpose,
		owner.CreatedAt, owner.CleanupRequestedAt, owner.CleanupReason,
		owner.LastCleanupAttemptAt, owner.LastCleanupError, owner.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create sandbox volume owner: %w", err)
	}
	return nil
}

// GetSandboxVolumeOwner retrieves ownership metadata for a volume.
func (r *Repository) GetSandboxVolumeOwner(ctx context.Context, volumeID string) (*SandboxVolumeOwner, error) {
	var owner SandboxVolumeOwner
	err := r.pool.QueryRow(ctx, `
		SELECT
			volume_id, owner_kind, owner_sandbox_id, owner_cluster_id, purpose,
			created_at, cleanup_requested_at, cleanup_reason,
			last_cleanup_attempt_at, last_cleanup_error, updated_at
		FROM sandbox_volume_owners
		WHERE volume_id = $1
	`, volumeID).Scan(
		&owner.VolumeID, &owner.OwnerKind, &owner.OwnerSandboxID, &owner.OwnerClusterID, &owner.Purpose,
		&owner.CreatedAt, &owner.CleanupRequestedAt, &owner.CleanupReason,
		&owner.LastCleanupAttemptAt, &owner.LastCleanupError, &owner.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query sandbox volume owner: %w", err)
	}
	return &owner, nil
}

// GetOwnedSandboxVolumeByOwner retrieves a live system volume for a sandbox and purpose.
func (r *Repository) GetOwnedSandboxVolumeByOwner(ctx context.Context, clusterID, sandboxID, purpose string) (*OwnedSandboxVolume, error) {
	rows, err := r.queryOwnedSandboxVolumes(ctx, `
		WHERE o.owner_cluster_id = $1
			AND o.owner_sandbox_id = $2
			AND o.purpose = $3
			AND o.cleanup_requested_at IS NULL
		ORDER BY o.created_at DESC
		LIMIT 1
	`, clusterID, sandboxID, purpose)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("query owned sandbox volume: %w", err)
		}
		return nil, ErrNotFound
	}
	owned, err := scanOwnedSandboxVolume(rows)
	if err != nil {
		return nil, err
	}
	return owned, nil
}

// ListOwnedSandboxVolumes lists manager-created system volumes for a cluster.
func (r *Repository) ListOwnedSandboxVolumes(ctx context.Context, clusterID string, cleanupRequested *bool) ([]*OwnedSandboxVolume, error) {
	where := "WHERE o.owner_cluster_id = $1"
	args := []any{clusterID}
	if cleanupRequested != nil {
		if *cleanupRequested {
			where += " AND o.cleanup_requested_at IS NOT NULL"
		} else {
			where += " AND o.cleanup_requested_at IS NULL"
		}
	}
	rows, err := r.queryOwnedSandboxVolumes(ctx, where+" ORDER BY o.created_at ASC", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var owned []*OwnedSandboxVolume
	for rows.Next() {
		item, err := scanOwnedSandboxVolume(rows)
		if err != nil {
			return nil, err
		}
		owned = append(owned, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query owned sandbox volumes: %w", err)
	}
	return owned, nil
}

// MarkOwnedSandboxVolumesForCleanup marks all live system volumes for a sandbox.
func (r *Repository) MarkOwnedSandboxVolumesForCleanup(ctx context.Context, clusterID, sandboxID, reason string) (int64, error) {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE sandbox_volume_owners
		SET cleanup_requested_at = COALESCE(cleanup_requested_at, NOW()),
			cleanup_reason = COALESCE(NULLIF($3, ''), cleanup_reason),
			updated_at = NOW()
		WHERE owner_cluster_id = $1
			AND owner_sandbox_id = $2
			AND cleanup_requested_at IS NULL
	`, clusterID, sandboxID, reason)
	if err != nil {
		return 0, fmt.Errorf("mark owned sandbox volumes for cleanup: %w", err)
	}
	return cmdTag.RowsAffected(), nil
}

// MarkOwnedSandboxVolumeCleanupAttempt records the result of a cleanup attempt.
func (r *Repository) MarkOwnedSandboxVolumeCleanupAttempt(ctx context.Context, volumeID string, cleanupErr error) error {
	var errText *string
	if cleanupErr != nil {
		value := strings.TrimSpace(cleanupErr.Error())
		errText = &value
	}
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE sandbox_volume_owners
		SET last_cleanup_attempt_at = NOW(),
			last_cleanup_error = $2,
			updated_at = NOW()
		WHERE volume_id = $1
	`, volumeID, errText)
	if err != nil {
		return fmt.Errorf("mark owned sandbox volume cleanup attempt: %w", err)
	}
	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) queryOwnedSandboxVolumes(ctx context.Context, suffix string, args ...any) (pgx.Rows, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			v.id, v.team_id, v.user_id,
			v.source_volume_id,
			v.default_posix_uid, v.default_posix_gid,
			v.access_mode,
			v.created_at, v.updated_at,
			o.volume_id, o.owner_kind, o.owner_sandbox_id, o.owner_cluster_id, o.purpose,
			o.created_at, o.cleanup_requested_at, o.cleanup_reason,
			o.last_cleanup_attempt_at, o.last_cleanup_error, o.updated_at
		FROM sandbox_volume_owners o
		JOIN sandbox_volumes v ON v.id = o.volume_id
		`+suffix, args...)
	if err != nil {
		return nil, fmt.Errorf("query owned sandbox volumes: %w", err)
	}
	return rows, nil
}

func scanOwnedSandboxVolume(rows pgx.Rows) (*OwnedSandboxVolume, error) {
	var item OwnedSandboxVolume
	err := rows.Scan(
		&item.Volume.ID, &item.Volume.TeamID, &item.Volume.UserID,
		&item.Volume.SourceVolumeID,
		&item.Volume.DefaultPosixUID, &item.Volume.DefaultPosixGID,
		&item.Volume.AccessMode,
		&item.Volume.CreatedAt, &item.Volume.UpdatedAt,
		&item.Owner.VolumeID, &item.Owner.OwnerKind, &item.Owner.OwnerSandboxID, &item.Owner.OwnerClusterID, &item.Owner.Purpose,
		&item.Owner.CreatedAt, &item.Owner.CleanupRequestedAt, &item.Owner.CleanupReason,
		&item.Owner.LastCleanupAttemptAt, &item.Owner.LastCleanupError, &item.Owner.UpdatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("scan owned sandbox volume: %w", err)
	}
	return &item, nil
}

// ============================================================
// Snapshot Repository Methods
// ============================================================

// CreateSnapshot creates a new snapshot record
func (r *Repository) CreateSnapshot(ctx context.Context, snapshot *Snapshot) error {
	return r.createSnapshot(ctx, r.pool, snapshot)
}

// CreateSnapshotTx creates a new snapshot record within a transaction
func (r *Repository) CreateSnapshotTx(ctx context.Context, tx pgx.Tx, snapshot *Snapshot) error {
	return r.createSnapshot(ctx, tx, snapshot)
}

// createSnapshot internal implementation supporting both pool and transaction
func (r *Repository) createSnapshot(ctx context.Context, db DB, snapshot *Snapshot) error {
	_, err := db.Exec(ctx, `
		INSERT INTO sandbox_volume_snapshots (
			id, volume_id, team_id, user_id,
			root_inode, source_inode,
			name, description, size_bytes,
			created_at, expires_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6,
			$7, $8, $9,
			$10, $11
		)
	`,
		snapshot.ID, snapshot.VolumeID, snapshot.TeamID, snapshot.UserID,
		snapshot.RootInode, snapshot.SourceInode,
		snapshot.Name, snapshot.Description, snapshot.SizeBytes,
		snapshot.CreatedAt, snapshot.ExpiresAt,
	)

	if err != nil {
		return fmt.Errorf("create snapshot: %w", err)
	}

	return nil
}

// GetSnapshot retrieves a snapshot by ID
func (r *Repository) GetSnapshot(ctx context.Context, id string) (*Snapshot, error) {
	return r.getSnapshot(ctx, r.pool, id, false)
}

// GetSnapshotTx retrieves a snapshot by ID within a transaction
func (r *Repository) GetSnapshotTx(ctx context.Context, tx pgx.Tx, id string) (*Snapshot, error) {
	return r.getSnapshot(ctx, tx, id, false)
}

// GetSnapshotForUpdate retrieves a snapshot with FOR UPDATE NOWAIT lock
func (r *Repository) GetSnapshotForUpdate(ctx context.Context, tx pgx.Tx, id string) (*Snapshot, error) {
	return r.getSnapshot(ctx, tx, id, true)
}

// getSnapshot internal implementation supporting both locked and unlocked reads
func (r *Repository) getSnapshot(ctx context.Context, db DB, id string, forUpdate bool) (*Snapshot, error) {
	var s Snapshot

	query := `
		SELECT
			id, volume_id, team_id, user_id,
			root_inode, source_inode,
			name, description, size_bytes,
			created_at, expires_at
		FROM sandbox_volume_snapshots
		WHERE id = $1
	`

	if forUpdate {
		query += " FOR UPDATE NOWAIT"
	}

	err := db.QueryRow(ctx, query, id).Scan(
		&s.ID, &s.VolumeID, &s.TeamID, &s.UserID,
		&s.RootInode, &s.SourceInode,
		&s.Name, &s.Description, &s.SizeBytes,
		&s.CreatedAt, &s.ExpiresAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query snapshot: %w", err)
	}

	return &s, nil
}

// ListSnapshotsByVolume retrieves all snapshots for a volume
func (r *Repository) ListSnapshotsByVolume(ctx context.Context, volumeID string) ([]*Snapshot, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			id, volume_id, team_id, user_id,
			root_inode, source_inode,
			name, description, size_bytes,
			created_at, expires_at
		FROM sandbox_volume_snapshots
		WHERE volume_id = $1
		ORDER BY created_at DESC
	`, volumeID)
	if err != nil {
		return nil, fmt.Errorf("query snapshots: %w", err)
	}
	defer rows.Close()

	var snapshots []*Snapshot
	for rows.Next() {
		var s Snapshot
		err := rows.Scan(
			&s.ID, &s.VolumeID, &s.TeamID, &s.UserID,
			&s.RootInode, &s.SourceInode,
			&s.Name, &s.Description, &s.SizeBytes,
			&s.CreatedAt, &s.ExpiresAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		snapshots = append(snapshots, &s)
	}

	return snapshots, nil
}

// DeleteSnapshot deletes a snapshot record
func (r *Repository) DeleteSnapshot(ctx context.Context, id string) error {
	return r.deleteSnapshot(ctx, r.pool, id)
}

// DeleteSnapshotTx deletes a snapshot record within a transaction
func (r *Repository) DeleteSnapshotTx(ctx context.Context, tx pgx.Tx, id string) error {
	return r.deleteSnapshot(ctx, tx, id)
}

// deleteSnapshot internal implementation supporting both pool and transaction
func (r *Repository) deleteSnapshot(ctx context.Context, db DB, id string) error {
	cmdTag, err := db.Exec(ctx, `
		DELETE FROM sandbox_volume_snapshots WHERE id = $1
	`, id)

	if err != nil {
		return fmt.Errorf("delete snapshot: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// ============================================================
// Volume Mount Repository Methods (for cross-cluster coordination)
// ============================================================

// AcquireMount atomically enforces the volume access mode and upserts the active mount row.
func (r *Repository) AcquireMount(ctx context.Context, mount *VolumeMount, heartbeatTimeout int) error {
	if mount == nil {
		return fmt.Errorf("mount is required")
	}
	if heartbeatTimeout <= 0 {
		heartbeatTimeout = 15
	}
	return r.WithTx(ctx, func(tx pgx.Tx) error {
		var accessMode string
		if err := tx.QueryRow(ctx, `SELECT access_mode FROM sandbox_volumes WHERE id = $1 FOR UPDATE`, mount.VolumeID).Scan(&accessMode); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("lock sandbox volume: %w", err)
		}
		activeMounts, err := r.getActiveMounts(ctx, tx, mount.VolumeID, heartbeatTimeout)
		if err != nil {
			return err
		}
		switch normalizeMountAccessMode(accessMode) {
		case "RWO":
			for _, active := range activeMounts {
				if active.ClusterID == mount.ClusterID && active.PodID == mount.PodID {
					continue
				}
				return fmt.Errorf("%w: volume %s already mounted on another instance", ErrConflict, mount.VolumeID)
			}
		case "ROX":
			for _, active := range activeMounts {
				if decodeMountAccessMode(active.MountOptions) != "ROX" {
					return fmt.Errorf("%w: volume %s already mounted read-write", ErrConflict, mount.VolumeID)
				}
			}
		case "RWX":
		default:
			return fmt.Errorf("invalid access_mode %q", accessMode)
		}
		return r.createMount(ctx, tx, mount)
	})
}

// CreateMount creates a volume mount record
func (r *Repository) CreateMount(ctx context.Context, mount *VolumeMount) error {
	return r.createMount(ctx, r.pool, mount)
}

func (r *Repository) createMount(ctx context.Context, db DB, mount *VolumeMount) error {
	_, err := db.Exec(ctx, `
		INSERT INTO sandbox_volume_mounts (
			id, volume_id, cluster_id, pod_id,
			last_heartbeat, mounted_at, mount_options
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7
		)
		ON CONFLICT (volume_id, cluster_id, pod_id) 
		DO UPDATE SET last_heartbeat = $5, mount_options = $7
	`,
		mount.ID, mount.VolumeID, mount.ClusterID, mount.PodID,
		mount.LastHeartbeat, mount.MountedAt, mount.MountOptions,
	)

	if err != nil {
		return fmt.Errorf("create mount: %w", err)
	}

	return nil
}

// UpdateMountHeartbeat updates the heartbeat for a mount
func (r *Repository) UpdateMountHeartbeat(ctx context.Context, volumeID, clusterID, podID string) error {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE sandbox_volume_mounts 
		SET last_heartbeat = NOW()
		WHERE volume_id = $1 AND cluster_id = $2 AND pod_id = $3
	`, volumeID, clusterID, podID)

	if err != nil {
		return fmt.Errorf("update mount heartbeat: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// DeleteMount deletes a mount record
func (r *Repository) DeleteMount(ctx context.Context, volumeID, clusterID, podID string) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM sandbox_volume_mounts 
		WHERE volume_id = $1 AND cluster_id = $2 AND pod_id = $3
	`, volumeID, clusterID, podID)

	if err != nil {
		return fmt.Errorf("delete mount: %w", err)
	}

	return nil
}

// DeleteMountByPodID deletes all mount records for a pod in a cluster.
func (r *Repository) DeleteMountByPodID(ctx context.Context, clusterID, podID string) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM sandbox_volume_mounts
		WHERE cluster_id = $1 AND pod_id = $2
	`, clusterID, podID)
	if err != nil {
		return fmt.Errorf("delete mount by pod id: %w", err)
	}
	return nil
}

// GetActiveMounts retrieves active mounts for a volume (heartbeat within threshold)
func (r *Repository) GetActiveMounts(ctx context.Context, volumeID string, heartbeatTimeout int) ([]*VolumeMount, error) {
	return r.getActiveMounts(ctx, r.pool, volumeID, heartbeatTimeout)
}

func (r *Repository) getActiveMounts(ctx context.Context, db DB, volumeID string, heartbeatTimeout int) ([]*VolumeMount, error) {
	rows, err := db.Query(ctx, `
		SELECT
			id, volume_id, cluster_id, pod_id,
			last_heartbeat, mounted_at, mount_options
		FROM sandbox_volume_mounts
		WHERE volume_id = $1 
			AND last_heartbeat > NOW() - INTERVAL '1 second' * $2
		ORDER BY mounted_at DESC
	`, volumeID, heartbeatTimeout)
	if err != nil {
		return nil, fmt.Errorf("query active mounts: %w", err)
	}
	defer rows.Close()

	var mounts []*VolumeMount
	for rows.Next() {
		var m VolumeMount
		err := rows.Scan(
			&m.ID, &m.VolumeID, &m.ClusterID, &m.PodID,
			&m.LastHeartbeat, &m.MountedAt, &m.MountOptions,
		)
		if err != nil {
			return nil, fmt.Errorf("scan mount: %w", err)
		}
		mounts = append(mounts, &m)
	}

	return mounts, nil
}

// GetAllMounts retrieves all mount records.
func (r *Repository) GetAllMounts(ctx context.Context) ([]*VolumeMount, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			id, volume_id, cluster_id, pod_id,
			last_heartbeat, mounted_at, mount_options
		FROM sandbox_volume_mounts
	`)
	if err != nil {
		return nil, fmt.Errorf("query all mounts: %w", err)
	}
	defer rows.Close()

	var mounts []*VolumeMount
	for rows.Next() {
		var m VolumeMount
		err := rows.Scan(
			&m.ID, &m.VolumeID, &m.ClusterID, &m.PodID,
			&m.LastHeartbeat, &m.MountedAt, &m.MountOptions,
		)
		if err != nil {
			return nil, fmt.Errorf("scan mount: %w", err)
		}
		mounts = append(mounts, &m)
	}

	return mounts, nil
}

// DeleteStaleMounts deletes mounts with expired heartbeats
func (r *Repository) DeleteStaleMounts(ctx context.Context, heartbeatTimeout int) (int64, error) {
	cmdTag, err := r.pool.Exec(ctx, `
		DELETE FROM sandbox_volume_mounts 
		WHERE last_heartbeat < NOW() - INTERVAL '1 second' * $1
	`, heartbeatTimeout)

	if err != nil {
		return 0, fmt.Errorf("delete stale mounts: %w", err)
	}

	return cmdTag.RowsAffected(), nil
}

// ============================================================
// Snapshot Coordination Repository Methods
// ============================================================

// CreateCoordination creates a snapshot coordination record
func (r *Repository) CreateCoordination(ctx context.Context, coord *SnapshotCoordination) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO snapshot_coordinations (
			id, volume_id, snapshot_id,
			status, expected_nodes, completed_nodes,
			created_at, updated_at, expires_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6,
			$7, $8, $9
		)
	`,
		coord.ID, coord.VolumeID, coord.SnapshotID,
		coord.Status, coord.ExpectedNodes, coord.CompletedNodes,
		coord.CreatedAt, coord.UpdatedAt, coord.ExpiresAt,
	)

	if err != nil {
		return fmt.Errorf("create coordination: %w", err)
	}

	return nil
}

// GetCoordination retrieves a coordination by ID
func (r *Repository) GetCoordination(ctx context.Context, id string) (*SnapshotCoordination, error) {
	var c SnapshotCoordination

	err := r.pool.QueryRow(ctx, `
		SELECT
			id, volume_id, snapshot_id,
			status, expected_nodes, completed_nodes,
			created_at, updated_at, expires_at
		FROM snapshot_coordinations
		WHERE id = $1
	`, id).Scan(
		&c.ID, &c.VolumeID, &c.SnapshotID,
		&c.Status, &c.ExpectedNodes, &c.CompletedNodes,
		&c.CreatedAt, &c.UpdatedAt, &c.ExpiresAt,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("query coordination: %w", err)
	}

	return &c, nil
}

// UpdateCoordinationStatus updates the status of a coordination
func (r *Repository) UpdateCoordinationStatus(ctx context.Context, id, status string) error {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE snapshot_coordinations 
		SET status = $2, updated_at = NOW()
		WHERE id = $1
	`, id, status)

	if err != nil {
		return fmt.Errorf("update coordination status: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// UpdateCoordinationSnapshotID sets the snapshot ID after successful creation
func (r *Repository) UpdateCoordinationSnapshotID(ctx context.Context, coordID, snapshotID string) error {
	cmdTag, err := r.pool.Exec(ctx, `
		UPDATE snapshot_coordinations 
		SET snapshot_id = $2, status = $3, updated_at = NOW()
		WHERE id = $1
	`, coordID, snapshotID, CoordStatusCompleted)

	if err != nil {
		return fmt.Errorf("update coordination snapshot_id: %w", err)
	}

	if cmdTag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

// CreateFlushResponse creates a flush response record
func (r *Repository) CreateFlushResponse(ctx context.Context, resp *FlushResponse) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO snapshot_flush_responses (
			id, coord_id, cluster_id, pod_id,
			success, flushed_at, error_message
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7
		)
		ON CONFLICT (coord_id, cluster_id, pod_id) 
		DO UPDATE SET success = $5, flushed_at = $6, error_message = $7
	`,
		resp.ID, resp.CoordID, resp.ClusterID, resp.PodID,
		resp.Success, resp.FlushedAt, resp.ErrorMessage,
	)

	if err != nil {
		return fmt.Errorf("create flush response: %w", err)
	}

	return nil
}

// CountCompletedFlushes counts successful flush responses for a coordination
func (r *Repository) CountCompletedFlushes(ctx context.Context, coordID string) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM snapshot_flush_responses 
		WHERE coord_id = $1 AND success = true
	`, coordID).Scan(&count)

	if err != nil {
		return 0, fmt.Errorf("count completed flushes: %w", err)
	}

	return count, nil
}

// GetFlushResponses retrieves all flush responses for a coordination
func (r *Repository) GetFlushResponses(ctx context.Context, coordID string) ([]*FlushResponse, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT
			id, coord_id, cluster_id, pod_id,
			success, flushed_at, error_message
		FROM snapshot_flush_responses
		WHERE coord_id = $1
	`, coordID)
	if err != nil {
		return nil, fmt.Errorf("query flush responses: %w", err)
	}
	defer rows.Close()

	var responses []*FlushResponse
	for rows.Next() {
		var r FlushResponse
		err := rows.Scan(
			&r.ID, &r.CoordID, &r.ClusterID, &r.PodID,
			&r.Success, &r.FlushedAt, &r.ErrorMessage,
		)
		if err != nil {
			return nil, fmt.Errorf("scan flush response: %w", err)
		}
		responses = append(responses, &r)
	}

	return responses, nil
}
