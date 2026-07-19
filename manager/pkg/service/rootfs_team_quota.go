package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota"
)

const (
	rootFSObjectQuotaOwnerKind       = "rootfs_object"
	rootFSSnapshotQuotaOwnerKind     = "rootfs_snapshot"
	rootFSPublishStageQuotaOwnerKind = "rootfs_publish_stage"
)

const (
	rootFSObjectPublishOperationKind       = "rootfs_object_publish"
	rootFSPublishStageOperationKind        = "rootfs_publish_stage"
	rootFSPublishTransferOperationKind     = "rootfs_publish_transfer"
	rootFSPublishStageReleaseOperationKind = "rootfs_publish_stage_release"
	rootFSObjectDeleteOperationKind        = "rootfs_object_delete"
	rootFSSnapshotCreateOperationKind      = "rootfs_snapshot_create"
	rootFSSnapshotDeleteOperationKind      = "rootfs_snapshot_delete"
)

const (
	rootFSObjectTombstoneRetention         = 24 * time.Hour
	maxRootFSObjectTombstonesPerTeam       = 4096
	rootFSObjectTombstonePruneBatchSize    = 256
	maxRootFSObjectTombstonePruneBatchSize = 4096
)

// RootFSObjectInventory is the durable physical-object view used to reconcile
// rootfs storage quota after a crash or process restart.
type RootFSObjectInventory struct {
	ObjectKey string
	TeamID    string
	SizeBytes int64
	Deleted   bool
}

// RootFSSnapshotInventory is the durable metadata view used to reconcile
// snapshot object-count quota after a crash or process restart.
type RootFSSnapshotInventory struct {
	SnapshotID string
	TeamID     string
}

// RootFSPublishStage is the durable manager-side identity charged before ctld
// generates any node-local diff artifact.
type RootFSPublishStage struct {
	StageID           string
	TeamID            string
	SandboxID         string
	CtldAddress       string
	RuntimeGeneration int64
	ExpiresAt         time.Time
	ReleaseAfter      time.Time
}

// PrepareRootFSPublishStage commits one storage-object slot before any ctld
// staging work starts. The row and quota allocation share the same transaction.
func (s *PGSandboxStore) PrepareRootFSPublishStage(
	ctx context.Context,
	stage RootFSPublishStage,
	quotaStore teamquota.CapacityTxStore,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("postgres sandbox store is required for rootfs publish staging")
	}
	if quotaStore == nil {
		return fmt.Errorf("transactional team quota store is required for rootfs publish staging")
	}
	stage = normalizeRootFSPublishStage(stage)
	if err := validateRootFSPublishStage(stage); err != nil {
		return err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs publish stage transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tag, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_publish_stages (
			stage_id, team_id, sandbox_id, ctld_address, runtime_generation,
			expires_at, release_after, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), NOW())
		ON CONFLICT (stage_id) DO UPDATE
		SET updated_at = NOW()
		WHERE manager.rootfs_publish_stages.team_id = EXCLUDED.team_id
			AND manager.rootfs_publish_stages.sandbox_id = EXCLUDED.sandbox_id
			AND manager.rootfs_publish_stages.ctld_address = EXCLUDED.ctld_address
			AND manager.rootfs_publish_stages.runtime_generation = EXCLUDED.runtime_generation
			AND manager.rootfs_publish_stages.expires_at = EXCLUDED.expires_at
			AND manager.rootfs_publish_stages.release_after = EXCLUDED.release_after
	`, stage.StageID, stage.TeamID, stage.SandboxID, stage.CtldAddress,
		stage.RuntimeGeneration, stage.ExpiresAt, stage.ReleaseAfter)
	if err != nil {
		return fmt.Errorf("persist rootfs publish stage: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs publish stage %q conflicts with another owner", stage.StageID)
	}
	owner, err := rootFSPublishStageQuotaOwner(stage.TeamID, stage.StageID)
	if err != nil {
		return err
	}
	operation := rootFSPublishStageQuotaOperation(rootFSPublishStageOperationKind, stage.StageID)
	if _, err := quotaStore.ReserveTargetTx(ctx, tx, teamquota.ReserveRequest{
		Owner:     owner,
		Operation: operation,
		Target: teamquota.Values{
			teamquota.KeyStorageObjectCount: 1,
		},
	}); err != nil {
		return err
	}
	if err := quotaStore.CommitTx(ctx, tx, teamquota.Ref(owner, operation)); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs publish stage transaction: %w", err)
	}
	return nil
}

// PrepareRootFSObjectPublish atomically persists the physical object inventory
// and deletion intent while transferring the pre-admitted stage slot to the
// exact physical object allocation.
func (s *PGSandboxStore) PrepareRootFSObjectPublish(
	ctx context.Context,
	stageID string,
	state *SandboxRootFSState,
	notBefore time.Time,
	quotaStore teamquota.CapacityTxStore,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("postgres sandbox store is required for rootfs quota admission")
	}
	if quotaStore == nil {
		return fmt.Errorf("transactional team quota store is required for rootfs publish")
	}
	if err := validateRootFSState(state); err != nil {
		return err
	}
	if state.DiffSize < 0 {
		return fmt.Errorf("rootfs diff size must be non-negative")
	}
	stageID = strings.TrimSpace(stageID)
	if stageID == "" {
		return fmt.Errorf("rootfs publish stage_id is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs object publish tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := queueUncommittedRootFSObjectDeletionTx(ctx, tx, state, notBefore); err != nil {
		return err
	}
	stage, err := lockRootFSPublishStageTx(ctx, tx, stageID)
	if err != nil {
		return err
	}
	if stage.TeamID != strings.TrimSpace(state.TeamID) ||
		stage.SandboxID != strings.TrimSpace(state.SandboxID) ||
		stage.RuntimeGeneration != state.RuntimeGeneration {
		return fmt.Errorf("rootfs publish stage %q does not match prepared object ownership", stageID)
	}
	owner, err := rootFSObjectQuotaOwner(state.TeamID, state.DiffObjectKey)
	if err != nil {
		return err
	}
	stageOwner, err := rootFSPublishStageQuotaOwner(stage.TeamID, stage.StageID)
	if err != nil {
		return err
	}
	operation := rootFSPublishStageQuotaOperation(rootFSPublishTransferOperationKind, stage.StageID)
	if _, err := quotaStore.TransferTargetTx(ctx, tx, teamquota.TransferRequest{
		Source:      stageOwner,
		Destination: owner,
		Operation:   operation,
		SourceDecrease: teamquota.Values{
			teamquota.KeyStorageObjectCount: 1,
		},
		DestinationTarget: teamquota.Values{
			teamquota.KeyRootFSStorageBytes: state.DiffSize,
			teamquota.KeyStorageObjectCount: 1,
		},
		// Rootfs objects have durable logical ownership but no Kubernetes
		// runtime identity. Keeping this empty allows physical-delete
		// confirmation to release the final allocation without inventing a
		// runtime reference that cannot be reconstructed from object metadata.
		Runtime: teamquota.RuntimeRef{},
	}); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_publish_stages
		WHERE stage_id = $1
	`, stage.StageID)
	if err != nil {
		return fmt.Errorf("delete transferred rootfs publish stage: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("rootfs publish stage %q disappeared during transfer", stage.StageID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs object publish tx: %w", err)
	}
	return nil
}

// ListRootFSObjectInventory returns the physical object ledger, including
// tombstones whose quota target must be reconciled to zero.
func (s *PGSandboxStore) ListRootFSObjectInventory(
	ctx context.Context,
	teamID string,
) ([]RootFSObjectInventory, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT object_key, team_id, diff_size, deleted_at IS NOT NULL
		FROM manager.rootfs_objects
		WHERE ($1 = '' OR team_id = $1)
		ORDER BY team_id, object_key
	`, strings.TrimSpace(teamID))
	if err != nil {
		return nil, fmt.Errorf("list rootfs object inventory: %w", err)
	}
	defer rows.Close()

	var inventory []RootFSObjectInventory
	for rows.Next() {
		var object RootFSObjectInventory
		if err := rows.Scan(
			&object.ObjectKey,
			&object.TeamID,
			&object.SizeBytes,
			&object.Deleted,
		); err != nil {
			return nil, fmt.Errorf("scan rootfs object inventory: %w", err)
		}
		inventory = append(inventory, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs object inventory: %w", err)
	}
	return inventory, nil
}

// ReconcileRootFSObjectQuota adopts the durable rootfs object inventory into
// quota state. Active objects retain their physical size; tombstones target
// zero.
func ReconcileRootFSObjectQuota(
	ctx context.Context,
	store *PGSandboxStore,
	quotaStore teamquota.CapacityTxStore,
) error {
	if store == nil {
		return fmt.Errorf("postgres sandbox store is required for rootfs quota reconciliation")
	}
	if quotaStore == nil {
		return fmt.Errorf("team quota store is required for rootfs quota reconciliation")
	}
	inventory, err := store.ListRootFSObjectInventory(ctx, "")
	if err != nil {
		return err
	}
	for _, object := range inventory {
		if err := store.reconcileRootFSObjectQuota(ctx, object.ObjectKey, quotaStore); err != nil {
			return fmt.Errorf("reconcile rootfs quota for %q: %w", object.ObjectKey, err)
		}
	}
	return nil
}

func (s *PGSandboxStore) reconcileRootFSObjectQuota(
	ctx context.Context,
	objectKey string,
	quotaStore teamquota.CapacityTxStore,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs object quota reconciliation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var object RootFSObjectInventory
	if err := tx.QueryRow(ctx, `
		SELECT object_key, team_id, diff_size, deleted_at IS NOT NULL
		FROM manager.rootfs_objects
		WHERE object_key = $1
		FOR UPDATE
	`, strings.TrimSpace(objectKey)).Scan(
		&object.ObjectKey,
		&object.TeamID,
		&object.SizeBytes,
		&object.Deleted,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("lock rootfs object inventory: %w", err)
	}

	owner, err := rootFSObjectQuotaOwner(object.TeamID, object.ObjectKey)
	if err != nil {
		return err
	}
	target := teamquota.Values{
		teamquota.KeyRootFSStorageBytes: 0,
		teamquota.KeyStorageObjectCount: 0,
	}
	if !object.Deleted {
		target[teamquota.KeyRootFSStorageBytes] = object.SizeBytes
		target[teamquota.KeyStorageObjectCount] = 1
	}
	if err := quotaStore.ReconcileTargetTx(ctx, tx, owner, target, teamquota.RuntimeRef{}); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs object quota reconciliation tx: %w", err)
	}
	return nil
}

// PruneDeletedRootFSObjectTombstones bounds completed physical-object
// inventory. Rows that still protect a layer, cleanup retry, or live quota
// allocation remain durable recovery evidence.
func (s *PGSandboxStore) PruneDeletedRootFSObjectTombstones(
	ctx context.Context,
	teamID string,
	limit int,
) (int, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	if limit <= 0 {
		limit = rootFSObjectTombstonePruneBatchSize
	}
	if limit > maxRootFSObjectTombstonePruneBatchSize {
		limit = maxRootFSObjectTombstonePruneBatchSize
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, fmt.Errorf("begin rootfs object tombstone retention tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	pruned, err := pruneDeletedRootFSObjectTombstonesTx(
		ctx,
		tx,
		strings.TrimSpace(teamID),
		limit,
	)
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit rootfs object tombstone retention tx: %w", err)
	}
	return pruned, nil
}

func pruneDeletedRootFSObjectTombstonesTx(
	ctx context.Context,
	tx pgx.Tx,
	teamID string,
	limit int,
) (int, error) {
	if tx == nil {
		return 0, fmt.Errorf("rootfs object tombstone retention transaction is required")
	}
	var quotaAllocationsAvailable bool
	if err := tx.QueryRow(ctx, `
		SELECT to_regclass('quota.allocations') IS NOT NULL
	`).Scan(&quotaAllocationsAvailable); err != nil {
		return 0, fmt.Errorf("inspect rootfs quota allocation catalog: %w", err)
	}
	if !quotaAllocationsAvailable {
		return 0, nil
	}

	tag, err := tx.Exec(ctx, `
		WITH eligible AS (
			SELECT o.object_key, o.team_id, o.deleted_at
			FROM manager.rootfs_objects o
			WHERE o.deleted_at IS NOT NULL
				AND ($1 = '' OR o.team_id = $1)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_layers l
					WHERE l.diff_object_key = o.object_key
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_object_deletions q
					WHERE q.object_key = o.object_key
				)
				AND NOT EXISTS (
					SELECT 1
					FROM quota.allocations a
					WHERE a.team_id = o.team_id
						AND a.owner_kind = $2
						AND a.owner_id = o.object_key
						AND a.state <> 'released'
				)
		),
		ranked AS (
			SELECT object_key, team_id, deleted_at,
				ROW_NUMBER() OVER (
					PARTITION BY team_id
					ORDER BY deleted_at DESC, object_key
				) AS retention_rank
			FROM eligible
		),
		candidates AS (
			SELECT object_key, team_id, deleted_at
			FROM ranked
			WHERE deleted_at < NOW() - $3::interval
				OR retention_rank > $4
			ORDER BY deleted_at, team_id, object_key
			LIMIT $5
		),
		locked AS (
			SELECT o.object_key
			FROM manager.rootfs_objects o
			JOIN candidates c
				ON c.object_key = o.object_key
				AND c.team_id = o.team_id
			WHERE o.deleted_at IS NOT NULL
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_layers l
					WHERE l.diff_object_key = o.object_key
				)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.rootfs_object_deletions q
					WHERE q.object_key = o.object_key
				)
				AND NOT EXISTS (
					SELECT 1
					FROM quota.allocations a
					WHERE a.team_id = o.team_id
						AND a.owner_kind = $2
						AND a.owner_id = o.object_key
						AND a.state <> 'released'
				)
			ORDER BY c.deleted_at, c.team_id, c.object_key
			FOR UPDATE OF o SKIP LOCKED
		)
		DELETE FROM manager.rootfs_objects o
		USING locked
		WHERE o.object_key = locked.object_key
	`, strings.TrimSpace(teamID), rootFSObjectQuotaOwnerKind,
		postgresDuration(rootFSObjectTombstoneRetention),
		maxRootFSObjectTombstonesPerTeam, limit)
	if err != nil {
		return 0, fmt.Errorf("prune deleted rootfs object tombstones: %w", err)
	}
	return int(tag.RowsAffected()), nil
}

// ReconcileRootFSSnapshotQuota adopts every durable rootfs snapshot into the
// shared storage object-count ledger before manager accepts traffic.
func ReconcileRootFSSnapshotQuota(
	ctx context.Context,
	store *PGSandboxStore,
	quotaStore teamquota.CapacityTxStore,
) error {
	if store == nil || store.pool == nil {
		return fmt.Errorf("postgres sandbox store is required for rootfs snapshot quota reconciliation")
	}
	if quotaStore == nil {
		return fmt.Errorf("team quota store is required for rootfs snapshot quota reconciliation")
	}
	rows, err := store.pool.Query(ctx, `
		SELECT snapshot_id, team_id
		FROM manager.rootfs_snapshots
		ORDER BY team_id, snapshot_id
	`)
	if err != nil {
		return fmt.Errorf("list rootfs snapshot inventory: %w", err)
	}

	var inventory []RootFSSnapshotInventory
	for rows.Next() {
		var snapshot RootFSSnapshotInventory
		if err := rows.Scan(&snapshot.SnapshotID, &snapshot.TeamID); err != nil {
			rows.Close()
			return fmt.Errorf("scan rootfs snapshot inventory: %w", err)
		}
		inventory = append(inventory, snapshot)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate rootfs snapshot inventory: %w", err)
	}
	rows.Close()

	for _, snapshot := range inventory {
		if err := store.reconcileRootFSSnapshotQuota(ctx, snapshot.SnapshotID, quotaStore); err != nil {
			return fmt.Errorf("reconcile rootfs snapshot quota for %q: %w", snapshot.SnapshotID, err)
		}
	}
	return nil
}

func (s *PGSandboxStore) reconcileRootFSSnapshotQuota(
	ctx context.Context,
	snapshotID string,
	quotaStore teamquota.CapacityTxStore,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs snapshot quota reconciliation tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var snapshot RootFSSnapshotInventory
	if err := tx.QueryRow(ctx, `
		SELECT snapshot_id, team_id
		FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1
		FOR UPDATE
	`, strings.TrimSpace(snapshotID)).Scan(&snapshot.SnapshotID, &snapshot.TeamID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("lock rootfs snapshot inventory: %w", err)
	}
	owner, err := rootFSSnapshotQuotaOwner(snapshot.TeamID, snapshot.SnapshotID)
	if err != nil {
		return err
	}
	if err := quotaStore.ReconcileTargetTx(
		ctx,
		tx,
		owner,
		teamquota.Values{teamquota.KeyStorageObjectCount: 1},
		teamquota.RuntimeRef{},
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs snapshot quota reconciliation tx: %w", err)
	}
	return nil
}

func reserveRootFSSnapshotQuotaTx(
	ctx context.Context,
	tx pgx.Tx,
	quotaStore teamquota.CapacityTxStore,
	teamID string,
	snapshotID string,
) error {
	if quotaStore == nil {
		return fmt.Errorf("transactional team quota store is required for rootfs snapshot creation")
	}
	owner, err := rootFSSnapshotQuotaOwner(teamID, snapshotID)
	if err != nil {
		return err
	}
	operation := rootFSSnapshotQuotaOperation(rootFSSnapshotCreateOperationKind, snapshotID)
	if _, err := quotaStore.ReserveTargetTx(ctx, tx, teamquota.ReserveRequest{
		Owner:     owner,
		Operation: operation,
		Target:    teamquota.Values{teamquota.KeyStorageObjectCount: 1},
	}); err != nil {
		return err
	}
	return quotaStore.CommitTx(ctx, tx, teamquota.Ref(owner, operation))
}

func releaseRootFSSnapshotQuotaTx(
	ctx context.Context,
	tx pgx.Tx,
	quotaStore teamquota.CapacityTxStore,
	teamID string,
	snapshotID string,
) error {
	if quotaStore == nil {
		return fmt.Errorf("transactional team quota store is required for rootfs snapshot deletion")
	}
	owner, err := rootFSSnapshotQuotaOwner(teamID, snapshotID)
	if err != nil {
		return err
	}
	operation := rootFSSnapshotQuotaOperation(rootFSSnapshotDeleteOperationKind, snapshotID)
	reservation, err := quotaStore.BeginReleaseTx(ctx, tx, teamquota.ReleaseRequest{
		Owner:     owner,
		Operation: operation,
		Target:    teamquota.Values{teamquota.KeyStorageObjectCount: 0},
	})
	if err != nil {
		return err
	}
	return quotaStore.ConfirmReleaseTx(
		ctx,
		tx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		teamquota.RuntimeRef{},
	)
}

// DeleteRootFSSnapshotWithQuota removes snapshot metadata and releases its
// object-count allocation in one PostgreSQL transaction.
func (s *PGSandboxStore) DeleteRootFSSnapshotWithQuota(
	ctx context.Context,
	snapshotID string,
	teamID string,
	quotaStore teamquota.CapacityTxStore,
) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("postgres sandbox store is required for rootfs snapshot deletion")
	}
	if quotaStore == nil {
		return fmt.Errorf("transactional team quota store is required for rootfs snapshot deletion")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs snapshot deletion tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := deleteRootFSSnapshotWithQuotaTx(
		ctx,
		tx,
		quotaStore,
		strings.TrimSpace(snapshotID),
		strings.TrimSpace(teamID),
	); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs snapshot deletion tx: %w", err)
	}
	return nil
}

func deleteRootFSSnapshotWithQuotaTx(
	ctx context.Context,
	tx pgx.Tx,
	quotaStore teamquota.CapacityTxStore,
	snapshotID string,
	teamID string,
) error {
	if tx == nil {
		return fmt.Errorf("rootfs snapshot deletion transaction is required")
	}
	if snapshotID == "" {
		return fmt.Errorf("snapshot_id is required")
	}
	var actualTeamID string
	if err := tx.QueryRow(ctx, `
		SELECT team_id
		FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1
			AND ($2 = '' OR team_id = $2)
		FOR UPDATE
	`, snapshotID, teamID).Scan(&actualTeamID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, snapshotID)
		}
		return fmt.Errorf("lock rootfs snapshot for deletion: %w", err)
	}
	if err := releaseRootFSSnapshotQuotaTx(
		ctx,
		tx,
		quotaStore,
		actualTeamID,
		snapshotID,
	); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_snapshots
		WHERE snapshot_id = $1 AND team_id = $2
	`, snapshotID, actualTeamID)
	if err != nil {
		return fmt.Errorf("delete rootfs snapshot: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: %s", ErrRootFSSnapshotNotFound, snapshotID)
	}
	return nil
}

func releaseRootFSObjectQuotaTx(
	ctx context.Context,
	tx pgx.Tx,
	quotaStore teamquota.CapacityTxStore,
	object RootFSObjectInventory,
) error {
	if quotaStore == nil {
		return fmt.Errorf("transactional team quota store is required for rootfs object deletion")
	}
	owner, err := rootFSObjectQuotaOwner(object.TeamID, object.ObjectKey)
	if err != nil {
		return err
	}
	operation := rootFSObjectQuotaOperation(rootFSObjectDeleteOperationKind, object.ObjectKey)
	operation.ID += ":" + uuid.NewString()
	reservation, err := quotaStore.BeginReleaseTx(ctx, tx, teamquota.ReleaseRequest{
		Owner:     owner,
		Operation: operation,
		Target: teamquota.Values{
			teamquota.KeyRootFSStorageBytes: 0,
			teamquota.KeyStorageObjectCount: 0,
		},
	})
	if err != nil {
		return fmt.Errorf("begin rootfs quota release for %q: %w", object.ObjectKey, err)
	}
	if err := quotaStore.ConfirmReleaseTx(
		ctx,
		tx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		teamquota.RuntimeRef{},
	); err != nil {
		return fmt.Errorf("confirm rootfs quota release for %q: %w", object.ObjectKey, err)
	}
	return nil
}

// deleteClaimedRootFSObject serializes physical deletion with publish and layer
// commits by holding the object and cleanup-queue row locks across the object
// store call. Quota is released only after the object store confirms deletion.
func (s *PGSandboxStore) deleteClaimedRootFSObject(
	ctx context.Context,
	item claimedRootFSObjectDeletion,
	claimedBy string,
	deleter RootFSObjectDeleter,
	quotaStore teamquota.CapacityTxStore,
) (completed bool, physicalDeleteConfirmed bool, err error) {
	if s == nil || s.pool == nil {
		return false, false, fmt.Errorf("postgres sandbox store is required for rootfs object deletion")
	}
	if deleter == nil {
		return false, false, fmt.Errorf("rootfs object deleter is required")
	}
	claimedBy = strings.TrimSpace(claimedBy)
	if claimedBy == "" {
		return false, false, fmt.Errorf("rootfs object deletion claimant is required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, false, fmt.Errorf("begin rootfs object deletion transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	object := RootFSObjectInventory{ObjectKey: strings.TrimSpace(item.ObjectKey)}
	if err := tx.QueryRow(ctx, `
		SELECT team_id, diff_size, deleted_at IS NOT NULL
		FROM manager.rootfs_objects
		WHERE object_key = $1
		FOR UPDATE
	`, object.ObjectKey).Scan(
		&object.TeamID,
		&object.SizeBytes,
		&object.Deleted,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, fmt.Errorf(
				"rootfs object inventory %q is missing for claimed physical deletion",
				object.ObjectKey,
			)
		}
		return false, false, fmt.Errorf("lock rootfs object deletion inventory: %w", err)
	}
	var queueObjectKey string
	if err := tx.QueryRow(ctx, `
		SELECT object_key
		FROM manager.rootfs_object_deletions
		WHERE object_key = $1
			AND claimed_by = $2
			FOR UPDATE
	`, object.ObjectKey, claimedBy).Scan(&queueObjectKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, false, nil
		}
		return false, false, fmt.Errorf("lock claimed rootfs object deletion: %w", err)
	}

	var refs rootFSObjectReferenceState
	if err := tx.QueryRow(ctx, `
		SELECT
			EXISTS (
				SELECT 1
				FROM manager.rootfs_layers
				WHERE diff_object_key = $1
			),
			EXISTS (
				SELECT 1
				FROM manager.rootfs_objects o
				JOIN manager.sandbox_lifecycle_txns t
					ON t.prepared_head_layer_id = o.first_layer_id
				WHERE o.object_key = $1
					AND t.phase IN ('preparing', 'barriered', 'publishing', 'committing')
					AND (
						t.kind = 'pause'
						OR t.updated_at >= NOW() - ($2::int * INTERVAL '1 second')
					)
			)
	`, object.ObjectKey, durationSeconds(sandboxRootFSSourceCheckpointLifecycleStaleAfter)).Scan(
		&refs.HasLayerReferences,
		&refs.HasActiveLifecycleReferences,
	); err != nil {
		return false, false, fmt.Errorf("recheck rootfs object references for %q: %w", object.ObjectKey, err)
	}
	if refs.HasLayerReferences {
		tag, err := tx.Exec(ctx, `
			DELETE FROM manager.rootfs_object_deletions
			WHERE object_key = $1
				AND claimed_by = $2
		`, object.ObjectKey, claimedBy)
		if err != nil {
			return false, false, fmt.Errorf("clear referenced rootfs object deletion %q: %w", object.ObjectKey, err)
		}
		if tag.RowsAffected() != 1 {
			return false, false, fmt.Errorf("rootfs object deletion claim for %q was lost", object.ObjectKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, false, fmt.Errorf("commit referenced rootfs object deletion cleanup: %w", err)
		}
		return false, false, nil
	}
	if refs.HasActiveLifecycleReferences {
		tag, err := tx.Exec(ctx, `
			UPDATE manager.rootfs_object_deletions
			SET claimed_by = '',
				claimed_until = NULL,
				next_attempt_at = NOW() + ($3::int * INTERVAL '1 second'),
				updated_at = NOW()
			WHERE object_key = $1
				AND claimed_by = $2
		`, object.ObjectKey, claimedBy, durationSeconds(rootFSObjectActiveLifecycleRetry))
		if err != nil {
			return false, false, fmt.Errorf("defer referenced rootfs object deletion %q: %w", object.ObjectKey, err)
		}
		if tag.RowsAffected() != 1 {
			return false, false, fmt.Errorf("rootfs object deletion claim for %q was lost", object.ObjectKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return false, false, fmt.Errorf("commit referenced rootfs object deletion deferral: %w", err)
		}
		return false, false, nil
	}

	if err := deleter.Delete(object.ObjectKey); err != nil {
		return false, false, fmt.Errorf("delete rootfs object %q: %w", object.ObjectKey, err)
	}

	if quotaStore != nil {
		if err := releaseRootFSObjectQuotaTx(ctx, tx, quotaStore, object); err != nil {
			return false, true, err
		}
	}
	tag, err := tx.Exec(ctx, `
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
	`, object.ObjectKey)
	if err != nil {
		return false, true, fmt.Errorf("mark rootfs object deleted %q: %w", object.ObjectKey, err)
	}
	if tag.RowsAffected() != 1 {
		return false, true, fmt.Errorf("rootfs object %q became referenced during deletion completion", object.ObjectKey)
	}
	tag, err = tx.Exec(ctx, `
		DELETE FROM manager.rootfs_object_deletions
		WHERE object_key = $1
			AND claimed_by = $2
	`, object.ObjectKey, claimedBy)
	if err != nil {
		return false, true, fmt.Errorf("clear rootfs object deletion %q: %w", object.ObjectKey, err)
	}
	if tag.RowsAffected() != 1 {
		return false, true, fmt.Errorf("rootfs object deletion claim for %q was lost", object.ObjectKey)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, true, fmt.Errorf("commit rootfs object deletion transaction: %w", err)
	}
	return true, true, nil
}

// ListDueRootFSPublishStages returns stages whose ctld expiry and conservative
// cleanup grace have elapsed. Quota remains charged until recovery remotely
// confirms the stable ctld handle is absent.
func (s *PGSandboxStore) ListDueRootFSPublishStages(
	ctx context.Context,
	limit int,
) ([]RootFSPublishStage, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("postgres sandbox store is required for rootfs publish stage recovery")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `
		SELECT stage_id, team_id, sandbox_id, ctld_address,
			runtime_generation, expires_at, release_after
		FROM manager.rootfs_publish_stages
		WHERE release_after <= NOW()
		ORDER BY release_after ASC, created_at ASC
		LIMIT $1
	`, limit)
	if err != nil {
		return nil, fmt.Errorf("list due rootfs publish stages: %w", err)
	}
	defer rows.Close()
	var stages []RootFSPublishStage
	for rows.Next() {
		var stage RootFSPublishStage
		if err := rows.Scan(
			&stage.StageID,
			&stage.TeamID,
			&stage.SandboxID,
			&stage.CtldAddress,
			&stage.RuntimeGeneration,
			&stage.ExpiresAt,
			&stage.ReleaseAfter,
		); err != nil {
			return nil, fmt.Errorf("scan due rootfs publish stage: %w", err)
		}
		stages = append(stages, stage)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate due rootfs publish stages: %w", err)
	}
	return stages, nil
}

// ReleaseRootFSPublishStage releases a pre-admitted slot only after the caller
// has confirmed the stable ctld stage handle is absent.
func (s *PGSandboxStore) ReleaseRootFSPublishStage(
	ctx context.Context,
	stageID string,
	quotaStore teamquota.CapacityTxStore,
) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("postgres sandbox store is required for rootfs publish stage release")
	}
	if quotaStore == nil {
		return false, fmt.Errorf("transactional team quota store is required for rootfs publish stage release")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin rootfs publish stage release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	stage, err := lockRootFSPublishStageTx(ctx, tx, stageID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	owner, err := rootFSPublishStageQuotaOwner(stage.TeamID, stage.StageID)
	if err != nil {
		return false, err
	}
	operation := rootFSPublishStageQuotaOperation(rootFSPublishStageReleaseOperationKind, stage.StageID)
	reservation, err := quotaStore.BeginReleaseTx(ctx, tx, teamquota.ReleaseRequest{
		Owner:     owner,
		Operation: operation,
		Target: teamquota.Values{
			teamquota.KeyStorageObjectCount: 0,
		},
	})
	if err != nil {
		return false, err
	}
	if err := quotaStore.ConfirmReleaseTx(
		ctx,
		tx,
		teamquota.Ref(reservation.Owner, reservation.Operation),
		teamquota.RuntimeRef{},
	); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_publish_stages
		WHERE stage_id = $1
	`, stage.StageID)
	if err != nil {
		return false, fmt.Errorf("delete released rootfs publish stage: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("rootfs publish stage %q disappeared during release", stage.StageID)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit rootfs publish stage release: %w", err)
	}
	return true, nil
}

func lockRootFSPublishStageTx(
	ctx context.Context,
	tx pgx.Tx,
	stageID string,
) (RootFSPublishStage, error) {
	var stage RootFSPublishStage
	err := tx.QueryRow(ctx, `
		SELECT stage_id, team_id, sandbox_id, ctld_address,
			runtime_generation, expires_at, release_after
		FROM manager.rootfs_publish_stages
		WHERE stage_id = $1
		FOR UPDATE
	`, strings.TrimSpace(stageID)).Scan(
		&stage.StageID,
		&stage.TeamID,
		&stage.SandboxID,
		&stage.CtldAddress,
		&stage.RuntimeGeneration,
		&stage.ExpiresAt,
		&stage.ReleaseAfter,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RootFSPublishStage{}, pgx.ErrNoRows
		}
		return RootFSPublishStage{}, fmt.Errorf("lock rootfs publish stage %q: %w", stageID, err)
	}
	return stage, nil
}

func normalizeRootFSPublishStage(stage RootFSPublishStage) RootFSPublishStage {
	stage.StageID = strings.TrimSpace(stage.StageID)
	stage.TeamID = strings.TrimSpace(stage.TeamID)
	stage.SandboxID = strings.TrimSpace(stage.SandboxID)
	stage.CtldAddress = strings.TrimSpace(stage.CtldAddress)
	stage.ExpiresAt = stage.ExpiresAt.UTC()
	stage.ReleaseAfter = stage.ReleaseAfter.UTC()
	return stage
}

func validateRootFSPublishStage(stage RootFSPublishStage) error {
	if stage.StageID == "" {
		return fmt.Errorf("rootfs publish stage_id is required")
	}
	if stage.TeamID == "" {
		return fmt.Errorf("rootfs publish stage team_id is required")
	}
	if stage.SandboxID == "" {
		return fmt.Errorf("rootfs publish stage sandbox_id is required")
	}
	if stage.CtldAddress == "" {
		return fmt.Errorf("rootfs publish stage ctld address is required")
	}
	if stage.RuntimeGeneration < 0 {
		return fmt.Errorf("rootfs publish stage runtime generation must be non-negative")
	}
	if stage.ExpiresAt.IsZero() {
		return fmt.Errorf("rootfs publish stage expiry is required")
	}
	if stage.ReleaseAfter.Before(stage.ExpiresAt) {
		return fmt.Errorf("rootfs publish stage release_after cannot precede expiry")
	}
	return nil
}

func rootFSObjectQuotaOwner(teamID, objectKey string) (teamquota.Owner, error) {
	owner := teamquota.Owner{
		TeamID: strings.TrimSpace(teamID),
		Kind:   rootFSObjectQuotaOwnerKind,
		ID:     strings.TrimSpace(objectKey),
	}
	if err := owner.Validate(); err != nil {
		return teamquota.Owner{}, fmt.Errorf("invalid rootfs quota owner: %w", err)
	}
	return owner, nil
}

func rootFSPublishStageQuotaOwner(teamID, stageID string) (teamquota.Owner, error) {
	owner := teamquota.Owner{
		TeamID: strings.TrimSpace(teamID),
		Kind:   rootFSPublishStageQuotaOwnerKind,
		ID:     strings.TrimSpace(stageID),
	}
	if err := owner.Validate(); err != nil {
		return teamquota.Owner{}, fmt.Errorf("invalid rootfs publish stage quota owner: %w", err)
	}
	return owner, nil
}

func rootFSPublishStageQuotaOperation(kind, stageID string) teamquota.Operation {
	sum := sha256.Sum256([]byte(strings.TrimSpace(stageID)))
	return teamquota.Operation{
		ID:   kind + ":" + hex.EncodeToString(sum[:]),
		Kind: kind,
	}
}

func rootFSObjectQuotaOperation(kind, objectKey string) teamquota.Operation {
	sum := sha256.Sum256([]byte(strings.TrimSpace(objectKey)))
	return teamquota.Operation{
		ID:   kind + ":" + hex.EncodeToString(sum[:]),
		Kind: kind,
	}
}

func rootFSSnapshotQuotaOwner(teamID, snapshotID string) (teamquota.Owner, error) {
	owner := teamquota.Owner{
		TeamID: strings.TrimSpace(teamID),
		Kind:   rootFSSnapshotQuotaOwnerKind,
		ID:     strings.TrimSpace(snapshotID),
	}
	if err := owner.Validate(); err != nil {
		return teamquota.Owner{}, fmt.Errorf("invalid rootfs snapshot quota owner: %w", err)
	}
	return owner, nil
}

func rootFSSnapshotQuotaOperation(kind, snapshotID string) teamquota.Operation {
	sum := sha256.Sum256([]byte(strings.TrimSpace(snapshotID)))
	return teamquota.Operation{
		ID:   kind + ":" + hex.EncodeToString(sum[:]),
		Kind: kind,
	}
}
