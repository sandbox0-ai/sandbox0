package outbox

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

const claimLockID int64 = 0x6d65746572696e67

type db interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// Repository captures metering mutations transactionally in PostgreSQL.
// ClickHouse delivery is intentionally handled by Projector.
type Repository struct {
	pool *pgxpool.Pool
	now  func() time.Time
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		pool: pool,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

func (r *Repository) Pool() *pgxpool.Pool {
	if r == nil {
		return nil
	}
	return r.pool
}

func (r *Repository) InTx(ctx context.Context, fn func(pgx.Tx) error) error {
	if fn == nil {
		return nil
	}
	if r == nil || r.pool == nil {
		return fmt.Errorf("metering outbox pool is not configured")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin metering outbox transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit metering outbox transaction: %w", err)
	}
	return nil
}

func (r *Repository) AppendEvent(ctx context.Context, event *metering.Event) error {
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.AppendEventTx(ctx, tx, event)
	})
}

func (r *Repository) AppendEventTx(ctx context.Context, tx pgx.Tx, event *metering.Event) error {
	if tx == nil {
		return fmt.Errorf("metering event transaction is nil")
	}
	if err := validateEvent(event); err != nil {
		return err
	}
	if event.RecordedAt.IsZero() {
		event.RecordedAt = r.timestamp()
	} else {
		event.RecordedAt = event.RecordedAt.UTC()
	}
	return enqueue(ctx, tx, OperationEvent, event.EventID, event)
}

func (r *Repository) AppendWindow(ctx context.Context, window *metering.Window) error {
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.AppendWindowTx(ctx, tx, window)
	})
}

func (r *Repository) AppendWindowTx(ctx context.Context, tx pgx.Tx, window *metering.Window) error {
	if tx == nil {
		return fmt.Errorf("metering window transaction is nil")
	}
	if err := validateWindow(window); err != nil {
		return err
	}
	if window.RecordedAt.IsZero() {
		window.RecordedAt = r.timestamp()
	} else {
		window.RecordedAt = window.RecordedAt.UTC()
	}
	return enqueue(ctx, tx, OperationWindow, window.WindowID, window)
}

func (r *Repository) UpsertProducerWatermark(ctx context.Context, producer, regionID string, completeBefore time.Time) error {
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.UpsertProducerWatermarkTx(ctx, tx, producer, regionID, completeBefore)
	})
}

func (r *Repository) UpsertProducerWatermarkTx(ctx context.Context, tx pgx.Tx, producer, regionID string, completeBefore time.Time) error {
	if tx == nil {
		return fmt.Errorf("metering watermark transaction is nil")
	}
	if strings.TrimSpace(producer) == "" {
		return fmt.Errorf("producer is required")
	}
	if completeBefore.IsZero() {
		return fmt.Errorf("complete_before is required")
	}
	operation := &WatermarkOperation{
		Producer:       producer,
		RegionID:       regionID,
		CompleteBefore: completeBefore.UTC(),
	}
	key := fmt.Sprintf("%s/%s/%d", producer, regionID, completeBefore.UTC().UnixNano())
	return enqueue(ctx, tx, OperationWatermark, key, operation)
}

func (r *Repository) GetSandboxProjectionState(ctx context.Context, sandboxID string) (*metering.SandboxProjectionState, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("metering outbox pool is not configured")
	}
	return getSandboxProjectionState(ctx, r.pool, sandboxID, false)
}

func (r *Repository) GetSandboxProjectionStateTx(ctx context.Context, tx pgx.Tx, sandboxID string) (*metering.SandboxProjectionState, error) {
	if tx == nil {
		return nil, fmt.Errorf("metering sandbox state transaction is nil")
	}
	return getSandboxProjectionState(ctx, tx, sandboxID, true)
}

func (r *Repository) UpsertSandboxProjectionState(ctx context.Context, state *metering.SandboxProjectionState) error {
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.UpsertSandboxProjectionStateTx(ctx, tx, state)
	})
}

func (r *Repository) UpsertSandboxProjectionStateTx(ctx context.Context, tx pgx.Tx, state *metering.SandboxProjectionState) error {
	if tx == nil {
		return fmt.Errorf("metering sandbox state transaction is nil")
	}
	if state == nil {
		return fmt.Errorf("sandbox projection state is nil")
	}
	if state.SandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if state.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if state.LastObservedAt.IsZero() {
		state.LastObservedAt = r.timestamp()
	} else {
		state.LastObservedAt = state.LastObservedAt.UTC()
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO metering.manager_sandbox_projection_state (
			sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
			owner_kind, resource_millicpu, resource_memory_mib,
			claimed_at, active_since, paused, paused_at, terminated_at,
			last_observed_at, last_resource_version
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9,
			$10, $11, $12, $13, $14,
			$15, $16
		)
		ON CONFLICT (sandbox_id) DO UPDATE
		SET namespace = EXCLUDED.namespace,
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			template_id = EXCLUDED.template_id,
			cluster_id = EXCLUDED.cluster_id,
			owner_kind = EXCLUDED.owner_kind,
			resource_millicpu = EXCLUDED.resource_millicpu,
			resource_memory_mib = EXCLUDED.resource_memory_mib,
			claimed_at = EXCLUDED.claimed_at,
			active_since = EXCLUDED.active_since,
			paused = EXCLUDED.paused,
			paused_at = EXCLUDED.paused_at,
			terminated_at = EXCLUDED.terminated_at,
			last_observed_at = EXCLUDED.last_observed_at,
			last_resource_version = EXCLUDED.last_resource_version
	`, state.SandboxID, state.Namespace, state.TeamID, state.UserID, state.TemplateID, state.ClusterID,
		state.OwnerKind, state.ResourceMillicpu, state.ResourceMemoryMiB,
		state.ClaimedAt, state.ActiveSince, state.Paused, state.PausedAt, state.TerminatedAt,
		state.LastObservedAt, state.LastResourceVer,
	)
	if err != nil {
		return fmt.Errorf("upsert sandbox projection state: %w", err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal sandbox projection state: %w", err)
	}
	key := versionedKey(state.SandboxID, state.LastObservedAt, payload)
	return enqueueJSON(ctx, tx, OperationSandboxState, key, payload)
}

func (r *Repository) RecordStorageObservation(ctx context.Context, observation *metering.StorageObservation) error {
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.RecordStorageObservationTx(ctx, tx, observation)
	})
}

func (r *Repository) RecordStorageObservationTx(ctx context.Context, tx pgx.Tx, observation *metering.StorageObservation) error {
	if tx == nil {
		return fmt.Errorf("metering storage observation transaction is nil")
	}
	state, err := r.normalizeStorageObservation(ctx, tx, observation)
	if err != nil {
		return err
	}
	previous, err := getStorageProjectionStateForUpdate(ctx, tx, state.SubjectType, state.SubjectID)
	if err != nil {
		return err
	}
	if previous != nil {
		if state.ObservedAt.Before(previous.ObservedAt) {
			return nil
		}
		window, remainder := metering.StorageWindowFromState(previous, state.ObservedAt)
		if window != nil {
			if err := r.AppendWindowTx(ctx, tx, window); err != nil {
				return err
			}
		}
		state.UnbilledByteNanoseconds = remainder
	}
	return r.upsertStorageProjectionState(ctx, tx, state)
}

func (r *Repository) CloseStorageObservation(ctx context.Context, observation *metering.StorageObservation) error {
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.CloseStorageObservationTx(ctx, tx, observation)
	})
}

func (r *Repository) CloseStorageObservationTx(ctx context.Context, tx pgx.Tx, observation *metering.StorageObservation) error {
	if tx == nil {
		return fmt.Errorf("metering storage close transaction is nil")
	}
	state, err := r.normalizeStorageObservation(ctx, tx, observation)
	if err != nil {
		return err
	}
	previous, err := getStorageProjectionStateForUpdate(ctx, tx, state.SubjectType, state.SubjectID)
	if err != nil {
		return err
	}
	if previous == nil {
		if observation.ResourceCreatedAt.IsZero() || !state.ObservedAt.After(observation.ResourceCreatedAt) {
			return nil
		}
		end := state.ObservedAt
		state.ObservedAt = observation.ResourceCreatedAt.UTC()
		if window, _ := metering.StorageWindowFromState(state, end); window != nil {
			return r.AppendWindowTx(ctx, tx, window)
		}
		return nil
	}
	if state.ObservedAt.Before(previous.ObservedAt) {
		return r.deleteStorageProjectionState(ctx, tx, previous, state.ObservedAt)
	}
	if window, _ := metering.StorageWindowFromState(previous, state.ObservedAt); window != nil {
		if err := r.AppendWindowTx(ctx, tx, window); err != nil {
			return err
		}
	}
	return r.deleteStorageProjectionState(ctx, tx, previous, state.ObservedAt)
}

func (r *Repository) FlushStorageProjectionWindows(ctx context.Context, before time.Time, limit int) (int, error) {
	if before.IsZero() {
		before = r.timestamp()
	} else {
		before = before.UTC()
	}
	if limit <= 0 {
		limit = 500
	}
	processed := 0
	err := r.InTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT
				subject_type, subject_id, product, owner_kind,
				team_id, user_id, COALESCE(sandbox_id, ''), COALESCE(volume_id, ''),
				COALESCE(snapshot_id, ''), COALESCE(cluster_id, ''), region_id,
				size_bytes, observed_at, unbilled_byte_nanoseconds
			FROM metering.storage_projection_state
			WHERE observed_at < $1
			ORDER BY observed_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		`, before, limit)
		if err != nil {
			return fmt.Errorf("query storage projection states: %w", err)
		}
		defer rows.Close()
		states := make([]*metering.StorageProjectionState, 0, limit)
		for rows.Next() {
			state := &metering.StorageProjectionState{}
			if err := scanStorageState(rows, state); err != nil {
				return fmt.Errorf("scan storage projection state: %w", err)
			}
			states = append(states, state)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate storage projection states: %w", err)
		}
		for _, state := range states {
			window, remainder := metering.StorageWindowFromState(state, before)
			if window != nil {
				if err := r.AppendWindowTx(ctx, tx, window); err != nil {
					return err
				}
			}
			state.ObservedAt = before
			state.UnbilledByteNanoseconds = remainder
			if err := r.upsertStorageProjectionState(ctx, tx, state); err != nil {
				return err
			}
		}
		processed = len(states)
		return nil
	})
	return processed, err
}

func (r *Repository) ClaimNextBatch(ctx context.Context, workerID string, lease time.Duration) (*Batch, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("metering outbox pool is not configured")
	}
	if strings.TrimSpace(workerID) == "" {
		return nil, fmt.Errorf("worker_id is required")
	}
	if lease <= 0 {
		lease = 30 * time.Second
	}
	var batch *Batch
	err := r.InTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, claimLockID); err != nil {
			return fmt.Errorf("lock metering outbox claim: %w", err)
		}
		var batchID int64
		var availableAt time.Time
		var claimExpiresAt *time.Time
		err := tx.QueryRow(ctx, `
			SELECT batch_id, available_at, claim_expires_at
			FROM metering.projection_outbox
			WHERE delivered_at IS NULL
			ORDER BY sequence ASC
			LIMIT 1
			FOR UPDATE
		`).Scan(&batchID, &availableAt, &claimExpiresAt)
		if err == pgx.ErrNoRows {
			return nil
		}
		if err != nil {
			return fmt.Errorf("select oldest metering outbox batch: %w", err)
		}
		now := r.timestamp()
		if availableAt.After(now) || (claimExpiresAt != nil && claimExpiresAt.After(now)) {
			return nil
		}
		rows, err := tx.Query(ctx, `
			UPDATE metering.projection_outbox
			SET claimed_by = $2,
				claim_expires_at = $3,
				attempts = attempts + 1
			WHERE batch_id = $1 AND delivered_at IS NULL
			RETURNING sequence, batch_id, operation_type, dedupe_key, payload,
				attempts, created_at, claim_expires_at
		`, batchID, workerID, now.Add(lease))
		if err != nil {
			return fmt.Errorf("claim metering outbox batch: %w", err)
		}
		defer rows.Close()
		operations := make([]*Operation, 0, 4)
		for rows.Next() {
			operation := &Operation{}
			if err := rows.Scan(
				&operation.Sequence, &operation.BatchID, &operation.Type,
				&operation.DedupeKey, &operation.Payload, &operation.Attempts,
				&operation.CreatedAt, &operation.ClaimExpiresAt,
			); err != nil {
				return fmt.Errorf("scan claimed metering operation: %w", err)
			}
			operations = append(operations, operation)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate claimed metering operations: %w", err)
		}
		if len(operations) > 0 {
			sort.Slice(operations, func(i, j int) bool {
				return operations[i].Sequence < operations[j].Sequence
			})
			batch = &Batch{ID: batchID, Operations: operations}
		}
		return nil
	})
	return batch, err
}

func (r *Repository) MarkDelivered(ctx context.Context, batchID int64, workerID string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("metering outbox pool is not configured")
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE metering.projection_outbox
		SET delivered_at = NOW(),
			claimed_by = '',
			claim_expires_at = NULL,
			last_error = ''
		WHERE batch_id = $1 AND delivered_at IS NULL AND claimed_by = $2
	`, batchID, workerID)
	if err != nil {
		return fmt.Errorf("mark metering outbox batch delivered: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("metering outbox batch %d is not claimed by %q", batchID, workerID)
	}
	return nil
}

func (r *Repository) MarkFailed(ctx context.Context, batchID int64, workerID, message string, retryAt time.Time) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("metering outbox pool is not configured")
	}
	if retryAt.IsZero() {
		retryAt = r.timestamp()
	}
	_, err := r.pool.Exec(ctx, `
		UPDATE metering.projection_outbox
		SET available_at = $3,
			claimed_by = '',
			claim_expires_at = NULL,
			last_error = $4
		WHERE batch_id = $1 AND delivered_at IS NULL AND claimed_by = $2
	`, batchID, workerID, retryAt.UTC(), truncateError(message))
	if err != nil {
		return fmt.Errorf("release failed metering outbox batch: %w", err)
	}
	return nil
}

func (r *Repository) Stats(ctx context.Context) (*Stats, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("metering outbox pool is not configured")
	}
	stats := &Stats{}
	if err := r.pool.QueryRow(ctx, `
		SELECT COUNT(*), MIN(created_at)
		FROM metering.projection_outbox
		WHERE delivered_at IS NULL
	`).Scan(&stats.Pending, &stats.OldestPending); err != nil {
		return nil, fmt.Errorf("query metering outbox stats: %w", err)
	}
	return stats, nil
}

func (r *Repository) DeleteDeliveredBefore(ctx context.Context, before time.Time, limit int) (int64, error) {
	if r == nil || r.pool == nil {
		return 0, fmt.Errorf("metering outbox pool is not configured")
	}
	if before.IsZero() {
		return 0, fmt.Errorf("cleanup cutoff is required")
	}
	if limit <= 0 {
		limit = 1000
	}
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM metering.projection_outbox
		WHERE sequence IN (
			SELECT sequence
			FROM metering.projection_outbox
			WHERE delivered_at < $1
			ORDER BY sequence ASC
			LIMIT $2
		)
	`, before.UTC(), limit)
	if err != nil {
		return 0, fmt.Errorf("delete delivered metering outbox operations: %w", err)
	}
	return tag.RowsAffected(), nil
}

func (r *Repository) normalizeStorageObservation(ctx context.Context, tx pgx.Tx, observation *metering.StorageObservation) (*metering.StorageProjectionState, error) {
	if observation == nil {
		return nil, fmt.Errorf("storage observation is nil")
	}
	if observation.SubjectType == "" || observation.SubjectID == "" {
		return nil, fmt.Errorf("storage subject_type and subject_id are required")
	}
	if observation.SubjectType != metering.SubjectTypeVolume && observation.SubjectType != metering.SubjectTypeSnapshot && observation.SubjectType != metering.SubjectTypeRootFS {
		return nil, fmt.Errorf("unsupported storage subject_type %q", observation.SubjectType)
	}
	if observation.SizeBytes < 0 {
		return nil, fmt.Errorf("storage size_bytes must be non-negative")
	}
	observedAt := observation.ObservedAt
	if observedAt.IsZero() {
		observedAt = r.timestamp()
	} else {
		observedAt = observedAt.UTC()
	}
	product := observation.Product
	if product == "" {
		product = metering.ProductSandbox
	}
	state := &metering.StorageProjectionState{
		SubjectType: observation.SubjectType,
		SubjectID:   observation.SubjectID,
		Product:     product,
		OwnerKind:   observation.OwnerKind,
		TeamID:      observation.TeamID,
		UserID:      observation.UserID,
		SandboxID:   observation.SandboxID,
		VolumeID:    observation.VolumeID,
		SnapshotID:  observation.SnapshotID,
		ClusterID:   observation.ClusterID,
		RegionID:    observation.RegionID,
		SizeBytes:   observation.SizeBytes,
		ObservedAt:  observedAt,
	}
	if state.SandboxID != "" && state.OwnerKind == "" {
		var ownerKind string
		err := tx.QueryRow(ctx, `
			SELECT owner_kind
			FROM metering.manager_sandbox_projection_state
			WHERE sandbox_id = $1
		`, state.SandboxID).Scan(&ownerKind)
		if err != nil && err != pgx.ErrNoRows {
			return nil, fmt.Errorf("query sandbox storage owner state: %w", err)
		}
		if err == nil {
			state.OwnerKind = ownerKind
		}
	}
	return state, nil
}

func (r *Repository) upsertStorageProjectionState(ctx context.Context, tx pgx.Tx, state *metering.StorageProjectionState) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO metering.storage_projection_state (
			subject_type, subject_id, product, owner_kind,
			team_id, user_id, sandbox_id, volume_id, snapshot_id,
			cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14
		)
		ON CONFLICT (subject_type, subject_id) DO UPDATE
		SET product = EXCLUDED.product,
			owner_kind = EXCLUDED.owner_kind,
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			sandbox_id = EXCLUDED.sandbox_id,
			volume_id = EXCLUDED.volume_id,
			snapshot_id = EXCLUDED.snapshot_id,
			cluster_id = EXCLUDED.cluster_id,
			region_id = EXCLUDED.region_id,
			size_bytes = EXCLUDED.size_bytes,
			observed_at = EXCLUDED.observed_at,
			unbilled_byte_nanoseconds = EXCLUDED.unbilled_byte_nanoseconds,
			updated_at = NOW()
	`, state.SubjectType, state.SubjectID, state.Product, state.OwnerKind,
		state.TeamID, state.UserID, nullableString(state.SandboxID), nullableString(state.VolumeID), nullableString(state.SnapshotID),
		nullableString(state.ClusterID), state.RegionID, state.SizeBytes, state.ObservedAt, state.UnbilledByteNanoseconds)
	if err != nil {
		return fmt.Errorf("upsert storage projection state: %w", err)
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal storage projection state: %w", err)
	}
	key := versionedKey(state.SubjectType+"/"+state.SubjectID, state.ObservedAt, payload)
	return enqueueJSON(ctx, tx, OperationStorageState, key, payload)
}

func (r *Repository) deleteStorageProjectionState(ctx context.Context, tx pgx.Tx, state *metering.StorageProjectionState, deletedAt time.Time) error {
	if deletedAt.IsZero() {
		deletedAt = r.timestamp()
	} else {
		deletedAt = deletedAt.UTC()
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM metering.storage_projection_state
		WHERE subject_type = $1 AND subject_id = $2
	`, state.SubjectType, state.SubjectID); err != nil {
		return fmt.Errorf("delete storage projection state: %w", err)
	}
	operation := &StorageStateDeleteOperation{State: state, DeletedAt: deletedAt}
	payload, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("marshal storage projection delete: %w", err)
	}
	key := versionedKey(state.SubjectType+"/"+state.SubjectID, deletedAt, payload)
	return enqueueJSON(ctx, tx, OperationStorageStateDelete, key, payload)
}

func getSandboxProjectionState(ctx context.Context, source db, sandboxID string, forUpdate bool) (*metering.SandboxProjectionState, error) {
	if strings.TrimSpace(sandboxID) == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	query := `
		SELECT
			sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
			owner_kind, resource_millicpu, resource_memory_mib,
			claimed_at, active_since, paused, paused_at, terminated_at,
			last_observed_at, last_resource_version
		FROM metering.manager_sandbox_projection_state
		WHERE sandbox_id = $1
	`
	if forUpdate {
		query += " FOR UPDATE"
	}
	state := &metering.SandboxProjectionState{}
	err := source.QueryRow(ctx, query, sandboxID).Scan(
		&state.SandboxID, &state.Namespace, &state.TeamID, &state.UserID, &state.TemplateID, &state.ClusterID,
		&state.OwnerKind, &state.ResourceMillicpu, &state.ResourceMemoryMiB,
		&state.ClaimedAt, &state.ActiveSince, &state.Paused, &state.PausedAt, &state.TerminatedAt,
		&state.LastObservedAt, &state.LastResourceVer,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query sandbox projection state: %w", err)
	}
	return state, nil
}

func getStorageProjectionStateForUpdate(ctx context.Context, tx pgx.Tx, subjectType, subjectID string) (*metering.StorageProjectionState, error) {
	state := &metering.StorageProjectionState{}
	err := tx.QueryRow(ctx, `
		SELECT
			subject_type, subject_id, product, owner_kind,
			team_id, user_id, COALESCE(sandbox_id, ''), COALESCE(volume_id, ''),
			COALESCE(snapshot_id, ''), COALESCE(cluster_id, ''), region_id,
			size_bytes, observed_at, unbilled_byte_nanoseconds
		FROM metering.storage_projection_state
		WHERE subject_type = $1 AND subject_id = $2
		FOR UPDATE
	`, subjectType, subjectID).Scan(
		&state.SubjectType, &state.SubjectID, &state.Product, &state.OwnerKind,
		&state.TeamID, &state.UserID, &state.SandboxID, &state.VolumeID,
		&state.SnapshotID, &state.ClusterID, &state.RegionID,
		&state.SizeBytes, &state.ObservedAt, &state.UnbilledByteNanoseconds,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query storage projection state: %w", err)
	}
	return state, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanStorageState(row rowScanner, state *metering.StorageProjectionState) error {
	return row.Scan(
		&state.SubjectType, &state.SubjectID, &state.Product, &state.OwnerKind,
		&state.TeamID, &state.UserID, &state.SandboxID, &state.VolumeID,
		&state.SnapshotID, &state.ClusterID, &state.RegionID,
		&state.SizeBytes, &state.ObservedAt, &state.UnbilledByteNanoseconds,
	)
}

func enqueue(ctx context.Context, tx pgx.Tx, operationType OperationType, dedupeKey string, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal metering %s operation: %w", operationType, err)
	}
	return enqueueJSON(ctx, tx, operationType, dedupeKey, raw)
}

func enqueueJSON(ctx context.Context, tx pgx.Tx, operationType OperationType, dedupeKey string, payload []byte) error {
	if operationType == "" {
		return fmt.Errorf("metering operation type is required")
	}
	if strings.TrimSpace(dedupeKey) == "" {
		return fmt.Errorf("metering operation dedupe key is required")
	}
	if !json.Valid(payload) {
		return fmt.Errorf("metering operation payload is not valid JSON")
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO metering.projection_outbox (operation_type, dedupe_key, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT (operation_type, dedupe_key) DO NOTHING
	`, operationType, dedupeKey, payload)
	if err != nil {
		return fmt.Errorf("enqueue metering %s operation: %w", operationType, err)
	}
	return nil
}

func validateEvent(event *metering.Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if event.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if event.Producer == "" {
		return fmt.Errorf("producer is required")
	}
	if event.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	if event.SubjectType == "" || event.SubjectID == "" {
		return fmt.Errorf("subject_type and subject_id are required")
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("occurred_at is required")
	}
	return nil
}

func validateWindow(window *metering.Window) error {
	if window == nil {
		return fmt.Errorf("window is nil")
	}
	if window.WindowID == "" {
		return fmt.Errorf("window_id is required")
	}
	if window.Producer == "" {
		return fmt.Errorf("producer is required")
	}
	if window.WindowType == "" {
		return fmt.Errorf("window_type is required")
	}
	if window.SubjectType == "" || window.SubjectID == "" {
		return fmt.Errorf("subject_type and subject_id are required")
	}
	if window.WindowStart.IsZero() || window.WindowEnd.IsZero() {
		return fmt.Errorf("window_start and window_end are required")
	}
	if window.WindowEnd.Before(window.WindowStart) {
		return fmt.Errorf("window_end must be greater than or equal to window_start")
	}
	if window.Unit == "" {
		return fmt.Errorf("unit is required")
	}
	if window.Value < 0 {
		return fmt.Errorf("value must be non-negative")
	}
	return nil
}

func versionedKey(identity string, at time.Time, payload []byte) string {
	hash := sha256.Sum256(payload)
	return fmt.Sprintf("%s/%d/%s", identity, at.UTC().UnixNano(), hex.EncodeToString(hash[:8]))
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func truncateError(value string) string {
	const max = 4000
	if len(value) <= max {
		return value
	}
	return value[:max]
}

func (r *Repository) timestamp() time.Time {
	if r == nil || r.now == nil {
		return time.Now().UTC()
	}
	return r.now().UTC()
}
