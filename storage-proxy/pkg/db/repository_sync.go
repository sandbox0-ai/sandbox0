package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/storage-proxy/pkg/pathnorm"
)

// UpsertSyncReplica creates or updates durable replica state for a volume.
func (r *Repository) UpsertSyncReplica(ctx context.Context, replica *SyncReplica) error {
	return r.UpsertSyncReplicaTx(ctx, r.pool, replica)
}

// UpsertSyncReplicaTx creates or updates durable replica state for a volume within an existing transaction.
func (r *Repository) UpsertSyncReplicaTx(ctx context.Context, db DB, replica *SyncReplica) error {
	capsPayload, err := json.Marshal(replica.Capabilities)
	if err != nil {
		return fmt.Errorf("marshal sync replica capabilities: %w", err)
	}
	_, err = db.Exec(ctx, `
		INSERT INTO sandbox_volume_sync_replicas (
			volume_id, id, team_id,
			display_name, platform, root_path, capabilities,
			last_seen_at, last_applied_seq,
			created_at, updated_at
		) VALUES (
			$1, $2, $3,
			$4, $5, $6, $7,
			$8, $9,
			$10, $11
		)
		ON CONFLICT (volume_id, id) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			display_name = EXCLUDED.display_name,
			platform = EXCLUDED.platform,
			root_path = EXCLUDED.root_path,
			capabilities = EXCLUDED.capabilities,
			last_seen_at = EXCLUDED.last_seen_at
	`,
		replica.VolumeID, replica.ID, replica.TeamID,
		replica.DisplayName, replica.Platform, replica.RootPath, capsPayload,
		replica.LastSeenAt, replica.LastAppliedSeq,
		replica.CreatedAt, replica.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("upsert sync replica: %w", err)
	}
	return nil
}

// GetSyncNamespacePolicy returns the effective filesystem compatibility policy for a volume.
func (r *Repository) GetSyncNamespacePolicy(ctx context.Context, volumeID string) (*SyncNamespacePolicy, error) {
	return r.getSyncNamespacePolicy(ctx, r.pool, volumeID, false)
}

// GetSyncNamespacePolicyForUpdateTx returns the compatibility policy for a volume with a row lock.
func (r *Repository) GetSyncNamespacePolicyForUpdateTx(ctx context.Context, tx pgx.Tx, volumeID string) (*SyncNamespacePolicy, error) {
	return r.getSyncNamespacePolicy(ctx, tx, volumeID, true)
}

func (r *Repository) getSyncNamespacePolicy(ctx context.Context, db DB, volumeID string, forUpdate bool) (*SyncNamespacePolicy, error) {
	query := `
		SELECT volume_id, team_id, capabilities, updated_at
		FROM sandbox_volume_sync_namespace_policy
		WHERE volume_id = $1
	`
	if forUpdate {
		query += " FOR UPDATE"
	}

	var (
		policy      SyncNamespacePolicy
		capsPayload []byte
	)
	err := db.QueryRow(ctx, query, volumeID).Scan(
		&policy.VolumeID, &policy.TeamID, &capsPayload, &policy.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sync namespace policy: %w", err)
	}
	if len(capsPayload) > 0 {
		if err := json.Unmarshal(capsPayload, &policy.Capabilities); err != nil {
			return nil, fmt.Errorf("unmarshal sync namespace policy capabilities: %w", err)
		}
	}
	return &policy, nil
}

// UpsertSyncNamespacePolicyTx persists the effective filesystem compatibility policy for one volume.
func (r *Repository) UpsertSyncNamespacePolicyTx(ctx context.Context, tx pgx.Tx, policy *SyncNamespacePolicy) error {
	capsPayload, err := json.Marshal(policy.Capabilities)
	if err != nil {
		return fmt.Errorf("marshal sync namespace policy capabilities: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO sandbox_volume_sync_namespace_policy (
			volume_id, team_id, capabilities, updated_at
		) VALUES (
			$1, $2, $3, $4
		)
		ON CONFLICT (volume_id) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			capabilities = EXCLUDED.capabilities,
			updated_at = EXCLUDED.updated_at
	`, policy.VolumeID, policy.TeamID, capsPayload, policy.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert sync namespace policy: %w", err)
	}
	return nil
}

// ListSyncVolumeHeads returns active sync volumes and their current journal heads.
func (r *Repository) ListSyncVolumeHeads(ctx context.Context) ([]*SyncVolumeHead, error) {
	rows, err := r.pool.Query(ctx, `
		WITH active_sync_volumes AS (
			SELECT DISTINCT volume_id FROM sandbox_volume_sync_replicas
			UNION
			SELECT DISTINCT volume_id FROM sandbox_volume_sync_journal
			UNION
			SELECT DISTINCT volume_id FROM sandbox_volume_sync_retention
		)
		SELECT
			v.id,
			v.team_id,
			COALESCE(MAX(j.seq), 0) AS head_seq
		FROM active_sync_volumes a
		JOIN sandbox_volumes v ON v.id = a.volume_id
		LEFT JOIN sandbox_volume_sync_journal j ON j.volume_id = v.id
		GROUP BY v.id, v.team_id
		ORDER BY v.id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list sync volume heads: %w", err)
	}
	defer rows.Close()

	var heads []*SyncVolumeHead
	for rows.Next() {
		var head SyncVolumeHead
		if err := rows.Scan(&head.VolumeID, &head.TeamID, &head.HeadSeq); err != nil {
			return nil, fmt.Errorf("scan sync volume head: %w", err)
		}
		heads = append(heads, &head)
	}
	return heads, nil
}

// GetSyncReplica retrieves replica state for a volume.
func (r *Repository) GetSyncReplica(ctx context.Context, volumeID, replicaID string) (*SyncReplica, error) {
	return r.getSyncReplica(ctx, r.pool, volumeID, replicaID, false)
}

// GetSyncReplicaForUpdate retrieves replica state with a row lock.
func (r *Repository) GetSyncReplicaForUpdate(ctx context.Context, tx pgx.Tx, volumeID, replicaID string) (*SyncReplica, error) {
	return r.getSyncReplica(ctx, tx, volumeID, replicaID, true)
}

func (r *Repository) getSyncReplica(ctx context.Context, db DB, volumeID, replicaID string, forUpdate bool) (*SyncReplica, error) {
	query := `
		SELECT
			volume_id, id, team_id,
			display_name, platform, root_path, capabilities,
			last_seen_at, last_applied_seq,
			created_at, updated_at
		FROM sandbox_volume_sync_replicas
		WHERE volume_id = $1 AND id = $2
	`
	if forUpdate {
		query += " FOR UPDATE NOWAIT"
	}

	var replica SyncReplica
	var capsPayload []byte
	err := db.QueryRow(ctx, query, volumeID, replicaID).Scan(
		&replica.VolumeID, &replica.ID, &replica.TeamID,
		&replica.DisplayName, &replica.Platform, &replica.RootPath, &capsPayload,
		&replica.LastSeenAt, &replica.LastAppliedSeq,
		&replica.CreatedAt, &replica.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sync replica: %w", err)
	}
	if len(capsPayload) > 0 {
		if err := json.Unmarshal(capsPayload, &replica.Capabilities); err != nil {
			return nil, fmt.Errorf("unmarshal sync replica capabilities: %w", err)
		}
	}
	replica.Capabilities = pathnorm.NormalizeFilesystemCapabilities(replica.Platform, replica.CaseSensitive, &replica.Capabilities)
	replica.CaseSensitive = replica.Capabilities.CaseSensitive
	return &replica, nil
}

// TouchSyncReplicaTx refreshes last_seen_at without changing cursor state.
func (r *Repository) TouchSyncReplicaTx(ctx context.Context, tx pgx.Tx, volumeID, replicaID string, lastSeenAt time.Time) error {
	cmd, err := tx.Exec(ctx, `
		UPDATE sandbox_volume_sync_replicas
		SET last_seen_at = $3, updated_at = NOW()
		WHERE volume_id = $1 AND id = $2
	`, volumeID, replicaID, lastSeenAt)
	if err != nil {
		return fmt.Errorf("touch sync replica: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// UpdateSyncReplicaCursorTx advances the durable replica cursor.
func (r *Repository) UpdateSyncReplicaCursorTx(ctx context.Context, tx pgx.Tx, volumeID, replicaID string, lastAppliedSeq int64, lastSeenAt time.Time) error {
	cmd, err := tx.Exec(ctx, `
		UPDATE sandbox_volume_sync_replicas
		SET last_applied_seq = $3,
			last_seen_at = $4,
			updated_at = NOW()
		WHERE volume_id = $1 AND id = $2
	`, volumeID, replicaID, lastAppliedSeq, lastSeenAt)
	if err != nil {
		return fmt.Errorf("update sync replica cursor: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetSyncHead returns the latest journal sequence for a volume.
func (r *Repository) GetSyncHead(ctx context.Context, volumeID string) (int64, error) {
	var head int64
	if err := r.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(seq), 0)
		FROM sandbox_volume_sync_journal
		WHERE volume_id = $1
	`, volumeID).Scan(&head); err != nil {
		return 0, fmt.Errorf("get sync head: %w", err)
	}
	return head, nil
}

// GetSyncRetentionState returns the retained journal floor for a volume.
func (r *Repository) GetSyncRetentionState(ctx context.Context, volumeID string) (*SyncRetentionState, error) {
	return r.getSyncRetentionState(ctx, r.pool, volumeID, false)
}

// GetSyncRetentionStateForUpdateTx returns the retained journal floor for a volume with a row lock.
func (r *Repository) GetSyncRetentionStateForUpdateTx(ctx context.Context, tx pgx.Tx, volumeID string) (*SyncRetentionState, error) {
	return r.getSyncRetentionState(ctx, tx, volumeID, true)
}

func (r *Repository) getSyncRetentionState(ctx context.Context, db DB, volumeID string, forUpdate bool) (*SyncRetentionState, error) {
	query := `
		SELECT volume_id, team_id, compacted_through_seq, updated_at
		FROM sandbox_volume_sync_retention
		WHERE volume_id = $1
	`
	if forUpdate {
		query += " FOR UPDATE"
	}

	var state SyncRetentionState
	err := db.QueryRow(ctx, query, volumeID).Scan(
		&state.VolumeID, &state.TeamID, &state.CompactedThroughSeq, &state.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sync retention state: %w", err)
	}
	return &state, nil
}

// ListSyncJournalEntries returns journal entries after a given sequence.
func (r *Repository) ListSyncJournalEntries(ctx context.Context, volumeID string, afterSeq int64, limit int) ([]*SyncJournalEntry, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `
		SELECT
			seq, volume_id, team_id, source, replica_id,
			event_type, path, normalized_path,
			old_path, normalized_old_path,
			tombstone, content_sha256, size_bytes, metadata, created_at
		FROM sandbox_volume_sync_journal
		WHERE volume_id = $1 AND seq > $2
		ORDER BY seq ASC
		LIMIT $3
	`, volumeID, afterSeq, limit)
	if err != nil {
		return nil, fmt.Errorf("list sync journal entries: %w", err)
	}
	defer rows.Close()

	var entries []*SyncJournalEntry
	for rows.Next() {
		entry, err := scanSyncJournalEntry(rows)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return entries, nil
}

// DeleteSyncJournalEntriesUpToTx removes retained journal history up to and including maxSeq.
func (r *Repository) DeleteSyncJournalEntriesUpToTx(ctx context.Context, tx pgx.Tx, volumeID string, maxSeq int64) (int64, error) {
	cmd, err := tx.Exec(ctx, `
		DELETE FROM sandbox_volume_sync_journal
		WHERE volume_id = $1 AND seq <= $2
	`, volumeID, maxSeq)
	if err != nil {
		return 0, fmt.Errorf("delete sync journal entries: %w", err)
	}
	return cmd.RowsAffected(), nil
}

// UpsertSyncRetentionStateTx advances the retained journal floor for a volume.
func (r *Repository) UpsertSyncRetentionStateTx(ctx context.Context, tx pgx.Tx, state *SyncRetentionState) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO sandbox_volume_sync_retention (
			volume_id, team_id, compacted_through_seq, updated_at
		) VALUES (
			$1, $2, $3, $4
		)
		ON CONFLICT (volume_id) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			compacted_through_seq = GREATEST(
				sandbox_volume_sync_retention.compacted_through_seq,
				EXCLUDED.compacted_through_seq
			),
			updated_at = EXCLUDED.updated_at
	`, state.VolumeID, state.TeamID, state.CompactedThroughSeq, state.UpdatedAt)
	if err != nil {
		return fmt.Errorf("upsert sync retention state: %w", err)
	}
	return nil
}

// CreateSyncJournalEntryTx inserts one journal entry and returns its sequence.
func (r *Repository) CreateSyncJournalEntryTx(ctx context.Context, tx pgx.Tx, entry *SyncJournalEntry) error {
	err := tx.QueryRow(ctx, `
		INSERT INTO sandbox_volume_sync_journal (
			volume_id, team_id, source, replica_id,
			event_type, path, normalized_path,
			old_path, normalized_old_path,
			tombstone, content_sha256, size_bytes, metadata, created_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7,
			$8, $9,
			$10, $11, $12, $13, $14
		)
		RETURNING seq, created_at
	`,
		entry.VolumeID, entry.TeamID, entry.Source, entry.ReplicaID,
		entry.EventType, entry.Path, entry.NormalizedPath,
		entry.OldPath, entry.NormalizedOldPath,
		entry.Tombstone, entry.ContentSHA256, entry.SizeBytes, entry.Metadata, entry.CreatedAt,
	).Scan(&entry.Seq, &entry.CreatedAt)
	if err != nil {
		return fmt.Errorf("create sync journal entry: %w", err)
	}
	return nil
}

// CreateSyncConflictTx stores a durable sync conflict record.
func (r *Repository) CreateSyncConflictTx(ctx context.Context, tx pgx.Tx, conflict *SyncConflict) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO sandbox_volume_sync_conflicts (
			id, volume_id, team_id, replica_id,
			path, normalized_path, artifact_path, incoming_path, incoming_old_path,
			existing_seq, reason, status, metadata, created_at, updated_at
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14, $15
		)
	`,
		conflict.ID, conflict.VolumeID, conflict.TeamID, conflict.ReplicaID,
		conflict.Path, conflict.NormalizedPath, conflict.ArtifactPath, conflict.IncomingPath, conflict.IncomingOldPath,
		conflict.ExistingSeq, conflict.Reason, conflict.Status, conflict.Metadata, conflict.CreatedAt, conflict.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("create sync conflict: %w", err)
	}
	return nil
}

// ListSyncConflicts returns sync conflicts for a volume ordered by recency.
func (r *Repository) ListSyncConflicts(ctx context.Context, volumeID, status string, limit int) ([]*SyncConflict, error) {
	if limit <= 0 {
		limit = 100
	}

	var (
		rows pgx.Rows
		err  error
	)
	if status == "" {
		rows, err = r.pool.Query(ctx, `
			SELECT
				id, volume_id, team_id, replica_id,
				path, normalized_path, artifact_path,
				incoming_path, incoming_old_path,
				existing_seq, reason, status, metadata, created_at, updated_at
			FROM sandbox_volume_sync_conflicts
			WHERE volume_id = $1
			ORDER BY created_at DESC
			LIMIT $2
		`, volumeID, limit)
	} else {
		rows, err = r.pool.Query(ctx, `
			SELECT
				id, volume_id, team_id, replica_id,
				path, normalized_path, artifact_path,
				incoming_path, incoming_old_path,
				existing_seq, reason, status, metadata, created_at, updated_at
			FROM sandbox_volume_sync_conflicts
			WHERE volume_id = $1 AND status = $2
			ORDER BY created_at DESC
			LIMIT $3
		`, volumeID, status, limit)
	}
	if err != nil {
		return nil, fmt.Errorf("list sync conflicts: %w", err)
	}
	defer rows.Close()

	var conflicts []*SyncConflict
	for rows.Next() {
		var conflict SyncConflict
		if err := rows.Scan(
			&conflict.ID, &conflict.VolumeID, &conflict.TeamID, &conflict.ReplicaID,
			&conflict.Path, &conflict.NormalizedPath, &conflict.ArtifactPath,
			&conflict.IncomingPath, &conflict.IncomingOldPath,
			&conflict.ExistingSeq, &conflict.Reason, &conflict.Status, &conflict.Metadata, &conflict.CreatedAt, &conflict.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan sync conflict: %w", err)
		}
		conflicts = append(conflicts, &conflict)
	}
	return conflicts, nil
}

// GetSyncConflict retrieves one sync conflict by volume and id.
func (r *Repository) GetSyncConflict(ctx context.Context, volumeID, conflictID string) (*SyncConflict, error) {
	var conflict SyncConflict
	err := r.pool.QueryRow(ctx, `
		SELECT
			id, volume_id, team_id, replica_id,
			path, normalized_path, artifact_path,
			incoming_path, incoming_old_path,
			existing_seq, reason, status, metadata, created_at, updated_at
		FROM sandbox_volume_sync_conflicts
		WHERE volume_id = $1 AND id = $2
	`, volumeID, conflictID).Scan(
		&conflict.ID, &conflict.VolumeID, &conflict.TeamID, &conflict.ReplicaID,
		&conflict.Path, &conflict.NormalizedPath, &conflict.ArtifactPath,
		&conflict.IncomingPath, &conflict.IncomingOldPath,
		&conflict.ExistingSeq, &conflict.Reason, &conflict.Status, &conflict.Metadata, &conflict.CreatedAt, &conflict.UpdatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sync conflict: %w", err)
	}
	return &conflict, nil
}

// UpdateSyncConflictTx updates status and merges metadata for a conflict.
func (r *Repository) UpdateSyncConflictTx(ctx context.Context, tx pgx.Tx, volumeID, conflictID, status string, metadata *json.RawMessage) error {
	cmd, err := tx.Exec(ctx, `
		UPDATE sandbox_volume_sync_conflicts
		SET status = $3,
			metadata = COALESCE(metadata, '{}'::jsonb) || COALESCE($4::jsonb, '{}'::jsonb),
			updated_at = NOW()
		WHERE volume_id = $1 AND id = $2
	`, volumeID, conflictID, status, metadata)
	if err != nil {
		return fmt.Errorf("update sync conflict: %w", err)
	}
	if cmd.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// GetSyncRequestTx loads a committed idempotent sync request inside a transaction.
func (r *Repository) GetSyncRequestTx(ctx context.Context, tx pgx.Tx, volumeID, replicaID, requestID string) (*SyncRequest, error) {
	var request SyncRequest
	err := tx.QueryRow(ctx, `
		SELECT volume_id, replica_id, request_id, request_fingerprint, response_payload, created_at
		FROM sandbox_volume_sync_requests
		WHERE volume_id = $1 AND replica_id = $2 AND request_id = $3
	`, volumeID, replicaID, requestID).Scan(
		&request.VolumeID, &request.ReplicaID, &request.RequestID, &request.RequestFingerprint, &request.ResponsePayload, &request.CreatedAt,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get sync request: %w", err)
	}
	return &request, nil
}

// CreateSyncRequestTx stores the committed response for one replica mutation request id.
func (r *Repository) CreateSyncRequestTx(ctx context.Context, tx pgx.Tx, request *SyncRequest) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO sandbox_volume_sync_requests (
			volume_id, replica_id, request_id, request_fingerprint, response_payload, created_at
		) VALUES (
			$1, $2, $3, $4, $5, $6
		)
	`, request.VolumeID, request.ReplicaID, request.RequestID, request.RequestFingerprint, request.ResponsePayload, request.CreatedAt)
	if err != nil {
		return fmt.Errorf("create sync request: %w", err)
	}
	return nil
}

// DeleteExpiredSyncRequests removes stale idempotency records before the given timestamp.
func (r *Repository) DeleteExpiredSyncRequests(ctx context.Context, before time.Time) (int64, error) {
	cmd, err := r.pool.Exec(ctx, `
		DELETE FROM sandbox_volume_sync_requests
		WHERE created_at < $1
	`, before)
	if err != nil {
		return 0, fmt.Errorf("delete expired sync requests: %w", err)
	}
	return cmd.RowsAffected(), nil
}

// GetLatestSyncJournalEntryByNormalizedPath retrieves the most recent entry for one normalized path.
func (r *Repository) GetLatestSyncJournalEntryByNormalizedPath(ctx context.Context, volumeID, normalizedPath string) (*SyncJournalEntry, error) {
	return r.getLatestSyncJournalEntryByNormalizedPath(ctx, r.pool, volumeID, normalizedPath)
}

// GetLatestSyncJournalEntryByNormalizedPathTx retrieves the most recent entry for one normalized path within a transaction.
func (r *Repository) GetLatestSyncJournalEntryByNormalizedPathTx(ctx context.Context, tx pgx.Tx, volumeID, normalizedPath string) (*SyncJournalEntry, error) {
	return r.getLatestSyncJournalEntryByNormalizedPath(ctx, tx, volumeID, normalizedPath)
}

func (r *Repository) getLatestSyncJournalEntryByNormalizedPath(ctx context.Context, db DB, volumeID, normalizedPath string) (*SyncJournalEntry, error) {
	var row pgx.Row
	if normalizedPath == "" {
		row = db.QueryRow(ctx, `
			SELECT
				seq, volume_id, team_id, source, replica_id,
				event_type, path, normalized_path,
				old_path, normalized_old_path,
				tombstone, content_sha256, size_bytes, metadata, created_at
			FROM sandbox_volume_sync_journal
			WHERE volume_id = $1 AND (normalized_path = '' OR normalized_old_path = '')
			ORDER BY seq DESC
			LIMIT 1
		`, volumeID)
	} else {
		row = db.QueryRow(ctx, `
			SELECT
				seq, volume_id, team_id, source, replica_id,
				event_type, path, normalized_path,
				old_path, normalized_old_path,
				tombstone, content_sha256, size_bytes, metadata, created_at
			FROM sandbox_volume_sync_journal
			WHERE volume_id = $1
			  AND (normalized_path = $2 OR normalized_old_path = $2)
			ORDER BY seq DESC
			LIMIT 1
		`, volumeID, normalizedPath)
	}

	entry, err := scanSyncJournalEntry(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get latest sync journal entry: %w", err)
	}
	return entry, nil
}

type syncJournalScanner interface {
	Scan(dest ...any) error
}

func scanSyncJournalEntry(scanner syncJournalScanner) (*SyncJournalEntry, error) {
	var entry SyncJournalEntry
	err := scanner.Scan(
		&entry.Seq, &entry.VolumeID, &entry.TeamID, &entry.Source, &entry.ReplicaID,
		&entry.EventType, &entry.Path, &entry.NormalizedPath,
		&entry.OldPath, &entry.NormalizedOldPath,
		&entry.Tombstone, &entry.ContentSHA256, &entry.SizeBytes, &entry.Metadata, &entry.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return &entry, nil
}
