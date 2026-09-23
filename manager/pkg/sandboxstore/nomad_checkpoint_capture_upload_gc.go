package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const checkpointCaptureUploadGCPredicate = `c.capture_upload_gc_completed_at IS NULL
    AND c.evidence->'staging' ? 'capture_upload'
    AND (c.image_gc_completed_at IS NOT NULL OR c.evidence ? 'capture_failure_gc_ack'
        OR c.evidence ? 'staging_released')
    AND manager.runtime_checkpoint_capture_upload_releasable(c.operation_id)`

func (s *PGSandboxStore) ListNomadCheckpointCaptureUploadGC(ctx context.Context, after string, limit int) ([]string, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 512 {
		return nil, ErrNomadCheckpointConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT c.operation_id FROM manager.sandbox_runtime_checkpoints c
		WHERE `+checkpointCaptureUploadGCPredicate+` AND c.operation_id COLLATE "C">$1 COLLATE "C"
		ORDER BY c.operation_id COLLATE "C" LIMIT $2`, after, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *PGSandboxStore) AuthorizeNomadCheckpointCaptureUploadGC(ctx context.Context, id string) (*runtimecheckpoint.CaptureScope, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var payload []byte
	var retained *string
	if err := tx.QueryRow(ctx, `SELECT c.evidence->'staging',c.capture_upload_gc_scope_digest
		FROM manager.sandbox_runtime_checkpoints c WHERE c.operation_id=$1 AND `+checkpointCaptureUploadGCPredicate+` FOR UPDATE`, id).Scan(&payload, &retained); err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	var request protocol.MigrationStagingRequest
	if json.Unmarshal(payload, &request) != nil || request.Validate() != nil || !request.CaptureOnly || request.Source.OperationID != id {
		return nil, ErrNomadCheckpointConflict
	}
	scope, err := request.CaptureUpload.Scope(request.Source)
	if err != nil {
		return nil, err
	}
	digest, err := scope.Digest()
	if err != nil || retained != nil && *retained != digest {
		return nil, ErrNomadCheckpointConflict
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints SET capture_upload_gc_scope_digest=$2 WHERE operation_id=$1`, id, digest); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &scope, nil
}

func (s *PGSandboxStore) CompleteNomadCheckpointCaptureUploadGC(ctx context.Context, scope runtimecheckpoint.CaptureScope) error {
	digest, err := scope.Digest()
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints
		SET capture_upload_gc_completed_at=COALESCE(capture_upload_gc_completed_at,clock_timestamp())
		WHERE operation_id=$1 AND capture_upload_gc_scope_digest=$2
			AND manager.runtime_checkpoint_capture_upload_releasable(operation_id)`, scope.OperationID(), digest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNomadCheckpointConflict
	}
	return nil
}
