package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const checkpointImageGCPredicate = `c.image_gc_completed_at IS NULL AND c.evidence ? 'published'
    AND manager.runtime_checkpoint_image_releasable(c.operation_id)`

func (s *PGSandboxStore) ListNomadCheckpointImageGC(ctx context.Context, after string, limit int) ([]string, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 512 {
		return nil, ErrNomadCheckpointConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT c.operation_id FROM manager.sandbox_runtime_checkpoints c
		WHERE `+checkpointImageGCPredicate+` AND c.operation_id COLLATE "C">$1 COLLATE "C"
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

// The object prefix is immutable and authorized only when every owner and
// physical source has released the image. Publication history remains durable.
func (s *PGSandboxStore) AuthorizeNomadCheckpointImageGC(ctx context.Context, id string) (*runtimecheckpoint.Binding, error) {
	var publication, published []byte
	var retained string
	err := s.pool.QueryRow(ctx, `UPDATE manager.sandbox_runtime_checkpoints c
		SET image_gc_binding_digest=c.evidence->'published'->'reference'->>'binding_digest'
		WHERE c.operation_id=$1 AND `+checkpointImageGCPredicate+`
		RETURNING c.evidence->'publication',c.evidence->'published',c.image_gc_binding_digest`, id).Scan(&publication, &published, &retained)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var command protocol.MigrationPublicationRequest
	var receipt protocol.MigrationPublication
	if json.Unmarshal(publication, &command) != nil || json.Unmarshal(published, &receipt) != nil || receipt.ValidateFor(command) != nil || command.CheckpointSource == nil {
		return nil, ErrNomadCheckpointConflict
	}
	binding, err := command.Binding()
	digest, digestErr := binding.Digest()
	if err != nil || digestErr != nil || binding.OperationID != id || digest != retained {
		return nil, ErrNomadCheckpointConflict
	}
	return &binding, nil
}

func (s *PGSandboxStore) CompleteNomadCheckpointImageGC(ctx context.Context, binding runtimecheckpoint.Binding) error {
	digest, err := binding.Digest()
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints c
		SET image_gc_completed_at=COALESCE(image_gc_completed_at,clock_timestamp())
		WHERE c.operation_id=$1 AND c.image_gc_binding_digest=$2
			AND manager.runtime_checkpoint_image_releasable(c.operation_id)`, binding.OperationID, digest)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNomadCheckpointConflict
	}
	return nil
}
