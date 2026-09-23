package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// validateRuntimeSlotMemoryBudget requires the existing regional restore mode;
// a caller flag cannot extend a cold claim's resource reservation. Admission
// already holds the sandbox and lifecycle locks, including committed retries.
func validateRuntimeSlotMemoryBudget(ctx context.Context, tx pgx.Tx, request *AcquireRuntimeSlotRequest) error {
	var payload []byte
	var compatibility string
	err := tx.QueryRow(ctx, `SELECT authority,compatibility_digest FROM manager.sandbox_runtime_checkpoint_restores
        WHERE operation_id=$1 FOR SHARE`, request.OperationID).Scan(&payload, &compatibility)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNomadCheckpointConflict
	}
	if err != nil {
		return err
	}
	var authority protocol.CheckpointRestoreAuthority
	if json.Unmarshal(payload, &authority) != nil || authority.Assignment.Validate() != nil ||
		authority.Assignment.OperationID != request.OperationID || authority.Assignment.Target.SandboxID != request.SandboxID ||
		compatibility != request.CompatibilityDigest {
		return ErrNomadCheckpointConflict
	}
	revision, err := authority.Assignment.Target.Revision()
	if err != nil || revision != request.RuntimeAssignmentRevision {
		return ErrNomadCheckpointConflict
	}
	return nil
}
