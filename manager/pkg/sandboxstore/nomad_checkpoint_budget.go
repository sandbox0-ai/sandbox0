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
	var payload, captured []byte
	var compatibility string
	err := tx.QueryRow(ctx, `SELECT r.authority,r.compatibility_digest,c.evidence
        FROM manager.sandbox_runtime_checkpoint_restores r
        JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=r.checkpoint_id
        WHERE r.operation_id=$1 FOR SHARE OF r`, request.OperationID).Scan(&payload, &compatibility, &captured)
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
	var capture NomadCheckpointEvidence
	if json.Unmarshal(captured, &capture) != nil || capture.validate() != nil || capture.Finalized == nil ||
		authority.ValidateFor(*capture.Publication, *capture.Published) != nil {
		return ErrNomadCheckpointConflict
	}
	source := capture.Publication.Capture.Request.Target
	request.preferredCheckpointNodeID = source.NodeID
	request.preferredCheckpointNodeUID = source.NodeUID
	request.preferredCheckpointNodeBootID = source.NodeBootID
	return nil
}
