package sandboxstore

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
)

// retainNomadCheckpointFork joins memory ownership to the ordinary atomic
// paused fork. Only a reference is copied; immutable image bytes and disk
// blocks remain shared. The child receives no runtime or parent writer lease.
func retainNomadCheckpointFork(ctx context.Context, tx pgx.Tx, source, target *SandboxRecord, lifecycle *SandboxLifecycleTxn) error {
	capture, _, err := loadOwnedNomadCheckpoint(ctx, tx, source, lifecycle.ExpectedGenerationID)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_checkpoint_forks
		(operation_id,checkpoint_id,target_sandbox_id,generation_id) VALUES ($1,$2,$3,$4)`,
		lifecycle.ID, capture.Retained.CheckpointID, target.ID, lifecycle.ExpectedGenerationID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_checkpoint_refs
		(sandbox_id,checkpoint_id,generation_id,runtime_generation) VALUES ($1,$2,$3,0)`, target.ID, capture.Retained.CheckpointID, lifecycle.ExpectedGenerationID)
	return err
}

// A completed fork's mode is historical authority, independent of subsequent
// source resumes or deletion of the source's current paused reference.
func validateNomadCheckpointForkRetry(ctx context.Context, tx pgx.Tx, lifecycle *SandboxLifecycleTxn, request *NomadSandboxForkRequest) error {
	var target, generation string
	err := tx.QueryRow(ctx, `SELECT target_sandbox_id,generation_id FROM manager.sandbox_runtime_checkpoint_forks WHERE operation_id=$1`, lifecycle.ID).Scan(&target, &generation)
	if errors.Is(err, pgx.ErrNoRows) {
		if request.Memory {
			return ErrNomadCheckpointConflict
		}
		return nil
	}
	if err != nil {
		return err
	}
	if !request.Memory || target != request.Target.ID || generation != lifecycle.ExpectedGenerationID {
		return ErrNomadCheckpointConflict
	}
	return nil
}
