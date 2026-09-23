package sandboxstore

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
)

// NomadSandboxMemoryPauseCandidate reports accepted durable intent, not capture
// completion. Its operation is owned by the source checkpoint worker.
type NomadSandboxMemoryPauseCandidate struct {
	SandboxID     string
	OperationID   string
	AlreadyPaused bool
}

// RequestNomadSandboxMemoryPause reads immutable launch inputs under the same
// owner lock used to reserve the lifecycle. It never rebuilds an assignment from
// a mutable template, and never reports a disk-only pause as preserved memory.
func (s *PGSandboxStore) RequestNomadSandboxMemoryPause(ctx context.Context, sandboxID string) (*NomadSandboxMemoryPauseCandidate, error) {
	if sandboxID == "" || sandboxID != strings.TrimSpace(sandboxID) || len(sandboxID) > 512 {
		return nil, ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if !record.DeletedAt.IsZero() || record.DesiredState == SandboxDesiredStateDeleted {
		return nil, ErrSandboxRecordNotFound
	}
	active, err := getActiveLifecycleTxn(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	candidate := &NomadSandboxMemoryPauseCandidate{SandboxID: sandboxID}
	switch record.DesiredState {
	case SandboxDesiredStatePaused:
		if active != nil {
			return nil, ErrNomadCheckpointConflict
		}
		_, head, _, err := lockNomadSandboxResumeHead(ctx, tx, sandboxID)
		if err != nil {
			return nil, err
		}
		retained, _, err := loadOwnedNomadCheckpoint(ctx, tx, record, head)
		if err != nil {
			return nil, err
		}
		candidate.OperationID = retained.Retained.CheckpointID
		candidate.AlreadyPaused = true
	case SandboxDesiredStateActive:
		if active != nil {
			if !nomadCheckpointLifecycleMatches(active, record) {
				return nil, ErrNomadCheckpointConflict
			}
			// Do not inspect a source carrier that may already be physically
			// gone. Its immutable checkpoint evidence owns completion retries.
			checkpoint, err := loadNomadCheckpoint(ctx, tx, active)
			if err != nil {
				return nil, err
			}
			if checkpoint.Evidence.Assignment.TeamID != record.TeamID {
				return nil, ErrNomadCheckpointConflict
			}
			candidate.OperationID = checkpoint.Lifecycle.ID
		} else {
			source, err := lockExactNomadLiveWriter(ctx, tx, record)
			if err != nil {
				return nil, err
			}
			inputs, err := getRuntimeSlotClaimInputs(ctx, tx, source.slot.ID)
			if err != nil {
				return nil, err
			}
			if inputs == nil {
				return nil, ErrNomadCheckpointConflict
			}
			// The next lifecycle epoch admits a new attempt after a safe abort;
			// retries above reuse the already-admitted operation and epoch.
			identity := fmt.Sprintf("memory-pause\x00%s\x00%d\x00%d", sandboxID, record.RuntimeGeneration, record.LifecycleEpoch+1)
			operation := fmt.Sprintf("memory-pause-%x", sha256.Sum256([]byte(identity)))
			checkpoint, err := reserveNomadSandboxMemoryPauseTx(ctx, tx, record, operation, inputs.Runtime, inputs.NetworkPolicy)
			if err != nil {
				return nil, err
			}
			candidate.OperationID = checkpoint.Lifecycle.ID
		}
	default:
		return nil, ErrNomadCheckpointConflict
	}
	return candidate, tx.Commit(ctx)
}
