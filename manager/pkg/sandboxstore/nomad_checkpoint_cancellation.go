package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"reflect"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const nomadCheckpointCancellationDue = `(NOT c.evidence ? 'capture_authorized' AND (
 c.evidence ? 'cancel_authorized' OR l.created_at+INTERVAL '2 minutes'<=clock_timestamp()
 OR c.created_at+INTERVAL '2 minutes'<=clock_timestamp()
 OR EXISTS (SELECT 1 FROM manager.sandboxes owner WHERE owner.sandbox_id=l.sandbox_id
   AND (owner.desired_state<>'active' OR owner.deleted_at IS NOT NULL OR owner.hard_expires_at<=clock_timestamp()))))`

// Cancellation retains historical authority after expiry or deletion. It never
// requires a fresh writer lease, and it cannot race a committed capture grant.
func (s *PGSandboxStore) mutateNomadCheckpointCancellation(ctx context.Context, id string, fn func(pgx.Tx, *NomadSandboxCheckpoint) error) (*NomadSandboxCheckpoint, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeSlotClaimOperation(ctx, tx, id); err != nil {
		return nil, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, id).Scan(&sandbox); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox)
	if err != nil {
		return nil, err
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, id))
	if err != nil {
		return nil, err
	}
	c, err := loadNomadCheckpoint(ctx, tx, life)
	if err != nil {
		return nil, err
	}
	if c.Evidence.Assignment.TeamID != record.TeamID || life.Cancelable || c.Evidence.CaptureAuthorized ||
		(life.Phase != SandboxLifecyclePhasePreparing && life.Phase != SandboxLifecyclePhaseBarriered && life.Phase != SandboxLifecyclePhaseAborted) {
		return nil, ErrNomadCheckpointConflict
	}
	if err := fn(tx, c); err != nil {
		return nil, err
	}
	if err := c.Evidence.validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(c.Evidence)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=$2 WHERE operation_id=$1`, id, payload); err != nil {
		return nil, err
	}
	return c, tx.Commit(ctx)
}

func (s *PGSandboxStore) AuthorizeNomadCheckpointCancellation(ctx context.Context, id string) (*nomadmigration.CheckpointCancellation, error) {
	var due bool
	var sourceTerminal bool
	c, err := s.mutateNomadCheckpointCancellation(ctx, id, func(tx pgx.Tx, c *NomadSandboxCheckpoint) error {
		if c.Lifecycle.Phase == SandboxLifecyclePhaseAborted {
			return nil
		}
		if err := tx.QueryRow(ctx, `SELECT `+nomadCheckpointCancellationDue+` FROM manager.sandbox_runtime_checkpoints c
            JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id WHERE c.operation_id=$1`, id).Scan(&due); err != nil {
			return err
		}
		if due {
			c.Evidence.CancelAuthorized = true
			var state string
			if err := tx.QueryRow(ctx, `SELECT state FROM manager.runtime_slots WHERE slot_id=$1`, c.SourceSlotID).Scan(&state); err != nil {
				return err
			}
			sourceTerminal = state == RuntimeSlotStateQuiescing || state == RuntimeSlotStateOrphaned || state == RuntimeSlotStateTerminal
		}
		return nil
	})
	if err != nil || !due {
		return nil, err
	}
	work := &nomadmigration.CheckpointCancellation{OperationID: id, Address: c.Evidence.Address,
		Source: c.Evidence.Preflight.Source.Target, Namespace: c.Lifecycle.FromRuntimeNamespace,
		Canceled: c.Evidence.Canceled, SourceAbsentProof: c.Evidence.SourceAbsentProof, SourceTerminal: sourceTerminal,
		Staging: c.Evidence.Staging, Released: c.Evidence.StagingReleased}
	if c.Evidence.Preparation != nil {
		request := *c.Evidence.Preparation
		request.Action = procdapi.MigrationCancel
		work.Preparation = &request
	}
	return work, nil
}

// CommitNomadCheckpointSourceAbsent accepts only a direct-client absence
// observation of the original allocation. It replaces an unreachable procd
// cancellation acknowledgement; ordinary terminal cleanup still must fence
// the writer and prove physical resource removal.
func (s *PGSandboxStore) CommitNomadCheckpointSourceAbsent(ctx context.Context, id string, target protocol.NodeChannelTarget, proof []byte) error {
	if len(proof) != sha256.Size {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpointCancellation(ctx, id, func(tx pgx.Tx, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		var state string
		if err := tx.QueryRow(ctx, `SELECT state FROM manager.runtime_slots WHERE slot_id=$1`, c.SourceSlotID).Scan(&state); err != nil {
			return err
		}
		if !e.CancelAuthorized || e.Preparation == nil || e.Preflight.Source.Target != target ||
			c.Lifecycle.FromRuntimeID != target.AllocationID ||
			(state != RuntimeSlotStateQuiescing && state != RuntimeSlotStateOrphaned && state != RuntimeSlotStateTerminal) ||
			len(e.SourceAbsentProof) != 0 && !reflect.DeepEqual(e.SourceAbsentProof, proof) {
			return ErrNomadCheckpointConflict
		}
		e.SourceAbsentProof = append([]byte(nil), proof...)
		return nil
	})
	return err
}

func (s *PGSandboxStore) CommitNomadCheckpointCancellation(ctx context.Context, request procdapi.RuntimeCheckpointRequest, receipt procdapi.RuntimeCheckpointResponse) error {
	if request.Action != procdapi.MigrationCancel || receipt.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpointCancellation(ctx, request.Capture.OperationID, func(_ pgx.Tx, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if !e.CancelAuthorized || e.Preparation == nil {
			return ErrNomadCheckpointConflict
		}
		expected := *e.Preparation
		expected.Action = procdapi.MigrationCancel
		if !reflect.DeepEqual(expected, request) || e.Canceled != nil && *e.Canceled != receipt {
			return ErrNomadCheckpointConflict
		}
		e.Canceled = &receipt
		return nil
	})
	return err
}

func (s *PGSandboxStore) CommitNomadCheckpointCanceledStaging(ctx context.Context, request protocol.MigrationStagingRequest, receipt protocol.MigrationStagingReleased) error {
	if receipt.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpointCancellation(ctx, request.Source.OperationID, func(_ pgx.Tx, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if !e.CancelAuthorized || e.Staging == nil || *e.Staging != request || e.Preparation != nil && e.Canceled == nil && len(e.SourceAbsentProof) == 0 ||
			e.StagingReleased != nil && *e.StagingReleased != receipt {
			return ErrNomadCheckpointConflict
		}
		e.StagingReleased = &receipt
		return nil
	})
	return err
}

func (s *PGSandboxStore) CompleteNomadCheckpointCancellation(ctx context.Context, id string) error {
	_, err := s.mutateNomadCheckpointCancellation(ctx, id, func(tx pgx.Tx, c *NomadSandboxCheckpoint) error {
		e := c.Evidence
		if !e.CancelAuthorized || e.Preparation != nil && e.Canceled == nil && len(e.SourceAbsentProof) == 0 || e.Staging != nil && e.StagingReleased == nil {
			return ErrNomadCheckpointConflict
		}
		if c.Lifecycle.Phase == SandboxLifecyclePhaseAborted {
			return nil
		}
		// Source execution and its writer/resource lease remain owned. This is
		// an aborted memory operation, never a completed filesystem pause.
		return (sandboxStoreTx{tx: tx}).AbortLifecycleTxn(ctx, id, "memory checkpoint preparation canceled before capture")
	})
	return err
}
