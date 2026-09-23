package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var _ nomadmigration.CheckpointPauseStore = (*PGSandboxStore)(nil)

const nomadCheckpointPauseWorkPredicate = `l.kind='pause' AND l.source='manual' AND NOT l.cancelable
 AND l.phase IN ('preparing','barriered','publishing') AND NOT (c.evidence ? 'fenced') AND NOT (c.evidence ? 'capture_failure_staging_released')`

// ListNomadCheckpointPauses projects pending lifecycle work without reserving
// another carrier. After fencing, the existing terminal queue owns cleanup.
func (s *PGSandboxStore) ListNomadCheckpointPauses(ctx context.Context, after string, limit int) ([]string, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit || len(after) > 256 {
		return nil, ErrNomadCheckpointConflict
	}
	rows, err := s.pool.Query(ctx, `SELECT c.operation_id FROM manager.sandbox_runtime_checkpoints c
 JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id WHERE `+nomadCheckpointPauseWorkPredicate+`
 AND c.operation_id COLLATE "C">$1 COLLATE "C" ORDER BY c.operation_id COLLATE "C" LIMIT $2`, after, limit)
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

// GetNomadCheckpointPauseWork is one read-only snapshot, never permission to
// start capture. Execution dispatch rechecks the exact live regional owner.
func (s *PGSandboxStore) GetNomadCheckpointPauseWork(ctx context.Context, id string) (*nomadmigration.CheckpointPauseWork, error) {
	var payload []byte
	var slot, sandbox, allocation string
	var epoch, generation int64
	var cancelDue bool
	err := s.pool.QueryRow(ctx, `SELECT c.evidence,c.source_slot_id,l.sandbox_id,l.from_runtime_id,l.epoch,l.from_generation,`+nomadCheckpointCancellationDue+`
 FROM manager.sandbox_runtime_checkpoints c JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
 WHERE c.operation_id=$1 AND `+nomadCheckpointPauseWorkPredicate, id).Scan(&payload, &slot, &sandbox, &allocation, &epoch, &generation, &cancelDue)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e NomadCheckpointEvidence
	if json.Unmarshal(payload, &e) != nil || e.validate() != nil {
		return nil, ErrNomadCheckpointConflict
	}
	source := e.Preflight.Source
	if source.OperationID != id || source.SandboxID != sandbox || source.Target.SlotID != slot || source.Target.AllocationID != allocation || source.LifecycleEpoch != epoch || source.SourceGeneration != generation {
		return nil, ErrNomadCheckpointConflict
	}
	work := &nomadmigration.CheckpointPauseWork{Staging: e.Staging, FailureFinalized: e.CaptureFailureFinalized, FailureStagingReleased: e.CaptureFailureStagingReleased, CancelDue: cancelDue, Address: e.Address, Preflight: e.Preflight, CPU: e.CPU, Staged: e.Staged,
		Preparation: e.Preparation, CaptureAuthorized: e.CaptureAuthorized, Publication: e.Publication, Published: e.Published}
	if e.CaptureFailure != nil {
		work.Failure = &nomadmigration.CaptureFailureWork{Request: *e.CaptureFailure, Cleanup: e.CaptureFailureCleanup}
	}
	return work, nil
}

// AuthorizeNomadCheckpointCaptureDispatch accepts an already-admitted capture
// beyond its CPU preflight window, but cannot revive a stale writer or owner.
// The node's durable one-shot intent remains the authority after dispatch.
func (s *PGSandboxStore) AuthorizeNomadCheckpointCaptureDispatch(ctx context.Context, request protocol.MigrationCaptureRequest) error {
	if request.Validate() != nil {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpoint(ctx, request.OperationID, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		if c.Lifecycle.Phase != SandboxLifecyclePhasePublishing || !c.Evidence.CaptureAuthorized || c.Evidence.Publication != nil || c.Evidence.Preflight.Source != request {
			return ErrNomadCheckpointConflict
		}
		_, err := validateNomadCheckpointSource(ctx, tx, record, c, false)
		return err
	})
	return err
}
