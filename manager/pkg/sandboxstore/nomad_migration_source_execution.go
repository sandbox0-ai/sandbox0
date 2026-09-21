package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var _ nomadmigration.SourceExecutionStore = (*PGSandboxStore)(nil)

const nomadMigrationSourceExecutionPredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
 AND m.assignment_request IS NOT NULL AND m.preparation_cancel_request IS NULL AND m.publication_request IS NULL
 AND m.staging_source_receipt IS NOT NULL AND m.staging_destination_receipt IS NOT NULL
 AND NOT m.staging_source_release_requested AND NOT m.staging_destination_release_requested AND (
  (l.phase='publishing' AND m.capture_request IS NOT NULL AND m.preparation_address IS NOT NULL) OR
  (l.phase IN ('preparing','barriered') AND m.capture_request IS NULL
   AND m.cpu_preflight_source IS NOT NULL AND m.cpu_preflight_destination IS NOT NULL
   AND l.created_at<=clock_timestamp() AND l.created_at + INTERVAL '2 minutes'>clock_timestamp()
   AND m.cpu_preflight_requested_at<=clock_timestamp() AND m.cpu_preflight_requested_at + INTERVAL '2 minutes'>clock_timestamp()
   AND ((m.preparation_request IS NOT NULL AND m.preparation_address IS NOT NULL) OR
    (m.preparation_request IS NULL AND EXISTS (SELECT 1 FROM manager.runtime_slots r WHERE r.slot_id=m.source_slot_id
      AND r.claim_runtime_assignment IS NOT NULL AND r.claim_network_policy IS NOT NULL)))))`

func (s *PGSandboxStore) ListNomadMigrationSourceExecutions(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationSourceExecutionPredicate)
}

// This read grants no dispatch authority. Both command paths subsequently
// recheck lifecycle and retained evidence under the canonical operation locks.
func (s *PGSandboxStore) GetNomadMigrationSourceExecution(ctx context.Context, id string) (*nomadmigration.SourceExecution, error) {
	var assignment, preparation, capture []byte
	var ad string
	var pd, cd, address, policy *string
	var source, sandbox string
	var from, to int64
	err := s.pool.QueryRow(ctx, `SELECT m.assignment_request,m.assignment_digest,m.preparation_request,m.preparation_digest,
  m.capture_request,m.capture_digest,m.preparation_address,m.source_network_policy,m.source_slot_id,l.sandbox_id,l.from_generation,l.to_generation
  FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
  WHERE m.operation_id=$1 AND `+nomadMigrationSourceExecutionPredicate, id).Scan(&assignment, &ad, &preparation, &pd, &capture, &cd, &address, &policy, &source, &sandbox, &from, &to)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	w := &nomadmigration.SourceExecution{}
	if json.Unmarshal(assignment, &w.Assignment) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	got, err := w.Assignment.Digest()
	if err != nil || got != ad || w.Assignment.OperationID != id || w.Assignment.Target.SandboxID != sandbox || w.Assignment.SourceGeneration != from || w.Assignment.Target.RuntimeGeneration != to {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(preparation) != 0 {
		w.Preparation = &procdapi.RuntimeMigrationRequest{}
		if json.Unmarshal(preparation, w.Preparation) != nil || pd == nil || address == nil || policy == nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		got, err := w.Preparation.Digest()
		if err != nil || got != *pd {
			return nil, ErrNomadSandboxMigrationConflict
		}
		w.Address, w.Policy = *address, *policy
	} else {
		inputs, err := s.GetRuntimeSlotClaimInputs(ctx, source)
		if err != nil || inputs == nil {
			return nil, err
		}
		revision, err := inputs.Runtime.Revision()
		if err != nil || revision != w.Assignment.SourceRevision {
			return nil, ErrNomadSandboxMigrationConflict
		}
		w.Policy = inputs.NetworkPolicy
	}
	if len(capture) != 0 {
		w.Capture = &protocol.MigrationCaptureRequest{}
		if json.Unmarshal(capture, w.Capture) != nil || cd == nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		got, err := w.Capture.Digest()
		if err != nil || got != *cd || w.Capture.Target.SlotID != source {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	if w.Validate() != nil || validateMigrationSourcePolicy(w.Assignment, w.Policy) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return w, nil
}

// A committed capture may be retried after its original CPU window expires,
// but a queue snapshot cannot revive a terminated or replaced sandbox. The
// node driver owns the once-only checkpoint intent and uncertain-outcome fence.
func (s *PGSandboxStore) AuthorizeNomadMigrationSourceCaptureDispatch(ctx context.Context, request protocol.MigrationCaptureRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	historical, err := s.GetNomadMigrationSourceRecovery(ctx, request.OperationID)
	if err != nil {
		return err
	}
	if historical == nil || historical.Capture != request {
		return ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	r, err := lockNomadMigrationAuthority(ctx, tx, historical.Assignment)
	if err != nil {
		return err
	}
	if r.Lifecycle.Phase != SandboxLifecyclePhasePublishing {
		return ErrNomadSandboxMigrationConflict
	}
	var pending bool
	if err := tx.QueryRow(ctx, `SELECT publication_request IS NULL AND preparation_address IS NOT NULL
  FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, request.OperationID).Scan(&pending); err != nil {
		return err
	}
	if !pending {
		return ErrNomadSandboxMigrationConflict
	}
	return tx.Commit(ctx)
}
