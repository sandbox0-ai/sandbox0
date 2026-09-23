package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func (s *PGSandboxStore) ListNomadMigrationFailureFinalizations(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadExecutionFailureWork(ctx, after, limit, `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
        AND l.phase='committing' AND m.failure_cleanup_receipt IS NOT NULL AND m.failure_finalization_receipt IS NULL`, `r.evidence ? 'failure_cleaned' AND NOT r.evidence ? 'failure_finalized'`)
}

// The immutable cleanup receipt was committed atomically with writer retirement
// and is the finalization authority. Constructing this request cannot retire a
// writer or replace missing node evidence.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationFailureFinalization(ctx context.Context, id string) (*protocol.MigrationFailureFinalizeRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	request, err := lockNomadMigrationFailureFinalization(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	return request, tx.Commit(ctx)
}

func lockNomadMigrationFailureFinalization(ctx context.Context, tx pgx.Tx, id string) (*protocol.MigrationFailureFinalizeRequest, error) {
	cleanup, life, authorized, err := lockNomadMigrationFailureCleanup(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if !authorized || !executionFailurePending(life) && life.Phase != SandboxLifecyclePhaseAborted {
		return nil, ErrNomadSandboxMigrationConflict
	}
	payload, err := readNomadExecutionFailureEvidence(ctx, tx, id, "failure_cleaned")
	if err != nil {
		return nil, err
	}
	request := &protocol.MigrationFailureFinalizeRequest{Request: *cleanup}
	if json.Unmarshal(payload, &request.Proof) != nil || request.Proof.ValidateFor(*cleanup) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, cleanup.Cleanup.WriterGrantID)
	if err != nil {
		return nil, err
	}
	stage := cleanup.Failure.Request.Restore.Stage
	binding, _ := stage.BindingDigest()
	if !rootFSWriterGrantMatchesRetireBase(grant, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch, stage.BindingVersion, binding[:]) ||
		grant.NodeUID != cleanup.Cleanup.NodeUID || grant.NodeBootID != cleanup.Cleanup.NodeBootID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if cleanup.Cleanup.WriterRetireKind == protocol.WriterRetireKindCanceled {
		if grant.State != RootFSWriterGrantStateCanceled {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else if grant.State != RootFSWriterGrantStateRetired || grant.RetireKind != RootFSWriterRetireKindCrashAbandon ||
		grant.RetireOperationID != cleanup.Cleanup.WriterOperationID || hex.EncodeToString(grant.RetireProofDigest) != request.Proof.Cleanup.ProofDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return request, nil
}

func (s *PGSandboxStore) CommitNomadSandboxMigrationFailureFinalization(ctx context.Context, request protocol.MigrationFailureFinalizeRequest, proof protocol.MigrationFailureFinalizeProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	id := request.Request.Failure.Request.Restore.Image.OperationID()
	stored, err := lockNomadMigrationFailureFinalization(ctx, tx, id)
	if err != nil {
		return err
	}
	if proof.ValidateFor(*stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	payload, err := readNomadExecutionFailureEvidence(ctx, tx, id, "failure_finalized")
	if err != nil {
		return err
	}
	if len(payload) != 0 {
		var prior protocol.MigrationFailureFinalizeProof
		if json.Unmarshal(payload, &prior) != nil || prior != proof {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, err = json.Marshal(proof)
	if err != nil {
		return err
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1`, id))
	if err != nil {
		return err
	}
	if err := saveNomadExecutionFailureEvidence(ctx, tx, life, "failure_finalized", payload, `UPDATE manager.sandbox_runtime_migrations SET failure_finalization_receipt=$2 WHERE operation_id=$1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
