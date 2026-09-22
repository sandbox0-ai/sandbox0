package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Release recovery uses historical authority, not current sandbox generation,
// writer freshness or CPU TTL. A later lifecycle must not strand old custody.
func lockNomadMigrationStagingRelease(ctx context.Context, tx pgx.Tx, id string) (*nomadMigrationStaging, bool, bool, error) {
	if err := lockRuntimeSlotClaimOperation(ctx, tx, id); err != nil {
		return nil, false, false, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, id).Scan(&sandbox); err != nil {
		return nil, false, false, err
	}
	if _, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox); err != nil {
		return nil, false, false, err
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, id))
	if err != nil {
		return nil, false, false, err
	}
	if life == nil || life.Kind != SandboxLifecycleKindMigrate || life.Source != SandboxLifecycleSourceAuto || life.Cancelable {
		return nil, false, false, ErrNomadSandboxMigrationConflict
	}
	var sourceSlot, targetSlot, procd string
	var binding, finalization, finalized, adoption, adopted []byte
	var cancellation, canceled, failedCleanup, failedCleanupProof, failedFinalized []byte
	var captureFailure, captureCleanup, captureFinalized []byte
	var unused bool
	if err := tx.QueryRow(ctx, `SELECT source_slot_id,target_slot_id,source_procd_instance_id,source_binding_digest,
        preparation_request IS NULL AND capture_request IS NULL,source_finalization_request,source_finalization_receipt,adoption_request,adoption_receipt,
        preparation_cancel_request,preparation_cancel_receipt,failure_cleanup_request,failure_cleanup_receipt,failure_finalization_receipt,
        capture_failure_request,capture_failure_cleanup_receipt,capture_failure_finalization_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, id).Scan(&sourceSlot, &targetSlot, &procd, &binding,
		&unused, &finalization, &finalized, &adoption, &adopted, &cancellation, &canceled, &failedCleanup, &failedCleanupProof, &failedFinalized,
		&captureFailure, &captureCleanup, &captureFinalized); err != nil {
		return nil, false, false, err
	}
	p, err := loadNomadMigrationStaging(ctx, tx, id)
	if err != nil || p == nil {
		return nil, false, false, err
	}
	r := p.request
	if r.Source.SandboxID != sandbox || r.Source.LifecycleEpoch != life.Epoch || r.Source.SourceGeneration != life.FromGeneration ||
		r.Source.Target.SlotID != sourceSlot || r.Destination.SlotID != targetSlot || r.Source.ProcdInstanceID != procd || r.Source.BindingDigest != hex.EncodeToString(binding) {
		return nil, false, false, ErrNomadSandboxMigrationConflict
	}
	if len(canceled) != 0 {
		var command nomadmigration.PreparationCancellation
		var receipt procdapi.RuntimeMigrationResponse
		if json.Unmarshal(cancellation, &command) != nil || json.Unmarshal(canceled, &receipt) != nil || receipt.ValidateFor(command.Request) != nil {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		if _, err := command.Digest(); err != nil || command.Request.Assignment.OperationID != id ||
			command.Request.InstanceID != procd || command.Request.LifecycleEpoch != life.Epoch ||
			command.Request.Assignment.SourceGeneration != life.FromGeneration || command.Request.Assignment.Target.RuntimeGeneration != life.ToGeneration ||
			command.Request.Assignment.SourceRevision != r.Source.AssignmentRevision || command.Request.Assignment.Target.SandboxID != sandbox {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		unused = true // The schema forbids cancellation evidence beside capture authority.
	}
	unused = unused && life.Phase == SandboxLifecyclePhaseAborted
	sourceAllowed, targetAllowed := unused || p.sourceRelease, unused || p.destinationRelease
	if len(captureFailure) != 0 {
		var command protocol.MigrationCaptureFailureRequest
		if json.Unmarshal(captureFailure, &command) != nil || command.Capture.Request != r.Source {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		if _, err := command.Digest(); err != nil {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		// The immutable failure transaction proves this destination was never
		// attached, releases its reservation, and forever excludes publication.
		targetAllowed = true
		if len(captureFinalized) != 0 {
			var final protocol.MigrationCaptureFailureReceipt
			final.Request.Request = command
			if json.Unmarshal(captureCleanup, &final.Request.Proof) != nil || json.Unmarshal(captureFinalized, &final.Proof) != nil || final.Validate() != nil {
				return nil, false, false, ErrNomadSandboxMigrationConflict
			}
			sourceAllowed = true
		}
	}
	if len(finalized) != 0 {
		var command protocol.MigrationSourceFinalizeRequest
		var proof protocol.MigrationSourceFinalizeProof
		if json.Unmarshal(finalization, &command) != nil || json.Unmarshal(finalized, &proof) != nil || proof.ValidateFor(command) != nil ||
			command.Fence.PublicationRequest.Capture.Request != r.Source || command.Destination() != r.Destination {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		sourceAllowed = true
	}
	if len(adopted) != 0 {
		var command protocol.MigrationAdoptionRequest
		var proof protocol.MigrationAdoptionProof
		if json.Unmarshal(adoption, &command) != nil || json.Unmarshal(adopted, &proof) != nil || proof.ValidateFor(command) != nil ||
			command.Target != r.Destination || command.OperationID != id || command.SandboxID != sandbox ||
			command.RuntimeGeneration != life.ToGeneration || command.ProcdInstanceID != procd {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		targetAllowed = true
	}
	if len(failedFinalized) != 0 {
		var command protocol.MigrationFailureFinalizeRequest
		var proof protocol.MigrationFailureFinalizeProof
		if json.Unmarshal(failedCleanup, &command.Request) != nil || json.Unmarshal(failedCleanupProof, &command.Proof) != nil ||
			json.Unmarshal(failedFinalized, &proof) != nil || proof.ValidateFor(command) != nil ||
			command.Request.Failure.Request.Restore.Image.Target != r.Destination ||
			command.Request.Failure.Request.Restore.Image.Publication.Capture.Request != r.Source {
			return nil, false, false, ErrNomadSandboxMigrationConflict
		}
		targetAllowed = true
	}
	return p, sourceAllowed, targetAllowed, nil
}

// AuthorizeNomadSandboxMigrationStagingRelease records a release command before
// dispatch, including for a side whose reserve reply was never received. The
// node's cancellation tombstone closes the late-reserve race for that side.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationStagingRelease(ctx context.Context, id string) (*protocol.MigrationStagingRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	p, source, target, err := lockNomadMigrationStagingRelease(ctx, tx, id)
	if err != nil || p == nil {
		return nil, err
	}
	var request protocol.MigrationStagingRequest
	query := ""
	if source && p.sourceReleased == nil {
		request = p.request
		query = `UPDATE manager.sandbox_runtime_migrations SET staging_source_release_requested=true WHERE operation_id=$1`
	} else if target && p.destinationReleased == nil {
		request = p.targetRequest()
		query = `UPDATE manager.sandbox_runtime_migrations SET staging_destination_release_requested=true WHERE operation_id=$1`
	} else {
		return nil, nil
	}
	if _, err := tx.Exec(ctx, query, id); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

func (s *PGSandboxStore) CommitNomadSandboxMigrationStagingRelease(ctx context.Context, request protocol.MigrationStagingRequest, receipt protocol.MigrationStagingReleased) error {
	if err := receipt.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	p, _, _, err := lockNomadMigrationStagingRelease(ctx, tx, request.Source.OperationID)
	if err != nil {
		return err
	}
	if p == nil {
		return ErrNomadSandboxMigrationConflict
	}
	expected, authorized, prior := p.request, p.sourceRelease, p.sourceReleased
	query := `UPDATE manager.sandbox_runtime_migrations SET staging_source_release_receipt=$2 WHERE operation_id=$1`
	if !request.IsSource() {
		expected, authorized, prior = p.targetRequest(), p.destinationRelease, p.destinationReleased
		query = `UPDATE manager.sandbox_runtime_migrations SET staging_destination_release_receipt=$2 WHERE operation_id=$1`
	}
	if !authorized || request != expected {
		return ErrNomadSandboxMigrationConflict
	}
	if prior != nil {
		if *prior != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, query, request.Source.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
