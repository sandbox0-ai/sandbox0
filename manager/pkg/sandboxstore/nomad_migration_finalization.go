package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// ErrNomadSandboxMigrationNotReady keeps source custody while the destination
// has not yet committed adoption or irreversible failure-stop evidence.
// It remains a conflict for direct callers.
var ErrNomadSandboxMigrationNotReady = fmt.Errorf("%w: target disposition is pending", ErrNomadSandboxMigrationConflict)

// GetNomadSandboxMigrationSourceFinalizationForSlot uses the immutable source
// marker, never the sandbox's current allocation or a historical reservation.
// It remains readable after terminal release without renewing any authority.
func (s *PGSandboxStore) GetNomadSandboxMigrationSourceFinalizationForSlot(ctx context.Context, slot string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	if protocol.ValidateSlotID(slot) != nil {
		return nil, ErrRuntimeSlotInvalid
	}
	var operation *string
	err := s.pool.QueryRow(ctx, `SELECT migration_source_operation_id FROM manager.runtime_slots WHERE slot_id=$1`, slot).Scan(&operation)
	if err == pgx.ErrNoRows {
		return nil, ErrRuntimeSlotNotFound
	}
	if err != nil {
		return nil, err
	}
	if operation == nil {
		return nil, nil
	}
	receipt, err := s.GetNomadSandboxMigrationSourceFinalization(ctx, *operation)
	if err != nil {
		return nil, err
	}
	if receipt != nil && receipt.Request.Cleanup.SlotID != slot {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return receipt, nil
}

// GetNomadSandboxMigrationSourceFinalization reads historical node evidence so
// reconciliation can finish allocation purge after ctld becomes unavailable.
func (s *PGSandboxStore) GetNomadSandboxMigrationSourceFinalization(ctx context.Context, operation string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	var requestJSON, proofJSON []byte
	var digest *string
	err := s.pool.QueryRow(ctx, `SELECT source_finalization_request,source_finalization_digest,source_finalization_receipt
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, operation).Scan(&requestJSON, &digest, &proofJSON)
	if err == pgx.ErrNoRows {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return nil, err
	}
	if len(proofJSON) == 0 {
		return nil, nil
	}
	var receipt protocol.MigrationSourceFinalizationReceipt
	if json.Unmarshal(requestJSON, &receipt.Request) != nil || json.Unmarshal(proofJSON, &receipt.Proof) != nil ||
		digest == nil || receipt.Proof.RequestDigest != *digest || receipt.Validate() != nil ||
		receipt.Request.Fence.PublicationRequest.Assignment.OperationID != operation {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &receipt, nil
}

// AuthorizeNomadSandboxMigrationSourceFinalization derives the source command
// only from committed regional evidence. Expired TTL, desired termination and
// target lease loss do not revoke cleanup of this already-fenced predecessor.
// This method grants no execution and releases neither resource lease.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationSourceFinalization(ctx context.Context, operation string) (*protocol.MigrationSourceFinalizeRequest, error) {
	if operation == "" {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeSlotClaimOperation(ctx, tx, operation); err != nil {
		return nil, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, operation).Scan(&sandbox); err != nil {
		if err == pgx.ErrNoRows {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return nil, err
	}
	// Keep the same sandbox/lifecycle lock order as claim and handover, but do
	// not apply their live-execution/TTL predicate to historical cleanup.
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox)
	if err != nil {
		return nil, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, operation))
	if err != nil {
		return nil, err
	}
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindMigrate || lifecycle.Source != SandboxLifecycleSourceAuto || lifecycle.Cancelable {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var sourceID, targetID, writerID, assignmentDigest string
	var sourceBinding, fenceJSON, sourceProofJSON, adoptionJSON, adoptionProofJSON, restoreJSON, priorJSON []byte
	var failureJSON, failureProofJSON []byte
	var priorDigest *string
	var committed bool
	err = tx.QueryRow(ctx, `SELECT source_slot_id,target_slot_id,source_writer_grant_id,source_binding_digest,assignment_digest,
        source_fence_request,source_fence_proof,adoption_request,adoption_receipt,restore_receipt,
        source_finalization_request,source_finalization_digest,generation_committed_at IS NOT NULL,failure_request,failure_stop_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, operation).Scan(
		&sourceID, &targetID, &writerID, &sourceBinding, &assignmentDigest, &fenceJSON, &sourceProofJSON, &adoptionJSON, &adoptionProofJSON, &restoreJSON, &priorJSON, &priorDigest, &committed, &failureJSON, &failureProofJSON)
	if err == pgx.ErrNoRows {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return nil, err
	}
	request := protocol.MigrationSourceFinalizeRequest{}
	var restored protocol.MigrationRestoreObservation
	if len(failureJSON) == 0 && (!committed || len(adoptionProofJSON) == 0) {
		return nil, ErrNomadSandboxMigrationNotReady
	}
	if json.Unmarshal(fenceJSON, &request.Fence) != nil || json.Unmarshal(sourceProofJSON, &request.SourceProof) != nil || request.SourceProof.ValidateFor(request.Fence) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(failureJSON) != 0 {
		if len(failureProofJSON) == 0 {
			return nil, ErrNomadSandboxMigrationNotReady
		}
		request.Failure = &protocol.MigrationFailureStopReceipt{}
		if committed || len(adoptionJSON) != 0 || json.Unmarshal(failureJSON, &request.Failure.Request) != nil ||
			json.Unmarshal(failureProofJSON, &request.Failure.Proof) != nil || request.Failure.Proof.ValidateFor(request.Failure.Request) != nil ||
			request.SourceProof.Digest != request.Failure.Request.Restore.Proof.Digest {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else {
		if !committed || len(adoptionProofJSON) == 0 {
			return nil, ErrNomadSandboxMigrationNotReady
		}
		if json.Unmarshal(adoptionJSON, &request.Adoption.Request) != nil || json.Unmarshal(adoptionProofJSON, &request.Adoption.Proof) != nil ||
			json.Unmarshal(restoreJSON, &restored) != nil || request.Adoption.Request.ValidateFor(restored) != nil || request.Adoption.Proof.ValidateFor(request.Adoption.Request) != nil ||
			request.SourceProof.Digest != restored.Request.Proof.Digest {
			return nil, fmt.Errorf("%w: incomplete committed adoption or fence evidence", ErrNomadSandboxMigrationConflict)
		}
	}
	capture := request.Fence.PublicationRequest.Capture.Request
	assignment := request.Fence.PublicationRequest.Assignment
	actualAssignment, err := assignment.Digest()
	if err != nil || actualAssignment != assignmentDigest || assignment.Target.TeamID != record.TeamID ||
		capture.OperationID != operation || capture.SandboxID != sandbox || capture.LifecycleEpoch != lifecycle.Epoch ||
		capture.SourceGeneration != lifecycle.FromGeneration || assignment.Target.RuntimeGeneration != lifecycle.ToGeneration ||
		capture.Target.SlotID != sourceID || capture.Target.AllocationID != lifecycle.FromRuntimeID ||
		request.Destination().SlotID != targetID || request.Destination().AllocationID != lifecycle.ToRuntimeID ||
		capture.BindingDigest != hex.EncodeToString(sourceBinding) {
		return nil, fmt.Errorf("%w: cleanup lifecycle identity changed", ErrNomadSandboxMigrationConflict)
	}
	if len(priorJSON) != 0 {
		var prior protocol.MigrationSourceFinalizeRequest
		if json.Unmarshal(priorJSON, &prior) != nil || priorDigest == nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		actual, err := prior.Digest()
		if err != nil || actual != *priorDigest || prior.SourceProof.Digest != request.SourceProof.Digest ||
			prior.Adoption != request.Adoption || (prior.Failure == nil) != (request.Failure == nil) || prior.Cleanup.WriterGrantID != writerID {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if prior.Failure != nil && prior.Failure.Proof != request.Failure.Proof {
			return nil, ErrNomadSandboxMigrationConflict
		}
		// Once authorized, retry the original incarnation even if later slot
		// observations change. Never reconstruct a command for a new endpoint.
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &prior, nil
	}
	source, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, sourceID))
	if err != nil {
		return nil, err
	}
	target := protocol.NodeChannelTarget{SlotID: source.ID, ClusterID: source.ClusterID, NodeID: source.NodeID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, AllocationID: source.AllocationID, ControlEndpoint: source.ControlEndpoint}
	if target != capture.Target || source.SandboxID != sandbox || source.AllocationNamespace != lifecycle.FromRuntimeNamespace || source.WriterGrantID != writerID ||
		!bytes.Equal(source.RootFSBindingDigest, sourceBinding) || source.ProcdInstanceID != capture.ProcdInstanceID ||
		source.ClaimRuntimeAssignmentRevision != capture.AssignmentRevision || hex.EncodeToString(source.ResourceLeaseDigest) != capture.ResourceLeaseDigest {
		return nil, fmt.Errorf("%w: cleanup source slot identity changed", ErrNomadSandboxMigrationConflict)
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, writerID)
	if err != nil {
		return nil, err
	}
	if grant.State != RootFSWriterGrantStateRetired || grant.RetireKind != RootFSWriterRetireKindMigration || grant.RetireOperationID != operation ||
		hex.EncodeToString(grant.RetireProofDigest) != request.SourceProof.Digest || !bytes.Equal(grant.BindingDigest, sourceBinding) {
		return nil, fmt.Errorf("%w: source writer lacks exact terminal fence", ErrNomadSandboxMigrationConflict)
	}
	request.Cleanup = protocol.NodeCleanupControlRequest{OperationID: protocol.MigrationSourceCleanupOperationID(operation), WriterOperationID: operation, WriterRetireKind: protocol.WriterRetireKindMigration,
		SlotID: source.ID, ClusterID: source.ClusterID, AllocationID: source.AllocationID, NodeID: source.NodeID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID, NetNSIdentity: source.NetNSIdentity,
		RunscContainerID: source.RunscContainerID, WriterGrantID: writerID, WriterAuthorityDigest: request.SourceProof.Digest, Resources: source.ResourceLease, ResourceLeaseDigest: capture.ResourceLeaseDigest}
	want, err := request.Digest()
	if err != nil {
		return nil, fmt.Errorf("%w: source cleanup command: %v", ErrNomadSandboxMigrationConflict, err)
	}
	var predecessor bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(migration_source_operation_id=$2,FALSE) FROM manager.runtime_slots WHERE slot_id=$1`, sourceID, operation).Scan(&predecessor); err != nil {
		return nil, err
	}
	if !predecessor || lifecycle.Phase != SandboxLifecyclePhaseCommitting ||
		(source.State != RuntimeSlotStateQuiescing && source.State != RuntimeSlotStateOrphaned) || source.ResourceLeaseState != RuntimeResourceLeaseActive || !source.ResourceLeaseReleasedAt.IsZero() {
		return nil, ErrNomadSandboxMigrationConflict
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET source_finalization_request=$2,source_finalization_digest=$3 WHERE operation_id=$1`, operation, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationSourceFinalization retains the exact node receipt
// even after TTL/termination. Allocation disappearance and capacity release are
// separate gates; a physical cleanup response cannot complete the lifecycle.
func (s *PGSandboxStore) CommitNomadSandboxMigrationSourceFinalization(ctx context.Context, request protocol.MigrationSourceFinalizeRequest, proof protocol.MigrationSourceFinalizeProof) error {
	if proof.ValidateFor(request) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	operation := request.Fence.PublicationRequest.Assignment.OperationID
	var payload, priorPayload []byte
	var storedDigest *string
	err = tx.QueryRow(ctx, `SELECT source_finalization_request,source_finalization_digest,source_finalization_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, operation).Scan(&payload, &storedDigest, &priorPayload)
	if err == pgx.ErrNoRows {
		return ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return err
	}
	want, _ := request.Digest()
	var stored protocol.MigrationSourceFinalizeRequest
	if storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil || proof.ValidateFor(stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	if len(priorPayload) != 0 {
		var prior protocol.MigrationSourceFinalizeProof
		if json.Unmarshal(priorPayload, &prior) != nil || prior != proof {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, err = json.Marshal(proof)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET source_finalization_receipt=$2 WHERE operation_id=$1`, operation, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
