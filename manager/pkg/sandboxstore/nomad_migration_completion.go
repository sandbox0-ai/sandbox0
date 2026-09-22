package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// CompleteNomadSandboxMigrationSource joins the previously committed node
// receipt and allocation-absence observation. Source lease release and lifecycle
// completion share one PostgreSQL transaction on success; failure cleanup only
// releases the source and retains the unfinished lifecycle. Neither public routing nor the
// target writer/lease is changed. Expired or terminating sandboxes can finish
// this historical cleanup without acquiring new execution authority.
func (s *PGSandboxStore) CompleteNomadSandboxMigrationSource(ctx context.Context, operation string) (*RuntimeSlot, error) {
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
	if _, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox); err != nil {
		return nil, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, operation))
	if err != nil {
		return nil, err
	}
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindMigrate || lifecycle.Source != SandboxLifecycleSourceAuto || lifecycle.Cancelable ||
		(lifecycle.Phase != SandboxLifecyclePhaseCommitting && lifecycle.Phase != SandboxLifecyclePhaseCommitted) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var sourceID string
	var requestJSON, proofJSON []byte
	var storedDigest *string
	err = tx.QueryRow(ctx, `SELECT source_slot_id,source_finalization_request,source_finalization_digest,source_finalization_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, operation).Scan(&sourceID, &requestJSON, &storedDigest, &proofJSON)
	if err == pgx.ErrNoRows {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return nil, err
	}
	var request protocol.MigrationSourceFinalizeRequest
	var proof protocol.MigrationSourceFinalizeProof
	if storedDigest == nil || json.Unmarshal(requestJSON, &request) != nil || json.Unmarshal(proofJSON, &proof) != nil || proof.ValidateFor(request) != nil || proof.RequestDigest != *storedDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	c := request.Cleanup
	capture := request.Fence.PublicationRequest.Capture
	if capture.Request.OperationID != operation || capture.Request.SandboxID != sandbox || capture.Request.LifecycleEpoch != lifecycle.Epoch ||
		capture.Request.SourceGeneration != lifecycle.FromGeneration || request.Fence.PublicationRequest.Assignment.Target.RuntimeGeneration != lifecycle.ToGeneration ||
		capture.RootFS.Generation.GenerationID != lifecycle.PreparedGenerationID || c.SlotID != sourceID || c.AllocationID != lifecycle.FromRuntimeID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, sourceID))
	if err != nil {
		return nil, err
	}
	var predecessor bool
	if err := tx.QueryRow(ctx, `SELECT COALESCE(migration_source_operation_id=$2,FALSE) FROM manager.runtime_slots WHERE slot_id=$1`, sourceID, operation).Scan(&predecessor); err != nil {
		return nil, err
	}
	if !predecessor || slot.SandboxID != sandbox || slot.AllocationID != c.AllocationID || slot.AllocationNamespace != lifecycle.FromRuntimeNamespace ||
		slot.ClusterID != c.ClusterID || slot.NodeID != c.NodeID || slot.NodeUID != c.NodeUID || slot.NodeBootID != c.NodeBootID || slot.NetNSIdentity != c.NetNSIdentity ||
		slot.RunscContainerID != c.RunscContainerID || slot.WriterGrantID != c.WriterGrantID || slot.ResourceLease != c.Resources ||
		hex.EncodeToString(slot.ResourceLeaseDigest) != c.ResourceLeaseDigest || hex.EncodeToString(slot.RootFSBindingDigest) != capture.Request.BindingDigest ||
		len(slot.OrphanObservationDigest) != sha256.Size || (slot.State != RuntimeSlotStateOrphaned && slot.State != RuntimeSlotStateTerminal) ||
		(lifecycle.Phase == SandboxLifecyclePhaseCommitted && slot.State != RuntimeSlotStateTerminal) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, c.WriterGrantID)
	if err != nil {
		return nil, err
	}
	if grant.State != RootFSWriterGrantStateRetired || grant.RetireKind != RootFSWriterRetireKindMigration || grant.RetireOperationID != operation || hex.EncodeToString(grant.RetireProofDigest) != request.SourceProof.Digest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	// The command digest binds the source incarnation, resource lease, storage
	// fence and target adoption. The node proof and stable orphan observation
	// add the two physical-absence gates to this terminal receipt.
	payload, err := json.Marshal(struct {
		Domain     string `json:"domain"`
		Operation  string `json:"operation"`
		Command    string `json:"command"`
		Node       string `json:"node"`
		Allocation string `json:"allocation"`
	}{"sandbox0-migration-source-completion-v1", operation, proof.RequestDigest, proof.Cleanup.ProofDigest, hex.EncodeToString(slot.OrphanObservationDigest)})
	if err != nil {
		return nil, err
	}
	terminalDigest := sha256.Sum256(payload)
	normalized, err := normalizeFinalizeRuntimeSlotRequest(&FinalizeRuntimeSlotRequest{SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, Reason: "migration_source",
		ProofDigest: terminalDigest[:], ResourceLeaseID: c.Resources.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest, ResourceCgroupAbsent: proof.Cleanup.ResourceCgroupAbsent})
	if err != nil {
		return nil, err
	}
	if _, err := finalizeRuntimeSlotTx(ctx, tx, slot, normalized); err != nil {
		return nil, err
	}
	// A failed destination still owns its independent writer and carrier.
	// Releasing the fully cleaned source cannot complete the migration.
	if lifecycle.Phase != SandboxLifecyclePhaseCommitted && request.Failure == nil {
		if err := (sandboxStoreTx{tx: tx}).CommitLifecycleTxn(ctx, operation, lifecycle.PreparedGenerationID); err != nil {
			return nil, err
		}
	}
	terminal, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, sourceID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return terminal, nil
}
