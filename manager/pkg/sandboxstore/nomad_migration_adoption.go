package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// GetNomadSandboxMigrationAdoptionForSlot reads the immutable command published
// by generation CAS. It does not reconstruct authority from current runtime
// state, renew TTL or allocate resources. Target slots are globally single-use.
func (s *PGSandboxStore) GetNomadSandboxMigrationAdoptionForSlot(ctx context.Context, slot string) (*protocol.MigrationAdoptionRequest, error) {
	if protocol.ValidateSlotID(slot) != nil {
		return nil, ErrRuntimeSlotInvalid
	}
	var operation, storedDigest string
	var payload, restorePayload []byte
	err := s.pool.QueryRow(ctx, `SELECT operation_id,adoption_request,adoption_digest,restore_receipt
		FROM manager.sandbox_runtime_migrations WHERE target_slot_id=$1 AND generation_committed_at IS NOT NULL`, slot).Scan(&operation, &payload, &storedDigest, &restorePayload)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var request protocol.MigrationAdoptionRequest
	var restored protocol.MigrationRestoreObservation
	if json.Unmarshal(payload, &request) != nil || json.Unmarshal(restorePayload, &restored) != nil || request.ValidateFor(restored) != nil || request.Target.SlotID != slot || request.OperationID != operation {
		return nil, ErrNomadSandboxMigrationConflict
	}
	digest, err := request.Digest()
	if err != nil || digest != storedDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &request, nil
}

// persistNomadMigrationAdoption runs inside the generation publication
// transaction. A retry returns the original command instead of authorizing a
// different image cleanup or acknowledging another command-ready proof.
func persistNomadMigrationAdoption(ctx context.Context, tx pgx.Tx, restored protocol.MigrationRestoreObservation, ready *MarkRuntimeSlotCommandReadyRequest) (*protocol.MigrationAdoptionRequest, error) {
	image := restored.Request.Image
	request := protocol.MigrationAdoptionRequest{Target: image.Target, OperationID: ready.OperationID, ClaimID: ready.ClaimID,
		SandboxID: image.Publication.Assignment.Target.SandboxID, RuntimeGeneration: image.Publication.Assignment.Target.RuntimeGeneration,
		ProcdInstanceID: ready.ProcdInstanceID, RestoreDigest: restored.RequestDigest, CommandReadyDigest: hex.EncodeToString(ready.CommandReadyDigest)}
	if request.ValidateFor(restored) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	want, _ := request.Digest()
	var payload []byte
	var priorDigest *string
	if err := tx.QueryRow(ctx, `SELECT adoption_request,adoption_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, ready.OperationID).Scan(&payload, &priorDigest); err != nil {
		return nil, err
	}
	if len(payload) != 0 {
		var prior protocol.MigrationAdoptionRequest
		if priorDigest == nil || *priorDigest != want || json.Unmarshal(payload, &prior) != nil || prior != request {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return &prior, nil
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET adoption_request=$2,adoption_digest=$3 WHERE operation_id=$1`, ready.OperationID, payload, want); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationAdoption consumes an authenticated target-node
// receipt. Recording historical cleanup remains possible after TTL expiry; it
// grants no execution authority and never releases either resource lease.
func (s *PGSandboxStore) CommitNomadSandboxMigrationAdoption(ctx context.Context, request protocol.MigrationAdoptionRequest, receipt protocol.MigrationAdoptionProof) error {
	if receipt.ValidateFor(request) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var payload, restorePayload, priorPayload []byte
	var storedDigest *string
	err = tx.QueryRow(ctx, `SELECT adoption_request,adoption_digest,restore_receipt,adoption_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 AND generation_committed_at IS NOT NULL FOR UPDATE`, request.OperationID).
		Scan(&payload, &storedDigest, &restorePayload, &priorPayload)
	if err == pgx.ErrNoRows {
		return ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return err
	}
	var stored protocol.MigrationAdoptionRequest
	var restored protocol.MigrationRestoreObservation
	want, _ := request.Digest()
	if storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil || stored != request ||
		json.Unmarshal(restorePayload, &restored) != nil || request.ValidateFor(restored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	if len(priorPayload) != 0 {
		var prior protocol.MigrationAdoptionProof
		if json.Unmarshal(priorPayload, &prior) != nil || prior != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, err = json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET adoption_receipt=$2 WHERE operation_id=$1`, request.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
