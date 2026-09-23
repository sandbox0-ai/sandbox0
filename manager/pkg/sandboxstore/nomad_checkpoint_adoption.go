package sandboxstore

import (
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// persistNomadCheckpointAdoption runs in resume's routing/generation commit.
// The existing node delivery/retry channel can then remove its private image.
// Regional immutable objects remain pinned by restore history and other forks.
func persistNomadCheckpointAdoption(ctx context.Context, tx pgx.Tx, slot *RuntimeSlot) error {
	var payload []byte
	err := tx.QueryRow(ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, slot.ClaimOperationID).Scan(&payload)
	if err == pgx.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	var e NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &e) != nil || e.HandedOver == nil || e.Restored == nil || e.Restored.Validate() != nil ||
		e.Restored.State != protocol.MigrationRestoreComplete || e.Restored.Request.Image.Checkpoint == nil {
		return ErrNomadCheckpointConflict
	}
	image := e.Restored.Request.Image
	checkpoint, _ := image.Checkpoint.Assignment.Digest()
	a := image.RuntimeAssignment()
	request := protocol.MigrationAdoptionRequest{Target: image.Target, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID,
		SandboxID: a.SandboxID, RuntimeGeneration: a.RuntimeGeneration, ProcdInstanceID: slot.ProcdInstanceID,
		RestoreDigest: e.Restored.RequestDigest, CommandReadyDigest: hex.EncodeToString(slot.CommandReadyDigest), CheckpointRestoreDigest: checkpoint}
	if request.ValidateFor(*e.Restored) != nil || (e.Adoption != nil && *e.Adoption != request) {
		return ErrNomadCheckpointConflict
	}
	if e.Adoption != nil {
		return nil
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=jsonb_set(evidence,'{adoption}',$2::jsonb) WHERE operation_id=$1`, slot.ClaimOperationID, payload)
	return err
}

func (s *PGSandboxStore) getNomadCheckpointAdoptionForSlot(ctx context.Context, slot string) (*protocol.MigrationAdoptionRequest, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT r.evidence FROM manager.sandbox_runtime_checkpoint_restores r
		JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id
		WHERE r.evidence ? 'image' AND r.evidence->'image'->'target'->>'slot_id'=$1 AND l.phase='committed'`, slot).Scan(&payload)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &e) != nil || e.Adoption == nil || e.Restored == nil || e.Adoption.Target.SlotID != slot || e.Adoption.ValidateFor(*e.Restored) != nil {
		return nil, ErrNomadCheckpointConflict
	}
	return e.Adoption, nil
}

// Adoption is historical cleanup, so it survives owner TTL/deletion and later
// lifecycles. It grants neither execution nor resource release.
func (s *PGSandboxStore) commitNomadCheckpointAdoption(ctx context.Context, request protocol.MigrationAdoptionRequest, receipt protocol.MigrationAdoptionProof) error {
	if request.CheckpointRestoreDigest == "" || receipt.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT r.evidence FROM manager.sandbox_runtime_checkpoint_restores r
		JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=r.operation_id WHERE r.operation_id=$1 AND l.phase='committed' FOR UPDATE OF r`, request.OperationID).Scan(&payload)
	if err == pgx.ErrNoRows {
		return ErrNomadCheckpointConflict
	}
	if err != nil {
		return err
	}
	var e NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &e) != nil || e.Adoption == nil || *e.Adoption != request || e.Restored == nil || request.ValidateFor(*e.Restored) != nil ||
		(e.Adopted != nil && *e.Adopted != receipt) {
		return ErrNomadCheckpointConflict
	}
	if e.Adopted != nil {
		return tx.Commit(ctx)
	}
	payload, err = json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=jsonb_set(evidence,'{adopted}',$2::jsonb) WHERE operation_id=$1`, request.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
