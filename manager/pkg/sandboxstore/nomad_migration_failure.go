package sandboxstore

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const nomadMigrationFailureDue = `(EXISTS (SELECT 1 FROM manager.sandboxes s WHERE s.sandbox_id=l.sandbox_id
    AND (s.desired_state<>'active' OR s.deleted_at IS NOT NULL OR s.hard_expires_at<=clock_timestamp()))
    OR EXISTS (SELECT 1 FROM manager.runtime_slots t WHERE t.slot_id=m.target_slot_id
        AND (t.claim_lease_expires_at<=clock_timestamp() OR t.state IN ('quiescing','orphaned','terminal'))))`

const nomadMigrationFailurePredicate = `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable AND l.phase='committing'
    AND m.restore_request IS NOT NULL AND m.failure_request IS NULL AND m.generation_committed_at IS NULL
    AND m.adoption_request IS NULL AND ` + nomadMigrationFailureDue

func (s *PGSandboxStore) ListNomadMigrationFailures(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, nomadMigrationFailurePredicate)
}

func (s *PGSandboxStore) ListNomadMigrationFailureStops(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadMigrationWork(ctx, after, limit, `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
        AND l.phase='committing' AND m.failure_request IS NOT NULL AND m.failure_stop_receipt IS NULL`)
}

func (s *PGSandboxStore) GetNomadMigrationFailure(ctx context.Context, id string) (*protocol.MigrationFailureRequest, error) {
	var payload []byte
	var digest *string
	err := s.pool.QueryRow(ctx, `SELECT failure_request,failure_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, id).Scan(&payload, &digest)
	if err == pgx.ErrNoRows || err == nil && len(payload) == 0 {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var request protocol.MigrationFailureRequest
	if digest == nil || json.Unmarshal(payload, &request) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	want, err := request.Digest()
	if err != nil || want != *digest || request.Restore.Image.Publication.Assignment.OperationID != id {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return &request, nil
}

// CommitNomadSandboxMigrationFailureStop stores only exact historical physical
// evidence. A response-lost retry cannot invent failure authority or release a
// writer, image, allocation or capacity lease.
func (s *PGSandboxStore) CommitNomadSandboxMigrationFailureStop(ctx context.Context, request protocol.MigrationFailureRequest, proof protocol.MigrationFailureStopProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	id := request.Restore.Image.Publication.Assignment.OperationID
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeSlotClaimOperation(ctx, tx, id); err != nil {
		return err
	}
	if _, err := lockNomadSandboxClaimRecord(ctx, tx, request.Restore.Image.Publication.Assignment.Target.SandboxID); err != nil {
		return err
	}
	var payload, prior []byte
	var digest *string
	if err := tx.QueryRow(ctx, `SELECT failure_request,failure_digest,failure_stop_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, id).Scan(&payload, &digest, &prior); err != nil {
		return err
	}
	var stored protocol.MigrationFailureRequest
	if digest == nil || *digest != proof.RequestDigest || json.Unmarshal(payload, &stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	want, err := stored.Digest()
	if err != nil || want != *digest {
		return ErrNomadSandboxMigrationConflict
	}
	if len(prior) != 0 {
		var existing protocol.MigrationFailureStopProof
		if json.Unmarshal(prior, &existing) != nil || existing != proof {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, err = json.Marshal(proof)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET failure_stop_receipt=$2 WHERE operation_id=$1`, id, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AuthorizeNomadSandboxMigrationFailure records an irreversible failure intent
// and quiesces the exact reserved target atomically. Historical retries do not
// require live leases, but a healthy or already-committed target is never failed.
// Both capacity leases, source fencing, and all images remain in custody until
// the separate physical cleanup protocol supplies its evidence.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationFailure(ctx context.Context, id string) (*protocol.MigrationFailureRequest, error) {
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
	if life == nil || life.Kind != SandboxLifecycleKindMigrate || life.Source != SandboxLifecycleSourceAuto || life.Cancelable || life.Phase != SandboxLifecyclePhaseCommitting {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var restoreJSON, priorJSON []byte
	var restoreDigest, priorDigest *string
	var source, target, lease, assignmentDigest string
	var committed bool
	if err := tx.QueryRow(ctx, `SELECT restore_request,restore_digest,failure_request,failure_digest,
        source_slot_id,target_slot_id,target_resource_lease_id,assignment_digest,
        generation_committed_at IS NOT NULL OR adoption_request IS NOT NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, id).Scan(
		&restoreJSON, &restoreDigest, &priorJSON, &priorDigest, &source, &target, &lease, &assignmentDigest, &committed); err != nil {
		return nil, err
	}
	var command protocol.MigrationFailureRequest
	if committed || restoreDigest == nil || json.Unmarshal(restoreJSON, &command.Restore) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	r := command.Restore
	digest, err := r.Digest()
	a := r.Image.Publication.Assignment
	ad, ae := a.Digest()
	if err != nil || digest != *restoreDigest || ae != nil || ad != assignmentDigest || a.OperationID != id ||
		a.Target.SandboxID != sandbox || a.Target.TeamID != record.TeamID || a.SourceGeneration != life.FromGeneration ||
		a.Target.RuntimeGeneration != life.ToGeneration || r.Image.Publication.Capture.Request.LifecycleEpoch != life.Epoch ||
		r.Image.Publication.Capture.Request.Target.SlotID != source || r.Image.Target.SlotID != target || r.Image.Resources.LeaseID != lease ||
		r.Image.Target.AllocationID != life.ToRuntimeID || r.Image.Publication.Capture.Request.Target.AllocationID != life.FromRuntimeID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(priorJSON) != 0 {
		if priorDigest == nil || json.Unmarshal(priorJSON, &command) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		actual, err := command.Digest()
		restored, re := command.Restore.Digest()
		if err != nil || re != nil || actual != *priorDigest || restored != *restoreDigest {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return &command, tx.Commit(ctx)
	}
	slot, err := lockRuntimeSlotByID(ctx, tx, target)
	if err != nil {
		return nil, err
	}
	if slot.SandboxID != sandbox || slot.AllocationID != r.Image.Target.AllocationID || slot.NodeUID != r.Image.Target.NodeUID ||
		slot.NodeBootID != r.Image.Target.NodeBootID || slot.ResourceLease.LeaseID != lease || slot.WriterGrantID != r.Stage.Identity.WriterGrantID ||
		slot.ClaimOperationID != id || record.RuntimeGeneration != life.FromGeneration || record.RuntimeID != life.FromRuntimeID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var due, terminating bool
	if err := tx.QueryRow(ctx, `SELECT `+nomadMigrationFailureDue+`,
        s.desired_state<>'active' OR s.deleted_at IS NOT NULL OR COALESCE(s.hard_expires_at<=clock_timestamp(),false)
        FROM manager.sandbox_runtime_migrations m JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=m.operation_id
        JOIN manager.sandboxes s ON s.sandbox_id=l.sandbox_id WHERE m.operation_id=$1`, id).Scan(&due, &terminating); err != nil {
		return nil, err
	}
	if !due {
		return nil, nil
	}
	command.Reason = protocol.MigrationFailureDestinationUnavailable
	if terminating {
		command.Reason = protocol.MigrationFailureTermination
	}
	want, err := command.Digest()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(command)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET failure_request=$2,failure_digest=$3 WHERE operation_id=$1`, id, payload, want); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state='quiescing',quiescing_at=COALESCE(quiescing_at,clock_timestamp()),
        revision=revision+1,updated_at=clock_timestamp() WHERE slot_id=$1 AND state IN ('claiming','starting','active')`, target); err != nil {
		return nil, err
	}
	return &command, tx.Commit(ctx)
}
