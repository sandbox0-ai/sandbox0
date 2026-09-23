package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationSourceFence fences regional renewal and stores
// the exact physical command atomically. Only an already-committed image may
// lose its source writer. No target writer or execution is authorized here.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationSourceFence(ctx context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceRequest, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, request.PublicationRequest.Assignment)
	if err != nil {
		return nil, err
	}
	var publication, payload []byte
	var storedDigest *string
	if err := tx.QueryRow(ctx, `SELECT publication_receipt,source_fence_request,source_fence_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, reservation.Lifecycle.ID).Scan(&publication, &payload, &storedDigest); err != nil {
		return nil, err
	}
	var receipt protocol.MigrationPublication
	if json.Unmarshal(publication, &receipt) != nil || receipt != request.Publication ||
		reservation.Lifecycle.PreparedGenerationID != request.PublicationRequest.Capture.RootFS.Generation.GenerationID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(payload) != 0 {
		var stored protocol.MigrationSourceFenceRequest
		if storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		actual, err := stored.Digest()
		if err != nil || actual != want {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing {
		return nil, ErrNomadSandboxMigrationConflict
	}
	policyDigest, err := migrationSourcePolicyDigest(ctx, tx, request.PublicationRequest.Assignment)
	if err != nil {
		return nil, err
	}
	if policyDigest != reservation.SourceSlot.ClaimNetworkPolicyDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := requireNomadMigrationTargetImage(ctx, tx, reservation); err != nil {
		return nil, err
	}
	if err := validateNomadMigrationTarget(ctx, tx, reservation); err != nil {
		return nil, err
	}
	if err := validateNomadMigrationPublicationSource(ctx, tx, reservation, request.PublicationRequest); err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET state='retiring',retire_kind=$2,retire_operation_id=$3,retire_started_at=NOW(),updated_at=NOW()
		WHERE grant_id=$1 AND state='consumed' AND retire_kind='' AND retire_operation_id='' AND retire_proof_digest IS NULL`, reservation.SourceWriterGrantID, RootFSWriterRetireKindMigration, reservation.Lifecycle.ID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET source_fence_request=$2,source_fence_digest=$3 WHERE operation_id=$1`, reservation.Lifecycle.ID, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationSourceFence installs the captured RootFS head
// only after the exact source execution and writer are physically absent.
// The lifecycle stays active in committing; this is not a completed migration
// and does not release source leases or attach the reserved destination.
func (s *PGSandboxStore) CommitNomadSandboxMigrationSourceFence(ctx context.Context, request protocol.MigrationSourceFenceRequest, proof protocol.MigrationSourceFenceProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, request.PublicationRequest.Assignment)
	if err != nil {
		return err
	}
	var payload, prior []byte
	var storedDigest *string
	if err := tx.QueryRow(ctx, `SELECT source_fence_request,source_fence_digest,source_fence_proof FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, reservation.Lifecycle.ID).Scan(&payload, &storedDigest, &prior); err != nil {
		return err
	}
	want, _ := request.Digest()
	var stored protocol.MigrationSourceFenceRequest
	if storedDigest == nil || *storedDigest != want || json.Unmarshal(payload, &stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	actual, err := stored.Digest()
	if err != nil || actual != want {
		return ErrNomadSandboxMigrationConflict
	}
	if len(prior) != 0 {
		var previous protocol.MigrationSourceFenceProof
		if json.Unmarshal(prior, &previous) != nil || previous.Digest != proof.Digest || previous.ValidateFor(request) != nil || reservation.Lifecycle.Phase != SandboxLifecyclePhaseCommitting {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing || reservation.Lifecycle.PreparedGenerationID != request.PublicationRequest.Capture.RootFS.Generation.GenerationID {
		return ErrNomadSandboxMigrationConflict
	}
	if err := commitNomadExecutionSourceFence(ctx, tx, reservation.Lifecycle, reservation.SourceWriterGrantID, reservation.SourceBindingDigest, proof); err != nil {
		return err
	}
	if err := (sandboxStoreTx{tx: tx}).UpdateLifecycleTxnPhase(ctx, reservation.Lifecycle.ID, SandboxLifecyclePhaseCommitting); err != nil {
		return err
	}
	payload, err = json.Marshal(proof)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET source_fence_proof=$2 WHERE operation_id=$1`, reservation.Lifecycle.ID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// commitNomadExecutionSourceFence shares the physical writer retirement and
// immutable RootFS head CAS between migration and retained memory pause.
// Callers must first lock and validate their exact authorized fence command.
func commitNomadExecutionSourceFence(ctx context.Context, tx pgx.Tx, lifecycle *SandboxLifecycleTxn, grantID string, binding []byte, proof protocol.MigrationSourceFenceProof) error {
	filesystem, initial, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, lifecycle.SandboxID)
	if err != nil {
		return err
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, grantID)
	if err != nil {
		return err
	}
	if grant.State != RootFSWriterGrantStateRetiring || grant.RetireKind != RootFSWriterRetireKindMigration || grant.RetireOperationID != lifecycle.ID ||
		len(grant.RetireProofDigest) != 0 || !bytes.Equal(grant.BindingDigest, binding) || grant.GateParent != proof.RootFS.Session.Parent ||
		grant.WriterEpoch != proof.RootFS.Session.WriterEpoch || filesystem.WriterEpoch != grant.WriterEpoch || filesystem.ID != grant.FilesystemID ||
		initial.ID != lifecycle.ExpectedGenerationID || grant.InitialGenerationID != initial.ID {
		return ErrNomadSandboxMigrationConflict
	}
	proofDigest, err := hex.DecodeString(proof.Digest)
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE manager.rootfs_filesystems SET head_generation_id=$2,updated_at=NOW()
		WHERE filesystem_id=$1 AND head_generation_id=$3 AND writer_epoch=$4`, filesystem.ID, lifecycle.PreparedGenerationID, initial.ID, grant.WriterEpoch)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNomadSandboxMigrationConflict
	}
	tag, err = tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET state='retired',retire_proof_digest=$2,retired_at=NOW(),lease_expires_at=NULL,updated_at=NOW()
		WHERE grant_id=$1 AND state='retiring' AND retire_kind=$3 AND retire_operation_id=$4`, grant.ID, proofDigest, RootFSWriterRetireKindMigration, lifecycle.ID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrNomadSandboxMigrationConflict
	}
	return nil
}
