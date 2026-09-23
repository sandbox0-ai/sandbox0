package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationRestore persists a tokenless execution command.
// Neither a prepared image nor an issued writer can substitute for this gate.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationRestore(ctx context.Context, assignment runtimecontrol.MigrationAssignment, stage rootfshandoff.StageRequest) (*protocol.MigrationRestoreRequest, error) {
	if stage.Identity.WriterGrantToken != "" {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	if _, err := validateNomadMigrationDestinationHandoff(ctx, tx, reservation); err != nil {
		return nil, err
	}
	var image, prepared, fence, proof, prior []byte
	var priorDigest *string
	if err := tx.QueryRow(ctx, `SELECT target_image_request,target_image_receipt,source_fence_request,source_fence_proof,restore_request,restore_digest
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&image, &prepared, &fence, &proof, &prior, &priorDigest); err != nil {
		return nil, err
	}
	request := protocol.MigrationRestoreRequest{Stage: stage}
	if json.Unmarshal(image, &request.Image) != nil || json.Unmarshal(prepared, &request.Prepared) != nil ||
		json.Unmarshal(fence, &request.Fence) != nil || json.Unmarshal(proof, &request.Proof) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if len(prior) != 0 {
		var stored protocol.MigrationRestoreRequest
		if priorDigest == nil || *priorDigest != want || json.Unmarshal(prior, &stored) != nil {
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
	target, err := lockRuntimeSlotByID(ctx, tx, reservation.TargetSlot.ID)
	if err != nil {
		return nil, err
	}
	if target.State != RuntimeSlotStateClaiming || !target.ClaimLeaseExpiresAt.After(target.AuthorityObservedAt) ||
		!target.HeartbeatExpiresAt.After(target.AuthorityObservedAt) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if _, err := validateNomadMigrationRestoreWriter(ctx, tx, reservation, target, request); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET restore_request=$2,restore_digest=$3 WHERE operation_id=$1`, assignment.OperationID, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// startNomadMigrationRestore follows sandbox-before-slot lock ordering and
// admits only the exact durable restore command after writer consumption.
func (s *PGSandboxStore) startNomadMigrationRestore(ctx context.Context, start *StartRuntimeSlotRequest) (*RuntimeSlot, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var payload []byte
	var digest *string
	if err := tx.QueryRow(ctx, `SELECT restore_request,restore_digest FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, start.OperationID).Scan(&payload, &digest); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNomadSandboxMigrationConflict
		}
		return nil, err
	}
	var request protocol.MigrationRestoreRequest
	if digest == nil || *digest != start.MigrationRestoreDigest || json.Unmarshal(payload, &request) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	want, err := request.Digest()
	if err != nil || want != *digest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	reservation, err := lockNomadMigrationAuthority(ctx, tx, request.Image.Publication.Assignment)
	if err != nil {
		return nil, err
	}
	if _, err := validateNomadMigrationDestinationHandoff(ctx, tx, reservation); err != nil {
		return nil, err
	}
	target, err := lockRuntimeSlotByID(ctx, tx, reservation.TargetSlot.ID)
	if err != nil {
		return nil, err
	}
	grant, err := validateNomadMigrationRestoreWriter(ctx, tx, reservation, target, request)
	if err != nil {
		return nil, err
	}
	if grant.State != RootFSWriterGrantStateConsumed || !grant.LeaseExpiresAt.After(grant.databaseNow) ||
		target.ID != start.SlotID || start.LaunchAttempt != request.Stage.Identity.LaunchAttempt ||
		start.RunscContainerID != protocol.NomadRunscContainerID(target.ID) || !bytes.Equal(start.RootFSBindingDigest, grant.BindingDigest) ||
		runtimeSlotPreCommandReadyClaimExpired(target) || !target.HeartbeatExpiresAt.After(target.AuthorityObservedAt) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if _, err := startRuntimeSlotTransition(ctx, tx, target, start); err != nil {
		return nil, err
	}
	result, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, target.ID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

func validateNomadMigrationRestoreWriter(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation, target *RuntimeSlot, request protocol.MigrationRestoreRequest) (*rootFSWriterGrantRecord, error) {
	if !nomadMigrationTargetClaimMatches(target, reservation, request.Image.Publication.Assignment) || target.WriterGrantID == "" ||
		target.WriterGrantID != request.Stage.Identity.WriterGrantID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return validateNomadExecutionRestoreWriter(ctx, tx, target, request)
}

// Both migration and retained-image restore use the same exact writer fence.
// The caller has locked the owner and matching filesystem before the slot.
func validateNomadExecutionRestoreWriter(ctx context.Context, tx pgx.Tx, target *RuntimeSlot, request protocol.MigrationRestoreRequest) (*rootFSWriterGrantRecord, error) {
	if target.WriterGrantID == "" || target.WriterGrantID != request.Stage.Identity.WriterGrantID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	// The handoff validator already locks this filesystem before the slot.
	// Recheck its current epoch so an old consumed grant cannot revive after
	// another fencing transition advanced storage authority.
	filesystem, _, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, target.SandboxID)
	if err != nil {
		return nil, err
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, target.WriterGrantID)
	if err != nil {
		return nil, err
	}
	binding, err := request.Stage.BindingDigest()
	if err != nil {
		return nil, err
	}
	if request.Stage.ExpectedPolicyToken.PolicyDigest != target.ClaimNetworkPolicyDigest ||
		(grant.State != RootFSWriterGrantStateIssued && grant.State != RootFSWriterGrantStateConsumed) ||
		grant.WriterEpoch != request.Stage.Identity.WriterEpoch || filesystem.WriterEpoch != grant.WriterEpoch ||
		grant.BindingVersion != request.Stage.BindingVersion || hex.EncodeToString(grant.tokenDigest) != request.Stage.Identity.WriterGrantTokenDigest ||
		grant.IssueOperationID != target.ClaimOperationID || grant.SandboxID != target.SandboxID ||
		grant.RuntimeID != request.Stage.Identity.TaskName || grant.NodeName != target.NodeID || !bytes.Equal(grant.BindingDigest, binding[:]) ||
		grant.SlotID != target.ID || grant.ClaimID != target.ClaimID || grant.FilesystemID != target.FilesystemID ||
		grant.InitialGenerationID != target.SourceGenerationID || grant.NodeUID != target.NodeUID || grant.NodeBootID != target.NodeBootID ||
		grant.RuntimeIncarnationID != target.AllocationID || grant.RuntimeNamespace != target.AllocationNamespace ||
		grant.GateParent != request.Stage.Parent || grant.RuntimeGeneration != request.Stage.Identity.RuntimeGeneration {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return grant, nil
}
