package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AcquireNomadSandboxMigrationTarget attaches the reserved carrier only after
// the captured image is durable and the original execution and writer are
// physically fenced. Neither carrier's resource lease is released here, and
// the public runtime identity remains on the source until restoration commits.
func (s *PGSandboxStore) AcquireNomadSandboxMigrationTarget(ctx context.Context, assignment runtimecontrol.MigrationAssignment) (*RuntimeSlot, error) {
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
	policyDigest, err := migrationSourcePolicyDigest(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	if policyDigest != reservation.SourceSlot.ClaimNetworkPolicyDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	target, err := lockRuntimeSlotByID(ctx, tx, reservation.TargetSlot.ID)
	if err != nil {
		return nil, err
	}
	if target.ClaimOperationID != "" {
		if !nomadMigrationTargetClaimMatches(target, reservation, assignment) {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return target, nil
	}
	if err := validateNomadMigrationTarget(ctx, tx, reservation); err != nil {
		return nil, err
	}
	// Preserve the old claim, writer and capacity as cleanup custody. The
	// database guard requires the exact committed physical-fence receipt.
	tag, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state='quiescing',
		migration_source_operation_id=$2,revision=revision+1,quiescing_at=COALESCE(quiescing_at,NOW()),updated_at=NOW()
		WHERE slot_id=$1 AND sandbox_id=$3 AND writer_grant_id=$4
			AND state IN ('active','quiescing') AND migration_source_operation_id IS NULL`,
		reservation.SourceSlot.ID, assignment.OperationID, assignment.Target.SandboxID, reservation.SourceWriterGrantID)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() != 1 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	revision, _ := assignment.Target.Revision()
	assignmentPayload, err := json.Marshal(assignment.Target)
	if err != nil {
		return nil, err
	}
	var policy string
	if err := tx.QueryRow(ctx, `SELECT source_network_policy FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&policy); err != nil {
		return nil, err
	}
	request := &AcquireRuntimeSlotRequest{OperationID: assignment.OperationID,
		ClaimID: reservation.TargetResourceLease.ClaimID, SandboxID: assignment.Target.SandboxID,
		FilesystemID: reservation.SourceSlot.FilesystemID, SourceGenerationID: reservation.Lifecycle.PreparedGenerationID,
		ClusterID: target.ClusterID, ClaimTTL: DefaultRuntimeSlotClaimTTL,
		RuntimeAssignmentRevision: revision, NetworkPolicyDigest: policyDigest,
		RuntimeAssignmentPayload: string(assignmentPayload), NetworkPolicy: policy}
	if err := validateRuntimeSlotClaimInputs(request); err != nil {
		return nil, err
	}
	claimed, err := attachRuntimeSlotClaim(ctx, tx, target, request, reservation.TargetResourceLease.LeaseID)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return claimed, nil
}

// IssueNomadSandboxMigrationTargetWriter uses the common writer epoch CAS and
// binds it atomically to the exact claimed destination. Its secret and stage
// binding are supplied by the internal planner, as for an ordinary claim.
// An issued writer is storage authority, not permission to start an entrypoint:
// the migration driver must restore the committed image under a separate gate.
func (s *PGSandboxStore) IssueNomadSandboxMigrationTargetWriter(ctx context.Context, assignment runtimecontrol.MigrationAssignment, request *IssueRootFSWriterGrantRequest) (*IssueAndBindRuntimeSlotWriterGrantResult, error) {
	normalized, _, err := validateIssueRootFSWriterGrantRequest(request)
	if err != nil {
		return nil, err
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
	source, err := validateNomadMigrationDestinationHandoff(ctx, tx, reservation)
	if err != nil {
		return nil, err
	}
	target, err := lockRuntimeSlotByID(ctx, tx, reservation.TargetSlot.ID)
	if err != nil {
		return nil, err
	}
	if !nomadMigrationTargetClaimMatches(target, reservation, assignment) ||
		normalized.OperationID != assignment.OperationID || normalized.SandboxID != target.SandboxID ||
		normalized.SlotID != target.ID || normalized.ClaimID != target.ClaimID ||
		normalized.ExpectedFilesystemID != target.FilesystemID || normalized.InitialGenerationID != target.SourceGenerationID ||
		normalized.ExpectedWriterEpoch != source.WriterEpoch || normalized.NodeUID != target.NodeUID || normalized.NodeBootID != target.NodeBootID ||
		normalized.RuntimeNamespace != target.AllocationNamespace || normalized.RuntimeIncarnationID != target.AllocationID ||
		normalized.NodeName != target.NodeID || normalized.RuntimeID != protocol.NomadTaskName ||
		normalized.RuntimeGeneration != strconv.FormatInt(assignment.Target.RuntimeGeneration, 10) ||
		!normalized.ConsumeExpiresAt.Equal(target.ClaimLeaseExpiresAt) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if target.WriterGrantID == "" && (target.State != RuntimeSlotStateClaiming ||
		!target.ClaimLeaseExpiresAt.After(target.AuthorityObservedAt) || !target.HeartbeatExpiresAt.After(target.AuthorityObservedAt)) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	issued, err := issueRootFSWriterGrant(ctx, tx, normalized)
	if err != nil {
		return nil, err
	}
	bound, err := bindRuntimeSlotWriterGrant(ctx, tx, target, &BindRuntimeSlotWriterGrantRequest{
		SlotID: target.ID, OperationID: assignment.OperationID, ClaimID: target.ClaimID, GrantID: normalized.GrantID})
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &IssueAndBindRuntimeSlotWriterGrantResult{Issued: issued, Slot: bound}, nil
}

// validateNomadMigrationDestinationHandoff verifies stored evidence rather
// than accepting a new caller-provided fence or treating a timeout as one.
func validateNomadMigrationDestinationHandoff(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation) (*rootFSWriterGrantRecord, error) {
	if reservation.Lifecycle.Phase != SandboxLifecyclePhaseCommitting {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := requireNomadMigrationTargetImage(ctx, tx, reservation); err != nil {
		return nil, err
	}
	var command, receipt []byte
	if err := tx.QueryRow(ctx, `SELECT source_fence_request,source_fence_proof FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, reservation.Lifecycle.ID).Scan(&command, &receipt); err != nil {
		return nil, err
	}
	var request protocol.MigrationSourceFenceRequest
	var proof protocol.MigrationSourceFenceProof
	if json.Unmarshal(command, &request) != nil || json.Unmarshal(receipt, &proof) != nil || proof.ValidateFor(request) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	policyDigest, err := migrationSourcePolicyDigest(ctx, tx, request.PublicationRequest.Assignment)
	if err != nil {
		return nil, err
	}
	if policyDigest != reservation.SourceSlot.ClaimNetworkPolicyDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	digest, _ := request.PublicationRequest.Assignment.Digest()
	if digest != reservation.AssignmentDigest || request.PublicationRequest.Capture.RootFS.Generation.GenerationID != reservation.Lifecycle.PreparedGenerationID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	filesystem, generation, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, reservation.Lifecycle.SandboxID)
	if err != nil {
		return nil, err
	}
	source, err := getRootFSWriterGrantForUpdate(ctx, tx, reservation.SourceWriterGrantID)
	if err != nil {
		return nil, err
	}
	if source.State != RootFSWriterGrantStateRetired || source.RetireKind != RootFSWriterRetireKindMigration ||
		source.RetireOperationID != reservation.Lifecycle.ID || hex.EncodeToString(source.RetireProofDigest) != proof.Digest ||
		!bytes.Equal(source.BindingDigest, reservation.SourceBindingDigest) || source.FilesystemID != filesystem.ID ||
		generation.ID != reservation.Lifecycle.PreparedGenerationID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return source, nil
}

func nomadMigrationTargetClaimMatches(target *RuntimeSlot, reservation *NomadSandboxMigrationReservation, assignment runtimecontrol.MigrationAssignment) bool {
	revision, _ := assignment.Target.Revision()
	return target != nil && target.ID == reservation.TargetSlot.ID &&
		(target.State == RuntimeSlotStateClaiming || target.State == RuntimeSlotStateStarting || target.State == RuntimeSlotStateActive) &&
		target.ClaimOperationID == assignment.OperationID && target.SandboxID == assignment.Target.SandboxID &&
		target.ClaimID == reservation.TargetResourceLease.ClaimID && target.ResourceLease == reservation.TargetResourceLease &&
		target.ResourceLeaseState == RuntimeResourceLeaseActive && bytes.Equal(target.ResourceLeaseDigest, reservation.TargetResourceLeaseDigest) &&
		target.FilesystemID == reservation.SourceSlot.FilesystemID && target.SourceGenerationID == reservation.Lifecycle.PreparedGenerationID &&
		target.AllocationID == reservation.Lifecycle.ToRuntimeID && target.AllocationNamespace == reservation.Lifecycle.ToRuntimeNamespace &&
		target.ClaimRuntimeAssignmentRevision == revision && target.ClaimNetworkPolicyDigest == reservation.SourceSlot.ClaimNetworkPolicyDigest
}
