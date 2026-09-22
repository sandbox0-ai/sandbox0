package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationPreparation commits the exact source API-gate
// command before it may be sent. From this point even an unanswered request
// owns recovery: reservation expiry can no longer release target capacity.
// Exact source-applied policy bytes are retained atomically with that command.
// This does not authorize checkpoint, destination execution or another writer.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationPreparation(ctx context.Context, assignment runtimecontrol.MigrationAssignment, policy string) (*procdapi.RuntimeMigrationRequest, error) {
	if _, err := assignment.Digest(); err != nil {
		return nil, err
	}
	if err := validateMigrationSourcePolicy(assignment, policy); err != nil {
		return nil, err
	}
	policyDigest := protocol.NetworkPolicyDigest(policy)

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, assignment)
	if err != nil {
		return nil, err
	}
	var payload []byte
	var digest, storedPolicy, storedPolicyDigest *string
	if err := tx.QueryRow(ctx, `SELECT preparation_request,preparation_digest,source_network_policy,source_network_policy_digest
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&payload, &digest, &storedPolicy, &storedPolicyDigest); err != nil {
		return nil, err
	}
	request := procdapi.RuntimeMigrationRequest{Action: procdapi.MigrationPrepare, Assignment: assignment,
		InstanceID: reservation.SourceProcdInstanceID, LifecycleEpoch: reservation.Lifecycle.Epoch}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if len(payload) != 0 {
		var stored procdapi.RuntimeMigrationRequest
		if json.Unmarshal(payload, &stored) != nil || digest == nil || *digest != want || storedPolicy == nil || *storedPolicy != policy || storedPolicyDigest == nil || *storedPolicyDigest != policyDigest {
			return nil, ErrNomadSandboxMigrationConflict
		}
		actual, err := stored.Digest()
		if err != nil || actual != want || reservation.Lifecycle.Phase != SandboxLifecyclePhaseBarriered {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhasePreparing || reservation.SourceSlot.ClaimNetworkPolicyDigest != policyDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var unexpired bool
	if err := tx.QueryRow(ctx, `SELECT created_at + ($2 * INTERVAL '1 second') > NOW()
		FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, assignment.OperationID,
		int64(NomadMigrationReservationTTL.Seconds())).Scan(&unexpired); err != nil {
		return nil, err
	}
	if !unexpired {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationSourceAndTarget(ctx, tx, reservation, assignment); err != nil {
		return nil, err
	}
	if err := protocol.ValidateNomadProcdAddress(reservation.SourceSlot.ProcdAddress); err != nil {
		return nil, err
	}
	if err := requireNomadMigrationCPUPreflight(ctx, tx, reservation); err != nil {
		return nil, err
	}
	if err := requireNomadMigrationStaging(ctx, tx, reservation); err != nil {
		return nil, err
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if err := (sandboxStoreTx{tx: tx}).UpdateLifecycleTxnPhase(ctx, assignment.OperationID, SandboxLifecyclePhaseBarriered); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET preparation_request=$2,preparation_digest=$3,source_network_policy=$4,source_network_policy_digest=$5,preparation_address=$6
		WHERE operation_id=$1`, assignment.OperationID, payload, want, policy, policyDigest, reservation.SourceSlot.ProcdAddress); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// AuthorizeNomadSandboxMigrationCapture requires the authenticated response to
// the exact committed prepare command, fresh committed CPU evidence, and both
// durable staging reservations. A procd barrier drains API mutations; it is not
// a guest or host filesystem freeze. The source dispatcher must still establish
// storage eligibility before preparing the source.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationCapture(ctx context.Context, prepare procdapi.RuntimeMigrationRequest, prepared procdapi.RuntimeMigrationResponse) (*protocol.MigrationCaptureRequest, error) {
	if prepare.Action != procdapi.MigrationPrepare {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := prepared.ValidateFor(prepare); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthority(ctx, tx, prepare.Assignment)
	if err != nil {
		return nil, err
	}
	var prepPayload, payload []byte
	var prepDigest, digest *string
	if err := tx.QueryRow(ctx, `SELECT preparation_request,preparation_digest,capture_request,capture_digest
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, prepare.Assignment.OperationID).Scan(
		&prepPayload, &prepDigest, &payload, &digest); err != nil {
		return nil, err
	}
	want, _ := prepare.Digest()
	var storedPrepare procdapi.RuntimeMigrationRequest
	if prepDigest == nil || *prepDigest != want || json.Unmarshal(prepPayload, &storedPrepare) != nil ||
		prepare.LifecycleEpoch != reservation.Lifecycle.Epoch || prepare.InstanceID != reservation.SourceProcdInstanceID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	actual, err := storedPrepare.Digest()
	if err != nil || actual != want {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(payload) != 0 {
		var stored protocol.MigrationCaptureRequest
		if json.Unmarshal(payload, &stored) != nil || digest == nil || reservation.Lifecycle.Phase != SandboxLifecyclePhasePublishing {
			return nil, ErrNomadSandboxMigrationConflict
		}
		actual, err := stored.Digest()
		if err != nil || actual != *digest || stored.OperationID != reservation.Lifecycle.ID ||
			stored.SandboxID != reservation.Lifecycle.SandboxID || stored.LifecycleEpoch != reservation.Lifecycle.Epoch ||
			stored.SourceGeneration != reservation.Lifecycle.FromGeneration || stored.ProcdInstanceID != prepare.InstanceID ||
			stored.AssignmentRevision != prepare.Assignment.SourceRevision ||
			stored.BindingDigest != hex.EncodeToString(reservation.SourceBindingDigest) {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &stored, nil
	}
	if reservation.Lifecycle.Phase != SandboxLifecyclePhaseBarriered {
		return nil, ErrNomadSandboxMigrationConflict
	}
	policyDigest, err := migrationSourcePolicyDigest(ctx, tx, prepare.Assignment)
	if err != nil {
		return nil, err
	}
	if policyDigest != reservation.SourceSlot.ClaimNetworkPolicyDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := validateNomadMigrationSourceAndTarget(ctx, tx, reservation, prepare.Assignment); err != nil {
		return nil, err
	}
	if err := requireNomadMigrationCPUPreflight(ctx, tx, reservation); err != nil {
		return nil, err
	}
	if err := requireNomadMigrationStaging(ctx, tx, reservation); err != nil {
		return nil, err
	}
	request := nomadMigrationCaptureIdentity(reservation)
	captureDigest, err := request.Digest()
	if err != nil {
		return nil, err
	}
	payload, err = json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if err := (sandboxStoreTx{tx: tx}).UpdateLifecycleTxnPhase(ctx, reservation.Lifecycle.ID, SandboxLifecyclePhasePublishing); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET capture_request=$2,capture_digest=$3
		WHERE operation_id=$1`, reservation.Lifecycle.ID, payload, captureDigest); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

func lockNomadMigrationAuthority(ctx context.Context, tx pgx.Tx, assignment runtimecontrol.MigrationAssignment) (*NomadSandboxMigrationReservation, error) {
	return lockNomadMigrationAuthorityAtGeneration(ctx, tx, assignment, false)
}

// Only final handover acknowledgements may recover after the public generation advances.
func lockNomadMigrationAuthorityAtGeneration(ctx context.Context, tx pgx.Tx, assignment runtimecontrol.MigrationAssignment, allowDestination bool) (*NomadSandboxMigrationReservation, error) {
	digest, err := assignment.Digest()
	if err != nil {
		return nil, err
	}
	if err := lockRuntimeSlotClaimOperation(ctx, tx, assignment.OperationID); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, assignment.Target.SandboxID)
	if err != nil {
		return nil, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, assignment.OperationID))
	if err != nil {
		return nil, err
	}
	if lifecycle == nil || record.TeamID != assignment.Target.TeamID || record.DesiredState != SandboxDesiredStateActive ||
		!record.DeletedAt.IsZero() || lifecycle.Kind != SandboxLifecycleKindMigrate || lifecycle.Source != SandboxLifecycleSourceAuto ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() || lifecycle.SandboxID != record.ID ||
		lifecycle.Epoch != record.LifecycleEpoch ||
		lifecycle.FromGeneration != assignment.SourceGeneration || lifecycle.ToGeneration != assignment.Target.RuntimeGeneration ||
		(lifecycle.Phase != SandboxLifecyclePhasePreparing && lifecycle.Phase != SandboxLifecyclePhaseBarriered && lifecycle.Phase != SandboxLifecyclePhasePublishing && lifecycle.Phase != SandboxLifecyclePhaseCommitting) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	source := record.RuntimeGeneration == assignment.SourceGeneration && lifecycle.FromRuntimeID == record.RuntimeID && lifecycle.FromRuntimeNamespace == record.RuntimeNamespace
	destination := allowDestination && lifecycle.Phase == SandboxLifecyclePhaseCommitting &&
		record.RuntimeGeneration == assignment.Target.RuntimeGeneration && lifecycle.ToRuntimeID == record.RuntimeID && lifecycle.ToRuntimeNamespace == record.RuntimeNamespace
	if !source && !destination {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT hard_expires_at IS NULL OR hard_expires_at > NOW()
		FROM manager.sandboxes WHERE sandbox_id=$1`, record.ID).Scan(&live); err != nil {
		return nil, err
	}
	if !live {
		return nil, ErrNomadSandboxMigrationConflict
	}
	reservation, err := loadNomadMigrationReservation(ctx, tx, lifecycle)
	if err != nil {
		return nil, err
	}
	if reservation.AssignmentDigest != digest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var canceled bool
	if err := tx.QueryRow(ctx, `SELECT preparation_cancel_request IS NOT NULL OR failure_request IS NOT NULL OR capture_failure_request IS NOT NULL FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&canceled); err != nil {
		return nil, err
	}
	if canceled {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return reservation, nil
}

// Initial authorization rechecks the source writer and target reservation under
// the same transaction. Once dispatched, retries recover the stored command,
// never a request reconstructed from a replacement node boot or control socket.
func validateNomadMigrationSourceAndTarget(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation, assignment runtimecontrol.MigrationAssignment) error {
	record, err := lockNomadSandboxClaimRecord(ctx, tx, assignment.Target.SandboxID)
	if err != nil {
		return err
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, record)
	if err != nil {
		return err
	}
	source := writer.slot
	if source.ID != reservation.SourceSlot.ID || source.ResourceLeaseState != RuntimeResourceLeaseActive ||
		source.ClaimRuntimeAssignmentRevision != assignment.SourceRevision || source.ProcdInstanceID != reservation.SourceProcdInstanceID ||
		writer.grant.ID != reservation.SourceWriterGrantID || !bytes.Equal(writer.grant.BindingDigest, reservation.SourceBindingDigest) ||
		!bytes.Equal(source.RootFSBindingDigest, reservation.SourceBindingDigest) || writer.generation.ID != reservation.Lifecycle.ExpectedGenerationID {
		return ErrNomadSandboxMigrationConflict
	}
	reservation.SourceSlot = source
	return validateNomadMigrationTarget(ctx, tx, reservation)
}

// validateNomadMigrationTarget rechecks the reserved node without leasing its
// capacity twice. The caller holds the resource lease lock; this locks the
// target carrier before inspecting its admission state.
func validateNomadMigrationTarget(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation) error {
	source := reservation.SourceSlot
	target, err := lockRuntimeSlotByID(ctx, tx, reservation.TargetSlot.ID)
	if err != nil {
		return err
	}
	if target == nil {
		return ErrNomadSandboxMigrationConflict
	}
	var usable bool
	// Capacity has already been reserved in the common ledger. Recheck node
	// identity/health and maintenance fences without counting it a second time.
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM manager.runtime_slots target
		JOIN manager.runtime_node_capacities capacity ON capacity.cluster_id=target.cluster_id
			AND capacity.node_id=target.node_id AND capacity.node_uid=target.node_uid AND capacity.node_boot_id=target.node_boot_id
		WHERE target.slot_id=$1 AND target.state='fastpath_ready' AND NOT target.carrier_retired
			AND target.heartbeat_expires_at > NOW() AND capacity.heartbeat_expires_at > NOW()
			AND target.allocation_id=$2 AND target.allocation_namespace=$3
			AND target.node_uid=$4 AND target.node_boot_id=$5 AND target.cluster_id=$6
			AND target.compatibility_digest=$7 AND target.resource_lease_id IS NULL AND target.sandbox_id IS NULL
			AND NOT EXISTS (SELECT 1 FROM manager.runtime_node_fences fence WHERE fence.cluster_id=target.cluster_id
				AND fence.node_id=target.node_id AND fence.node_uid=target.node_uid AND fence.state IN ('warming','draining','revoked'))
			AND NOT EXISTS (SELECT 1 FROM manager.runtime_carrier_resizes resize WHERE resize.cluster_id=target.cluster_id
				AND resize.node_id=target.node_id AND resize.pending AND NOT (target.allocation_id=ANY(resize.retained_allocations)))
		)`, reservation.TargetSlot.ID, reservation.Lifecycle.ToRuntimeID, reservation.Lifecycle.ToRuntimeNamespace,
		reservation.TargetResourceLease.NodeUID, reservation.TargetResourceLease.NodeBootID, source.ClusterID, source.CompatibilityDigest).Scan(&usable); err != nil {
		return err
	}
	if !usable {
		return fmt.Errorf("%w: reserved destination is no longer eligible", ErrNomadSandboxMigrationConflict)
	}
	// The reservation was read before acquiring the carrier lock. Eligibility
	// evidence must use this locked row, including its exact control endpoint.
	reservation.TargetSlot = target
	return nil
}
