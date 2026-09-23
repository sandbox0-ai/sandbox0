package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var ErrNomadSandboxMigrationConflict = errors.New("nomad sandbox migration conflict")

// NomadMigrationReservationTTL bounds capacity held before any node command is
// authorized. Retries never extend this deadline; PostgreSQL owns its clock.
const NomadMigrationReservationTTL = 2 * time.Minute

// NomadSandboxMigrationReservation binds a system-selected destination to the
// source lifecycle without authorizing another writer or guest execution.
// Resource capacity comes only from TargetResourceLease in the shared ledger.
type NomadSandboxMigrationReservation struct {
	Lifecycle                 *SandboxLifecycleTxn
	SourceSlot                *RuntimeSlot
	TargetSlot                *RuntimeSlot
	TargetResourceLease       protocol.RuntimeResourceLease
	TargetResourceLeaseDigest []byte
	SourceWriterGrantID       string
	SourceBindingDigest       []byte
	SourceProcdInstanceID     string
	AssignmentDigest          string
}

// ReserveNomadSandboxMigration is an internal system operation. There is no
// destination input: normal admission selects a compatible slot on another
// node in the same cluster. It locks the sandbox before reserving capacity and
// rolls back the whole intent when capacity is unavailable, leaving the source
// running. CPU feature eligibility and sole execution fencing are subsequent
// gates; a successful reservation does not permit a source checkpoint.
func (s *PGSandboxStore) ReserveNomadSandboxMigration(ctx context.Context, assignment runtimecontrol.MigrationAssignment) (*NomadSandboxMigrationReservation, error) {
	return s.reserveNomadSandboxMigration(ctx, assignment, false)
}

func (s *PGSandboxStore) reserveNomadSandboxMigration(ctx context.Context, assignment runtimecontrol.MigrationAssignment, evacuation bool) (*NomadSandboxMigrationReservation, error) {
	digest, err := assignment.Digest()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(assignment)
	if err != nil {
		return nil, err
	}
	if assignment.Target.TeamID == "" {
		return nil, fmt.Errorf("migration team identity is required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Match normal claim lock ordering: operation first, then sandbox.
	if err := lockRuntimeSlotClaimOperation(ctx, tx, assignment.OperationID); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, assignment.Target.SandboxID)
	if err != nil {
		return nil, err
	}
	if record.TeamID != assignment.Target.TeamID || record.DesiredState != SandboxDesiredStateActive ||
		!record.DeletedAt.IsZero() || record.RuntimeGeneration != assignment.SourceGeneration {
		return nil, ErrNomadSandboxMigrationConflict
	}
	existing, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, assignment.OperationID))
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.Kind != SandboxLifecycleKindMigrate || existing.Source != SandboxLifecycleSourceAuto ||
			existing.SandboxID != record.ID || existing.FromGeneration != assignment.SourceGeneration ||
			existing.ToGeneration != assignment.Target.RuntimeGeneration || existing.Phase != SandboxLifecyclePhasePreparing ||
			existing.Epoch != record.LifecycleEpoch || existing.FromRuntimeID != record.RuntimeID || existing.FromRuntimeNamespace != record.RuntimeNamespace {
			return nil, ErrNomadSandboxMigrationConflict
		}
		reservation, err := loadNomadMigrationReservation(ctx, tx, existing)
		if err != nil {
			return nil, err
		}
		if reservation.AssignmentDigest != digest {
			return nil, ErrNomadSandboxMigrationConflict
		}
		var expired bool
		if err := tx.QueryRow(ctx, `SELECT created_at + ($2 * INTERVAL '1 second') <= NOW()
			FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, existing.ID,
			int64(NomadMigrationReservationTTL/time.Second)).Scan(&expired); err != nil {
			return nil, err
		}
		if expired {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return reservation, nil
	}
	active, err := getActiveLifecycleTxn(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if active != nil || record.RuntimeGeneration != assignment.SourceGeneration {
		return nil, ErrNomadSandboxMigrationConflict
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, record)
	if err != nil {
		return nil, err
	}
	source := writer.slot
	if source.ClaimRuntimeAssignmentRevision != assignment.SourceRevision ||
		source.ResourceLeaseState != RuntimeResourceLeaseActive || source.ResourceLease.LeaseID == "" ||
		!bytes.Equal(source.RootFSBindingDigest, writer.grant.BindingDigest) {
		return nil, fmt.Errorf("%w: source assignment, resources or writer binding changed", ErrNomadSandboxMigrationConflict)
	}
	excludedNodes := []string{}
	requireFixedDestination := false
	if evacuation {
		excludedNodes, requireFixedDestination, err = lockNomadMigrationEvacuation(ctx, tx, source, assignment)
		if err != nil {
			return nil, err
		}
	}
	resources := protocol.RuntimeResourceRequest{Version: protocol.RuntimeResourceRequestVersion,
		CPUMillicores: source.ResourceLease.CPUMillicores, MemoryBytes: source.ResourceLease.MemoryBytes,
		PIDsLimit: source.ResourceLease.PIDsLimit}
	if err := resources.Validate(); err != nil {
		return nil, err
	}
	targetRevision, _ := assignment.Target.Revision()
	request := &AcquireRuntimeSlotRequest{OperationID: assignment.OperationID, ClaimID: "migration-" + digest,
		SandboxID: record.ID, FilesystemID: writer.filesystem.ID, SourceGenerationID: writer.generation.ID,
		CompatibilityDigest: source.CompatibilityDigest, ClusterID: source.ClusterID,
		RuntimeAssignmentRevision: targetRevision, NetworkPolicyDigest: source.ClaimNetworkPolicyDigest,
		ClaimTTL: DefaultRuntimeSlotClaimTTL, Resources: resources}
	target, lease, leaseDigest, err := selectRuntimeSlotResourceLeaseExcludingNodes(ctx, tx, request, source.NodeID, source.NodeUID, excludedNodes, requireFixedDestination)
	if err != nil {
		return nil, err
	}
	if err := insertRuntimeResourceLease(ctx, tx, lease, leaseDigest); err != nil {
		return nil, err
	}
	lifecycle := &SandboxLifecycleTxn{ID: assignment.OperationID, SandboxID: record.ID,
		Kind: SandboxLifecycleKindMigrate, Source: SandboxLifecycleSourceAuto, Phase: SandboxLifecyclePhasePreparing,
		FromGeneration: record.RuntimeGeneration, ToGeneration: assignment.Target.RuntimeGeneration,
		FromRuntimeID: source.AllocationID, FromRuntimeNamespace: source.AllocationNamespace,
		ToRuntimeID: target.AllocationID, ToRuntimeNamespace: target.AllocationNamespace,
		ExpectedGenerationID: writer.generation.ID}
	if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, lifecycle); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_migrations (
		operation_id, assignment_digest, source_slot_id, target_slot_id, target_resource_lease_id,
		source_writer_grant_id, source_binding_digest, source_procd_instance_id, assignment_request)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`, lifecycle.ID, digest, source.ID, target.ID, lease.LeaseID,
		writer.grant.ID, writer.grant.BindingDigest, source.ProcdInstanceID, payload)
	if err != nil {
		return nil, fmt.Errorf("persist migration reservation: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &NomadSandboxMigrationReservation{Lifecycle: lifecycle, SourceSlot: source, TargetSlot: target,
		TargetResourceLease: lease, TargetResourceLeaseDigest: leaseDigest, AssignmentDigest: digest,
		SourceWriterGrantID: writer.grant.ID, SourceBindingDigest: append([]byte(nil), writer.grant.BindingDigest...),
		SourceProcdInstanceID: source.ProcdInstanceID}, nil
}

// AbortNomadSandboxMigrationReservation releases only a never-authorized
// destination reservation. Once preparation/capture has been authorized, this
// path is permanently unavailable; later cleanup requires execution proofs.
// The one-shot target carrier is admission-fenced for normal physical cleanup.
func (s *PGSandboxStore) AbortNomadSandboxMigrationReservation(ctx context.Context, sandboxID, operationID, reason string) error {
	_, err := s.abortNomadSandboxMigrationReservation(ctx, sandboxID, operationID, reason, false)
	return err
}

// RecoverExpiredNomadMigrationReservations reclaims only preparation intents
// that have never authorized node work. Each candidate is rechecked while the
// sandbox and lifecycle are locked, so an authorization racing this scan wins
// or loses atomically. A failed candidate does not prevent independent cleanup.
func (s *PGSandboxStore) RecoverExpiredNomadMigrationReservations(ctx context.Context, limit int) (int, error) {
	if limit < 1 || limit > MaxRuntimeSlotReconcileLimit {
		return 0, fmt.Errorf("migration recovery limit must be between 1 and %d", MaxRuntimeSlotReconcileLimit)
	}
	rows, err := s.pool.Query(ctx, `SELECT lifecycle.sandbox_id,lifecycle.txn_id
		FROM manager.sandbox_lifecycle_txns lifecycle
		JOIN manager.sandbox_runtime_migrations migration ON migration.operation_id=lifecycle.txn_id
		JOIN manager.runtime_resource_leases lease ON lease.lease_id=migration.target_resource_lease_id AND lease.lease_state='active'
		WHERE lifecycle.kind='migrate' AND lifecycle.source='auto'
			AND migration.preparation_request IS NULL AND migration.capture_request IS NULL
			AND (lifecycle.phase='aborted' OR (lifecycle.phase='preparing'
				AND lifecycle.created_at <= NOW() - ($1 * INTERVAL '1 second')))
		ORDER BY lifecycle.created_at,lifecycle.txn_id LIMIT $2`, int64(NomadMigrationReservationTTL/time.Second), limit)
	if err != nil {
		return 0, err
	}
	type candidate struct{ sandboxID, operationID string }
	var candidates []candidate
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.sandboxID, &value.operationID); err != nil {
			rows.Close()
			return 0, err
		}
		candidates = append(candidates, value)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	completed := 0
	var failures []error
	for _, value := range candidates {
		if err := ctx.Err(); err != nil {
			return completed, errors.Join(append(failures, err)...)
		}
		changed, err := s.abortNomadSandboxMigrationReservation(ctx, value.sandboxID, value.operationID,
			"migration_reservation_expired", true)
		if err != nil {
			failures = append(failures, fmt.Errorf("recover migration reservation %s: %w", value.operationID, err))
		} else if changed {
			completed++
		}
	}
	return completed, errors.Join(failures...)
}

func (s *PGSandboxStore) abortNomadSandboxMigrationReservation(ctx context.Context, sandboxID, operationID, reason string, expiredOnly bool) (bool, error) {
	if sandboxID == "" || operationID == "" || len(reason) > 4096 {
		return false, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if expiredOnly {
		// Maintenance must not wait behind an in-flight sandbox transaction.
		// A skipped owner remains due and will be reconsidered on a later pass.
		record, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
			WHERE sandbox_id=$1 FOR UPDATE SKIP LOCKED`, sandboxID))
		if err != nil || record == nil {
			return false, err
		}
	} else if _, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID); err != nil {
		return false, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, operationID))
	if err != nil {
		return false, err
	}
	if lifecycle == nil || lifecycle.SandboxID != sandboxID || lifecycle.Kind != SandboxLifecycleKindMigrate ||
		lifecycle.Source != SandboxLifecycleSourceAuto {
		return false, ErrNomadSandboxMigrationConflict
	}
	if expiredOnly && lifecycle.Phase != SandboxLifecyclePhasePreparing && lifecycle.Phase != SandboxLifecyclePhaseAborted {
		return false, nil // A later phase owns recovery; expiry is not execution fencing.
	}
	if lifecycle.Phase != SandboxLifecyclePhasePreparing && lifecycle.Phase != SandboxLifecyclePhaseAborted {
		return false, ErrNomadSandboxMigrationConflict
	}
	if expiredOnly {
		var due bool
		if err := tx.QueryRow(ctx, `SELECT phase='aborted' OR (phase='preparing'
			AND created_at + ($2 * INTERVAL '1 second') <= NOW())
			FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, operationID,
			int64(NomadMigrationReservationTTL/time.Second)).Scan(&due); err != nil {
			return false, err
		}
		if !due {
			return false, nil
		}
	}
	var slotID, leaseID string
	var authorized bool
	err = tx.QueryRow(ctx, `SELECT target_slot_id,target_resource_lease_id,
		preparation_request IS NOT NULL OR capture_request IS NOT NULL
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, operationID).Scan(&slotID, &leaseID, &authorized)
	if err != nil {
		return false, err
	}
	// Durable commands remain authority even if another recovery path changed
	// the lifecycle phase. An unused destination alone cannot prove that the
	// source never received preparation or checkpoint authorization.
	if authorized {
		if expiredOnly {
			return false, nil
		}
		return false, ErrNomadSandboxMigrationConflict
	}
	changed, err := releaseUnusedMigrationDestination(ctx, tx, lifecycle, slotID, leaseID, reason)
	if err != nil {
		return false, err
	}
	return changed, tx.Commit(ctx)
}

// Only callers with unprepared or acknowledged preparation-cancellation
// authority may release an unattached target. Captured work cannot use this path.
func releaseUnusedMigrationDestination(ctx context.Context, tx pgx.Tx, lifecycle *SandboxLifecycleTxn, slotID, leaseID, reason string) (bool, error) {
	operationID := lifecycle.ID
	target, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, slotID))
	if err != nil {
		return false, err
	}
	// No claim has ever been attached or delivered to ctld. This check is
	// stronger than an expired lease or a missing Nomad allocation.
	if target.ClaimID != "" || target.ClaimOperationID != "" || target.ResourceLease.LeaseID != "" || target.SandboxID != "" || target.WriterGrantID != "" {
		return false, ErrNomadSandboxMigrationConflict
	}
	lease, _, state, err := loadMigrationResourceLease(ctx, tx, leaseID)
	if err != nil {
		return false, err
	}
	if lease.SlotID != target.ID || lease.OperationID != operationID {
		return false, ErrNomadSandboxMigrationConflict
	}
	if lifecycle.Phase == SandboxLifecyclePhaseAborted && state == RuntimeResourceLeaseReleased {
		return false, nil
	}
	if state != RuntimeResourceLeaseActive {
		return false, ErrNomadSandboxMigrationConflict
	}
	if _, err = tx.Exec(ctx, `UPDATE manager.runtime_resource_leases SET lease_state='released',released_at=NOW(),updated_at=NOW() WHERE lease_id=$1`, leaseID); err != nil {
		return false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE manager.runtime_slots SET carrier_retired=true,revision=revision+1,updated_at=NOW() WHERE slot_id=$1`, slotID); err != nil {
		return false, err
	}
	// Older termination paths could abort the lifecycle before releasing its
	// unattached target. Preserve that original abort evidence during repair.
	if _, err = tx.Exec(ctx, `UPDATE manager.sandbox_lifecycle_txns SET phase='aborted',aborted_at=NOW(),updated_at=NOW(),error=$2 WHERE txn_id=$1 AND phase<>'aborted'`, operationID, reason); err != nil {
		return false, err
	}
	return true, nil
}

func loadNomadMigrationReservation(ctx context.Context, tx pgx.Tx, lifecycle *SandboxLifecycleTxn) (*NomadSandboxMigrationReservation, error) {
	result := &NomadSandboxMigrationReservation{Lifecycle: lifecycle}
	var sourceID, targetID, leaseID string
	err := tx.QueryRow(ctx, `SELECT assignment_digest,source_slot_id,target_slot_id,target_resource_lease_id,
		source_writer_grant_id,source_binding_digest,source_procd_instance_id
		FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, lifecycle.ID).Scan(
		&result.AssignmentDigest, &sourceID, &targetID, &leaseID, &result.SourceWriterGrantID, &result.SourceBindingDigest, &result.SourceProcdInstanceID)
	if err != nil {
		return nil, err
	}
	result.SourceSlot, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, sourceID))
	if err != nil {
		return nil, err
	}
	result.TargetSlot, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, targetID))
	if err != nil {
		return nil, err
	}
	var state string
	result.TargetResourceLease, result.TargetResourceLeaseDigest, state, err = loadMigrationResourceLease(ctx, tx, leaseID)
	if err != nil {
		return nil, err
	}
	if state != RuntimeResourceLeaseActive || result.TargetResourceLease.SlotID != targetID || result.TargetResourceLease.OperationID != lifecycle.ID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return result, nil
}

func loadMigrationResourceLease(ctx context.Context, tx pgx.Tx, id string) (protocol.RuntimeResourceLease, []byte, string, error) {
	lease := protocol.RuntimeResourceLease{Version: protocol.RuntimeResourceLeaseVersion}
	var digest []byte
	var state string
	err := tx.QueryRow(ctx, `SELECT lease_id,operation_id,claim_id,slot_id,cluster_id,node_id,node_uid,node_boot_id,
		cpu_millicores,cpu_period_micros,cpu_quota_micros,cpu_shares,cpu_weight,cpuset_cpus,cpuset_mems,
		memory_bytes,pids_limit,cgroup_name,lease_digest,lease_state
		FROM manager.runtime_resource_leases WHERE lease_id=$1 FOR UPDATE`, id).Scan(
		&lease.LeaseID, &lease.OperationID, &lease.ClaimID, &lease.SlotID, &lease.ClusterID, &lease.NodeID, &lease.NodeUID, &lease.NodeBootID,
		&lease.CPUMillicores, &lease.CPUPeriodMicros, &lease.CPUQuotaMicros, &lease.CPUShares, &lease.CPUWeight, &lease.CPUSetCPUs, &lease.CPUSetMems,
		&lease.MemoryBytes, &lease.PIDsLimit, &lease.CgroupName, &digest, &state)
	if err != nil {
		return lease, nil, "", err
	}
	want, err := lease.Digest()
	if err != nil {
		return lease, nil, "", err
	}
	if strings.TrimPrefix(want, "sha256:") != hex.EncodeToString(digest) {
		return lease, nil, "", ErrNomadSandboxMigrationConflict
	}
	return lease, digest, state, nil
}
