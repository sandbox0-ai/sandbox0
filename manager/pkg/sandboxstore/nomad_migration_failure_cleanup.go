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

func (s *PGSandboxStore) ListNomadMigrationFailureCleanups(ctx context.Context, after string, limit int) ([]string, error) {
	return s.listNomadExecutionFailureWork(ctx, after, limit, `l.kind='migrate' AND l.source='auto' AND NOT l.cancelable
        AND l.phase='committing' AND m.failure_stop_receipt IS NOT NULL AND m.failure_cleanup_receipt IS NULL`, `r.evidence ? 'failure_stopped' AND NOT r.evidence ? 'failure_cleaned'`)
}

// AuthorizeNomadSandboxMigrationFailureCleanup atomically records the exact
// physical command and revokes the writer. The latest committed source cut is
// retained; no target writes are published and no resource lease is released.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationFailureCleanup(ctx context.Context, id string) (*protocol.MigrationFailureCleanupRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	request, life, prior, err := lockNomadMigrationFailureCleanup(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if prior {
		return request, tx.Commit(ctx)
	}
	if !executionFailurePending(life) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	restore := request.Failure.Request.Restore
	target := restore.Image.Target
	slot, err := lockRuntimeSlotByID(ctx, tx, target.SlotID)
	if err != nil {
		return nil, err
	}
	binding, err := restore.Stage.BindingDigest()
	if err != nil {
		return nil, err
	}
	if slot.State != RuntimeSlotStateQuiescing && slot.State != RuntimeSlotStateOrphaned || slot.ResourceLeaseState != RuntimeResourceLeaseActive ||
		slot.SandboxID != life.SandboxID || slot.ClaimOperationID != id || slot.AllocationID != target.AllocationID ||
		slot.NodeUID != target.NodeUID || slot.NodeBootID != target.NodeBootID || slot.WriterGrantID != restore.Stage.Identity.WriterGrantID ||
		slot.NetNSIdentity != restore.Stage.ExpectedPolicyToken.NetNSIdentity ||
		(slot.RunscContainerID != "" && slot.RunscContainerID != request.Failure.Proof.ContainerID) ||
		slot.ResourceLease != restore.Image.Resources || (len(slot.RootFSBindingDigest) != 0 && !bytes.Equal(slot.RootFSBindingDigest, binding[:])) {
		return nil, fmt.Errorf("%w: target cleanup slot binding changed", ErrNomadSandboxMigrationConflict)
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, slot.WriterGrantID)
	if err != nil {
		return nil, err
	}
	if !rootFSWriterGrantMatchesRetireBase(grant, slot.WriterGrantID, restore.Stage.Identity.WriterEpoch, restore.Stage.BindingVersion, binding[:]) ||
		grant.SandboxID != life.SandboxID || grant.NodeUID != target.NodeUID || grant.NodeBootID != target.NodeBootID ||
		grant.SlotID != slot.ID || grant.RuntimeIncarnationID != target.AllocationID || grant.RuntimeID != protocol.NomadTaskName ||
		grant.RuntimeNamespace != slot.AllocationNamespace || grant.RuntimeGeneration != restore.Stage.Identity.RuntimeGeneration ||
		grant.NodeName != slot.NodeID || grant.GateParent != restore.Stage.Parent ||
		grant.ClaimID != restore.Stage.Identity.ClaimID || grant.InitialGenerationID != restore.Stage.InitialGeneration ||
		grant.IssueOperationID != id || grant.RetireKind != "" || grant.RetireOperationID != "" || len(grant.RetireProofDigest) != 0 {
		return nil, fmt.Errorf("%w: target cleanup writer binding changed", ErrNomadSandboxMigrationConflict)
	}
	if err := lockRootFSWriterCrashFallbackGeneration(ctx, tx, grant, restore.Stage.InitialGeneration, false); err != nil {
		return nil, err
	}
	kind := protocol.WriterRetireKindCrashAbandon
	switch grant.State {
	case RootFSWriterGrantStateIssued, RootFSWriterGrantStateCanceled:
		if _, err := cancelRootFSWriterGrant(ctx, tx, &CancelRootFSWriterGrantRequest{GrantID: grant.ID, WriterEpoch: grant.WriterEpoch,
			OperationID: id, BindingVersion: grant.BindingVersion, BindingDigest: grant.BindingDigest}); err != nil {
			return nil, err
		}
		kind = protocol.WriterRetireKindCanceled
	case RootFSWriterGrantStateConsumed:
		if grant.ConsumerNodeUID != target.NodeUID {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET state='retiring',retire_kind=$2,retire_operation_id=$3,
            retire_started_at=clock_timestamp(),updated_at=clock_timestamp() WHERE grant_id=$1`, grant.ID, RootFSWriterRetireKindCrashAbandon, protocol.MigrationFailureCleanupOperationID(id)); err != nil {
			return nil, err
		}
	default:
		return nil, ErrNomadSandboxMigrationConflict
	}
	request.Cleanup = protocol.NodeCleanupControlRequest{OperationID: protocol.MigrationFailureCleanupOperationID(id), WriterOperationID: protocol.MigrationFailureCleanupOperationID(id),
		WriterRetireKind: kind, WriterGrantID: grant.ID, WriterAuthorityDigest: request.Failure.Proof.RequestDigest,
		SlotID: slot.ID, ClusterID: slot.ClusterID, NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
		AllocationID: slot.AllocationID, NetNSIdentity: slot.NetNSIdentity, RunscContainerID: request.Failure.Proof.ContainerID,
		Resources: slot.ResourceLease, ResourceLeaseDigest: hex.EncodeToString(slot.ResourceLeaseDigest)}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if err := saveNomadExecutionFailureEvidence(ctx, tx, life, "failure_cleanup", payload, `UPDATE manager.sandbox_runtime_migrations SET failure_cleanup_request=$2,failure_cleanup_digest=$3 WHERE operation_id=$1`, want); err != nil {
		return nil, err
	}
	return request, tx.Commit(ctx)
}

// This lock path preserves operation -> sandbox -> lifecycle -> migration order.
// A stored cleanup is historical authority; a missing stop never creates it.
func lockNomadMigrationFailureCleanup(ctx context.Context, tx pgx.Tx, id string) (*protocol.MigrationFailureCleanupRequest, *SandboxLifecycleTxn, bool, error) {
	if err := lockRuntimeSlotClaimOperation(ctx, tx, id); err != nil {
		return nil, nil, false, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, id).Scan(&sandbox); err != nil {
		return nil, nil, false, err
	}
	if _, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox); err != nil {
		return nil, nil, false, err
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, id))
	if err != nil {
		return nil, nil, false, err
	}
	if life != nil && life.Kind == SandboxLifecycleKindResume {
		return lockNomadCheckpointRestoreFailureCleanup(ctx, tx, life)
	}
	if life == nil || life.Kind != SandboxLifecycleKindMigrate || life.Source != SandboxLifecycleSourceAuto || life.Cancelable {
		return nil, nil, false, ErrNomadSandboxMigrationConflict
	}
	var failure, stopped, payload []byte
	var digest *string
	if err := tx.QueryRow(ctx, `SELECT failure_request,failure_stop_receipt,failure_cleanup_request,failure_cleanup_digest
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, id).Scan(&failure, &stopped, &payload, &digest); err != nil {
		return nil, nil, false, err
	}
	result := &protocol.MigrationFailureCleanupRequest{}
	if json.Unmarshal(failure, &result.Failure.Request) != nil || json.Unmarshal(stopped, &result.Failure.Proof) != nil ||
		result.Failure.Proof.ValidateFor(result.Failure.Request) != nil {
		return nil, nil, false, ErrNomadSandboxMigrationConflict
	}
	r := result.Failure.Request.Restore
	if r.Image.Publication.Assignment.OperationID != id || r.Image.Publication.Assignment.Target.SandboxID != sandbox ||
		r.Image.Publication.Assignment.Target.RuntimeGeneration != life.ToGeneration || r.Image.Target.AllocationID != life.ToRuntimeID ||
		r.Image.Publication.Capture.Request.LifecycleEpoch != life.Epoch {
		return nil, nil, false, ErrNomadSandboxMigrationConflict
	}
	prior := len(payload) != 0
	if prior {
		failureProof := result.Failure.Proof
		if json.Unmarshal(payload, result) != nil || digest == nil {
			return nil, nil, false, ErrNomadSandboxMigrationConflict
		}
		want, err := result.Digest()
		if err != nil || want != *digest || result.Failure.Proof != failureProof {
			return nil, nil, false, ErrNomadSandboxMigrationConflict
		}
	}
	return result, life, prior, nil
}

// CommitNomadSandboxMigrationFailureCleanup retires only the exact target writer
// after authenticated physical evidence. Lifecycle, routing, allocation, image
// custody and both storage-retention references remain unchanged.
func (s *PGSandboxStore) CommitNomadSandboxMigrationFailureCleanup(ctx context.Context, request protocol.MigrationFailureCleanupRequest, proof protocol.MigrationFailureCleanupProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	id := request.Failure.Request.Restore.Image.OperationID()
	stored, life, authorized, err := lockNomadMigrationFailureCleanup(ctx, tx, id)
	if err != nil {
		return err
	}
	if !authorized || proof.ValidateFor(*stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	prior, err := readNomadExecutionFailureEvidence(ctx, tx, id, "failure_cleaned")
	if err != nil {
		return err
	}
	if len(prior) != 0 {
		var old protocol.MigrationFailureCleanupProof
		if json.Unmarshal(prior, &old) != nil || old != proof {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if !executionFailurePending(life) {
		return ErrNomadSandboxMigrationConflict
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, request.Cleanup.WriterGrantID)
	if err != nil {
		return err
	}
	stage := request.Failure.Request.Restore.Stage
	binding, _ := stage.BindingDigest()
	if !rootFSWriterGrantMatchesRetireBase(grant, stage.Identity.WriterGrantID, stage.Identity.WriterEpoch, stage.BindingVersion, binding[:]) ||
		grant.NodeUID != request.Cleanup.NodeUID || grant.NodeBootID != request.Cleanup.NodeBootID {
		return ErrNomadSandboxMigrationConflict
	}
	if err := lockRootFSWriterCrashFallbackGeneration(ctx, tx, grant, stage.InitialGeneration, false); err != nil {
		return err
	}
	if request.Cleanup.WriterRetireKind == protocol.WriterRetireKindCanceled {
		if grant.State != RootFSWriterGrantStateCanceled {
			return ErrNomadSandboxMigrationConflict
		}
	} else {
		if grant.State != RootFSWriterGrantStateRetiring || grant.RetireKind != RootFSWriterRetireKindCrashAbandon || grant.RetireOperationID != request.Cleanup.WriterOperationID || len(grant.RetireProofDigest) != 0 {
			return ErrNomadSandboxMigrationConflict
		}
		digest, _ := hex.DecodeString(proof.Cleanup.ProofDigest)
		if _, err := tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET state='retired',retire_proof_digest=$2,retired_at=clock_timestamp(),lease_expires_at=NULL,updated_at=clock_timestamp() WHERE grant_id=$1`, grant.ID, digest); err != nil {
			return err
		}
	}
	payload, err := json.Marshal(proof)
	if err != nil {
		return err
	}
	if err := saveNomadExecutionFailureEvidence(ctx, tx, life, "failure_cleaned", payload, `UPDATE manager.sandbox_runtime_migrations SET failure_cleanup_receipt=$2 WHERE operation_id=$1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
