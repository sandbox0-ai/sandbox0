package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"strconv"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type nomadCaptureFailure struct {
	checkpoint    *NomadSandboxCheckpoint
	life          *SandboxLifecycleTxn
	capture       protocol.MigrationCaptureRequest
	request       *protocol.MigrationCaptureFailureRequest
	cleanup       *protocol.MigrationCaptureFailureProof
	finalized     *protocol.MigrationCaptureFailureFinalizeProof
	sourceSlot    string
	targetSlot    string
	targetLease   string
	sourceWriter  string
	sourceBinding []byte
}

// Failure recovery preserves the original operation -> sandbox -> lifecycle ->
// migration lock order, without requiring a live guest or unexpired CPU probe.
func lockNomadCaptureFailure(ctx context.Context, tx pgx.Tx, id string) (*nomadCaptureFailure, error) {
	if err := lockRuntimeSlotClaimOperation(ctx, tx, id); err != nil {
		return nil, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, id).Scan(&sandbox); err != nil {
		return nil, err
	}
	if _, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox); err != nil {
		return nil, err
	}
	life, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, id))
	if err != nil {
		return nil, err
	}
	if life != nil && life.Kind == SandboxLifecycleKindPause && life.Source == SandboxLifecycleSourceManual {
		return lockNomadCheckpointCaptureFailure(ctx, tx, life)
	}
	if life == nil || life.Kind != SandboxLifecycleKindMigrate || life.Source != SandboxLifecycleSourceAuto || life.Cancelable {
		return nil, ErrNomadSandboxMigrationConflict
	}
	w := &nomadCaptureFailure{life: life}
	var capture, request, cleanup, finalized []byte
	var captureDigest string
	var requestDigest *string
	var excluded bool
	if err := tx.QueryRow(ctx, `SELECT source_slot_id,target_slot_id,target_resource_lease_id,source_writer_grant_id,source_binding_digest,
        capture_request,capture_digest,capture_failure_request,capture_failure_digest,capture_failure_cleanup_receipt,capture_failure_finalization_receipt,
        publication_request IS NOT NULL OR failure_request IS NOT NULL OR preparation_cancel_request IS NOT NULL
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1 FOR UPDATE`, id).Scan(&w.sourceSlot, &w.targetSlot, &w.targetLease, &w.sourceWriter, &w.sourceBinding,
		&capture, &captureDigest, &request, &requestDigest, &cleanup, &finalized, &excluded); err != nil {
		return nil, err
	}
	if excluded || json.Unmarshal(capture, &w.capture) != nil || w.capture.Validate() != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	cd, _ := w.capture.Digest()
	if cd != captureDigest || w.capture.OperationID != id || w.capture.SandboxID != sandbox || w.capture.LifecycleEpoch != life.Epoch ||
		w.capture.SourceGeneration != life.FromGeneration || w.capture.Target.SlotID != w.sourceSlot || w.capture.Target.AllocationID != life.FromRuntimeID ||
		w.capture.BindingDigest != hex.EncodeToString(w.sourceBinding) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if len(request) != 0 {
		w.request = &protocol.MigrationCaptureFailureRequest{}
		if json.Unmarshal(request, w.request) != nil || requestDigest == nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
		want, err := w.request.Digest()
		if err != nil || want != *requestDigest || w.request.Capture.Request != w.capture || w.request.Cleanup.WriterGrantID != w.sourceWriter {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	if len(cleanup) != 0 {
		w.cleanup = &protocol.MigrationCaptureFailureProof{}
		if w.request == nil || json.Unmarshal(cleanup, w.cleanup) != nil || w.cleanup.ValidateFor(*w.request) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	if len(finalized) != 0 {
		w.finalized = &protocol.MigrationCaptureFailureFinalizeProof{}
		if w.cleanup == nil || json.Unmarshal(finalized, w.finalized) != nil ||
			w.finalized.ValidateFor(protocol.MigrationCaptureFailureFinalizeRequest{Request: *w.request, Proof: *w.cleanup}) != nil {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	return w, nil
}

func (w *nomadCaptureFailure) lockSourceGrant(ctx context.Context, tx pgx.Tx) (*rootFSWriterGrantRecord, error) {
	g, err := getRootFSWriterGrantForUpdate(ctx, tx, w.sourceWriter)
	if err != nil {
		return nil, err
	}
	s := w.capture
	if g.SandboxID != s.SandboxID || g.SlotID != s.Target.SlotID || g.NodeUID != s.Target.NodeUID || g.NodeBootID != s.Target.NodeBootID ||
		g.NodeName != s.Target.NodeID || g.RuntimeIncarnationID != s.Target.AllocationID || g.RuntimeID != protocol.NomadTaskName ||
		g.RuntimeNamespace != w.life.FromRuntimeNamespace || g.RuntimeGeneration != strconv.FormatInt(s.SourceGeneration, 10) ||
		!bytes.Equal(g.BindingDigest, w.sourceBinding) || g.InitialGenerationID != w.life.ExpectedGenerationID {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if err := lockRootFSWriterCrashFallbackGeneration(ctx, tx, g, w.life.ExpectedGenerationID, false); err != nil {
		return nil, err
	}
	return g, nil
}

// AuthorizeNomadSandboxMigrationCaptureFailure consumes an authenticated uncertain
// source observation before publication. It fences the original writer and
// releases only the provably never-attached target reservation. Source capacity,
// capture images and filesystem artifacts still require physical receipts.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationCaptureFailure(ctx context.Context, capture protocol.MigrationCapture) (*protocol.MigrationCaptureFailureRequest, error) {
	if capture.Validate() != nil || capture.State != protocol.MigrationCaptureUncertain || capture.RootFS != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	w, err := lockNomadCaptureFailure(ctx, tx, capture.Request.OperationID)
	if err != nil {
		return nil, err
	}
	if w.capture != capture.Request {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if w.request != nil {
		return w.request, tx.Commit(ctx)
	}
	if w.life.Phase != SandboxLifecyclePhasePublishing {
		return nil, ErrNomadSandboxMigrationConflict
	}
	source, err := lockRuntimeSlotByID(ctx, tx, w.sourceSlot)
	if err != nil {
		return nil, err
	}
	t := w.capture.Target
	if source.State != RuntimeSlotStateActive && source.State != RuntimeSlotStateQuiescing && source.State != RuntimeSlotStateOrphaned ||
		source.ResourceLeaseState != RuntimeResourceLeaseActive || source.SandboxID != w.life.SandboxID || source.WriterGrantID != w.sourceWriter ||
		source.AllocationID != t.AllocationID || source.ClusterID != t.ClusterID || source.NodeID != t.NodeID || source.NodeUID != t.NodeUID || source.NodeBootID != t.NodeBootID ||
		source.ControlEndpoint != t.ControlEndpoint || source.ProcdInstanceID != w.capture.ProcdInstanceID || source.ClaimRuntimeAssignmentRevision != w.capture.AssignmentRevision ||
		hex.EncodeToString(source.ResourceLeaseDigest) != w.capture.ResourceLeaseDigest || !bytes.Equal(source.RootFSBindingDigest, w.sourceBinding) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	var target *RuntimeSlot
	if w.checkpoint == nil {
		target, err = lockRuntimeSlotByID(ctx, tx, w.targetSlot)
		if err != nil {
			return nil, err
		}
		if target.ClaimID != "" || target.ClaimOperationID != "" || !target.ResourceLease.IsZero() || target.SandboxID != "" || target.WriterGrantID != "" {
			return nil, ErrNomadSandboxMigrationConflict
		}
		lease, _, state, err := loadMigrationResourceLease(ctx, tx, w.targetLease)
		if err != nil {
			return nil, err
		}
		if state != RuntimeResourceLeaseActive || lease.SlotID != target.ID || lease.OperationID != w.life.ID {
			return nil, ErrNomadSandboxMigrationConflict
		}
	}
	g, err := w.lockSourceGrant(ctx, tx)
	if err != nil {
		return nil, err
	}
	if g.State != RootFSWriterGrantStateConsumed || g.ConsumerNodeUID != t.NodeUID || g.RetireKind != "" || g.RetireOperationID != "" || len(g.RetireProofDigest) != 0 {
		return nil, ErrNomadSandboxMigrationConflict
	}
	op := protocol.MigrationCaptureFailureOperationID(w.life.ID)
	request := &protocol.MigrationCaptureFailureRequest{Capture: capture, Cleanup: protocol.NodeCleanupControlRequest{OperationID: op, WriterOperationID: op,
		WriterRetireKind: protocol.WriterRetireKindCrashAbandon, WriterGrantID: g.ID, WriterAuthorityDigest: capture.RequestDigest,
		SlotID: source.ID, ClusterID: source.ClusterID, NodeID: source.NodeID, NodeUID: source.NodeUID, NodeBootID: source.NodeBootID,
		AllocationID: source.AllocationID, NetNSIdentity: source.NetNSIdentity, RunscContainerID: source.RunscContainerID,
		Resources: source.ResourceLease, ResourceLeaseDigest: capture.Request.ResourceLeaseDigest}}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET state='retiring',retire_kind='crash_abandon',retire_operation_id=$2,
        retire_started_at=clock_timestamp(),updated_at=clock_timestamp() WHERE grant_id=$1`, g.ID, op); err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state=CASE WHEN state='active' THEN 'quiescing' ELSE state END,
        carrier_retired=true,quiescing_at=COALESCE(quiescing_at,clock_timestamp()),revision=revision+1,updated_at=clock_timestamp() WHERE slot_id=$1`, source.ID); err != nil {
		return nil, err
	}
	if w.checkpoint == nil {
		if _, err := tx.Exec(ctx, `UPDATE manager.runtime_resource_leases SET lease_state='released',released_at=clock_timestamp(),updated_at=clock_timestamp() WHERE lease_id=$1`, w.targetLease); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET carrier_retired=true,revision=revision+1,updated_at=clock_timestamp() WHERE slot_id=$1`, target.ID); err != nil {
			return nil, err
		}
	}
	if err := w.saveCaptureFailureEvidence(ctx, tx, "capture_failure", payload, `UPDATE manager.sandbox_runtime_migrations SET capture_failure_request=$2,capture_failure_digest=$3 WHERE operation_id=$1`, want); err != nil {
		return nil, err
	}
	return request, tx.Commit(ctx)
}

func (s *PGSandboxStore) CommitNomadSandboxMigrationCaptureFailureCleanup(ctx context.Context, request protocol.MigrationCaptureFailureRequest, proof protocol.MigrationCaptureFailureProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	w, err := lockNomadCaptureFailure(ctx, tx, request.Capture.Request.OperationID)
	if err != nil {
		return err
	}
	if w.request == nil || proof.ValidateFor(*w.request) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	if w.cleanup != nil {
		if *w.cleanup != proof {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if w.life.Phase != SandboxLifecyclePhasePublishing {
		return ErrNomadSandboxMigrationConflict
	}
	g, err := w.lockSourceGrant(ctx, tx)
	if err != nil {
		return err
	}
	if g.State != RootFSWriterGrantStateRetiring || g.RetireKind != RootFSWriterRetireKindCrashAbandon || g.RetireOperationID != request.Cleanup.WriterOperationID || len(g.RetireProofDigest) != 0 {
		return ErrNomadSandboxMigrationConflict
	}
	digest, _ := hex.DecodeString(proof.Cleanup.ProofDigest)
	if _, err := tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants SET state='retired',retire_proof_digest=$2,retired_at=clock_timestamp(),lease_expires_at=NULL,updated_at=clock_timestamp() WHERE grant_id=$1`, g.ID, digest); err != nil {
		return err
	}
	payload, _ := json.Marshal(proof)
	if err := w.saveCaptureFailureEvidence(ctx, tx, "capture_failure_cleanup", payload, `UPDATE manager.sandbox_runtime_migrations SET capture_failure_cleanup_receipt=$2 WHERE operation_id=$1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (w *nomadCaptureFailure) authorizeFinalization(ctx context.Context, tx pgx.Tx) (*protocol.MigrationCaptureFailureFinalizeRequest, error) {
	if w.request == nil || w.cleanup == nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	request := &protocol.MigrationCaptureFailureFinalizeRequest{Request: *w.request, Proof: *w.cleanup}
	if w.finalized != nil {
		return request, nil
	}
	if w.life.Phase != SandboxLifecyclePhasePublishing {
		return nil, ErrNomadSandboxMigrationConflict
	}
	g, err := w.lockSourceGrant(ctx, tx)
	if err != nil {
		return nil, err
	}
	if g.State != RootFSWriterGrantStateRetired || g.RetireKind != RootFSWriterRetireKindCrashAbandon ||
		g.RetireOperationID != w.request.Cleanup.WriterOperationID || hex.EncodeToString(g.RetireProofDigest) != w.cleanup.Cleanup.ProofDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return request, nil
}

func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationCaptureFailureFinalization(ctx context.Context, id string) (*protocol.MigrationCaptureFailureFinalizeRequest, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	w, err := lockNomadCaptureFailure(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	request, err := w.authorizeFinalization(ctx, tx)
	if err != nil {
		return nil, err
	}
	return request, tx.Commit(ctx)
}

func (s *PGSandboxStore) CommitNomadSandboxMigrationCaptureFailureFinalization(ctx context.Context, request protocol.MigrationCaptureFailureFinalizeRequest, proof protocol.MigrationCaptureFailureFinalizeProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	w, err := lockNomadCaptureFailure(ctx, tx, request.Request.Capture.Request.OperationID)
	if err != nil {
		return err
	}
	stored, err := w.authorizeFinalization(ctx, tx)
	if err != nil {
		return err
	}
	if proof.ValidateFor(*stored) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	if w.finalized != nil {
		if *w.finalized != proof {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	payload, _ := json.Marshal(proof)
	if err := w.saveCaptureFailureEvidence(ctx, tx, "capture_failure_finalized", payload, `UPDATE manager.sandbox_runtime_migrations SET capture_failure_finalization_receipt=$2 WHERE operation_id=$1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
