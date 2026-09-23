package sandboxstore

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// GetNomadCheckpointRestorePreparation revalidates the current owner, placement
// and lease before reusing durable image custody. Unlike preparation mutations,
// this also permits exact retries after writer issuance or command readiness.
func (s *PGSandboxStore) GetNomadCheckpointRestorePreparation(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slotID string) (*NomadCheckpointRestoreEvidence, error) {
	return s.mutateNomadCheckpointRestore(ctx, authority, slotID, func(pgx.Tx, time.Time, *NomadCheckpointEvidence, *RuntimeSlot, *NomadCheckpointRestoreEvidence) error {
		return nil
	})
}

// AuthorizeNomadCheckpointRestoreCPU binds the archived CPU history to capacity
// already acquired by ordinary resume. The source may have been gone for days;
// only this new destination requires a fresh observation.
func (s *PGSandboxStore) AuthorizeNomadCheckpointRestoreCPU(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slotID string) (*protocol.MigrationCPUPreflightRequest, error) {
	e, err := s.mutateNomadCheckpointRestoreImage(ctx, authority, slotID, func(now time.Time, capture *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if e.CPURequest != nil {
			return requireCheckpointRestoreCPUFresh(now, e)
		}
		placement := nomadMigrationSlotTarget(target)
		e.CPURequest = &protocol.MigrationCPUPreflightRequest{Target: placement, Source: capture.Preflight.Source,
			SourceResources: capture.Preflight.SourceResources, Destination: placement, DestinationResources: target.ResourceLease,
			Launch: &capture.CPU.Launch, Checkpoint: &authority.Assignment}
		e.CPURequestedAt = &now
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.CPURequest, nil
}

// CommitNomadCheckpointRestoreCPU accepts only an authenticated exact-boot node
// response. It does not renew the observation window or authorize execution.
func (s *PGSandboxStore) CommitNomadCheckpointRestoreCPU(ctx context.Context, request protocol.MigrationCPUPreflightRequest, authority protocol.CheckpointRestoreAuthority, receipt protocol.MigrationCPUPreflight) error {
	if receipt.ValidateFor(request) != nil || !reflect.DeepEqual(request.Checkpoint, &authority.Assignment) {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpointRestoreImage(ctx, authority, request.Target.SlotID, func(now time.Time, _ *NomadCheckpointEvidence, _ *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if !reflect.DeepEqual(e.CPURequest, &request) || requireCheckpointRestoreCPUFresh(now, e) != nil {
			return ErrNomadCheckpointConflict
		}
		if e.CPU != nil && !reflect.DeepEqual(*e.CPU, receipt) {
			return ErrNomadCheckpointConflict
		}
		e.CPU = &receipt
		return nil
	})
	return err
}

// AuthorizeNomadCheckpointRestoreImage reuses migration's verified streaming
// download. It grants custody without issuing a filesystem writer or starting
// a guest; each fork references the same immutable regional image objects.
func (s *PGSandboxStore) AuthorizeNomadCheckpointRestoreImage(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slotID string) (*protocol.MigrationImagePrepareRequest, error) {
	e, err := s.mutateNomadCheckpointRestoreImage(ctx, authority, slotID, func(now time.Time, capture *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if e.Image != nil {
			return nil
		}
		if e.CPU == nil || requireCheckpointRestoreCPUFresh(now, e) != nil {
			return ErrNomadCheckpointConflict
		}
		e.Image = &protocol.MigrationImagePrepareRequest{Target: nomadMigrationSlotTarget(target),
			Publication: *capture.Publication, Receipt: *capture.Published, Resources: target.ResourceLease, Checkpoint: &authority}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.Image, nil
}

func (s *PGSandboxStore) CommitNomadCheckpointRestoreImage(ctx context.Context, request protocol.MigrationImagePrepareRequest, receipt protocol.MigrationImagePrepared) error {
	if request.Checkpoint == nil || receipt.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpointRestoreImage(ctx, *request.Checkpoint, request.Target.SlotID, func(_ time.Time, _ *NomadCheckpointEvidence, _ *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if !reflect.DeepEqual(e.Image, &request) || (e.Prepared != nil && *e.Prepared != receipt) {
			return ErrNomadCheckpointConflict
		}
		e.Prepared = &receipt
		return nil
	})
	return err
}

func requireCheckpointRestoreCPUFresh(now time.Time, e *NomadCheckpointRestoreEvidence) error {
	if e.CPURequestedAt == nil || now.Before(*e.CPURequestedAt) || !now.Before(e.CPURequestedAt.Add(NomadMigrationCPUPreflightTTL)) {
		return ErrNomadCheckpointConflict
	}
	return nil
}

// The lock order follows resume/claim: owner, lifecycle, filesystem, restore
// authority, target slot and its resource lease. No source-node liveness is
// consulted, and no resource is leased twice. Image transfer can exceed the CPU
// probe window; the driver remeasures CPU compatibility before actual restore.
func (s *PGSandboxStore) mutateNomadCheckpointRestoreImage(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slotID string,
	mutate func(time.Time, *NomadCheckpointEvidence, *RuntimeSlot, *NomadCheckpointRestoreEvidence) error) (*NomadCheckpointRestoreEvidence, error) {
	return s.mutateNomadCheckpointRestore(ctx, authority, slotID, func(_ pgx.Tx, now time.Time, capture *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if target.State != RuntimeSlotStateClaiming || target.WriterGrantID != "" {
			return ErrNomadCheckpointConflict
		}
		return mutate(now, capture, target, e)
	})
}

func (s *PGSandboxStore) mutateNomadCheckpointRestore(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slotID string,
	mutate func(pgx.Tx, time.Time, *NomadCheckpointEvidence, *RuntimeSlot, *NomadCheckpointRestoreEvidence) error) (*NomadCheckpointRestoreEvidence, error) {
	if authority.Assignment.Validate() != nil || authority.LifecycleEpoch <= 0 || slotID == "" {
		return nil, ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	record, err := lockNomadSandboxClaimRecord(ctx, tx, authority.Assignment.Target.SandboxID)
	if err != nil {
		return nil, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, authority.Assignment.OperationID))
	if err != nil {
		return nil, err
	}
	if lifecycle == nil || lifecycle.Epoch != authority.LifecycleEpoch || record.TeamID != authority.Assignment.Target.TeamID {
		return nil, ErrNomadCheckpointConflict
	}
	committed := lifecycle.Phase == SandboxLifecyclePhaseCommitted
	if (!committed && (lifecycle.Phase != SandboxLifecyclePhasePreparing || record.DesiredState != SandboxDesiredStatePaused)) ||
		(committed && record.DesiredState != SandboxDesiredStateActive) {
		return nil, ErrNomadCheckpointConflict
	}
	generation := lifecycle.ExpectedGenerationID
	if !nomadResumeLifecycleMatches(lifecycle, record, authority.Assignment.OperationID, generation, committed) {
		return nil, ErrNomadCheckpointConflict
	}
	// Execution callbacks recheck the writer epoch. Take the exclusive storage
	// lock before the slot rather than upgrading it while holding a slot lock.
	fs, _, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if !committed && fs.HeadGenerationID != generation {
		return nil, ErrNomadCheckpointConflict
	}
	filesystem := fs.ID
	var capture *NomadCheckpointEvidence
	var compatibility string
	if committed {
		var storedPayload, capturePayload []byte
		if err := tx.QueryRow(ctx, `SELECT r.authority,r.compatibility_digest,c.evidence
			FROM manager.sandbox_runtime_checkpoint_restores r JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=r.checkpoint_id
			WHERE r.operation_id=$1 FOR UPDATE OF r`, lifecycle.ID).Scan(&storedPayload, &compatibility, &capturePayload); err != nil {
			return nil, err
		}
		var stored protocol.CheckpointRestoreAuthority
		capture = &NomadCheckpointEvidence{}
		if json.Unmarshal(storedPayload, &stored) != nil || !reflect.DeepEqual(stored, authority) ||
			json.Unmarshal(capturePayload, capture) != nil || capture.validate() != nil || capture.Finalized == nil ||
			authority.ValidateFor(*capture.Publication, *capture.Published) != nil {
			return nil, ErrNomadCheckpointConflict
		}
	} else {
		stored, value, err := bindNomadResumeMode(ctx, tx, record, lifecycle, generation, true, false)
		if err != nil {
			return nil, err
		}
		if !reflect.DeepEqual(stored, &authority) {
			return nil, ErrNomadCheckpointConflict
		}
		compatibility = value
		capture, _, err = loadOwnedNomadCheckpoint(ctx, tx, record, generation)
		if err != nil {
			return nil, err
		}
	}
	var payload []byte
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT evidence,clock_timestamp() FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, lifecycle.ID).Scan(&payload, &now); err != nil {
		return nil, err
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(now) {
		return nil, ErrNomadCheckpointConflict
	}
	var evidence NomadCheckpointRestoreEvidence
	if json.Unmarshal(payload, &evidence) != nil || evidence.validate(authority, *capture) != nil || evidence.Failure != nil {
		return nil, ErrNomadCheckpointConflict
	}
	target, err := lockRuntimeSlotByID(ctx, tx, slotID)
	if err != nil {
		return nil, err
	}
	revision, _ := authority.Assignment.Target.Revision()
	if target == nil || target.ClaimOperationID != lifecycle.ID || target.SandboxID != record.ID ||
		target.FilesystemID != filesystem || target.SourceGenerationID != generation || target.CompatibilityDigest != compatibility ||
		target.ClaimRuntimeAssignmentRevision != revision ||
		(target.State != RuntimeSlotStateClaiming && target.State != RuntimeSlotStateStarting && target.State != RuntimeSlotStateActive) ||
		!target.HeartbeatExpiresAt.After(now) || runtimeSlotPreCommandReadyClaimExpired(target) {
		return nil, ErrNomadCheckpointConflict
	}
	if committed && (target.State != RuntimeSlotStateActive || target.AllocationID != record.RuntimeID || target.AllocationNamespace != record.RuntimeNamespace) {
		return nil, ErrNomadCheckpointConflict
	}
	lease, _, state, err := loadMigrationResourceLease(ctx, tx, target.ResourceLease.LeaseID)
	if err != nil {
		return nil, err
	}
	if state != RuntimeResourceLeaseActive || lease != target.ResourceLease || lease.OperationID != lifecycle.ID {
		return nil, ErrNomadCheckpointConflict
	}
	var eligible bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.runtime_node_capacities c
		WHERE c.cluster_id=$1 AND c.node_id=$2 AND c.node_uid=$3 AND c.node_boot_id=$4 AND c.heartbeat_expires_at>clock_timestamp()
		AND NOT EXISTS (SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=c.cluster_id AND f.node_id=c.node_id
			AND f.node_uid=c.node_uid AND f.state IN ('warming','draining','revoked'))
		AND NOT EXISTS (SELECT 1 FROM manager.runtime_carrier_resizes r WHERE r.cluster_id=c.cluster_id AND r.node_id=c.node_id
			AND r.pending AND NOT ($5=ANY(r.retained_allocations))))`, target.ClusterID, target.NodeID, target.NodeUID, target.NodeBootID, target.AllocationID).Scan(&eligible); err != nil {
		return nil, err
	}
	if !eligible || (evidence.CPURequest != nil && (evidence.CPURequest.Target != nomadMigrationSlotTarget(target) || evidence.CPURequest.DestinationResources != lease)) {
		return nil, ErrNomadCheckpointConflict
	}
	before, err := json.Marshal(evidence)
	if err != nil {
		return nil, err
	}
	if err := mutate(tx, now, capture, target, &evidence); err != nil {
		return nil, err
	}
	if err := evidence.validate(authority, *capture); err != nil {
		return nil, err
	}
	payload, err = json.Marshal(evidence)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(before, payload) {
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoint_restores SET evidence=$2 WHERE operation_id=$1`, lifecycle.ID, payload); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &evidence, nil
}
