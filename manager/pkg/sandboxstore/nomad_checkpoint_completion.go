package sandboxstore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadCheckpointSourceFinalization reuses migration's bounded node
// cleanup command. Retention replaces target adoption; neither this command nor
// its node receipt can independently release the regional resource lease.
func (s *PGSandboxStore) AuthorizeNomadCheckpointSourceFinalization(ctx context.Context, operation string) (*protocol.MigrationSourceFinalizeRequest, error) {
	c, err := s.mutateNomadCheckpointCleanup(ctx, operation, func(tx pgx.Tx, _ *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if e.Finalization != nil {
			return nil
		}
		slot, err := lockNomadCheckpointCleanupSource(ctx, tx, c)
		if err != nil {
			return err
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhaseCommitting ||
			(slot.State != RuntimeSlotStateQuiescing && slot.State != RuntimeSlotStateOrphaned) ||
			slot.ResourceLeaseState != RuntimeResourceLeaseActive || !slot.ResourceLeaseReleasedAt.IsZero() {
			return ErrNomadCheckpointConflict
		}
		e.Finalization = &protocol.MigrationSourceFinalizeRequest{Fence: *e.SourceFence, SourceProof: *e.Fenced, Checkpoint: e.Retained,
			Cleanup: protocol.NodeCleanupControlRequest{
				OperationID: protocol.MigrationSourceCleanupOperationID(operation), WriterOperationID: operation, WriterRetireKind: protocol.WriterRetireKindMigration,
				SlotID: slot.ID, ClusterID: slot.ClusterID, AllocationID: slot.AllocationID, NodeID: slot.NodeID, NodeUID: slot.NodeUID, NodeBootID: slot.NodeBootID,
				NetNSIdentity: slot.NetNSIdentity, RunscContainerID: slot.RunscContainerID, WriterGrantID: c.SourceWriterGrantID,
				WriterAuthorityDigest: e.Fenced.Digest, Resources: slot.ResourceLease, ResourceLeaseDigest: e.Preflight.Source.ResourceLeaseDigest,
			}}
		_, err = e.Finalization.Digest()
		return err
	})
	if err != nil {
		return nil, err
	}
	return c.Evidence.Finalization, nil
}

func lockNomadCheckpointCleanupSource(ctx context.Context, tx pgx.Tx, c *NomadSandboxCheckpoint) (*RuntimeSlot, error) {
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, c.SourceSlotID))
	if err != nil {
		return nil, err
	}
	e, source := c.Evidence, c.Evidence.Preflight.Source
	// A new control endpoint must not alter the original cleanup command.
	target := nomadMigrationSlotTarget(slot)
	target.ControlEndpoint = source.Target.ControlEndpoint
	if target != source.Target || slot.SandboxID != c.Lifecycle.SandboxID || slot.AllocationNamespace != c.Lifecycle.FromRuntimeNamespace ||
		slot.WriterGrantID != c.SourceWriterGrantID || hex.EncodeToString(slot.RootFSBindingDigest) != source.BindingDigest ||
		slot.ProcdInstanceID != source.ProcdInstanceID || slot.ClaimRuntimeAssignmentRevision != source.AssignmentRevision ||
		slot.ResourceLease != e.Preflight.SourceResources || hex.EncodeToString(slot.ResourceLeaseDigest) != source.ResourceLeaseDigest {
		return nil, ErrNomadCheckpointConflict
	}
	grant, err := getRootFSWriterGrantForUpdate(ctx, tx, c.SourceWriterGrantID)
	if err != nil {
		return nil, err
	}
	if grant.State != RootFSWriterGrantStateRetired || grant.RetireKind != RootFSWriterRetireKindMigration ||
		grant.RetireOperationID != c.Lifecycle.ID || hex.EncodeToString(grant.RetireProofDigest) != e.Fenced.Digest ||
		hex.EncodeToString(grant.BindingDigest) != source.BindingDigest {
		return nil, ErrNomadCheckpointConflict
	}
	return slot, nil
}

// CommitNomadCheckpointSourceFinalization retains authenticated physical cleanup
// evidence. Nomad allocation absence is still required before pause completion.
func (s *PGSandboxStore) CommitNomadCheckpointSourceFinalization(ctx context.Context, request protocol.MigrationSourceFinalizeRequest, proof protocol.MigrationSourceFinalizeProof) error {
	if request.Checkpoint == nil || proof.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	_, err := s.mutateNomadCheckpointCleanup(ctx, request.Checkpoint.CheckpointID,
		func(_ pgx.Tx, _ *SandboxRecord, c *NomadSandboxCheckpoint) error {
			e := &c.Evidence
			if e.Finalization == nil || !reflect.DeepEqual(*e.Finalization, request) ||
				(e.Finalized != nil && !reflect.DeepEqual(*e.Finalized, proof)) {
				return ErrNomadCheckpointConflict
			}
			e.Finalized = &proof
			return nil
		})
	return err
}

// CompleteNomadSandboxMemoryPause atomically releases source capacity, clears
// routing and commits pause only after both physical cleanup and allocation
// absence. It preserves an already-requested termination and never resurrects
// an owner that expires or is deleted while its source cleanup is in flight.
func (s *PGSandboxStore) CompleteNomadSandboxMemoryPause(ctx context.Context, operation string) (*RuntimeSlot, error) {
	var terminal *RuntimeSlot
	_, err := s.mutateNomadCheckpointCleanup(ctx, operation, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if e.Finalized == nil || e.Finalization == nil {
			return ErrNomadCheckpointConflict
		}
		slot, err := lockNomadCheckpointCleanupSource(ctx, tx, c)
		if err != nil {
			return err
		}
		command := e.Finalization.Cleanup
		if slot.NetNSIdentity != command.NetNSIdentity || slot.RunscContainerID != command.RunscContainerID ||
			len(slot.OrphanObservationDigest) != sha256.Size ||
			(slot.State != RuntimeSlotStateOrphaned && slot.State != RuntimeSlotStateTerminal) ||
			(c.Lifecycle.Phase == SandboxLifecyclePhaseCommitted && slot.State != RuntimeSlotStateTerminal) {
			return ErrNomadCheckpointConflict
		}
		payload, err := json.Marshal(struct {
			Domain     string `json:"domain"`
			Operation  string `json:"operation"`
			Command    string `json:"command"`
			Node       string `json:"node"`
			Allocation string `json:"allocation"`
		}{"sandbox0-memory-pause-completion-v1", operation, e.Finalized.RequestDigest, e.Finalized.Cleanup.ProofDigest, hex.EncodeToString(slot.OrphanObservationDigest)})
		if err != nil {
			return err
		}
		digest := sha256.Sum256(payload)
		normalized, err := normalizeFinalizeRuntimeSlotRequest(&FinalizeRuntimeSlotRequest{
			SlotID: slot.ID, OperationID: slot.ClaimOperationID, ClaimID: slot.ClaimID, Reason: "memory_pause",
			ProofDigest: digest[:], ResourceLeaseID: slot.ResourceLease.LeaseID, ResourceLeaseDigest: slot.ResourceLeaseDigest,
			ResourceCgroupAbsent: e.Finalized.Cleanup.ResourceCgroupAbsent})
		if err != nil {
			return err
		}
		if _, err := finalizeRuntimeSlotTx(ctx, tx, slot, normalized); err != nil {
			return err
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhaseCommitted {
			if record.DesiredState == SandboxDesiredStateActive && record.DeletedAt.IsZero() {
				if !nomadCheckpointLifecycleMatches(c.Lifecycle, record) {
					return ErrNomadCheckpointConflict
				}
				if err := (sandboxStoreTx{tx: tx}).MarkRuntimePaused(ctx, record.ID, c.Lifecycle.FromGeneration, time.Time{}); err != nil {
					return err
				}
			} else if record.DesiredState != SandboxDesiredStateTerminating && record.DesiredState != SandboxDesiredStateDeleted {
				return ErrNomadCheckpointConflict
			}
			if err := (sandboxStoreTx{tx: tx}).CommitLifecycleTxn(ctx, operation, c.Lifecycle.PreparedGenerationID); err != nil {
				return err
			}
		}
		if err := completeRunningMemoryForkHandoff(ctx, tx, operation); err != nil {
			return err
		}
		if e.Staging != nil && e.Staging.CaptureUpload.Version != 0 {
			// Retained image chunks remain pinned by checkpoint refs. The
			// tentative-upload budget can return once the source is physically
			// terminal and the pause commit has exclusive durable custody.
			tag, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints
				SET capture_upload_reservation_released_at=COALESCE(capture_upload_reservation_released_at,clock_timestamp())
				WHERE operation_id=$1`, operation)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrNomadCheckpointConflict
			}
		}
		terminal, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, slot.ID))
		return err
	})
	return terminal, err
}

// getNomadCheckpointFinalizationForSlot recovers historical source cleanup
// evidence through the existing node-reconciliation lookup after journal GC.
func (s *PGSandboxStore) getNomadCheckpointFinalizationForSlot(ctx context.Context, slot string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoints
		WHERE source_slot_id=$1 AND evidence ? 'finalized'`, slot).Scan(&payload)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var e NomadCheckpointEvidence
	if json.Unmarshal(payload, &e) != nil || e.validate() != nil || e.Finalization == nil || e.Finalized == nil ||
		e.Finalization.Cleanup.SlotID != slot {
		return nil, ErrNomadCheckpointConflict
	}
	return &protocol.MigrationSourceFinalizationReceipt{Request: *e.Finalization, Proof: *e.Finalized}, nil
}

// GetNomadCheckpointSourceFinalization distinguishes retained checkpoint custody
// from paired migration custody without consulting a mutable sandbox pointer.
// A known checkpoint with no receipt must still use checkpoint authorization.
func (s *PGSandboxStore) GetNomadCheckpointSourceFinalization(ctx context.Context, operation string) (*protocol.MigrationSourceFinalizationReceipt, bool, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT evidence FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, operation).Scan(&payload)
	if err == pgx.ErrNoRows {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	var e NomadCheckpointEvidence
	if json.Unmarshal(payload, &e) != nil || e.validate() != nil || e.Preflight.Source.OperationID != operation {
		return nil, true, ErrNomadCheckpointConflict
	}
	if e.Finalized == nil {
		return nil, true, nil
	}
	receipt := &protocol.MigrationSourceFinalizationReceipt{Request: *e.Finalization, Proof: *e.Finalized}
	if receipt.Validate() != nil {
		return nil, true, ErrNomadCheckpointConflict
	}
	return receipt, true, nil
}
