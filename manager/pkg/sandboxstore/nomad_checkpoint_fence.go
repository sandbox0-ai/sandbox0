package sandboxstore

import (
	"context"
	"encoding/hex"
	"errors"
	"reflect"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadCheckpointSourceFence revokes source writer renewal only after
// the region owns both the immutable memory image and its matching disk cut.
// No destination exists yet, so this path cannot depend on target readiness.
func (s *PGSandboxStore) AuthorizeNomadCheckpointSourceFence(ctx context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceRequest, error) {
	if _, err := request.Digest(); err != nil {
		return nil, err
	}
	c, err := s.mutateNomadCheckpointRetirement(ctx, request.PublicationRequest.Capture.Request.OperationID,
		func(tx pgx.Tx, _ *SandboxRecord, c *NomadSandboxCheckpoint) error {
			e := &c.Evidence
			if e.Retained == nil || e.Publication == nil || !reflect.DeepEqual(*e.Publication, request.PublicationRequest) ||
				e.Published == nil || *e.Published != request.Publication {
				return ErrNomadCheckpointConflict
			}
			if e.SourceFence != nil {
				if !reflect.DeepEqual(*e.SourceFence, request) {
					return ErrNomadCheckpointConflict
				}
				return nil
			}
			if c.Lifecycle.Phase != SandboxLifecyclePhasePublishing {
				return ErrNomadCheckpointConflict
			}
			var retained bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.sandbox_runtime_checkpoint_refs
				WHERE sandbox_id=$1 AND checkpoint_id=$2 AND generation_id=$3 AND runtime_generation=$4)`,
				c.Lifecycle.SandboxID, c.Lifecycle.ID, c.Lifecycle.PreparedGenerationID, c.Lifecycle.FromGeneration).Scan(&retained); err != nil {
				return err
			}
			if !retained {
				return ErrNomadCheckpointConflict
			}
			if err := validateNomadCheckpointPublicationSource(ctx, tx, c, request.PublicationRequest); err != nil {
				return err
			}
			tag, err := tx.Exec(ctx, `UPDATE manager.rootfs_writer_grants
				SET state='retiring',retire_kind=$2,retire_operation_id=$3,retire_started_at=NOW(),updated_at=NOW()
				WHERE grant_id=$1 AND state='consumed' AND retire_kind='' AND retire_operation_id='' AND retire_proof_digest IS NULL`,
				c.SourceWriterGrantID, RootFSWriterRetireKindMigration, c.Lifecycle.ID)
			if err != nil {
				return err
			}
			if tag.RowsAffected() != 1 {
				return ErrNomadCheckpointConflict
			}
			e.SourceFence = &request
			return nil
		})
	if err != nil {
		return nil, err
	}
	return c.Evidence.SourceFence, nil
}

// CommitNomadCheckpointSourceFence publishes the captured disk head after
// physical source execution and mounts are absent. The resource lease remains
// active until complete cleanup and authoritative allocation absence arrive.
func (s *PGSandboxStore) CommitNomadCheckpointSourceFence(ctx context.Context, request protocol.MigrationSourceFenceRequest, proof protocol.MigrationSourceFenceProof) error {
	if err := proof.ValidateFor(request); err != nil {
		return err
	}
	_, err := s.mutateNomadCheckpointRetirement(ctx, request.PublicationRequest.Capture.Request.OperationID,
		func(tx pgx.Tx, _ *SandboxRecord, c *NomadSandboxCheckpoint) error {
			e := &c.Evidence
			if e.SourceFence == nil || !reflect.DeepEqual(*e.SourceFence, request) {
				return ErrNomadCheckpointConflict
			}
			if e.Fenced != nil {
				if !reflect.DeepEqual(*e.Fenced, proof) || c.Lifecycle.Phase != SandboxLifecyclePhaseCommitting {
					return ErrNomadCheckpointConflict
				}
				return nil
			}
			if c.Lifecycle.Phase != SandboxLifecyclePhasePublishing ||
				c.Lifecycle.PreparedGenerationID != request.PublicationRequest.Capture.RootFS.Generation.GenerationID {
				return ErrNomadCheckpointConflict
			}
			// Keep the same slot-before-writer order as publication and ordinary
			// claim. This does not authorize generic source lease finalization.
			slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1 FOR UPDATE OF runtime_slots`, c.SourceSlotID))
			if err != nil {
				return err
			}
			if nomadMigrationSlotTarget(slot) != e.Preflight.Source.Target || slot.WriterGrantID != c.SourceWriterGrantID ||
				slot.ResourceLeaseState != RuntimeResourceLeaseActive ||
				(slot.State != RuntimeSlotStateActive && slot.State != RuntimeSlotStateQuiescing && slot.State != RuntimeSlotStateOrphaned) {
				return ErrNomadCheckpointConflict
			}
			binding, err := hex.DecodeString(e.Preflight.Source.BindingDigest)
			if err != nil {
				return err
			}
			if err := commitNomadExecutionSourceFence(ctx, tx, c.Lifecycle, c.SourceWriterGrantID, binding, proof); err != nil {
				if errors.Is(err, ErrNomadSandboxMigrationConflict) {
					return ErrNomadCheckpointConflict
				}
				return err
			}
			if slot.State == RuntimeSlotStateActive {
				if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state='quiescing',revision=revision+1,updated_at=NOW()
					WHERE slot_id=$1`, slot.ID); err != nil {
					return err
				}
			}
			if err := (sandboxStoreTx{tx: tx}).UpdateLifecycleTxnPhase(ctx, c.Lifecycle.ID, SandboxLifecyclePhaseCommitting); err != nil {
				return err
			}
			e.Fenced = &proof
			return nil
		})
	return err
}
