package sandboxstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadCheckpointRestore persists the exact tokenless execution
// command after verified image custody and atomic writer binding. Region and
// driver both retain this command, so an uncertain restore cannot become start.
func (s *PGSandboxStore) AuthorizeNomadCheckpointRestore(ctx context.Context, authority protocol.CheckpointRestoreAuthority, slotID string, stage rootfshandoff.StageRequest) (*protocol.MigrationRestoreRequest, error) {
	if stage.Identity.WriterGrantToken != "" {
		return nil, ErrNomadCheckpointConflict
	}
	e, err := s.mutateNomadCheckpointRestore(ctx, authority, slotID, func(tx pgx.Tx, _ time.Time, capture *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if e.Prepared == nil || capture.SourceFence == nil || capture.Fenced == nil {
			return ErrNomadCheckpointConflict
		}
		request := protocol.MigrationRestoreRequest{Image: *e.Image, Prepared: *e.Prepared,
			Fence: *capture.SourceFence, Proof: *capture.Fenced, Stage: stage}
		if request.Validate() != nil || (e.Restore != nil && !reflect.DeepEqual(*e.Restore, request)) {
			return ErrNomadCheckpointConflict
		}
		if _, err := checkpointRestoreWriter(ctx, tx, target, request, false); err != nil {
			return err
		}
		if e.Restore == nil && target.State != RuntimeSlotStateClaiming {
			return ErrNomadCheckpointConflict
		}
		e.Restore = &request
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.Restore, nil
}

// The immutable mode record chooses the execution protocol; caller-supplied
// digests alone must never select a different restore implementation.
func (s *PGSandboxStore) getNomadCheckpointRestoreAuthority(ctx context.Context, operation string) (*protocol.CheckpointRestoreAuthority, error) {
	var payload []byte
	err := s.pool.QueryRow(ctx, `SELECT authority FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, operation).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var authority protocol.CheckpointRestoreAuthority
	if json.Unmarshal(payload, &authority) != nil || authority.Assignment.Validate() != nil || authority.Assignment.OperationID != operation {
		return nil, ErrNomadCheckpointConflict
	}
	return &authority, nil
}

func checkpointRestoreWriter(ctx context.Context, tx pgx.Tx, target *RuntimeSlot, request protocol.MigrationRestoreRequest, consumed bool) (*rootFSWriterGrantRecord, error) {
	grant, err := validateNomadExecutionRestoreWriter(ctx, tx, target, request)
	if errors.Is(err, ErrNomadSandboxMigrationConflict) {
		return nil, ErrNomadCheckpointConflict
	}
	if err != nil {
		return nil, err
	}
	if consumed && (grant.State != RootFSWriterGrantStateConsumed || !grant.LeaseExpiresAt.After(grant.databaseNow)) {
		return nil, ErrNomadCheckpointConflict
	}
	return grant, nil
}

func (s *PGSandboxStore) startNomadCheckpointRestore(ctx context.Context, authority protocol.CheckpointRestoreAuthority, start *StartRuntimeSlotRequest) (*RuntimeSlot, error) {
	var result *RuntimeSlot
	_, err := s.mutateNomadCheckpointRestore(ctx, authority, start.SlotID, func(tx pgx.Tx, _ time.Time, _ *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if e.Restore == nil {
			return ErrNomadCheckpointConflict
		}
		digest, _ := e.Restore.Digest()
		grant, err := checkpointRestoreWriter(ctx, tx, target, *e.Restore, true)
		if err != nil {
			return err
		}
		if start.MigrationRestoreDigest != digest || start.LaunchAttempt != e.Restore.Stage.Identity.LaunchAttempt ||
			start.RunscContainerID != protocol.NomadRunscContainerID(target.ID) || !bytes.Equal(start.RootFSBindingDigest, grant.BindingDigest) {
			return ErrNomadCheckpointConflict
		}
		if _, err := startRuntimeSlotTransition(ctx, tx, target, start); err != nil {
			return err
		}
		result, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, target.ID))
		return err
	})
	return result, err
}

// AuthorizeNomadCheckpointHandover requires an authenticated node receipt for
// the completed restore. The restored procd must retain the captured process
// identity while changing logical sandbox/generation under independent authority.
func (s *PGSandboxStore) AuthorizeNomadCheckpointHandover(ctx context.Context, restored protocol.MigrationRestoreObservation) (*procdapi.RuntimeCheckpointRequest, error) {
	if restored.Validate() != nil || restored.State != protocol.MigrationRestoreComplete || restored.Request.Image.Checkpoint == nil {
		return nil, ErrNomadCheckpointConflict
	}
	a := *restored.Request.Image.Checkpoint
	e, err := s.mutateNomadCheckpointRestore(ctx, a, restored.Request.Image.Target.SlotID, func(tx pgx.Tx, _ time.Time, capture *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if e.Restore == nil || !reflect.DeepEqual(*e.Restore, restored.Request) ||
			(e.Restored != nil && !reflect.DeepEqual(*e.Restored, restored)) {
			return ErrNomadCheckpointConflict
		}
		if err := checkpointRestoreRunning(ctx, tx, target, *e.Restore); err != nil {
			return err
		}
		e.Restored = &restored
		e.Handover = &procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationRestore,
			InstanceID: capture.Preparation.InstanceID, CaptureEpoch: capture.Preparation.CaptureEpoch,
			LifecycleEpoch: a.LifecycleEpoch, Capture: a.Assignment.Capture, Restore: &a.Assignment}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return e.Handover, nil
}

func (s *PGSandboxStore) CommitNomadCheckpointHandover(ctx context.Context, request procdapi.RuntimeCheckpointRequest, receipt procdapi.RuntimeCheckpointResponse) error {
	if request.Action != procdapi.MigrationRestore || request.Restore == nil || receipt.ValidateFor(request) != nil {
		return ErrNomadCheckpointConflict
	}
	a, err := s.getNomadCheckpointRestoreAuthority(ctx, request.Restore.OperationID)
	if err != nil {
		return err
	}
	if a == nil || !reflect.DeepEqual(a.Assignment, *request.Restore) {
		return ErrNomadCheckpointConflict
	}
	var slotID string
	if err := s.pool.QueryRow(ctx, `SELECT evidence->'image'->'target'->>'slot_id' FROM manager.sandbox_runtime_checkpoint_restores WHERE operation_id=$1`, request.Restore.OperationID).Scan(&slotID); err != nil {
		return err
	}
	_, err = s.mutateNomadCheckpointRestore(ctx, *a, slotID, func(tx pgx.Tx, _ time.Time, _ *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if !reflect.DeepEqual(e.Handover, &request) || (e.HandedOver != nil && *e.HandedOver != receipt) {
			return ErrNomadCheckpointConflict
		}
		if err := checkpointRestoreRunning(ctx, tx, target, *e.Restore); err != nil {
			return err
		}
		e.HandedOver = &receipt
		return nil
	})
	return err
}

func checkpointRestoreRunning(ctx context.Context, tx pgx.Tx, target *RuntimeSlot, restore protocol.MigrationRestoreRequest) error {
	grant, err := checkpointRestoreWriter(ctx, tx, target, restore, true)
	if err != nil {
		return err
	}
	if (target.State != RuntimeSlotStateStarting && target.State != RuntimeSlotStateActive) ||
		target.RunscContainerID != protocol.NomadRunscContainerID(target.ID) || target.LaunchAttempt != restore.Stage.Identity.LaunchAttempt ||
		!bytes.Equal(target.RootFSBindingDigest, grant.BindingDigest) {
		return ErrNomadCheckpointConflict
	}
	return nil
}

// Readiness leaves the ordinary resume transaction responsible for publishing
// routing and metering. The node receipt and procd handover are both mandatory.
func (s *PGSandboxStore) markNomadCheckpointCommandReady(ctx context.Context, authority protocol.CheckpointRestoreAuthority, ready *MarkRuntimeSlotCommandReadyRequest) (*RuntimeSlot, error) {
	var result *RuntimeSlot
	_, err := s.mutateNomadCheckpointRestore(ctx, authority, ready.SlotID, func(tx pgx.Tx, _ time.Time, _ *NomadCheckpointEvidence, target *RuntimeSlot, e *NomadCheckpointRestoreEvidence) error {
		if e.HandedOver == nil || e.Restored == nil || ready.MigrationRestoreDigest != e.Restored.RequestDigest {
			return ErrNomadCheckpointConflict
		}
		if err := checkpointRestoreRunning(ctx, tx, target, *e.Restore); err != nil {
			return err
		}
		address, err := protocol.NomadProcdAddress(e.Restore.Stage.ExpectedPolicyToken.SourceIP)
		if err != nil {
			return err
		}
		if !runtimeSlotCallerMatches(target, ready.AllocationID, ready.NodeUID, ready.NodeBootID) ||
			!runtimeSlotClaimIdentityMatches(target, ready.OperationID, ready.ClaimID) ||
			ready.ProcdInstanceID != e.HandedOver.InstanceID || ready.ProcdAddress != address {
			return ErrNomadCheckpointConflict
		}
		if target.State == RuntimeSlotStateActive {
			if target.ProcdInstanceID != ready.ProcdInstanceID || target.ProcdAddress != ready.ProcdAddress || !bytes.Equal(target.CommandReadyDigest, ready.CommandReadyDigest) {
				return ErrNomadCheckpointConflict
			}
		} else if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state='active',revision=revision+1,
			procd_instance_id=$2,procd_address=$3,command_ready_digest=$4,command_ready_at=NOW(),updated_at=NOW() WHERE slot_id=$1`,
			target.ID, ready.ProcdInstanceID, ready.ProcdAddress, ready.CommandReadyDigest); err != nil {
			return err
		}
		result, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, target.ID))
		return err
	})
	return result, err
}
