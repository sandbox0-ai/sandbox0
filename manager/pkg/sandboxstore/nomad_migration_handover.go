package sandboxstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AuthorizeNomadSandboxMigrationHandover consumes an authenticated target-node
// restore receipt. The persisted procd command is bound to the original process
// instance: a replacement entrypoint cannot satisfy the generation handover.
func (s *PGSandboxStore) AuthorizeNomadSandboxMigrationHandover(ctx context.Context, restored protocol.MigrationRestoreObservation) (*procdapi.RuntimeMigrationRequest, error) {
	if restored.Validate() != nil || restored.State != protocol.MigrationRestoreComplete {
		return nil, ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	assignment := restored.Request.Image.Publication.Assignment
	reservation, err := lockNomadMigrationAuthorityAtGeneration(ctx, tx, assignment, true)
	if err != nil {
		return nil, err
	}
	var restorePayload, prior []byte
	var restoreDigest, priorDigest *string
	if err := tx.QueryRow(ctx, `SELECT restore_request,restore_digest,procd_handover_request,procd_handover_digest
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, assignment.OperationID).Scan(&restorePayload, &restoreDigest, &prior, &priorDigest); err != nil {
		return nil, err
	}
	var authorized protocol.MigrationRestoreRequest
	if restoreDigest == nil || *restoreDigest != restored.RequestDigest || json.Unmarshal(restorePayload, &authorized) != nil {
		return nil, ErrNomadSandboxMigrationConflict
	}
	actual, err := authorized.Digest()
	if err != nil || actual != restored.RequestDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	request := procdapi.RuntimeMigrationRequest{Action: procdapi.MigrationRestore, Assignment: assignment,
		InstanceID: reservation.SourceProcdInstanceID, LifecycleEpoch: reservation.Lifecycle.Epoch}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if len(prior) != 0 {
		var stored procdapi.RuntimeMigrationRequest
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
	if _, err := lockNomadMigrationHandoverTarget(ctx, tx, reservation, authorized); err != nil {
		return nil, err
	}
	receipt, err := json.Marshal(restored)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET restore_receipt=$2,
        procd_handover_request=$3,procd_handover_digest=$4 WHERE operation_id=$1`, assignment.OperationID, receipt, payload, want); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &request, nil
}

// CommitNomadSandboxMigrationHandover records an authenticated procd response.
// Public routing stays on the source until the target's ordinary authenticated
// command probe is acknowledged through the migration-aware readiness gate.
func (s *PGSandboxStore) CommitNomadSandboxMigrationHandover(ctx context.Context, request procdapi.RuntimeMigrationRequest, receipt procdapi.RuntimeMigrationResponse) error {
	if request.Action != procdapi.MigrationRestore || receipt.ValidateFor(request) != nil {
		return ErrNomadSandboxMigrationConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	reservation, err := lockNomadMigrationAuthorityAtGeneration(ctx, tx, request.Assignment, true)
	if err != nil {
		return err
	}
	restored, stored, prior, err := loadNomadMigrationHandover(ctx, tx, request.Assignment.OperationID)
	if err != nil {
		return err
	}
	actual, _ := stored.Digest()
	want, _ := request.Digest()
	if actual != want || request.InstanceID != reservation.SourceProcdInstanceID || request.LifecycleEpoch != reservation.Lifecycle.Epoch {
		return ErrNomadSandboxMigrationConflict
	}
	if prior != nil {
		if *prior != receipt {
			return ErrNomadSandboxMigrationConflict
		}
		return tx.Commit(ctx)
	}
	if _, err := lockNomadMigrationHandoverTarget(ctx, tx, reservation, restored.Request); err != nil {
		return err
	}
	payload, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET procd_handover_receipt=$2 WHERE operation_id=$1`, request.Assignment.OperationID, payload); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func loadNomadMigrationHandover(ctx context.Context, tx pgx.Tx, operation string) (protocol.MigrationRestoreObservation, procdapi.RuntimeMigrationRequest, *procdapi.RuntimeMigrationResponse, error) {
	var restorePayload, requestPayload, receiptPayload []byte
	var digest *string
	var restored protocol.MigrationRestoreObservation
	var request procdapi.RuntimeMigrationRequest
	var receipt *procdapi.RuntimeMigrationResponse
	err := tx.QueryRow(ctx, `SELECT restore_receipt,procd_handover_request,procd_handover_digest,procd_handover_receipt
        FROM manager.sandbox_runtime_migrations WHERE operation_id=$1`, operation).Scan(&restorePayload, &requestPayload, &digest, &receiptPayload)
	if errors.Is(err, pgx.ErrNoRows) {
		err = ErrNomadSandboxMigrationConflict
	}
	if err != nil {
		return restored, request, nil, err
	}
	if json.Unmarshal(restorePayload, &restored) != nil || restored.Validate() != nil || restored.State != protocol.MigrationRestoreComplete ||
		json.Unmarshal(requestPayload, &request) != nil || digest == nil || request.Action != procdapi.MigrationRestore {
		return restored, request, nil, ErrNomadSandboxMigrationConflict
	}
	actual, err := request.Digest()
	assignment, _ := request.Assignment.Digest()
	expected, _ := restored.Request.Image.Publication.Assignment.Digest()
	if err != nil || actual != *digest || assignment != expected {
		return restored, request, nil, ErrNomadSandboxMigrationConflict
	}
	if len(receiptPayload) != 0 {
		if json.Unmarshal(receiptPayload, &receipt) != nil || receipt == nil || receipt.ValidateFor(request) != nil {
			return restored, request, nil, ErrNomadSandboxMigrationConflict
		}
	}
	return restored, request, receipt, nil
}

func lockNomadMigrationHandoverTarget(ctx context.Context, tx pgx.Tx, reservation *NomadSandboxMigrationReservation, restore protocol.MigrationRestoreRequest) (*RuntimeSlot, error) {
	if _, err := validateNomadMigrationDestinationHandoff(ctx, tx, reservation); err != nil {
		return nil, err
	}
	target, err := lockRuntimeSlotByID(ctx, tx, reservation.TargetSlot.ID)
	if err != nil {
		return nil, err
	}
	grant, err := validateNomadMigrationRestoreWriter(ctx, tx, reservation, target, restore)
	if err != nil {
		return nil, err
	}
	if (target.State != RuntimeSlotStateStarting && target.State != RuntimeSlotStateActive) ||
		grant.State != RootFSWriterGrantStateConsumed || !grant.LeaseExpiresAt.After(grant.databaseNow) ||
		!target.HeartbeatExpiresAt.After(target.AuthorityObservedAt) || runtimeSlotPreCommandReadyClaimExpired(target) ||
		target.RunscContainerID != protocol.NomadRunscContainerID(target.ID) || target.LaunchAttempt != restore.Stage.Identity.LaunchAttempt ||
		!bytes.Equal(target.RootFSBindingDigest, grant.BindingDigest) {
		return nil, ErrNomadSandboxMigrationConflict
	}
	return target, nil
}

// markNomadMigrationCommandReady atomically publishes the restored allocation
// and generation. It preserves TTL, metering and both physical resource leases;
// the migration stays committing until explicit custody cleanup finishes.
func (s *PGSandboxStore) markNomadMigrationCommandReady(ctx context.Context, ready *MarkRuntimeSlotCommandReadyRequest) (*RuntimeSlot, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	restored, request, receipt, err := loadNomadMigrationHandover(ctx, tx, ready.OperationID)
	if err != nil {
		return nil, err
	}
	if receipt == nil || restored.RequestDigest != ready.MigrationRestoreDigest {
		return nil, ErrNomadSandboxMigrationConflict
	}
	reservation, err := lockNomadMigrationAuthorityAtGeneration(ctx, tx, request.Assignment, true)
	if err != nil {
		return nil, err
	}
	target, err := lockNomadMigrationHandoverTarget(ctx, tx, reservation, restored.Request)
	if err != nil {
		return nil, err
	}
	address, err := protocol.NomadProcdAddress(restored.Request.Stage.ExpectedPolicyToken.SourceIP)
	if err != nil {
		return nil, err
	}
	if target.ID != ready.SlotID || !runtimeSlotCallerMatches(target, ready.AllocationID, ready.NodeUID, ready.NodeBootID) ||
		!runtimeSlotClaimIdentityMatches(target, ready.OperationID, ready.ClaimID) ||
		ready.ProcdInstanceID != reservation.SourceProcdInstanceID || ready.ProcdInstanceID != receipt.InstanceID ||
		ready.ProcdAddress != address {
		return nil, ErrNomadSandboxMigrationConflict
	}
	if target.State == RuntimeSlotStateActive {
		if target.ProcdInstanceID != ready.ProcdInstanceID || target.ProcdAddress != ready.ProcdAddress || !bytes.Equal(target.CommandReadyDigest, ready.CommandReadyDigest) {
			return nil, ErrNomadSandboxMigrationConflict
		}
		var committed bool
		if err := tx.QueryRow(ctx, `SELECT migration.generation_committed_at IS NOT NULL AND sandbox.runtime_generation=$2
            AND sandbox.runtime_id=$3 AND sandbox.runtime_namespace=$4
            FROM manager.sandbox_runtime_migrations migration
            JOIN manager.sandbox_lifecycle_txns lifecycle ON lifecycle.txn_id=migration.operation_id
            JOIN manager.sandboxes sandbox ON sandbox.sandbox_id=lifecycle.sandbox_id
            WHERE migration.operation_id=$1`, ready.OperationID, request.Assignment.Target.RuntimeGeneration,
			target.AllocationID, target.AllocationNamespace).Scan(&committed); err != nil {
			return nil, err
		}
		if !committed {
			return nil, ErrNomadSandboxMigrationConflict
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET state='active',revision=revision+1,
            procd_instance_id=$2,procd_address=$3,command_ready_digest=$4,command_ready_at=NOW(),updated_at=NOW() WHERE slot_id=$1`,
			target.ID, ready.ProcdInstanceID, ready.ProcdAddress, ready.CommandReadyDigest); err != nil {
			return nil, err
		}
		tag, err := tx.Exec(ctx, `UPDATE manager.sandboxes SET runtime_id=$2,runtime_namespace=$3,runtime_generation=$4,updated_at=NOW()
            WHERE sandbox_id=$1 AND runtime_generation=$5 AND runtime_id=$6 AND runtime_namespace=$7`, request.Assignment.Target.SandboxID,
			target.AllocationID, target.AllocationNamespace, request.Assignment.Target.RuntimeGeneration, request.Assignment.SourceGeneration,
			reservation.Lifecycle.FromRuntimeID, reservation.Lifecycle.FromRuntimeNamespace)
		if err != nil {
			return nil, err
		}
		if tag.RowsAffected() != 1 {
			return nil, ErrNomadSandboxMigrationConflict
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_migrations SET generation_committed_at=NOW() WHERE operation_id=$1 AND generation_committed_at IS NULL`, ready.OperationID); err != nil {
			return nil, err
		}
	}
	result, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id=$1`, target.ID))
	if err != nil {
		return nil, err
	}
	result.MigrationAdoption, err = persistNomadMigrationAdoption(ctx, tx, restored, ready)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}
