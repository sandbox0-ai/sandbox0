package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// AbortRuntimeSlotRegistration serializes against every registration, including
// old binaries through the database trigger. Existing slots retain their writer
// and resource authority; only an absent slot can receive a grantless fence.
func (s *PGSandboxStore) AbortRuntimeSlotRegistration(ctx context.Context, request protocol.RegistrationAbortRequest) (protocol.RegistrationAbortResponse, error) {
	if err := request.Validate(); err != nil {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("%w: %v", ErrRuntimeSlotInvalid, err)
	}
	cleanup, err := request.CleanupRequest()
	if err != nil {
		return protocol.RegistrationAbortResponse{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return protocol.RegistrationAbortResponse{}, err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('runtime-slot-registration:' || $1, 0))`, request.SlotID); err != nil {
		return protocol.RegistrationAbortResponse{}, err
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id = $1`, request.SlotID))
	if err == nil {
		if slot.ClusterID != request.ClusterID || slot.NodeID != request.NodeID ||
			slot.NodeUID != request.NodeUID || slot.NodeBootID != request.NodeBootID ||
			slot.AllocationID != request.AllocationID || slot.NetNSIdentity != request.NetNSIdentity || request.Proof != nil {
			return protocol.RegistrationAbortResponse{}, fmt.Errorf("%w: registration abort conflicts with a registered incarnation", ErrRuntimeSlotConflict)
		}
		return protocol.RegistrationAbortResponse{Registered: true}, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return protocol.RegistrationAbortResponse{}, err
	}
	payload, err := json.Marshal(cleanup)
	if err != nil {
		return protocol.RegistrationAbortResponse{}, err
	}
	if request.Proof == nil {
		if _, err := tx.Exec(ctx, `INSERT INTO manager.runtime_slot_registration_aborts (slot_id, cleanup_request)
			VALUES ($1, $2) ON CONFLICT (slot_id) DO NOTHING`, request.SlotID, payload); err != nil {
			return protocol.RegistrationAbortResponse{}, mapRuntimeSlotConflict("fence absent registration", err)
		}
	}
	var storedRequest, storedProof []byte
	if err := tx.QueryRow(ctx, `SELECT cleanup_request, cleanup_proof FROM manager.runtime_slot_registration_aborts WHERE slot_id = $1 FOR UPDATE`, request.SlotID).Scan(&storedRequest, &storedProof); err != nil {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("%w: durable registration fence is unavailable", ErrRuntimeSlotConflict)
	}
	var stored protocol.NodeCleanupControlRequest
	if err := json.Unmarshal(storedRequest, &stored); err != nil || stored != cleanup {
		return protocol.RegistrationAbortResponse{}, fmt.Errorf("%w: registration cleanup binding changed", ErrRuntimeSlotConflict)
	}
	if len(storedProof) > 0 {
		var proof protocol.NodeCleanupControlProof
		if err := json.Unmarshal(storedProof, &proof); err != nil || proof.Validate() != nil || proof.Request() != cleanup || (request.Proof != nil && proof != *request.Proof) {
			return protocol.RegistrationAbortResponse{}, fmt.Errorf("%w: registration cleanup proof changed", ErrRuntimeSlotConflict)
		}
	} else if request.Proof != nil {
		storedProof, err = json.Marshal(request.Proof)
		if err != nil {
			return protocol.RegistrationAbortResponse{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slot_registration_aborts SET cleanup_proof = $2, completed_at = NOW() WHERE slot_id = $1`, request.SlotID, storedProof); err != nil {
			return protocol.RegistrationAbortResponse{}, err
		}
	}
	response := protocol.RegistrationAbortResponse{Cleanup: &cleanup, Completed: len(storedProof) > 0}
	return response, tx.Commit(ctx)
}
