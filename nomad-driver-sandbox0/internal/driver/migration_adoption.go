package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// refreshMigrationAdoption imports completed ctld custody after the regional
// recovery lane delivered a lost command. It only reconciles historical proof;
// the existing adopter preserves phase and lease-loss fencing and never starts
// or restores a process. Incomplete custody cannot unlock ordinary controls.
func (h *taskHandle) refreshMigrationAdoption() error {
	h.mu.Lock()
	if h.closed || h.migration != nil || !h.hasMigrationRestoreLocked() {
		h.mu.Unlock()
		return nil
	}
	restore := *h.claim.MigrationRestore
	h.mu.Unlock()
	custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian)
	if !ok {
		return errdefs.ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeSlotCommandTimeout)
	custody, err := custodian.GetMigrationDestination(ctx, restore.Image.Target.SlotID)
	cancel()
	if err != nil {
		return err
	}
	if custody == nil || custody.Adoption == nil || custody.Adoption.Proof == nil {
		return nil
	}
	digest, err := restore.Digest()
	if err != nil {
		return err
	}
	request := custody.Adoption.Request
	observed := protocol.MigrationRestoreObservation{Request: restore, RequestDigest: digest, State: protocol.MigrationRestoreComplete}
	if request.ValidateFor(observed) != nil || custody.Restore == nil || custody.Restore.Validate() != nil || custody.Restore.RequestDigest != digest || custody.Restore.State != protocol.MigrationRestoreComplete || custody.Adoption.Proof.ValidateFor(request) != nil {
		return errdefs.ErrFailedPrecondition
	}
	h.mu.Lock()
	if h.closed || h.migration != nil || h.claim == nil || h.claim.MigrationRestore == nil {
		h.mu.Unlock()
		return errdefs.ErrFailedPrecondition
	}
	current, currentErr := h.claim.MigrationRestore.Digest()
	if currentErr != nil || current != digest || h.claim.ProcdInstanceID != "" && h.claim.ProcdInstanceID != request.ProcdInstanceID || h.claim.CommandReadyDigest != "" && h.claim.CommandReadyDigest != request.CommandReadyDigest {
		h.mu.Unlock()
		return errdefs.ErrFailedPrecondition
	}
	h.claim.ProcdInstanceID = request.ProcdInstanceID
	h.claim.CommandReadyDigest = request.CommandReadyDigest
	h.mu.Unlock()
	return h.adoptMigrationAfterReady(&request, restore, request.CommandReadyDigest)
}

func (h *taskHandle) adoptMigrationAfterReady(request *protocol.MigrationAdoptionRequest, restore protocol.MigrationRestoreRequest, readyDigest string) error {
	digest, err := restore.Digest()
	if err != nil {
		return err
	}
	observation := protocol.MigrationRestoreObservation{Request: restore, RequestDigest: digest, State: protocol.MigrationRestoreComplete}
	if request == nil || request.ValidateFor(observation) != nil || request.CommandReadyDigest != readyDigest {
		return fmt.Errorf("regional readiness lacks exact migration adoption authority: %w", errdefs.ErrFailedPrecondition)
	}
	adopter, ok := h.rootfs.(nomadruntime.MigrationAdopter)
	if !ok {
		return errdefs.ErrUnavailable
	}
	ctx, cancel := context.WithTimeout(context.Background(), runtimeSlotCommandTimeout)
	proof, err := adopter.AdoptMigrationDestination(ctx, *request)
	cancel()
	if err != nil {
		return err
	}
	if proof == nil || proof.ValidateFor(*request) != nil {
		return errdefs.ErrFailedPrecondition
	}
	// Never hold h.mu over the RPC: lease-loss fencing must remain responsive.
	// Ctld's committed receipt remains true even if that fence won the race.
	h.mu.Lock()
	if h.claim == nil || h.claim.MigrationRestore == nil {
		h.mu.Unlock()
		return errdefs.ErrFailedPrecondition
	}
	current, err := h.claim.MigrationRestore.Digest()
	if err != nil || current != digest {
		h.mu.Unlock()
		return errdefs.ErrFailedPrecondition
	}
	h.claim.MigrationAdoption = &protocol.MigrationAdoptionReceipt{Request: *request, Proof: *proof}
	fenced := h.migrationAdmissionFenced || h.phase != phaseActive
	h.mu.Unlock()
	persistErr := h.persist()
	if fenced || persistErr != nil {
		cause := fmt.Errorf("migration adopted after local authority loss: %w", errdefs.ErrFailedPrecondition)
		h.poisonLostWriter(cause)
		if persistErr != nil {
			return persistErr
		}
		return cause
	}
	return nil
}

// awaitMigrationAdoptionForExit retains one already-observed exit while ctld
// recovery finishes delivery of its adoption receipt. Dropping the exit here
// would leave a restored carrier active forever when no control request follows.
// It never takes closeMu: Stop holds that lock while waiting for h.done. Source
// checkpoint exits and uncertain destination custody remain region-owned.
func (h *taskHandle) awaitMigrationAdoptionForExit(ctx context.Context) bool {
	for {
		if ctx.Err() != nil {
			return false
		}
		h.mu.Lock()
		terminal := h.closed || h.phase == phaseExited || h.phase == phasePoisoned || h.migration != nil
		pending := h.hasMigrationRestoreLocked()
		h.mu.Unlock()
		if terminal {
			return false
		}
		if !pending {
			return true
		}
		// Both absent custody and transient RPC errors keep the exit pending.
		// Exact completed proof is the only way to unlock ordinary exit state.
		if err := h.refreshMigrationAdoption(); err == nil {
			h.mu.Lock()
			pending = h.hasMigrationRestoreLocked()
			h.mu.Unlock()
			if !pending {
				continue
			}
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
}
