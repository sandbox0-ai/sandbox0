package driver

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// Re-measure at execution boundaries. Regional preflight is admission evidence,
// not permission to reuse an earlier observation after journal or image I/O.
func (h *taskHandle) checkMigrationCaptureCPU(ctx context.Context, request protocol.MigrationCaptureRequest, capturing bool) error {
	observer, ok := h.runner.(gvisorcli.CPUPreflightRunsc)
	if !ok {
		return fmt.Errorf("checkpoint CPU observer unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	h.mu.Lock()
	err := h.validateMigrationCaptureCPULocked(request, capturing)
	var launch protocol.MigrationCPULaunch
	if err == nil {
		launch = *h.claim.MigrationCPULaunch.Clone()
	}
	h.mu.Unlock()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, protocol.MigrationCPUPreflightTimeout)
	defer cancel()
	if _, err := gvisorcli.CheckMigrationSourceCPU(ctx, observer, &launch, request, launch.LaunchAttempt, launch.Resources); err != nil {
		return fmt.Errorf("checkpoint CPU validation: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if err := h.validateMigrationCaptureCPULocked(request, capturing); err != nil {
		return err
	}
	before, _ := launch.Digest()
	after, _ := h.claim.MigrationCPULaunch.Digest()
	if before != after {
		return errdefs.ErrFailedPrecondition
	}
	return ctx.Err()
}

func (h *taskHandle) validateMigrationCaptureCPULocked(request protocol.MigrationCaptureRequest, capturing bool) error {
	if err := h.validateMigrationSourceLocked(request); err != nil {
		return err
	}
	if h.closed || h.migrationAdmissionFenced || h.migrationFinalized || h.hasMigrationRestoreLocked() ||
		h.claim.MigrationCPULaunch == nil || h.claim.MigrationCPULaunch.LaunchAttempt != h.claim.LaunchAttempt ||
		h.claim.LaunchAttempt != h.stage.Identity.LaunchAttempt {
		return errdefs.ErrFailedPrecondition
	}
	if err := h.validateMigrationCPULineageLocked(); err != nil {
		return err
	}
	if capturing {
		if h.phase != phaseMigrating || h.migration == nil || h.migration.State != protocol.MigrationCaptureIntent || h.migration.Request != request {
			return errdefs.ErrFailedPrecondition
		}
	} else if h.phase != phaseActive || h.migration != nil {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

// A restored claim cannot drop its lineage or borrow an ordinary launch record.
// Losing durable history after a driver crash never permits a fresh backfill.
func (h *taskHandle) validateMigrationCPULineageLocked() error {
	if h.claim == nil || h.claim.MigrationCPULaunch == nil {
		return errdefs.ErrFailedPrecondition
	}
	if h.claim.MigrationRestore != nil {
		if err := h.claim.MigrationCPULaunch.ValidateRestore(*h.claim.MigrationRestore); err != nil {
			return fmt.Errorf("source CPU restore lineage: %w: %w", err, errdefs.ErrFailedPrecondition)
		}
	} else if h.claim.MigrationCPULaunch.Restored != nil {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

func (h *taskHandle) checkMigrationRestoreCPU(ctx context.Context, request protocol.MigrationRestoreRequest) error {
	_, err := h.observeMigrationRestoreCPU(ctx, request)
	return err
}

func (h *taskHandle) observeMigrationRestoreCPU(ctx context.Context, request protocol.MigrationRestoreRequest) (*protocol.MigrationCPUObservation, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	launch := request.Image.Publication.CPULaunch
	if launch == nil {
		return nil, fmt.Errorf("restore lacks source CPU launch history: %w", errdefs.ErrFailedPrecondition)
	}
	observer, ok := h.runner.(gvisorcli.CPUPreflightRunsc)
	if !ok {
		return nil, fmt.Errorf("restore CPU observer unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	ctx, cancel := context.WithTimeout(ctx, protocol.MigrationCPUPreflightTimeout)
	defer cancel()
	observation, err := gvisorcli.CheckMigrationTargetCPU(ctx, observer, launch, request.Image.Resources)
	if err != nil {
		return nil, fmt.Errorf("restore CPU validation: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	return observation, ctx.Err()
}
