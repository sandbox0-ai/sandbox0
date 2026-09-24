package driver

import (
	"context"
	"fmt"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// PreflightMigrationCPU observes an unchanged live source or unclaimed warm
// destination. It reads source history only from this driver's durable claim.
// No request can fill missing history, claim capacity or advance lifecycle.
func (h *taskHandle) PreflightMigrationCPU(ctx context.Context, request protocol.MigrationCPUPreflightRequest) (*protocol.MigrationCPUPreflight, error) {
	if ctx == nil {
		return nil, errdefs.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	want, err := request.Digest()
	if err != nil {
		return nil, fmt.Errorf("CPU preflight: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	observer, ok := h.runner.(gvisorcli.CPUPreflightRunsc)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	// A read-only probe must not queue indefinitely behind capture/stop or
	// prevent their later progress. The authenticated caller may retry.
	if !h.closeMu.TryLock() {
		return nil, errdefs.ErrUnavailable
	}
	defer h.closeMu.Unlock()
	// Regional command readiness may have committed adoption after the
	// restored driver's reply was lost. Import that exact ctld receipt before
	// deciding whether this runtime can become a source again.
	if err := h.refreshMigrationAdoption(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	err = h.validateMigrationCPUPreflightLocked(request)
	var launch *protocol.MigrationCPULaunch
	if err == nil {
		if request.IsSource() {
			launch = h.claim.MigrationCPULaunch
		} else {
			launch = request.Launch
		}
		launch = launch.Clone()
	}
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if launch == nil {
		return nil, fmt.Errorf("CPU launch history unavailable: %w", errdefs.ErrFailedPrecondition)
	}
	ctx, cancel := context.WithTimeout(ctx, protocol.MigrationCPUPreflightTimeout)
	defer cancel()
	var current *protocol.MigrationCPUObservation
	if request.IsSource() {
		current, err = gvisorcli.CheckMigrationSourceCPU(ctx, observer, launch, request.Source, launch.LaunchAttempt, request.SourceResources)
	} else if request.Planning {
		current, err = gvisorcli.CheckMigrationPlanningTargetCPU(ctx, observer, launch, request.PlanningCPUSet)
	} else {
		current, err = gvisorcli.CheckMigrationTargetCPU(ctx, observer, launch, request.DestinationResources)
	}
	if err != nil {
		return nil, fmt.Errorf("CPU preflight observation failed: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	h.mu.Lock()
	err = h.validateMigrationCPUPreflightLocked(request)
	if err == nil && request.IsSource() {
		before, _ := launch.Digest()
		after := ""
		if h.claim.MigrationCPULaunch != nil {
			after, _ = h.claim.MigrationCPULaunch.Digest()
		}
		if before != after {
			err = errdefs.ErrFailedPrecondition
		}
	}
	h.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	result := &protocol.MigrationCPUPreflight{RequestDigest: want, Launch: *launch, Observation: *current}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	return result, nil
}

// Caller holds mu and closeMu. ctld authenticates the node UID; registration
// and the source Stage bind the boot, carrier and immutable local endpoint.
func (h *taskHandle) validateMigrationCPUPreflightLocked(request protocol.MigrationCPUPreflightRequest) error {
	if h.closed || h.migrationAdmissionFenced || h.migration != nil || h.migrationFinalized || h.hasMigrationRestoreLocked() || h.runtimeSlot == nil || h.taskConfig == nil {
		return errdefs.ErrFailedPrecondition
	}
	target, registration := request.Target, h.runtimeSlot.registration
	if target.SlotID != h.taskConfig.ID || target.AllocationID != h.taskConfig.AllocID || target.NodeID != h.taskConfig.NodeID ||
		target.ClusterID != registration.ClusterID || target.NodeBootID != registration.NodeBootID ||
		target.ControlEndpoint != registration.ControlEndpoint || target.ControlEndpoint != "unix://"+h.socketPath {
		return errdefs.ErrFailedPrecondition
	}
	if request.IsSource() {
		if h.phase != phaseActive {
			return errdefs.ErrFailedPrecondition
		}
		if err := h.validateMigrationSourceLocked(request.Source); err != nil {
			return err
		}
		if h.claim.MigrationCPULaunch == nil || h.claim.MigrationCPULaunch.LaunchAttempt != h.claim.LaunchAttempt || h.claim.LaunchAttempt != h.stage.Identity.LaunchAttempt {
			return errdefs.ErrFailedPrecondition
		}
		if err := h.validateMigrationCPULineageLocked(); err != nil {
			return err
		}
	} else if h.phase != phaseWarm || h.claim != nil || h.stage != nil || h.rootMounted {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}
