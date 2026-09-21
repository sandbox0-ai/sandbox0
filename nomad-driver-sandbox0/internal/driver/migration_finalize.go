package driver

import (
	"context"
	"errors"
	"os"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// observeMigrationFinalization allows Nomad to reap a source only after ctld
// has completed the entire physical cleanup. The caller serializes Stop/Close
// using closeMu, or invokes this before publishing a recovered handle.
func (h *taskHandle) observeMigrationFinalization() (bool, error) {
	h.mu.Lock()
	if h.migrationFinalized {
		h.mu.Unlock()
		return true, nil
	}
	slot := h.taskConfig.ID
	inFlight := h.migrationInFlight
	h.mu.Unlock()
	if inFlight {
		return false, errdefs.ErrUnavailable
	}
	if complete, err := h.observeFailedCaptureFinalization(); complete || err != nil {
		return complete, err
	}
	reader, ok := h.rootfs.(nomadruntime.MigrationSourceFinalizationReader)
	if !ok {
		return h.observeFailedMigrationFinalization()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	receipt, err := reader.GetMigrationSourceFinalization(ctx, slot)
	cancel()
	if err != nil {
		return false, err
	}
	if receipt == nil {
		return h.observeFailedMigrationFinalization()
	}
	if receipt.Validate() != nil {
		return false, errdefs.ErrFailedPrecondition
	}
	capture := receipt.Request.Fence.PublicationRequest.Capture
	h.mu.Lock()
	if h.validateMigrationFinalizationLocked(*receipt) != nil || h.migrationInFlight {
		h.mu.Unlock()
		return false, errdefs.ErrFailedPrecondition
	}
	// Retain capture history so neither a stale claim nor checkpoint retry can
	// revive this one-shot carrier. A restarted driver rechecks ctld's journal.
	capture.RootFS = nil
	h.migration = &capture
	return h.finishMigrationFinalizationLocked(&drivers.ExitResult{})
}

// The caller owns closeMu (or an unpublished recovery handle) and h.mu.
// Source and failed-target cleanup share the terminal handle transition, while
// retaining their different immutable execution histories.
func (h *taskHandle) finishMigrationFinalizationLocked(exit *drivers.ExitResult) (bool, error) {
	h.migrationFinalized = true
	h.migrationAdmissionFenced = true
	h.rootMounted = false
	h.phase = phaseExited
	h.exitResult = exit
	if h.completedAt.IsZero() {
		h.completedAt = time.Now()
	}
	closeDoneLocked(h.done)
	h.mu.Unlock()
	h.stopExitWatch()
	h.stopConsumerRenewal()
	err := h.persist()
	if errors.Is(err, os.ErrNotExist) {
		// Close may have removed the bundle before Nomad persisted GC. Do not
		// recreate it solely to cache a receipt owned by ctld. Every recovery
		// must read that receipt again; other persistence failures still fail.
		if _, statErr := os.Stat(h.bundleDir); errors.Is(statErr, os.ErrNotExist) {
			return true, nil
		}
	}
	return true, err
}

// A complete ctld receipt permits terminal recovery from Nomad's original warm
// handle even after the bundle (and its later claim metadata) is gone. This
// never grants execution or storage authority. Any surviving claim must still
// match in full, so missing metadata cannot hide a conflicting source identity.
func (h *taskHandle) validateMigrationFinalizationLocked(receipt protocol.MigrationSourceFinalizationReceipt) error {
	capture := receipt.Request.Fence.PublicationRequest.Capture
	target := capture.Request.Target
	if h.taskConfig == nil || h.taskConfig.ID != target.SlotID ||
		h.taskConfig.AllocID != target.AllocationID || h.taskConfig.NodeID != target.NodeID ||
		target.ControlEndpoint != "unix://"+h.socketPath || receipt.Request.Cleanup.RunscContainerID != h.containerID ||
		(h.migration != nil && (h.migration.Validate() != nil || h.migration.RequestDigest != capture.RequestDigest)) {
		return errdefs.ErrFailedPrecondition
	}
	if h.claim == nil && h.stage == nil && !h.rootMounted {
		return nil
	}
	if h.validateMigrationSourceIdentityLocked(capture.Request) != nil ||
		receipt.Request.Cleanup.WriterGrantID != h.stage.Identity.WriterGrantID {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}
