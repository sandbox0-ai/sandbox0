package driver

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
)

// Failed source capture never becomes successful handoff history. Only ctld's
// completed physical and artifact cleanup permits this carrier to exit.
func (h *taskHandle) observeFailedCaptureFinalization() (bool, error) {
	reader, ok := h.rootfs.(nomadruntime.MigrationCustodian)
	if !ok {
		return false, nil
	}
	h.mu.Lock()
	slot := h.taskConfig.ID
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	custody, err := reader.GetMigrationCapture(ctx, slot)
	cancel()
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	if err != nil || custody == nil || custody.Failure == nil {
		return false, err
	}
	if !custody.CaptureFailureFinalized() {
		return false, errdefs.ErrUnavailable
	}
	f := custody.Failure.Finalization
	want, err := f.Request.Digest()
	cleanupDigest, cleanupErr := custody.Failure.Request.Digest()
	finalCleanupDigest, finalCleanupErr := f.Request.Request.Digest()
	binding, bindingErr := custody.Failure.Stage.BindingDigest()
	capture := f.Request.Request.Capture
	target := capture.Request.Target
	if err != nil || want != f.RequestDigest || cleanupErr != nil || finalCleanupErr != nil || cleanupDigest != finalCleanupDigest || cleanupDigest != custody.Failure.RequestDigest ||
		bindingErr != nil || hex.EncodeToString(binding[:]) != capture.Request.BindingDigest || custody.Failure.Stage.Identity.WriterGrantToken != "" ||
		custody.Failure.Stage.Identity.WriterGrantID != f.Request.Request.Cleanup.WriterGrantID ||
		f.Proof.ValidateFor(f.Request) != nil || !custody.ExecutionInvalidated || custody.Capture.Validate() != nil ||
		custody.Capture.Request != capture.Request || custody.Capture.RootFS != nil || custody.PublicationRequest != nil || custody.Publication != nil || custody.SourceFenceRequest != nil || custody.SourceFenceProof != nil || custody.Finalization != nil {
		return false, errdefs.ErrFailedPrecondition
	}
	h.mu.Lock()
	if h.migrationInFlight || h.taskConfig.ID != target.SlotID || h.taskConfig.AllocID != target.AllocationID ||
		h.taskConfig.NodeID != target.NodeID || target.ControlEndpoint != "unix://"+h.socketPath ||
		f.Request.Request.Cleanup.RunscContainerID != h.containerID ||
		h.migration != nil && (h.migration.Validate() != nil || h.migration.RequestDigest != capture.RequestDigest) {
		h.mu.Unlock()
		return false, errdefs.ErrFailedPrecondition
	}
	if h.claim != nil || h.stage != nil || h.rootMounted {
		if h.validateMigrationSourceIdentityLocked(capture.Request) != nil || h.stage.Identity.WriterGrantID != f.Request.Request.Cleanup.WriterGrantID {
			h.mu.Unlock()
			return false, errdefs.ErrFailedPrecondition
		}
	}
	h.migration = &capture
	return h.finishMigrationFinalizationLocked(&drivers.ExitResult{ExitCode: 1})
}
