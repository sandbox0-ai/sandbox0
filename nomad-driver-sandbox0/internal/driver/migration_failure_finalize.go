package driver

import (
	"context"
	"time"

	"github.com/containerd/errdefs"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
)

// observeFailedMigrationFinalization exposes only an exited handle after ctld
// has retired the exact destination's physical and artifact custody. Nomad may
// then remove the one-shot carrier without invoking another writer retirement.
func (h *taskHandle) observeFailedMigrationFinalization() (bool, error) {
	reader, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian)
	if !ok {
		return false, nil
	}
	h.mu.Lock()
	slot := h.taskConfig.ID
	h.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	custody, err := reader.GetMigrationDestination(ctx, slot)
	cancel()
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	if err != nil || custody == nil || !custody.FailureFinalized() {
		return false, err
	}
	f := custody.Failure.Cleanup.Finalization
	want, err := f.Request.Digest()
	if err != nil || want != f.RequestDigest || f.Proof.ValidateFor(f.Request) != nil || custody.Adoption != nil {
		return false, errdefs.ErrFailedPrecondition
	}
	r := f.Request.Request.Failure.Request.Restore
	target := r.Image.Target
	image, err := r.Image.Digest()
	if err != nil || image != custody.RequestDigest {
		return false, errdefs.ErrFailedPrecondition
	}
	h.mu.Lock()
	if h.migrationInFlight || h.migration != nil || h.taskConfig.ID != target.SlotID || h.taskConfig.AllocID != target.AllocationID ||
		h.taskConfig.NodeID != target.NodeID || target.ControlEndpoint != "unix://"+h.socketPath ||
		f.Request.Request.Cleanup.RunscContainerID != h.containerID || h.rootMounted && h.stage == nil {
		h.mu.Unlock()
		return false, errdefs.ErrFailedPrecondition
	}
	if h.stage != nil {
		actual, err := h.stage.BindingDigest()
		binding, bindErr := r.Stage.BindingDigest()
		if err != nil || bindErr != nil || actual != binding {
			h.mu.Unlock()
			return false, errdefs.ErrFailedPrecondition
		}
	}
	if h.claim != nil && h.claim.MigrationRestore != nil {
		actual, err := h.claim.MigrationRestore.Digest()
		want, wantErr := r.Digest()
		if err != nil || wantErr != nil || actual != want {
			h.mu.Unlock()
			return false, errdefs.ErrFailedPrecondition
		}
	}
	if h.claim == nil {
		h.claim = &claimMetadata{}
	}
	h.stage = &r.Stage
	h.claim.Stage = &r.Stage
	h.claim.MigrationRestore = &r
	return h.finishMigrationFinalizationLocked(&drivers.ExitResult{ExitCode: 1})
}
