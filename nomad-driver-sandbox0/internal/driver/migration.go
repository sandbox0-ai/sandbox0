package driver

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

func cloneMigrationCapture(value *protocol.MigrationCapture) *protocol.MigrationCapture {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// CaptureMigration is reachable only on the root-owned local control socket.
// Both journals precede runsc checkpoint. A retry observes the same operation;
// an interrupted invocation never runs checkpoint again or calls Start.
func (h *taskHandle) CaptureMigration(ctx context.Context, request protocol.MigrationCaptureRequest) (*protocol.MigrationCapture, error) {
	if ctx == nil {
		return nil, errdefs.ErrInvalidArgument
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	digest, err := request.Digest()
	if err != nil {
		return nil, fmt.Errorf("migration capture request: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	custodian, ok := h.rootfs.(nomadruntime.MigrationCustodian)
	if !ok {
		return nil, fmt.Errorf("ctld migration custody is unavailable: %w", errdefs.ErrUnavailable)
	}
	runner, ok := h.runner.(gvisorcli.CheckpointRunsc)
	if !ok {
		return nil, fmt.Errorf("stock runsc checkpoint is unavailable: %w", errdefs.ErrUnavailable)
	}
	h.closeMu.Lock()
	defer h.closeMu.Unlock()
	if err := h.refreshMigrationAdoption(); err != nil {
		return nil, err
	}
	h.mu.Lock()
	if h.migrationFinalized {
		h.mu.Unlock()
		return nil, errdefs.ErrFailedPrecondition
	}
	if err := h.validateMigrationSourceLocked(request); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	if h.migration != nil {
		if h.migration.RequestDigest != digest {
			h.mu.Unlock()
			return nil, errdefs.ErrAlreadyExists
		}
		if h.migrationInFlight {
			pending := *h.migration
			pending.State = protocol.MigrationCaptureIntent
			h.mu.Unlock()
			return &pending, nil
		}
		if h.migration.State == protocol.MigrationCaptureIntent {
			h.migration.State = protocol.MigrationCaptureUncertain
		}
		capture := cloneMigrationCapture(h.migration)
		h.mu.Unlock()
		if err := h.persist(); err != nil {
			return nil, err
		}
		if err := custodian.RecordMigrationCapture(ctx, *capture); err != nil {
			return nil, err
		}
		return capture, nil
	}
	if h.phase != phaseActive || h.closed || h.migrationAdmissionFenced || h.hasMigrationRestoreLocked() {
		h.mu.Unlock()
		return nil, errdefs.ErrFailedPrecondition
	}
	h.mu.Unlock()
	if err := h.checkMigrationCaptureCPU(ctx, request, false); err != nil {
		return nil, err
	}
	h.mu.Lock()
	if err := h.validateMigrationCaptureCPULocked(request, false); err != nil {
		h.mu.Unlock()
		return nil, err
	}
	h.migration = &protocol.MigrationCapture{Request: request, RequestDigest: digest, State: protocol.MigrationCaptureIntent}
	h.phase = phaseMigrating
	intent := *h.migration
	h.mu.Unlock()
	if err := h.persist(); err != nil {
		return nil, err
	}
	if err := custodian.RecordMigrationCapture(ctx, intent); err != nil {
		return nil, err
	}
	custody, err := custodian.GetMigrationCapture(ctx, request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if custody == nil || custody.Capture != intent {
		return nil, fmt.Errorf("ctld source custody changed: %w", errdefs.ErrFailedPrecondition)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// The authenticated command starts bounded work owned by this driver, not
	// by one HTTP connection. Node channel rotation must not cancel checkpoint.
	captureCtx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	h.mu.Lock()
	if h.migration.State != protocol.MigrationCaptureIntent {
		h.mu.Unlock()
		cancel()
		return nil, errdefs.ErrFailedPrecondition
	}
	h.migrationCancel = cancel
	h.migrationInFlight = true
	h.mu.Unlock()
	go func() {
		defer cancel()
		defer func() { h.mu.Lock(); h.migrationInFlight = false; h.migrationCancel = nil; h.mu.Unlock() }()
		if err := h.completeMigrationCapture(captureCtx, custody.ImageDirectory, runner, custodian); err != nil {
			h.logger.Error("migration source checkpoint requires reconciliation", "error", err)
		}
	}()
	return &intent, nil
}

func (h *taskHandle) completeMigrationCapture(ctx context.Context, imageDirectory string, runner gvisorcli.CheckpointRunsc, custodian nomadruntime.MigrationCustodian) error {
	h.mu.Lock()
	request := h.migration.Request
	h.mu.Unlock()
	err := h.checkMigrationCaptureCPU(ctx, request, true)
	if err == nil {
		err = runner.Checkpoint(ctx, h.containerID, imageDirectory)
	}
	if err == nil {
		err = syncMigrationImage(imageDirectory)
	}
	h.mu.Lock()
	if h.migration.State == protocol.MigrationCaptureIntent {
		if err == nil {
			h.migration.State = protocol.MigrationCaptureComplete
		} else {
			h.migration.State = protocol.MigrationCaptureUncertain
		}
	}
	result := cloneMigrationCapture(h.migration)
	h.mu.Unlock()
	if persistErr := h.persist(); persistErr != nil {
		return errors.Join(err, persistErr)
	}
	journalCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if journalErr := custodian.RecordMigrationCapture(journalCtx, *result); journalErr != nil {
		return errors.Join(err, journalErr)
	}
	if err != nil {
		return fmt.Errorf("source checkpoint outcome is uncertain: %w", err)
	}
	if sealer, ok := h.rootfs.(nomadruntime.MigrationRootFSSealer); ok {
		cut, err := sealer.SealMigrationRootFS(ctx, result.Request)
		if err != nil {
			return fmt.Errorf("seal filesystem for captured source: %w", err)
		}
		if err := cut.ValidateFor(*h.stage, cut.Request); err != nil {
			return fmt.Errorf("captured source filesystem binding changed: %w", err)
		}
	}
	return nil
}

func (h *taskHandle) validateMigrationSourceLocked(request protocol.MigrationCaptureRequest) error {
	if !h.rootMounted {
		return errdefs.ErrFailedPrecondition
	}
	return h.validateMigrationSourceIdentityLocked(request)
}

func (h *taskHandle) validateMigrationSourceIdentityLocked(request protocol.MigrationCaptureRequest) error {
	if h.taskConfig == nil || h.stage == nil || h.claim == nil ||
		h.taskConfig.ID != request.Target.SlotID || h.taskConfig.AllocID != request.Target.AllocationID ||
		h.taskConfig.NodeID != request.Target.NodeID ||
		request.Target.ControlEndpoint != "unix://"+h.socketPath ||
		h.stage.Identity.NodeUID != request.Target.NodeUID || h.stage.Identity.BootID != request.Target.NodeBootID ||
		h.stage.Identity.RuntimeGeneration != strconv.FormatInt(request.SourceGeneration, 10) ||
		h.claim.SandboxID != request.SandboxID || h.claim.RuntimeRevision != request.AssignmentRevision ||
		h.claim.RootFSBindingDigest != request.BindingDigest || h.claim.ResourceLeaseDigest != request.ResourceLeaseDigest ||
		h.claim.ProcdInstanceID != request.ProcdInstanceID {
		return fmt.Errorf("migration changed the exact active source: %w", errdefs.ErrFailedPrecondition)
	}
	binding, err := h.stage.BindingDigest()
	if err != nil || h.stage.ValidateDurableBinding() != nil || h.stage.Identity.WriterGrantToken != "" || hex.EncodeToString(binding[:]) != request.BindingDigest {
		return fmt.Errorf("migration source writer binding changed: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

// recoverMigrationCapture consults ctld even if the driver's last file predates
// capture. A checkpoint intent is never interpreted as successful capture.
func (h *taskHandle) recoverMigrationCapture(state PersistedState) (bool, error) {
	custodian, ok := h.rootfs.(nomadruntime.MigrationCustodian)
	if !ok {
		if state.Migration != nil {
			return true, errdefs.ErrUnavailable
		}
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	custody, err := custodian.GetMigrationCapture(ctx, h.taskConfig.ID)
	if errdefs.IsNotFound(err) && state.Migration == nil && !state.RootMounted {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if custody == nil {
		if state.Migration == nil {
			return false, nil
		}
		if state.Migration.Validate() != nil {
			return true, errdefs.ErrFailedPrecondition
		}
		// Local intent may have preceded a failed ctld write. It authorizes no
		// checkpoint retry, but must still protect the source from generic GC.
		intent := *state.Migration
		intent.State = protocol.MigrationCaptureIntent
		if err := custodian.RecordMigrationCapture(ctx, intent); err != nil {
			return true, err
		}
		custody, err = custodian.GetMigrationCapture(ctx, h.taskConfig.ID)
		if err != nil || custody == nil {
			return true, errors.Join(err, errdefs.ErrUnavailable)
		}
	}
	capture := custody.Capture
	if capture.Validate() != nil {
		return true, errdefs.ErrFailedPrecondition
	}
	if custody.Finalization != nil || custody.Failure != nil {
		if state.Migration != nil && state.Migration.RequestDigest != capture.RequestDigest {
			return true, errdefs.ErrFailedPrecondition
		}
		if complete, err := h.observeMigrationFinalization(); complete || err != nil {
			return true, err
		}
		// Finalization may still be deleting artifacts. Do not overwrite its
		// immutable capture observation while waiting for the full receipt.
		return true, errdefs.ErrUnavailable
	}
	if state.Migration != nil {
		if state.Migration.Validate() != nil || state.Migration.RequestDigest != capture.RequestDigest {
			return true, errdefs.ErrFailedPrecondition
		}
		if state.Migration.State == protocol.MigrationCaptureComplete && capture.State == protocol.MigrationCaptureIntent && !custody.ExecutionInvalidated {
			capture = *state.Migration
		}
	}
	if capture.State == protocol.MigrationCaptureIntent {
		capture.State = protocol.MigrationCaptureUncertain
	}
	// Filesystem-cut evidence belongs to ctld. The driver persists and reports
	// only its execution capture, so it cannot overwrite that additional proof.
	capture.RootFS = nil
	h.mu.Lock()
	err = h.validateMigrationSourceLocked(capture.Request)
	if err == nil {
		h.migration = &capture
		h.phase = phaseMigrating
	}
	h.mu.Unlock()
	if err != nil {
		return true, err
	}
	// Killing is safe even when checkpoint never started. It does not delete
	// the image, unmount storage or invent a complete execution image.
	if err := h.stopMigrationExecution(ctx); err != nil {
		return true, err
	}
	if err := h.persist(); err != nil {
		return true, err
	}
	return true, custodian.RecordMigrationCapture(ctx, capture)
}

// fenceMigrationExecution preserves image custody on lease loss. Physical
// cleanup and source-capacity release remain regional operations.
func (h *taskHandle) fenceMigrationExecution(cause error) bool {
	h.mu.Lock()
	// Close admission in the same critical section that inspects custody. A
	// capture cannot slip between "no migration" and ordinary lease poisoning.
	h.migrationAdmissionFenced = true
	if h.migrationFinalized {
		h.mu.Unlock()
		return true
	}
	if h.migration == nil && !h.hasMigrationRestoreLocked() {
		h.mu.Unlock()
		return false
	}
	h.phase = phaseMigrating
	if h.migration != nil && h.migration.State == protocol.MigrationCaptureIntent {
		h.migration.State = protocol.MigrationCaptureUncertain
	}
	cancel := h.migrationCancel
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	h.migrationFenceOnce.Do(func() {
		if err := h.persist(); err != nil {
			h.logger.Error("persist migration lease fence", "error", err)
		}
		h.logger.Error("fencing migration execution", "error", cause)
		go func() {
			delay := 100 * time.Millisecond
			for {
				h.mu.Lock()
				released := h.closed || h.migrationFinalized || (h.migration == nil && !h.hasMigrationRestoreLocked())
				h.mu.Unlock()
				if released {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				h.mu.Lock()
				var restore *protocol.MigrationRestoreRequest
				if h.hasMigrationRestoreLocked() {
					restore = h.claim.MigrationRestore
				}
				h.mu.Unlock()
				var recordErr error
				if restore != nil {
					if custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian); ok {
						digest, digestErr := restore.Digest()
						recordErr = digestErr
						if recordErr == nil {
							recordErr = custodian.RecordMigrationRestore(ctx, protocol.MigrationRestoreObservation{Request: *restore, RequestDigest: digest, State: protocol.MigrationRestoreUncertain})
							if recordErr != nil {
								// Reconcile adoption committed before a lost RPC response.
								custody, err := custodian.GetMigrationDestination(ctx, restore.Image.Target.SlotID)
								if err == nil && custody != nil && custody.Adoption != nil {
									recordErr = h.adoptMigrationAfterReady(&custody.Adoption.Request, *restore, custody.Adoption.Request.CommandReadyDigest)
								}
							}
						}
					} else {
						recordErr = errdefs.ErrUnavailable
					}
				}
				// Even an unavailable journal must not prevent physical fencing.
				err := errors.Join(recordErr, h.stopMigrationExecution(ctx))
				cancel()
				h.mu.Lock()
				inFlight := h.migrationCancel != nil
				h.mu.Unlock()
				if err == nil && !inFlight {
					return
				}
				if err != nil {
					h.logger.Error("fence migration execution", "error", err)
				}
				time.Sleep(delay)
				delay = min(2*delay, 5*time.Second)
			}
		}()
	})
	return true
}

func (h *taskHandle) stopMigrationExecution(ctx context.Context) error {
	state, err := h.runner.State(ctx, h.containerID)
	if errdefs.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if state.ID != h.containerID {
		return fmt.Errorf("migration runtime identity changed")
	}
	if state.Status == "stopped" {
		return nil
	}
	if state.Status != "running" && state.Status != "paused" && state.Status != "created" {
		return fmt.Errorf("unexpected migration state %q", state.Status)
	}
	if err := h.runner.Kill(ctx, h.containerID, "KILL"); err != nil {
		return err
	}
	// A successful signal is not proof that execution has stopped. Keep image
	// custody and wait for the exact runtime to report stopped before returning.
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := h.runner.State(ctx, h.containerID)
		if err != nil {
			return err
		}
		if state.ID != h.containerID {
			return fmt.Errorf("migration runtime identity changed")
		}
		if state.Status == "stopped" {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func syncMigrationImage(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	entries, err := directory.ReadDir(257)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if len(entries) == 0 || len(entries) > 256 {
		return fmt.Errorf("invalid checkpoint image file count")
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("checkpoint contains a non-regular file")
		}
		file, err := os.Open(filepath.Join(path, entry.Name()))
		if err != nil {
			return err
		}
		err = file.Sync()
		closeErr := file.Close()
		if err != nil || closeErr != nil {
			return errors.Join(err, closeErr)
		}
	}
	return directory.Sync()
}
