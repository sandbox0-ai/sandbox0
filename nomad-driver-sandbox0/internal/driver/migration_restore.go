package driver

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/gvisorcli"
	"github.com/sandbox0-ai/sandbox0/pkg/nomadruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// prepareMigrationRestore verifies image custody and attaches the authorized
// RootFS concurrently. Both must finish before any container is created. On
// failure, cancellation is followed by joining both operations so no attach or
// intent write can race the caller's transition to uncertain migration custody.
func (h *taskHandle) prepareMigrationRestore(request protocol.MigrationRestoreRequest, stage rootfshandoff.StageRequest) (string, string, time.Duration, error) {
	if err := request.Validate(); err != nil {
		return "", "", 0, err
	}
	custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian)
	if !ok {
		return "", "", 0, errdefs.ErrUnavailable
	}
	if _, ok := h.runner.(gvisorcli.CheckpointRunsc); !ok {
		return "", "", 0, errdefs.ErrUnavailable
	}
	target := request.Image.Target
	if target.SlotID != h.taskConfig.ID || target.AllocationID != h.taskConfig.AllocID || target.NodeID != h.taskConfig.NodeID {
		return "", "", 0, errdefs.ErrFailedPrecondition
	}
	digest, _ := request.Digest()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := h.checkMigrationRestoreCPU(ctx, request); err != nil {
		return "", "", 0, err
	}
	var imageDirectory string
	imageDone := make(chan error, 1)
	go func() {
		imageErr := custodian.RecordMigrationRestore(ctx, protocol.MigrationRestoreObservation{Request: request, RequestDigest: digest, State: protocol.MigrationRestoreIntent})
		if imageErr == nil {
			var custody *nomadruntime.MigrationDestinationCustody
			custody, imageErr = custodian.GetMigrationDestination(ctx, target.SlotID)
			if imageErr == nil {
				if custody == nil || custody.Restore == nil || custody.Restore.RequestDigest != digest || custody.Restore.State != protocol.MigrationRestoreIntent {
					imageErr = errdefs.ErrFailedPrecondition
				} else {
					imageDirectory = custody.ImageDirectory
				}
			}
		}
		if imageErr != nil {
			cancel()
		}
		imageDone <- imageErr
	}()
	attachCtx, attachCancel := context.WithTimeout(ctx, 3*time.Minute)
	started := time.Now()
	mount, attachErr := h.rootfs.Ensure(attachCtx, stage, h.handleWriterLeaseLoss)
	attachElapsed := time.Since(started)
	attachCancel()
	if attachErr != nil {
		cancel()
		attachErr = fmt.Errorf("attach migration RootFS session: %w", attachErr)
	}
	imageErr := <-imageDone
	if err := errors.Join(imageErr, attachErr, ctx.Err()); err != nil {
		return "", "", attachElapsed, err
	}
	return imageDirectory, mount.Source, attachElapsed, nil
}

func (h *taskHandle) restoreMigrationExecution(ctx context.Context, request protocol.MigrationRestoreRequest, directory string) (*protocol.MigrationCPULaunch, error) {
	started := time.Now()
	var intentElapsed, beforeElapsed, restoreElapsed, afterElapsed, commitElapsed time.Duration
	completed := false
	var accounting migrationCPUCounters
	accountingValid := false
	defer func() {
		if h.logger != nil {
			// The enclosing claim timer includes node RPCs and journal commits.
			// Keep those separate from stock runsc loading when diagnosing tails.
			h.logger.Info("Migration restore execution timing", "operation_id", request.Image.OperationID(),
				"restore_success", completed, "restore_total_us", time.Since(started).Microseconds(),
				"restore_intent_us", intentElapsed.Microseconds(), "restore_cpu_before_us", beforeElapsed.Microseconds(),
				"restore_command_us", restoreElapsed.Microseconds(), "restore_cpu_after_us", afterElapsed.Microseconds(),
				"restore_commit_us", commitElapsed.Microseconds(),
				"restore_cpu_accounting_valid", accountingValid,
				"restore_cgroup_cpu_usage_us", accounting.usage, "restore_cgroup_cpu_user_us", accounting.user,
				"restore_cgroup_cpu_system_us", accounting.system, "restore_cgroup_periods", accounting.periods,
				"restore_cgroup_throttled_periods", accounting.throttledPeriods, "restore_cgroup_throttled_us", accounting.throttled)
		}
	}()
	custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	runner, ok := h.runner.(gvisorcli.CheckpointRunsc)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	digest, _ := request.Digest()
	observation := protocol.MigrationRestoreObservation{Request: request, RequestDigest: digest, State: protocol.MigrationRestoreExecuting}
	stepStarted := time.Now()
	err := custodian.RecordMigrationRestore(ctx, observation)
	intentElapsed = time.Since(stepStarted)
	if err != nil {
		return nil, err
	}
	stepStarted = time.Now()
	before, err := h.observeMigrationRestoreCPU(ctx, request)
	beforeElapsed = time.Since(stepStarted)
	if err != nil {
		return nil, err
	}
	// This is the only execution call for a migration destination. Never use
	// Start after any restore error, including an ambiguous transport result.
	// Read only the already validated target lease's cgroup. Accounting failures
	// must not change restore behavior, and aggregated throttle time is not a
	// wall-clock component that can be added to the command duration.
	sample := openMigrationCPUAccounting(h.resourceCgroupRoot, request.Image.Resources.CgroupName)
	defer sample.close()
	stepStarted = time.Now()
	err = runner.Restore(ctx, h.containerID, directory)
	restoreElapsed = time.Since(stepStarted)
	accounting, accountingValid = sample.finish()
	if err != nil {
		return nil, fmt.Errorf("runsc restore: %w", err)
	}
	stepStarted = time.Now()
	after, err := h.observeMigrationRestoreCPU(ctx, request)
	afterElapsed = time.Since(stepStarted)
	if err != nil {
		return nil, err
	}
	launch, err := protocol.BindMigrationCPURestore(request, *before, *after)
	if err != nil {
		return nil, err
	}
	observation.State = protocol.MigrationRestoreComplete
	stepStarted = time.Now()
	err = custodian.RecordMigrationRestore(ctx, observation)
	commitElapsed = time.Since(stepStarted)
	if err != nil {
		return nil, err
	}
	completed = true
	return launch, nil
}

func (h *taskHandle) hasMigrationRestoreLocked() bool {
	return h.claim != nil && h.claim.MigrationRestore != nil && h.claim.MigrationAdoption == nil
}

func (h *taskHandle) failMigrationRestore(cause error) error {
	h.stopConsumerRenewal()
	h.mu.Lock()
	request := h.claim.MigrationRestore
	h.phase = phaseMigrating
	h.mu.Unlock()
	var recordErr error
	if custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian); ok {
		digest, err := request.Digest()
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			recordErr = custodian.RecordMigrationRestore(ctx, protocol.MigrationRestoreObservation{Request: *request, RequestDigest: digest, State: protocol.MigrationRestoreUncertain})
			cancel()
		} else {
			recordErr = err
		}
	} else {
		recordErr = errdefs.ErrUnavailable
	}
	h.fenceMigrationExecution(cause)
	return errors.Join(cause, recordErr, h.persist())
}

// Recovery cannot infer whether an interrupted restore executed user code.
// Keep both image and target dirty state, fence execution, and let the regional
// transaction reconcile the failure. Never replay the image after restart.
func (h *taskHandle) recoverMigrationRestore(state PersistedState) (bool, error) {
	custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian)
	if !ok {
		if state.Claim != nil && state.Claim.MigrationRestore != nil {
			return true, errdefs.ErrUnavailable
		}
		return false, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	custody, err := custodian.GetMigrationDestination(ctx, h.taskConfig.ID)
	if errdefs.IsNotFound(err) && state.Claim == nil && !state.RootMounted {
		return false, nil
	}
	if err != nil {
		return true, err
	}
	if custody != nil && custody.Failure != nil {
		// The regional stop may precede the driver's first restore journal
		// write. Even a stale warm handle must never recreate this carrier.
		failure := custody.Failure
		request := failure.Request.Restore
		target := request.Image.Target
		want, err := failure.Request.Digest()
		if err != nil || want != failure.RequestDigest || target.SlotID != h.taskConfig.ID ||
			target.AllocationID != h.taskConfig.AllocID || target.NodeID != h.taskConfig.NodeID {
			return true, errdefs.ErrFailedPrecondition
		}
		h.mu.Lock()
		if h.claim == nil {
			h.claim = &claimMetadata{Stage: &request.Stage, WriterEpoch: strconv.FormatInt(request.Stage.Identity.WriterEpoch, 10)}
		}
		h.claim.MigrationRestore = &request
		h.phase = phaseMigrating
		h.migrationAdmissionFenced = true
		h.mu.Unlock()
		return true, errors.Join(h.stopMigrationExecution(ctx), h.persist())
	}
	if custody == nil || custody.Restore == nil {
		if state.Claim != nil && state.Claim.MigrationRestore != nil {
			return true, errdefs.ErrFailedPrecondition
		}
		return false, nil
	}
	request := custody.Restore.Request
	target := request.Image.Target
	if custody.Restore.Validate() != nil || target.SlotID != h.taskConfig.ID ||
		target.AllocationID != h.taskConfig.AllocID || target.NodeID != h.taskConfig.NodeID {
		return true, errdefs.ErrFailedPrecondition
	}
	var localErr error
	if state.Claim != nil && state.Claim.MigrationRestore != nil {
		digest, err := state.Claim.MigrationRestore.Digest()
		if err != nil || digest != custody.Restore.RequestDigest {
			localErr = errdefs.ErrFailedPrecondition
		}
	}
	if custody.Adoption != nil {
		if localErr != nil || custody.Adoption.Request.ValidateFor(*custody.Restore) != nil {
			return true, errors.Join(errdefs.ErrFailedPrecondition, h.stopMigrationExecution(ctx))
		}
		adopter, ok := h.rootfs.(nomadruntime.MigrationAdopter)
		if !ok {
			return true, errdefs.ErrUnavailable
		}
		proof, err := adopter.AdoptMigrationDestination(ctx, custody.Adoption.Request)
		if err != nil {
			return true, err
		}
		if proof == nil || proof.ValidateFor(custody.Adoption.Request) != nil {
			return true, errdefs.ErrFailedPrecondition
		}
		h.mu.Lock()
		if h.claim == nil {
			h.claim = &claimMetadata{Stage: &request.Stage, WriterEpoch: strconv.FormatInt(request.Stage.Identity.WriterEpoch, 10)}
		}
		revision, _ := request.Image.RuntimeAssignment().Revision()
		binding, _ := request.Stage.BindingDigest()
		resources, _ := request.Image.Resources.Digest()
		h.rootMounted = true
		h.claim.SandboxID = custody.Adoption.Request.SandboxID
		h.claim.RuntimeRevision = revision
		h.claim.RootFSBindingDigest = hex.EncodeToString(binding[:])
		h.claim.ResourceLeaseDigest = strings.TrimPrefix(resources, "sha256:")
		h.claim.ProcdInstanceID = custody.Adoption.Request.ProcdInstanceID
		h.claim.CommandReadyDigest = custody.Adoption.Request.CommandReadyDigest
		h.stage = &request.Stage
		h.claim.Stage = &request.Stage
		h.claim.MigrationRestore = &request
		h.claim.MigrationAdoption = &protocol.MigrationAdoptionReceipt{Request: custody.Adoption.Request, Proof: *proof}
		h.mu.Unlock()
		// Ctld custody overrides a stale warm Nomad handle: the restored
		// process must follow ordinary driver-restart fencing, never warm reuse.
		persistErr := h.persist()
		// This adopted runtime may already own the source image of a later
		// migration. Its newer custody takes precedence over ordinary cleanup.
		if handled, err := h.recoverMigrationCapture(state); handled || err != nil {
			return true, errors.Join(persistErr, err)
		}
		return true, errors.Join(persistErr, h.recoverCrashedRootFS())
	}
	// ctld custody survives a stale Nomad handle. Missing local metadata must
	// not leave an already-restored workload executing without its authority.
	h.mu.Lock()
	if h.claim == nil {
		h.claim = &claimMetadata{Stage: &request.Stage, WriterEpoch: strconv.FormatInt(request.Stage.Identity.WriterEpoch, 10)}
	}
	h.claim.MigrationRestore = &request
	h.phase = phaseMigrating
	h.mu.Unlock()
	observation := *custody.Restore
	observation.State = protocol.MigrationRestoreUncertain
	recordErr := custodian.RecordMigrationRestore(ctx, observation)
	stopErr := h.stopMigrationExecution(ctx)
	return true, errors.Join(localErr, recordErr, stopErr, h.persist())
}

// migrationRestoreReceipt reads ctld's durable completion instead of deriving
// success from the driver's local phase. Uncertain custody cannot authorize the
// regional procd handover, including after a lost claim response.
func (h *taskHandle) migrationRestoreReceipt(ctx context.Context, request protocol.MigrationRestoreRequest) (*protocol.MigrationRestoreObservation, error) {
	custodian, ok := h.rootfs.(nomadruntime.MigrationRestoreCustodian)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	custody, err := custodian.GetMigrationDestination(ctx, request.Image.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if custody == nil || custody.Failure != nil || custody.Restore == nil || custody.Restore.Validate() != nil || custody.Restore.RequestDigest != want || custody.Restore.State != protocol.MigrationRestoreComplete {
		return nil, errdefs.ErrFailedPrecondition
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.phase != phaseActive || h.migrationAdmissionFenced || h.claim == nil || h.claim.MigrationRestore == nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	current, err := h.claim.MigrationRestore.Digest()
	if err != nil || current != want {
		return nil, errdefs.ErrFailedPrecondition
	}
	return custody.Restore, nil
}
