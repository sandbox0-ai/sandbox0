package nomadruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationRootFSRuntime interface {
	CaptureMigrationRootFS(context.Context, rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSCutRequest) (rootfshandoff.MigrationRootFSCut, error)
}

// MigrationRootFSSealer is private node control after successful execution
// capture. It does not expose images, acquire a writer or publish a new head.
type MigrationRootFSSealer interface {
	SealMigrationRootFS(context.Context, protocol.MigrationCaptureRequest) (rootfshandoff.MigrationRootFSCut, error)
}

func (r *rootfsRuntime) CaptureMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest) (rootfshandoff.MigrationRootFSCut, error) {
	return r.sessions.CaptureMigrationRootFS(ctx, stage, request)
}

func (d *nodeRuntime) SealMigrationRootFS(ctx context.Context, request protocol.MigrationCaptureRequest) (rootfshandoff.MigrationRootFSCut, error) {
	var zero rootfshandoff.MigrationRootFSCut
	if err := request.Validate(); err != nil {
		return zero, err
	}
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil {
		return zero, errdefs.ErrUnavailable
	}
	if request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID {
		return zero, errdefs.ErrPermissionDenied
	}
	runtime, ok := d.runtime.(migrationRootFSRuntime)
	if !ok {
		return zero, errdefs.ErrUnavailable
	}
	if !d.beginReconciliation(request.Target.SlotID, nil) {
		return zero, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(request.Target.SlotID)
	custody, err := d.GetMigrationCapture(ctx, request.Target.SlotID)
	if err != nil {
		return zero, err
	}
	if custody == nil || custody.Capture.Request != request || custody.Capture.State != protocol.MigrationCaptureComplete {
		return zero, fmt.Errorf("RootFS cut requires the exact successful memory capture: %w", errdefs.ErrFailedPrecondition)
	}
	matched, err := d.migrationSourceSession(request)
	if err != nil {
		return zero, err
	}
	// Never repair an unexpectedly executing captured source by killing it and
	// sealing a later filesystem. That would pair old memory with new writes.
	if err := d.stopMigrationSource(ctx, *matched, false); err != nil {
		return zero, err
	}
	cutRequest := rootfshandoff.MigrationRootFSCutRequest{OperationID: request.OperationID,
		CaptureRequestDigest: custody.Capture.RequestDigest, SourceBindingDigest: request.BindingDigest,
		GenerationID: "migration-" + custody.Capture.RequestDigest}
	// Inspect only the already durable, stopped memory image. Inspection may
	// stage missing chunks under existing upload admission while RootFS sealing
	// proceeds. Both share reconciliation; no worker survives custody release.
	// Priming failure is a cache miss, never evidence for a completed image.
	sealed := false
	if primer, ok := d.runtime.(interface {
		PrimeMigrationImageInventory(context.Context, string) error
	}); ok && d.migrationPeer != nil && custody.Capture.RootFS == nil {
		primeCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})
		go func() {
			defer close(done)
			if attempted, _ := d.primeMigrationCaptureUpload(primeCtx, request, custody.ImageDirectory); !attempted {
				_ = primer.PrimeMigrationImageInventory(primeCtx, custody.ImageDirectory)
			}
		}()
		defer func() {
			if !sealed {
				cancel()
			}
			<-done
			cancel()
		}()
	}
	cutStarted := time.Now()
	cut, err := runtime.CaptureMigrationRootFS(ctx, matched.Stage, cutRequest)
	if err != nil {
		if errors.Is(err, rootfssession.ErrMigrationCutOwnerLost) {
			// Persist invalidation before reporting an unusable memory/filesystem
			// pair. A later worker may request exact regional failure authority;
			// this observation does not clean the source or authorize a retry.
			if invalidateErr := d.journal.invalidateMigrationExecution(request.Target.SlotID); invalidateErr != nil {
				return zero, errors.Join(err, invalidateErr)
			}
		}
		return zero, err
	}
	if err := cut.ValidateFor(matched.Stage, cutRequest); err != nil {
		return zero, err
	}
	capture := custody.Capture
	capture.RootFS = &cut
	if err := d.journal.RecordMigrationCapture(capture); err != nil {
		return zero, err
	}
	sealed = true
	d.logMigrationTiming(request.OperationID, "rootfs-cut", cutStarted)
	return cut, nil
}

// migrationCustodyForSession guards every ordinary crash/lease recovery path,
// including the interval before the filesystem's own cut intent is recorded.
// Its caller holds the existing per-slot reconciliation admission; recording
// new capture custody uses that same admission and cannot race past this gate.
func (d *nodeRuntime) migrationCustodyForSession(session rootfssession.RecoverySession) (*protocol.MigrationCapture, error) {
	if d.journal == nil || session.Stage.Identity.SlotNonce == "" {
		if session.Kind == rootfssession.RecoveryMigration {
			return nil, errdefs.ErrUnavailable
		}
		return nil, nil
	}
	record, err := d.journal.Get(session.Stage.Identity.SlotNonce)
	if errdefs.IsNotFound(err) && session.Kind != rootfssession.RecoveryMigration {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if record.Migration == nil {
		if session.Kind == rootfssession.RecoveryMigration {
			return nil, errdefs.ErrFailedPrecondition
		}
		return nil, nil
	}
	capture := record.Migration.Capture
	request, stage := capture.Request, session.Stage
	binding, err := stage.BindingDigest()
	if err != nil || stage.ValidateDurableBinding() != nil || stage.Identity.WriterGrantToken != "" ||
		hex.EncodeToString(binding[:]) != request.BindingDigest || stage.Identity.SlotNonce != request.Target.SlotID ||
		stage.Identity.AllocationID != request.Target.AllocationID || stage.Identity.NodeUID != request.Target.NodeUID ||
		stage.Identity.BootID != request.Target.NodeBootID || stage.Identity.RuntimeGeneration != strconv.FormatInt(request.SourceGeneration, 10) ||
		request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID ||
		session.Consumer == nil || session.Consumer.ActiveKey != request.Target.SlotID ||
		session.Consumer.ContainerID != record.Registration.RunscContainerID {
		return nil, fmt.Errorf("migration RootFS custody changed its physical source: %w", errdefs.ErrFailedPrecondition)
	}
	return &capture, nil
}

func (d *nodeRuntime) stopMigrationSource(ctx context.Context, session rootfssession.RecoverySession, mayKill bool) error {
	if d.runner == nil || session.Consumer == nil {
		return errdefs.ErrUnavailable
	}
	id := session.Consumer.ContainerID
	signaled := false
	for {
		state, err := d.runner.State(ctx, id)
		if errdefs.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if state.ID != id {
			return errdefs.ErrFailedPrecondition
		}
		if state.Status == "stopped" {
			return nil
		}
		if !mayKill || (state.Status != "running" && state.Status != "paused") {
			if state.Status == "running" || state.Status == "paused" {
				if d.journal == nil {
					return errdefs.ErrUnavailable
				}
				if err := d.journal.invalidateMigrationExecution(session.Stage.Identity.SlotNonce); err != nil {
					return err
				}
			}
			return fmt.Errorf("migration source has unexpected execution state %q: %w", state.Status, errdefs.ErrFailedPrecondition)
		}
		if !signaled {
			// Execution after a capture (or during failed capture recovery) makes
			// its temporal cut unusable. Persist that fact before killing; a later
			// retry cannot pair the old memory image with a newer filesystem.
			if d.journal == nil {
				return errdefs.ErrUnavailable
			}
			if err := d.journal.invalidateMigrationExecution(session.Stage.Identity.SlotNonce); err != nil {
				return err
			}
			if err := d.runner.Kill(ctx, id, "KILL"); err != nil {
				return err
			}
			signaled = true
		}
		timer := time.NewTimer(25 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

// migrationSourceSession joins both node journals under per-slot admission.
func (d *nodeRuntime) migrationSourceSession(request protocol.MigrationCaptureRequest) (*rootfssession.RecoverySession, error) {
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return nil, err
	}
	var matched *rootfssession.RecoverySession
	for i := range sessions {
		if sessions[i].Stage.Identity.SlotNonce != request.Target.SlotID {
			continue
		}
		capture, err := d.migrationCustodyForSession(sessions[i])
		if err != nil {
			return nil, err
		}
		if capture == nil || capture.Request != request || matched != nil {
			return nil, errdefs.ErrFailedPrecondition
		}
		matched = &sessions[i]
	}
	if matched == nil {
		return nil, errdefs.ErrNotFound
	}
	return matched, nil
}
