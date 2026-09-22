package nomadruntime

import (
	"context"
	"errors"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type migrationCaptureUploadRuntime interface {
	OpenMigrationCaptureUpload(context.Context, runtimecheckpoint.CaptureScope, int64) (*runtimecheckpoint.CaptureStager, error)
	PlanMigrationCaptureUpload(context.Context, *runtimecheckpoint.CaptureStager, runtimecheckpoint.Binding, string) (runtimecheckpoint.LocalImagePlan, error)
}

type migrationCaptureUploadCache struct {
	bytes int64
	stage *runtimecheckpoint.CaptureStager
}

func (r *rootfsRuntime) OpenMigrationCaptureUpload(ctx context.Context, scope runtimecheckpoint.CaptureScope, bytes int64) (*runtimecheckpoint.CaptureStager, error) {
	if r == nil || r.checkpoints == nil {
		return nil, errdefs.ErrUnavailable
	}
	key, err := scope.Digest()
	if err != nil {
		return nil, err
	}
	r.migrationUploadMu.Lock()
	cached, ok := r.migrationUploads[key]
	r.migrationUploadMu.Unlock()
	if ok {
		if cached.bytes != bytes {
			return nil, errdefs.ErrFailedPrecondition
		}
		return cached.stage, ctx.Err()
	}
	stage, err := r.checkpoints.OpenCaptureStaging(ctx, scope, bytes)
	if err != nil {
		return nil, err
	}
	// The node reconciliation serializes an exact capture. Eviction only loses
	// upload knowledge; reopening verifies the immutable regional reservation
	// and existing objects rather than resetting admission or ownership.
	r.migrationUploadMu.Lock()
	if len(r.migrationUploads) >= maxMigrationImageCustodies {
		r.migrationUploads = nil
	}
	if r.migrationUploads == nil {
		r.migrationUploads = make(map[string]migrationCaptureUploadCache)
	}
	r.migrationUploads[key] = migrationCaptureUploadCache{bytes: bytes, stage: stage}
	r.migrationUploadMu.Unlock()
	return stage, nil
}

func (r *rootfsRuntime) PlanMigrationCaptureUpload(ctx context.Context, stage *runtimecheckpoint.CaptureStager, binding runtimecheckpoint.Binding, directory string) (runtimecheckpoint.LocalImagePlan, error) {
	if inventory, found := r.takeMigrationInventory(directory); found {
		return stage.BindLocalInventory(ctx, binding, inventory)
	}
	return stage.PlanLocal(ctx, binding, directory)
}

func (d *nodeRuntime) publicationCaptureUpload(ctx context.Context, capture protocol.MigrationCaptureRequest, binding runtimecheckpoint.Binding) (migrationCaptureUploadRuntime, *runtimecheckpoint.CaptureStager, error) {
	runtime, ok := d.runtime.(migrationCaptureUploadRuntime)
	if !ok {
		return nil, nil, nil
	}
	record, err := d.journal.Get(capture.Target.SlotID)
	if err != nil {
		return nil, nil, err
	}
	staging := record.MigrationStaging
	if staging == nil || staging.Request.CaptureUpload.Version == 0 {
		return nil, nil, nil
	}
	if !staging.Ready || staging.Released || !staging.Request.IsSource() || staging.Request.Source != capture {
		return nil, nil, errdefs.ErrFailedPrecondition
	}
	scope, err := staging.Request.CaptureUpload.Scope(capture)
	if err != nil {
		return nil, nil, err
	}
	binding.RootFSGenerationID, binding.RootFSDescriptorDigest = "", ""
	expected, err := runtimecheckpoint.NewCaptureScope(binding)
	if err != nil || expected != scope {
		return nil, nil, errdefs.ErrFailedPrecondition
	}
	stage, err := runtime.OpenMigrationCaptureUpload(ctx, scope, staging.Request.CaptureUpload.MaxBytes)
	return runtime, stage, err
}

func (d *nodeRuntime) hasMigrationCaptureUpload(capture protocol.MigrationCapture) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.migrationCaptureUploads[capture.Request.Target.SlotID] == capture.RequestDigest
}

// startMigrationCaptureUpload transfers the caller's slot reconciliation to a
// disposable worker only after the durable capture intent exists. Outcome
// recording and regional cleanup cancel and join that worker through the same
// reconciliation scope. No goroutine survives custody release or primary loss.
func (d *nodeRuntime) startMigrationCaptureUpload(record runtimeSlotJournalRecord) bool {
	runtime, ok := d.runtime.(migrationCaptureUploadRuntime)
	if !ok || record.Migration == nil || record.MigrationStaging == nil {
		return false
	}
	capture, staging := record.Migration.Capture, record.MigrationStaging
	if capture.State != protocol.MigrationCaptureIntent || !staging.Ready || staging.Released ||
		!staging.Request.IsSource() || staging.Request.Source != capture.Request || staging.Request.CaptureUpload.Version == 0 {
		return false
	}
	scope, err := staging.Request.CaptureUpload.Scope(capture.Request)
	if err != nil {
		return false
	}
	if _, err := d.migrationSourceSession(capture.Request); err != nil {
		return false
	}
	slot := capture.Request.Target.SlotID
	d.mu.Lock()
	parent := d.migrationContext
	if parent == nil {
		parent = context.Background()
	}
	if parent.Err() != nil || d.inflight[slot] == nil {
		d.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Minute)
	d.inflight[slot].cancel = cancel
	if d.migrationCaptureUploads == nil {
		d.migrationCaptureUploads = make(map[string]string)
	}
	d.migrationCaptureUploads[slot] = capture.RequestDigest
	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		defer cancel()
		started := time.Now()
		stage, err := runtime.OpenMigrationCaptureUpload(ctx, scope, staging.Request.CaptureUpload.MaxBytes)
		if err == nil {
			peer := d.openMigrationCapturePeerUpload(ctx, record, scope)
			if peer == nil {
				err = stage.UploadGrowing(ctx, record.Migration.ImageDirectory)
			} else {
				err = stage.UploadGrowingWithPeer(ctx, record.Migration.ImageDirectory, peer.writeChunk)
				peer.close()
			}
		}
		d.logMigrationTiming(capture.Request.OperationID, "capture-upload", started,
			"canceled", errors.Is(err, context.Canceled), "success", err == nil || errors.Is(err, context.Canceled))
		d.mu.Lock()
		delete(d.migrationCaptureUploads, slot)
		d.endReconciliationLocked(slot)
		d.mu.Unlock()
	}()
	d.mu.Unlock()
	return true
}

// primeMigrationCaptureUpload reuses the exact journaled source reservation.
// False means no staged-upload capability is configured, so ordinary local
// inspection remains available. An attempted upload failure is only a cache
// miss; normal publication retries under the same charged capture scope.
func (d *nodeRuntime) primeMigrationCaptureUpload(ctx context.Context, request protocol.MigrationCaptureRequest, directory string) (bool, error) {
	primer, ok := d.runtime.(interface {
		PrimeMigrationCaptureInventory(context.Context, *runtimecheckpoint.CaptureStager, string) error
	})
	if !ok {
		return false, nil
	}
	runtime, ok := d.runtime.(migrationCaptureUploadRuntime)
	if !ok {
		return false, nil
	}
	record, err := d.journal.Get(request.Target.SlotID)
	if err != nil {
		return true, err
	}
	staging := record.MigrationStaging
	if staging == nil || staging.Request.CaptureUpload.Version == 0 {
		return false, nil
	}
	if !staging.Ready || staging.Released || !staging.Request.IsSource() || staging.Request.Source != request ||
		record.Migration == nil || record.Migration.ExecutionInvalidated || record.Migration.Capture.State != protocol.MigrationCaptureComplete ||
		record.Migration.Capture.Request != request || record.Migration.ImageDirectory != directory {
		return true, errdefs.ErrFailedPrecondition
	}
	scope, err := staging.Request.CaptureUpload.Scope(request)
	if err != nil {
		return true, err
	}
	// This optional overlap cannot pin the RootFS sealing scope for an entire
	// object-store outage. Cancel and join before reverting to normal publication.
	bounded, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	started := time.Now()
	stage, err := runtime.OpenMigrationCaptureUpload(bounded, scope, staging.Request.CaptureUpload.MaxBytes)
	if err == nil {
		err = primer.PrimeMigrationCaptureInventory(bounded, stage, directory)
	}
	d.logMigrationTiming(request.OperationID, "capture-seal-upload", started, "success", err == nil)
	return true, err
}
