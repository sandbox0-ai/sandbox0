package nomadruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

type MigrationImagePrefetcher interface {
	PrefetchMigrationImage(context.Context, protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error)
}

type migrationPrefetchWorker struct {
	digest string
	done   chan struct{}
	result *protocol.MigrationImagePrefetched
	err    error
}

// PrefetchMigrationImage shares one bounded cache fill across channel rotation.
// The staging journal owns partial files even when every caller disconnects.
func (d *nodeRuntime) PrefetchMigrationImage(ctx context.Context, request protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	var owned protocol.MigrationImagePrefetchRequest
	if err := json.Unmarshal(payload, &owned); err != nil {
		return nil, err
	}
	d.mu.Lock()
	parent := d.migrationContext
	if parent == nil {
		parent = context.Background()
	}
	if parent.Err() != nil {
		d.mu.Unlock()
		return nil, parent.Err()
	}
	if d.migrationPrefetches == nil {
		d.migrationPrefetches = make(map[string]*migrationPrefetchWorker)
	}
	key := owned.Staging.Target.SlotID
	worker := d.migrationPrefetches[key]
	if worker != nil && worker.digest != want {
		d.mu.Unlock()
		return nil, errdefs.ErrAlreadyExists
	}
	if worker == nil {
		worker = &migrationPrefetchWorker{digest: want, done: make(chan struct{})}
		d.migrationPrefetches[key] = worker
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			operationCtx, cancel := context.WithTimeout(parent, 2*time.Minute)
			defer cancel()
			worker.result, worker.err = d.prefetchMigrationImage(operationCtx, owned)
			d.mu.Lock()
			delete(d.migrationPrefetches, key)
			close(worker.done)
			d.mu.Unlock()
		}()
	}
	d.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-worker.done:
		if worker.result == nil {
			return nil, worker.err
		}
		copy := *worker.result
		return &copy, worker.err
	}
}

func (d *nodeRuntime) prefetchMigrationImage(ctx context.Context, request protocol.MigrationImagePrefetchRequest) (*protocol.MigrationImagePrefetched, error) {
	if err := d.validateMigrationStagingRequest(ctx, request.Staging); err != nil {
		return nil, err
	}
	if d.runtime == nil || d.runner == nil {
		return nil, errdefs.ErrUnavailable
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	slot := request.Staging.Target.SlotID
	prior, err := d.journal.Get(slot)
	if err != nil {
		return nil, err
	}
	if prior.hasMigrationCapturePeerCache() && prior.MigrationStaging.Request == request.Staging {
		// The final plan supersedes a tentative stream. Join its receiver before
		// reading its inventory or reusing any of its original write descriptors.
		if err := d.beginExternalReconciliation(ctx, slot); err != nil {
			return nil, err
		}
		d.mu.Lock()
		d.inflight[slot].cancel = cancel
		d.mu.Unlock()
	} else if !d.beginReconciliation(slot, cancel) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(slot)
	if err := d.checkMigrationStaging(false); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationPrefetch(request, nil); err != nil {
		return nil, err
	}
	record, err := d.journal.Get(slot)
	if err != nil {
		return nil, err
	}
	c := record.MigrationStaging.Prefetch
	if c.Receipt != nil {
		// This acknowledges the historical cache fill only. Preparation always
		// revalidates its contents against the real published manifest.
		copy := *c.Receipt
		return &copy, nil
	}
	if d.migrationPeer == nil || runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity) != request.Publication.DestinationPeerCertificateSHA256 {
		return nil, errdefs.ErrFailedPrecondition
	}
	if err := d.requireUnusedMigrationDestination(ctx, record.Registration, request.Staging.Target); err != nil {
		return nil, err
	}
	if err := ensureMigrationStagingDirectory(d.journal.migrationRoot); err != nil {
		return nil, err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, c.ImageDirectory); err != nil {
		return nil, err
	}
	admit := func(bytes int64, inodes uint64) error {
		if bytes > request.Staging.Bytes || inodes > request.Staging.Inodes {
			return errdefs.ErrResourceExhausted
		}
		return d.checkMigrationStagingBudget(bytes, inodes)
	}
	started := time.Now()
	manifest, reused, err := d.repairMigrationCapturePeer(ctx, record, request, c.ImageDirectory)
	if err != nil {
		return nil, err
	}
	if !reused {
		manifest, err = d.receiveMigrationPeerImage(ctx, request.Plan.Peer,
			migrationPeerRead{SlotID: request.Publication.Capture.Request.Target.SlotID, RequestDigest: request.Plan.RequestDigest, Planned: true},
			request.Plan.Binding, request.Plan.Reference, c.ImageDirectory, min(request.Staging.Bytes, runtimecheckpoint.MaxImageBytes), admit)
	}
	if err != nil {
		return nil, err
	}
	result := &protocol.MigrationImagePrefetched{RequestDigest: c.RequestDigest, ManifestDigest: request.Plan.Reference.ManifestDigest}
	for _, file := range manifest.Files {
		result.TotalBytes += file.Size
	}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationPrefetch(request, result); err != nil {
		return nil, err
	}
	d.logMigrationTiming(request.Publication.Assignment.OperationID, "image-prefetch", started, "image_bytes", result.TotalBytes)
	return result, nil
}

func (d *nodeRuntime) requireUnusedMigrationDestination(ctx context.Context, registration RuntimeSlotRegistration, target protocol.NodeChannelTarget) error {
	if _, err := d.runner.State(ctx, registration.RunscContainerID); !errdefs.IsNotFound(err) {
		return fmt.Errorf("migration image transfer requires an unused destination: %w", errdefs.ErrFailedPrecondition)
	}
	sessions, err := d.runtime.RecoverySessions()
	if err != nil {
		return err
	}
	for _, session := range sessions {
		if session.Stage.Identity.SlotNonce == target.SlotID || session.Stage.Identity.AllocationID == target.AllocationID {
			return fmt.Errorf("migration image transfer cannot replace attached RootFS custody: %w", errdefs.ErrFailedPrecondition)
		}
	}
	return nil
}

func ensureMigrationStagingDirectory(root string) error {
	if err := os.MkdirAll(root, 0o700); err != nil {
		return err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("migration staging root is not private: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

// The caller holds exclusive slot reconciliation. Physical cache absence is
// fsynced before journaling its tombstone or releasing the staging reservation.
func (d *nodeRuntime) discardMigrationPrefetch(ctx context.Context, slot string, c *MigrationPrefetchCustody) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := d.checkMigrationStaging(false); err != nil {
		return err
	}
	if err := ensureMigrationStagingDirectory(d.journal.migrationRoot); err != nil {
		return err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, c.ImageDirectory); err != nil {
		return err
	}
	return d.journal.recordMigrationPrefetchRemoved(slot, c.RequestDigest)
}

// consumeMigrationPrefetch runs only under ordinary, regionally authorized
// image preparation. A cached receipt alone cannot skip verification against
// the actual regional manifest. An invalid or incomplete cache is disposable.
func (d *nodeRuntime) consumeMigrationPrefetch(ctx context.Context, request protocol.MigrationImagePrepareRequest, destination string) (*runtimecheckpoint.Manifest, error) {
	record, err := d.journal.Get(request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if !record.hasMigrationPrefetch() {
		return nil, nil
	}
	c := record.MigrationStaging.Prefetch
	want, err := request.Publication.Digest()
	if err != nil {
		return nil, err
	}
	var cached *runtimecheckpoint.Manifest
	if c.Receipt != nil && c.Request.Plan.RequestDigest == want && c.Request.Plan.Reference == request.Receipt.Reference &&
		c.Request.Plan.Binding == request.Receipt.Binding && record.matchesMigrationStagingImage(request) == nil {
		runtime := d.runtime.(migrationImageDownloadRuntime)
		manifest, verifyErr := runtime.PrepareMigrationImageFiles(ctx, request.Receipt.Binding, request.Receipt.Reference, c.ImageDirectory, true, d.checkMigrationStagingBudget)
		if verifyErr == nil {
			if err := os.Rename(c.ImageDirectory, destination); err != nil {
				return nil, err
			}
			cached = &manifest
		} else if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	// This also fsyncs the parent after a successful same-directory rename.
	// If the journal write fails, the next preparation safely redownloads.
	if err := d.discardMigrationPrefetch(ctx, request.Target.SlotID, c); err != nil {
		return nil, err
	}
	return cached, nil
}

// A final repair that narrowly outlives the regional observer should not lose
// its retained bytes immediately. Wait only for this process's exact worker;
// normal preparation still verifies the published manifest and cancels/joins
// an unfinished fill after this bounded grace. Cleanup never uses this wait.
const migrationPrefetchPrepareGrace = 150 * time.Millisecond

func (d *nodeRuntime) awaitMigrationPrefetch(ctx context.Context, request protocol.MigrationImagePrepareRequest, record runtimeSlotJournalRecord) error {
	if record.MigrationStaging == nil {
		return nil
	}
	receipt := request.Receipt
	prefetch := protocol.MigrationImagePrefetchRequest{Staging: record.MigrationStaging.Request,
		Publication: request.Publication, Plan: protocol.MigrationPublicationPlan{
			RequestDigest: receipt.RequestDigest, Binding: receipt.Binding, Reference: receipt.Reference, Peer: receipt.Peer}}
	want, err := prefetch.Digest()
	if err != nil {
		return nil
	}
	d.mu.Lock()
	worker := d.migrationPrefetches[request.Target.SlotID]
	if worker == nil || worker.digest != want {
		d.mu.Unlock()
		return nil
	}
	done := worker.done
	d.mu.Unlock()
	timer := time.NewTimer(migrationPrefetchPrepareGrace)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
	case <-timer.C:
	}
	return nil
}
