package nomadruntime

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationImagePreparer interface {
	PrepareMigrationImage(context.Context, protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error)
}

type migrationImageDownloadRuntime interface {
	PrepareMigrationImageFiles(context.Context, runtimecheckpoint.Binding, runtimecheckpoint.Reference, string, bool, func(int64, uint64) error) (runtimecheckpoint.Manifest, error)
}

func (r *rootfsRuntime) PrepareMigrationImageFiles(ctx context.Context, binding runtimecheckpoint.Binding, reference runtimecheckpoint.Reference, directory string, verify bool, admit func(int64, uint64) error) (runtimecheckpoint.Manifest, error) {
	if r == nil || r.checkpoints == nil {
		return runtimecheckpoint.Manifest{}, errdefs.ErrUnavailable
	}
	if verify {
		return r.checkpoints.VerifyLocal(ctx, binding, reference, directory)
	}
	return r.checkpoints.DownloadWithAdmission(ctx, binding, reference, directory, admit)
}

// MigrationDestinationCustody is kept in the existing exclusive slot journal.
// ImageDirectory is derived locally and never appears on the regional channel.
type MigrationDestinationCustody struct {
	Failure        *MigrationFailureStopCustody          `json:"failure,omitempty"`
	Adoption       *MigrationAdoptionCustody             `json:"adoption,omitempty"`
	Restore        *protocol.MigrationRestoreObservation `json:"restore,omitempty"`
	Request        protocol.MigrationImagePrepareRequest `json:"request"`
	RequestDigest  string                                `json:"request_digest"`
	ImageDirectory string                                `json:"image_directory"`
	Prepared       *protocol.MigrationImagePrepared      `json:"prepared,omitempty"`
}

type migrationImagePrepareWorker struct {
	digest string
	done   chan struct{}
	result *protocol.MigrationImagePrepared
	err    error
}

// PrepareMigrationImage owns a bounded download independently of the channel
// connection. All retries share one worker and one durable staging identity.
func (d *nodeRuntime) PrepareMigrationImage(ctx context.Context, request protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error) {
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
	var owned protocol.MigrationImagePrepareRequest
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
	if d.migrationImagePreparations == nil {
		d.migrationImagePreparations = make(map[string]*migrationImagePrepareWorker)
	}
	key := owned.Target.SlotID
	worker := d.migrationImagePreparations[key]
	if worker != nil && worker.digest != want {
		d.mu.Unlock()
		return nil, errdefs.ErrAlreadyExists
	}
	if worker == nil {
		worker = &migrationImagePrepareWorker{digest: want, done: make(chan struct{})}
		d.migrationImagePreparations[key] = worker
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			workerCtx, cancel := context.WithTimeout(parent, 5*time.Minute)
			defer cancel()
			worker.result, worker.err = d.prepareMigrationImage(workerCtx, owned)
			d.mu.Lock()
			delete(d.migrationImagePreparations, key)
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

func (d *nodeRuntime) prepareMigrationImage(ctx context.Context, request protocol.MigrationImagePrepareRequest) (*protocol.MigrationImagePrepared, error) {
	if d.runtime == nil || d.runner == nil {
		return nil, errdefs.ErrUnavailable
	}
	if request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	runtime, ok := d.runtime.(migrationImageDownloadRuntime)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	prior, err := d.journal.Get(request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if (prior.hasMigrationPrefetch() || prior.hasMigrationCapturePeerCache()) && prior.matchesMigrationStagingImage(request) == nil {
		if err := d.awaitMigrationPrefetch(ctx, request, prior); err != nil {
			return nil, err
		}
		// A lost prefetch reply must not strand normal preparation behind a
		// disposable fill. Its own publication authority permits taking over
		// the exact reserved destination after canceling and joining that fill.
		if err := d.beginExternalReconciliation(ctx, request.Target.SlotID); err != nil {
			return nil, err
		}
	} else if !d.beginReconciliation(request.Target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(request.Target.SlotID)
	if err := d.checkMigrationStaging(false); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationDestination(request, nil); err != nil {
		return nil, err
	}
	record, err := d.journal.Get(request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	custody := record.MigrationDestination
	// Retrying an incomplete download may discard only this exact private
	// directory, and only before a target filesystem or container exists.
	if custody.Prepared == nil {
		if err := d.requireUnusedMigrationDestination(ctx, record.Registration, request.Target); err != nil {
			return nil, err
		}
		// Tentative bytes are disposable. Until final repair has transferred
		// them into verified prefetch custody, ordinary preparation removes
		// this exact cache and continues through its published-image fallback.
		if record.hasMigrationCapturePeerCache() {
			if err := d.discardMigrationCapturePeer(ctx, request.Target.SlotID, record.MigrationStaging.CapturePeer); err != nil {
				return nil, err
			}
		}
	}
	if err := ensureMigrationStagingDirectory(d.journal.migrationRoot); err != nil {
		return nil, err
	}
	var cached *runtimecheckpoint.Manifest
	if custody.Prepared == nil {
		if err := os.RemoveAll(custody.ImageDirectory); err != nil {
			return nil, err
		}
	}
	transferStarted := time.Now()
	if custody.Prepared == nil {
		cached, err = d.consumeMigrationPrefetch(ctx, request, custody.ImageDirectory)
		if err != nil {
			return nil, err
		}
	}
	if custody.Prepared == nil && cached == nil {
		if err := d.checkMigrationStaging(true); err != nil {
			return nil, err
		}
	}
	transport := "regional"
	if custody.Prepared != nil {
		transport = "local-verification"
	}
	var manifest runtimecheckpoint.Manifest
	if cached != nil {
		manifest = *cached
		transport = "prefetch"
	}
	peer := cached == nil && custody.Prepared == nil && d.migrationPeer != nil && request.Receipt.Peer != (runtimecheckpoint.PeerEndpoint{}) &&
		runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity) == request.Publication.DestinationPeerCertificateSHA256
	if peer {
		transport = "peer"
		manifest, err = d.receiveMigrationPeer(ctx, request, custody.ImageDirectory)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			// A partial peer image is never mixed with a regional download.
			// The same exclusive, unused destination checks above still apply.
			if err := os.RemoveAll(custody.ImageDirectory); err != nil {
				return nil, err
			}
		}
	}
	if cached == nil && (!peer || err != nil) {
		if peer {
			transport = "regional-fallback"
		}
		manifest, err = runtime.PrepareMigrationImageFiles(ctx, request.Receipt.Binding, request.Receipt.Reference, custody.ImageDirectory, custody.Prepared != nil, d.checkMigrationStagingBudget)
	}
	if err != nil {
		return nil, err
	}
	if err := manifest.Validate(runtimecheckpoint.MaxImageBytes); err != nil || manifest.Binding != request.Receipt.Binding {
		return nil, errdefs.ErrFailedPrecondition
	}
	result := protocol.MigrationImagePrepared{RequestDigest: custody.RequestDigest, ManifestDigest: request.Receipt.Reference.ManifestDigest}
	for _, file := range manifest.Files {
		result.TotalBytes += file.Size
	}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationDestination(request, &result); err != nil {
		return nil, err
	}
	d.logMigrationTiming(request.Publication.Assignment.OperationID, "image-preparation", transferStarted,
		"transport", transport, "image_bytes", result.TotalBytes)
	return &result, nil
}

func (j *runtimeSlotJournal) recordMigrationDestination(request protocol.MigrationImagePrepareRequest, result *protocol.MigrationImagePrepared) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if result != nil {
		if err := result.ValidateFor(request); err != nil {
			return err
		}
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Target.SlotID)))
		if err != nil {
			return err
		}
		if current.Cleanup != nil || current.Proof != nil || current.Migration != nil || current.matchesMigrationDestination(request) != nil {
			return errdefs.ErrFailedPrecondition
		}
		if result != nil && current.hasMigrationCapturePeerCache() {
			return errdefs.ErrFailedPrecondition
		}
		if prior := current.MigrationDestination; prior != nil {
			if prior.Adoption != nil {
				return errdefs.ErrFailedPrecondition
			}
			if prior.RequestDigest != want {
				return errdefs.ErrAlreadyExists
			}
			if prior.Prepared != nil {
				if result != nil && *prior.Prepared != *result {
					return errdefs.ErrAlreadyExists
				}
				return nil
			}
		} else {
			if result != nil {
				return errdefs.ErrFailedPrecondition
			}
			if err := j.checkMigrationStagingWrite(bucket, current, nil, &request); err != nil {
				return err
			}
			retained := 0
			if err := bucket.ForEach(func(_, payload []byte) error {
				record, excluded, err := j.migrationPoolScanRecord(payload)
				if err != nil || excluded {
					return err
				}
				if record.retainsMigrationCountAdmission() {
					retained++
				}
				return nil
			}); err != nil {
				return err
			}
			if retained >= maxMigrationImageCustodies {
				return errdefs.ErrResourceExhausted
			}
			current.MigrationDestination = &MigrationDestinationCustody{Request: request, RequestDigest: want,
				ImageDirectory: filepath.Join(j.migrationRoot, "destination-"+want)}
		}
		current.MigrationDestination.Prepared = result
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

func (r runtimeSlotJournalRecord) matchesMigrationDestination(request protocol.MigrationImagePrepareRequest) error {
	t, registration := request.Target, r.Registration
	if t.SlotID != registration.SlotID || t.ClusterID != registration.ClusterID || t.NodeID != registration.NodeID ||
		t.AllocationID != registration.AllocationID || t.NodeBootID != registration.NodeBootID {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

func (r runtimeSlotJournalRecord) validateMigrationDestination() error {
	c := r.MigrationDestination
	if c == nil {
		return nil
	}
	want, err := c.Request.Digest()
	if err != nil || want != c.RequestDigest || r.matchesMigrationDestination(c.Request) != nil || (r.Migration != nil && !c.Adopted()) ||
		(r.Cleanup != nil && !c.readyForCleanup(*r.Cleanup)) || (r.Proof != nil && r.Cleanup == nil) ||
		!filepath.IsAbs(c.ImageDirectory) || filepath.Clean(c.ImageDirectory) != c.ImageDirectory || filepath.Base(c.ImageDirectory) != "destination-"+want {
		return errdefs.ErrFailedPrecondition
	}
	if err := c.validateRestore(); err != nil {
		return err
	}
	if err := c.validateFailure(); err != nil {
		return err
	}
	if c.Failure != nil && c.Failure.Cleanup != nil && c.Failure.Cleanup.Finalization != nil &&
		(r.Proof == nil || c.Failure.Cleanup.Finalization.Request.Proof.Cleanup != *r.Proof) {
		return errdefs.ErrFailedPrecondition
	}
	if c.Prepared != nil {
		return c.Prepared.ValidateFor(c.Request)
	}
	return nil
}
