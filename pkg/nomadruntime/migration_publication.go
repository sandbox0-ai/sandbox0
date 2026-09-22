package nomadruntime

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationImagePublisher interface {
	PublishMigration(context.Context, protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error)
}

type migrationImageRuntime interface {
	PublishMigrationImage(context.Context, runtimecheckpoint.Binding, string) (runtimecheckpoint.Reference, error)
}

func (r *rootfsRuntime) PublishMigrationImage(ctx context.Context, binding runtimecheckpoint.Binding, directory string) (runtimecheckpoint.Reference, error) {
	if r == nil || r.checkpoints == nil {
		return runtimecheckpoint.Reference{}, errdefs.ErrUnavailable
	}
	return r.checkpoints.Publish(ctx, binding, directory)
}

type migrationPublicationWorker struct {
	digest    string
	done      chan struct{}
	result    *protocol.MigrationPublication
	err       error
	peer      *migrationPublicationPeerRead
	planReady chan struct{}
	plan      *protocol.MigrationPublicationPlan
}

// PublishMigration shares one bounded worker across authenticated stream
// rotation. Daemon shutdown cancels it; losing a response cannot truncate every
// large transfer at the channel's shorter connection lifetime.
func (d *nodeRuntime) PublishMigration(ctx context.Context, request protocol.MigrationPublicationRequest) (*protocol.MigrationPublication, error) {
	worker, err := d.startMigrationPublication(ctx, request)
	if err != nil {
		return nil, err
	}
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

func (d *nodeRuntime) startMigrationPublication(ctx context.Context, request protocol.MigrationPublicationRequest) (*migrationPublicationWorker, error) {
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
	// The worker may outlive this call. Own the descriptor and assignment maps
	// instead of retaining memory that a canceled caller can reuse or modify.
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	var owned protocol.MigrationPublicationRequest
	if err := json.Unmarshal(payload, &owned); err != nil {
		return nil, err
	}
	request = owned
	d.mu.Lock()
	parent := d.migrationContext
	if parent == nil {
		parent = context.Background()
	}
	if parent.Err() != nil {
		d.mu.Unlock()
		return nil, parent.Err()
	}
	if d.migrationPublications == nil {
		d.migrationPublications = make(map[string]*migrationPublicationWorker)
	}
	key := request.Capture.Request.Target.SlotID
	worker := d.migrationPublications[key]
	if worker != nil && worker.digest != want {
		d.mu.Unlock()
		return nil, errdefs.ErrAlreadyExists
	}
	if worker == nil {
		worker = &migrationPublicationWorker{digest: want, done: make(chan struct{}), planReady: make(chan struct{})}
		d.migrationPublications[key] = worker
		d.wg.Add(1)
		go func() {
			defer d.wg.Done()
			workerCtx, cancel := context.WithTimeout(parent, 5*time.Minute)
			defer cancel()
			worker.result, worker.err = d.publishMigration(workerCtx, request, worker)
			d.mu.Lock()
			delete(d.migrationPublications, key)
			close(worker.done)
			d.mu.Unlock()
		}()
	}
	d.mu.Unlock()
	return worker, nil
}

// publishMigration keeps sole source custody while uploading through ctld's
// existing encrypted regional store. Neither a caller-supplied path nor a
// generic filesystem snapshot may substitute for the journaled source cut.
func (d *nodeRuntime) publishMigration(ctx context.Context, request protocol.MigrationPublicationRequest, worker *migrationPublicationWorker) (*protocol.MigrationPublication, error) {
	requestDigest, err := request.Digest()
	if err != nil {
		return nil, err
	}
	capture := request.Capture.Request
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil {
		return nil, errdefs.ErrUnavailable
	}
	if capture.Target.ClusterID != d.clusterID || capture.Target.NodeID != d.nodeID || capture.Target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	runtime, ok := d.runtime.(migrationImageRuntime)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	if !d.beginReconciliation(capture.Target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(capture.Target.SlotID)
	custody, err := d.GetMigrationCapture(ctx, capture.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if custody == nil || custody.ExecutionInvalidated || custody.Capture.State != protocol.MigrationCaptureComplete ||
		custody.Capture.Request != capture || custody.Capture.RootFS == nil || custody.Capture.RootFS.Digest != request.Capture.RootFS.Digest {
		return nil, fmt.Errorf("image publication requires the retained source cut: %w", errdefs.ErrFailedPrecondition)
	}
	session, err := d.migrationSourceSession(capture)
	if err != nil {
		return nil, err
	}
	if err := custody.Capture.RootFS.ValidateFor(session.Stage, custody.Capture.RootFS.Request); err != nil {
		return nil, err
	}
	if err := d.stopMigrationSource(ctx, *session, false); err != nil {
		return nil, err
	}
	source := request.Assignment.Target
	source.RuntimeGeneration = request.Assignment.SourceGeneration
	binding, err := runtimecheckpoint.Bind(capture.OperationID, session.Stage, source,
		request.CompatibilityDigest, request.CPUFeaturesDigest, custody.Capture.RootFS.Generation)
	if err != nil {
		return nil, err
	}
	expected, err := request.Binding()
	if err != nil || binding != expected {
		return nil, errdefs.ErrFailedPrecondition
	}
	if err := d.journal.recordMigrationPublication(request, nil); err != nil {
		return nil, err
	}
	if custody.Publication != nil {
		if err := custody.Publication.ValidateFor(request); err != nil {
			return nil, err
		}
		return custody.Publication, nil
	}
	publicationStarted := time.Now()
	var openElapsed, planElapsed, publishElapsed, sourceCheckElapsed, journalElapsed time.Duration
	stepStarted := publicationStarted
	var reference runtimecheckpoint.Reference
	manifestVersion := runtimecheckpoint.ManifestVersion
	published := false
	uploadRuntime, stage, err := d.publicationCaptureUpload(ctx, capture, binding)
	openElapsed = time.Since(stepStarted)
	if err != nil {
		return nil, err
	}
	if stage != nil {
		manifestVersion = runtimecheckpoint.StagedManifestVersion
		stepStarted = time.Now()
		plan, planErr := uploadRuntime.PlanMigrationCaptureUpload(ctx, stage, binding, custody.ImageDirectory)
		planElapsed = time.Since(stepStarted)
		if planErr != nil {
			return nil, planErr
		}
		if d.migrationPeer != nil && request.DestinationPeerCertificateSHA256 != "" {
			finish := d.shareMigrationPublication(ctx, worker, request, plan, custody.ImageDirectory)
			defer func() { finish(published) }()
		}
		stepStarted = time.Now()
		reference, err = stage.PublishPlanned(ctx, binding, plan, custody.ImageDirectory)
		publishElapsed = time.Since(stepStarted)
	} else if planned, ok := d.runtime.(migrationPlannedImageRuntime); ok && d.migrationPeer != nil && request.DestinationPeerCertificateSHA256 != "" {
		stepStarted = time.Now()
		plan, planErr := planned.PlanMigrationImage(ctx, binding, custody.ImageDirectory)
		planElapsed = time.Since(stepStarted)
		if planErr != nil {
			return nil, planErr
		}
		// The publication worker retains the exclusive slot until every peer
		// reader has left. Neither a completed stream nor this local plan is
		// a publication receipt or permission to execute on the destination.
		finish := d.shareMigrationPublication(ctx, worker, request, plan, custody.ImageDirectory)
		defer func() { finish(published) }()
		stepStarted = time.Now()
		reference, err = planned.PublishPlannedMigrationImage(ctx, binding, plan, custody.ImageDirectory)
		publishElapsed = time.Since(stepStarted)
	} else {
		stepStarted = time.Now()
		reference, err = runtime.PublishMigrationImage(ctx, binding, custody.ImageDirectory)
		publishElapsed = time.Since(stepStarted)
	}
	if err != nil {
		return nil, err
	}
	result := &protocol.MigrationPublication{RequestDigest: requestDigest, Binding: binding, Reference: reference}
	if d.migrationPeer != nil && request.DestinationPeerCertificateSHA256 != "" {
		result.Peer = d.migrationPeer.endpoint
	}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	// The source must still be stopped when the receipt becomes durable. An
	// unexpected execution observation cannot be repaired by sealing newer disk.
	stepStarted = time.Now()
	if err := d.stopMigrationSource(ctx, *session, false); err != nil {
		return nil, err
	}
	sourceCheckElapsed = time.Since(stepStarted)
	stepStarted = time.Now()
	if err := d.journal.recordMigrationPublication(request, result); err != nil {
		return nil, err
	}
	journalElapsed = time.Since(stepStarted)
	published = true
	d.logMigrationTiming(request.Assignment.OperationID, "image-publication", publicationStarted, "manifest_version", manifestVersion,
		"upload_open_us", openElapsed.Microseconds(), "plan_us", planElapsed.Microseconds(),
		"objects_publish_us", publishElapsed.Microseconds(), "source_check_us", sourceCheckElapsed.Microseconds(), "journal_us", journalElapsed.Microseconds())
	return result, nil
}

// Persist the publication identity before the first object upload. Interrupted
// retries can finish this exact immutable manifest, never choose another CPU
// binding or RootFS generation after some chunks have already been stored.
func (j *runtimeSlotJournal) recordMigrationPublication(request protocol.MigrationPublicationRequest, result *protocol.MigrationPublication) error {
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
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Capture.Request.Target.SlotID)))
		if err != nil {
			return err
		}
		custody := current.Migration
		if custody == nil || custody.ExecutionInvalidated || custody.Capture.State != protocol.MigrationCaptureComplete ||
			custody.Capture.Request != request.Capture.Request || custody.Capture.RootFS == nil ||
			custody.Capture.RootFS.Digest != request.Capture.RootFS.Digest {
			return errdefs.ErrFailedPrecondition
		}
		if custody.PublicationRequest != nil {
			prior, err := custody.PublicationRequest.Digest()
			if err != nil || prior != want {
				return errdefs.ErrAlreadyExists
			}
		}
		if custody.Publication != nil {
			if result != nil && *custody.Publication != *result {
				return errdefs.ErrAlreadyExists
			}
			return nil
		}
		custody.PublicationRequest, custody.Publication = &request, result
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

func (c MigrationCaptureCustody) validatePublication() error {
	if c.PublicationRequest == nil {
		if c.Publication != nil {
			return fmt.Errorf("image receipt has no publication intent")
		}
		return nil
	}
	if _, err := c.PublicationRequest.Digest(); err != nil {
		return err
	}
	if c.Capture.State != protocol.MigrationCaptureComplete || c.Capture.Request != c.PublicationRequest.Capture.Request ||
		c.Capture.RootFS == nil || c.Capture.RootFS.Digest != c.PublicationRequest.Capture.RootFS.Digest {
		return fmt.Errorf("publication intent changed its retained source cut")
	}
	if c.Publication != nil {
		return c.Publication.ValidateFor(*c.PublicationRequest)
	}
	return nil
}
