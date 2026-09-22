package nomadruntime

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

// MigrationStagingCustody uses the existing exclusive ctld journal. Intent
// excludes competing image writers even when quota verification is interrupted.
// Neither connection loss nor elapsed time releases this reservation.
type MigrationStagingCustody struct {
	CapturePeer           *MigrationCapturePeerCustody     `json:"capture_peer,omitempty"`
	Peer                  runtimecheckpoint.PeerEndpoint   `json:"peer,omitzero"`
	Prefetch              *MigrationPrefetchCustody        `json:"prefetch,omitempty"`
	PeerCertificateSHA256 string                           `json:"peer_certificate_sha256,omitempty"`
	Request               protocol.MigrationStagingRequest `json:"request"`
	RequestDigest         string                           `json:"request_digest"`
	Ready                 bool                             `json:"ready"`
	Released              bool                             `json:"released"`
}

func (d *nodeRuntime) ReserveMigrationStaging(ctx context.Context, request protocol.MigrationStagingRequest) (result *protocol.MigrationStagingReserved, resultErr error) {
	if err := d.validateMigrationStagingRequest(ctx, request); err != nil {
		return nil, err
	}
	started := time.Now()
	var intentElapsed, budgetElapsed, readyElapsed time.Duration
	defer func() {
		d.logMigrationTiming(request.Source.OperationID, "staging-reservation", started,
			"success", resultErr == nil, "intent_us", intentElapsed.Microseconds(),
			"budget_us", budgetElapsed.Microseconds(), "ready_us", readyElapsed.Microseconds())
	}()
	if !d.beginReconciliation(request.Target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(request.Target.SlotID)
	certificate := ""
	var endpoint runtimecheckpoint.PeerEndpoint
	if d.migrationPeer != nil {
		certificate = runtimecheckpoint.PeerCertificateDigest(d.migrationPeer.identity)
		endpoint = d.migrationPeer.endpoint
	}
	stepStarted := time.Now()
	custody, err := d.journal.reserveMigrationStagingWithEndpoint(request, false, certificate, endpoint)
	intentElapsed = time.Since(stepStarted)
	if err != nil {
		return nil, err
	}
	if custody.Ready {
		// A retry may follow allocation of the admitted bytes. Do not charge
		// them a second time, but still require the exact enforced pool.
		if err := d.checkMigrationStaging(false); err != nil {
			return nil, err
		}
	} else {
		stepStarted = time.Now()
		err = d.checkMigrationStagingBudget(request.Bytes, request.Inodes)
		budgetElapsed = time.Since(stepStarted)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		stepStarted = time.Now()
		custody, err = d.journal.reserveMigrationStaging(request, true)
		readyElapsed = time.Since(stepStarted)
		if err != nil {
			return nil, err
		}
	}
	return &protocol.MigrationStagingReserved{RequestDigest: custody.RequestDigest, PeerCertificateSHA256: custody.PeerCertificateSHA256, Peer: custody.Peer}, nil
}

// ReleaseMigrationStaging requires an exact regional cancellation/finalization
// decision. Local expiry is not proof that a capture command was never sent.
// The journal also refuses release while either execution image remains owned.
func (d *nodeRuntime) ReleaseMigrationStaging(ctx context.Context, request protocol.MigrationStagingRequest) error {
	if err := d.validateMigrationStagingRequest(ctx, request); err != nil {
		return err
	}
	record, err := d.journal.Get(request.Target.SlotID)
	if err != nil {
		return err
	}
	if (record.hasMigrationPrefetch() || record.hasMigrationCapturePeerCache()) && record.MigrationStaging.Request == request {
		if err := d.beginExternalReconciliation(ctx, request.Target.SlotID); err != nil {
			return err
		}
	} else if !d.beginReconciliation(request.Target.SlotID, nil) {
		return errdefs.ErrUnavailable
	}
	defer d.endReconciliation(request.Target.SlotID)
	record, err = d.journal.Get(request.Target.SlotID)
	if err != nil {
		return err
	}
	if record.hasMigrationCapturePeerCache() && record.MigrationStaging.Request == request {
		if err := d.discardMigrationCapturePeer(ctx, request.Target.SlotID, record.MigrationStaging.CapturePeer); err != nil {
			return err
		}
	}
	if record.hasMigrationPrefetch() && record.MigrationStaging.Request == request {
		if err := d.discardMigrationPrefetch(ctx, request.Target.SlotID, record.MigrationStaging.Prefetch); err != nil {
			return err
		}
	}
	return d.journal.releaseMigrationStaging(request)
}

func (d *nodeRuntime) validateMigrationStagingRequest(ctx context.Context, request protocol.MigrationStagingRequest) error {
	if err := request.Validate(); err != nil {
		return err
	}
	if d == nil || d.journal == nil {
		return errdefs.ErrUnavailable
	}
	if request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID {
		return errdefs.ErrPermissionDenied
	}
	return ctx.Err()
}

func (j *runtimeSlotJournal) reserveMigrationStaging(request protocol.MigrationStagingRequest, ready bool) (*MigrationStagingCustody, error) {
	return j.reserveMigrationStagingWithPeer(request, ready, "")
}

// Persist the first peer key (including disabled) with the reservation intent.
// Retries after restart must not change the receipt already held by the region.
func (j *runtimeSlotJournal) reserveMigrationStagingWithPeer(request protocol.MigrationStagingRequest, ready bool, certificate string) (*MigrationStagingCustody, error) {
	return j.reserveMigrationStagingWithEndpoint(request, ready, certificate, runtimecheckpoint.PeerEndpoint{})
}

func (j *runtimeSlotJournal) reserveMigrationStagingWithEndpoint(request protocol.MigrationStagingRequest, ready bool, certificate string, endpoint runtimecheckpoint.PeerEndpoint) (*MigrationStagingCustody, error) {
	if certificate != "" {
		if err := runtimecheckpoint.ValidatePeerCertificateDigest(certificate); err != nil {
			return nil, err
		}
	}
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if err := (protocol.MigrationStagingReserved{RequestDigest: want, PeerCertificateSHA256: certificate, Peer: endpoint}).ValidateFor(request); err != nil {
		return nil, err
	}
	var result *MigrationStagingCustody
	err = j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Target.SlotID)))
		if err != nil {
			return err
		}
		if record.matchesMigrationStaging(request) != nil || record.Cleanup != nil || record.Proof != nil {
			return errdefs.ErrFailedPrecondition
		}
		prior := record.MigrationStaging
		if prior != nil && prior.RequestDigest == want {
			if prior.Released {
				return errdefs.ErrFailedPrecondition
			}
			// An already-ready exact reservation remains readable after capture.
			if prior.Ready {
				copy := *prior
				result = &copy
				return nil
			}
		} else {
			if prior != nil && !canReplaceMigrationStaging(*prior, request) {
				return errdefs.ErrAlreadyExists
			}
			if ready || record.Migration != nil || record.hasMigrationImageCustody() || record.MigrationDestination != nil && !request.IsSource() {
				return errdefs.ErrFailedPrecondition
			}
			record.MigrationStaging = &MigrationStagingCustody{Request: request, RequestDigest: want, PeerCertificateSHA256: certificate, Peer: endpoint}
		}
		// Bolt serializes this scan with first source/destination image intent.
		// Older image commands without reservations cannot race past it.
		if err := j.checkMigrationStagingPool(bucket, request.Target.SlotID); err != nil {
			return err
		}
		record.MigrationStaging.Ready = ready
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		if err := putRuntimeSlotJournalRecord(bucket, record); err != nil {
			return err
		}
		copy := *record.MigrationStaging
		result = &copy
		return nil
	})
	return result, err
}

func (j *runtimeSlotJournal) releaseMigrationStaging(request protocol.MigrationStagingRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Target.SlotID)))
		if err != nil {
			return err
		}
		c := record.MigrationStaging
		if record.matchesMigrationStaging(request) != nil {
			return errdefs.ErrFailedPrecondition
		}
		if c == nil && record.Proof != nil && !record.hasMigrationImageCustody() {
			return nil // Terminal physical custody already rejects any late reserve.
		}
		if c != nil && c.RequestDigest != want && c.Request.Target == request.Target &&
			c.Request.Source.SandboxID == request.Source.SandboxID && c.Request.Source.OperationID != request.Source.OperationID &&
			c.Request.Source.LifecycleEpoch > request.Source.LifecycleEpoch {
			// A higher epoch makes the old reserve permanently inadmissible.
			// Acknowledge a lost release reply without modifying newer custody.
			return nil
		}
		if c == nil || c.RequestDigest != want {
			// Cancellation may overtake the first reserve on another channel
			// connection. Retain a tombstone instead of returning "not found"
			// and allowing the delayed command to acquire untracked custody.
			if record.Cleanup != nil || record.Proof != nil || record.Migration != nil || record.hasMigrationImageCustody() ||
				record.MigrationDestination != nil && !request.IsSource() || c != nil && !canReplaceMigrationStaging(*c, request) {
				return errdefs.ErrFailedPrecondition
			}
			c = &MigrationStagingCustody{Request: request, RequestDigest: want}
			record.MigrationStaging = c
		}
		if c.Released {
			return nil
		}
		if record.hasMigrationImageCustody() {
			return fmt.Errorf("migration images still own staging: %w", errdefs.ErrFailedPrecondition)
		}
		c.Released = true
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}

// A physical carrier is never reused for another sandbox. After cancellation
// only a later lifecycle of the same live source can acquire this pool again.
// Retaining its epoch prevents delayed reserve/release messages from replacing
// a newer tombstone after otherwise successful cleanup of an unused request.
func canReplaceMigrationStaging(prior MigrationStagingCustody, next protocol.MigrationStagingRequest) bool {
	return prior.Released && next.IsSource() && next.Target == prior.Request.Target &&
		next.Source.SandboxID == prior.Request.Source.SandboxID && next.Source.OperationID != prior.Request.Source.OperationID &&
		next.Source.LifecycleEpoch > prior.Request.Source.LifecycleEpoch
}

// The legacy count admission and exclusive reservations share one journal.
// Historical adoption receipts still consume count admission, but no longer
// own image space after their exact image-absence proof has been persisted.
func (r runtimeSlotJournalRecord) hasMigrationImageCustody() bool {
	return r.hasMigrationCapturePeerCache() || r.hasMigrationPrefetch() || r.Migration != nil && !r.Migration.CaptureFailureFinalized() && (r.Migration.Finalization == nil || !r.Migration.Finalization.ImageAbsent) ||
		r.MigrationDestination != nil && !r.MigrationDestination.Adopted() && !r.MigrationDestination.FailureFinalized()
}

func (r runtimeSlotJournalRecord) retainsMigrationCountAdmission() bool {
	return r.hasMigrationCapturePeerCache() || r.hasMigrationPrefetch() || r.Migration != nil && !r.Migration.CaptureFailureFinalized() && (r.Migration.Finalization == nil || !r.Migration.Finalization.ImageAbsent) ||
		r.MigrationDestination.pendingCustody()
}

func (j *runtimeSlotJournal) checkMigrationStagingPool(bucket *bolt.Bucket, ownSlot string) error {
	retained := 0
	err := bucket.ForEach(func(key, payload []byte) error {
		record, excluded, err := j.migrationPoolScanRecord(payload)
		if err != nil || excluded {
			return err
		}
		if record.retainsMigrationCountAdmission() {
			retained++
		}
		if string(key) == ownSlot {
			return nil
		}
		if record.hasMigrationImageCustody() || record.MigrationStaging != nil && !record.MigrationStaging.Released && record.Proof == nil {
			return fmt.Errorf("another migration owns the staging pool: %w", errdefs.ErrResourceExhausted)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if retained >= maxMigrationImageCustodies {
		return fmt.Errorf("migration receipt custody admission is exhausted: %w", errdefs.ErrResourceExhausted)
	}
	return nil
}

func (r runtimeSlotJournalRecord) matchesMigrationStaging(request protocol.MigrationStagingRequest) error {
	t, registration := request.Target, r.Registration
	if t.SlotID != registration.SlotID || t.ClusterID != registration.ClusterID || t.NodeID != registration.NodeID ||
		t.AllocationID != registration.AllocationID || t.NodeBootID != registration.NodeBootID {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

func (r runtimeSlotJournalRecord) validateMigrationStaging() error {
	c := r.MigrationStaging
	if c == nil {
		return nil
	}
	if err := (protocol.MigrationStagingReserved{RequestDigest: c.RequestDigest, PeerCertificateSHA256: c.PeerCertificateSHA256, Peer: c.Peer}).ValidateFor(c.Request); err != nil {
		return err
	}
	if c.PeerCertificateSHA256 != "" {
		if err := runtimecheckpoint.ValidatePeerCertificateDigest(c.PeerCertificateSHA256); err != nil {
			return err
		}
	}
	want, err := c.Request.Digest()
	if err != nil || want != c.RequestDigest || r.matchesMigrationStaging(c.Request) != nil || c.Released && r.hasMigrationImageCustody() {
		return errdefs.ErrFailedPrecondition
	}
	if r.Migration != nil {
		if !c.Ready || !c.Request.IsSource() || c.Request.Source != r.Migration.Capture.Request {
			return errdefs.ErrFailedPrecondition
		}
	}
	if r.MigrationDestination != nil && !r.MigrationDestination.Adopted() && !r.MigrationDestination.FailureFinalized() {
		if !c.Ready || c.Request.IsSource() || r.matchesMigrationStagingImage(r.MigrationDestination.Request) != nil {
			return errdefs.ErrFailedPrecondition
		}
	}
	return nil
}

func (r runtimeSlotJournalRecord) matchesMigrationStagingImage(request protocol.MigrationImagePrepareRequest) error {
	c := r.MigrationStaging
	if c == nil {
		return nil // Existing custody remains recoverable during staged rollout.
	}
	resources, err := request.Resources.Digest()
	if err != nil || !c.Ready || c.Released || c.Request.IsSource() || c.Request.Target != request.Target ||
		c.Request.Source != request.Publication.Capture.Request || c.Request.DestinationResourceLeaseDigest != strings.TrimPrefix(resources, "sha256:") {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

// checkMigrationStagingWrite is called in the transaction that first records
// image custody. Exact outcome retries do not need new admission. Legacy first
// writes are excluded by any durable reservation, including an unfinished one.
func (j *runtimeSlotJournal) checkMigrationStagingWrite(bucket *bolt.Bucket, record runtimeSlotJournalRecord, source *protocol.MigrationCaptureRequest, image *protocol.MigrationImagePrepareRequest) error {
	if c := record.MigrationStaging; c != nil {
		if !c.Ready || c.Released {
			return errdefs.ErrFailedPrecondition
		}
		if source != nil && (!c.Request.IsSource() || c.Request.Source != *source) {
			return errdefs.ErrFailedPrecondition
		}
		if image != nil {
			if err := record.matchesMigrationStagingImage(*image); err != nil {
				return err
			}
		}
		return j.checkMigrationStagingPool(bucket, record.Registration.SlotID)
	}
	return bucket.ForEach(func(_, payload []byte) error {
		other, excluded, err := j.migrationPoolScanRecord(payload)
		if err != nil || excluded {
			return err
		}
		if other.MigrationStaging != nil && !other.MigrationStaging.Released && other.Proof == nil {
			return errdefs.ErrResourceExhausted
		}
		return nil
	})
}
