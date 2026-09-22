package nomadruntime

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

// MigrationCapturePeerCustody retains regional peer authority before either
// side opens a stream. Only the destination owns a private cache path. Removed
// is a durable absence tombstone, never an expiry or a successful RPC alone.
type MigrationCapturePeerCustody struct {
	Request        protocol.MigrationCapturePeerRequest `json:"request"`
	RequestDigest  string                               `json:"request_digest"`
	ImageDirectory string                               `json:"image_directory,omitempty"`
	Removed        bool                                 `json:"removed"`
}

func (r runtimeSlotJournalRecord) hasMigrationCapturePeerCache() bool {
	return r.MigrationStaging != nil && r.MigrationStaging.CapturePeer != nil &&
		!r.MigrationStaging.CapturePeer.Request.Staging.IsSource() && !r.MigrationStaging.CapturePeer.Removed
}

func (r runtimeSlotJournalRecord) validateMigrationCapturePeer() error {
	if r.MigrationStaging == nil || r.MigrationStaging.CapturePeer == nil {
		return nil
	}
	s, c := r.MigrationStaging, r.MigrationStaging.CapturePeer
	want, err := c.Request.Digest()
	local := protocol.MigrationStagingReserved{RequestDigest: s.RequestDigest, PeerCertificateSHA256: s.PeerCertificateSHA256, Peer: s.Peer}
	if err != nil || want != c.RequestDigest || c.Request.Staging != s.Request || !s.Ready || c.Request.LocalReceipt() != local {
		return fmt.Errorf("early peer changed reserved authority: %w", errdefs.ErrFailedPrecondition)
	}
	if s.Request.IsSource() {
		if c.ImageDirectory != "" || c.Removed {
			return errdefs.ErrFailedPrecondition
		}
		return nil
	}
	if !filepath.IsAbs(c.ImageDirectory) || filepath.Clean(c.ImageDirectory) != c.ImageDirectory || filepath.Base(c.ImageDirectory) != "capture-peer-"+want {
		return errdefs.ErrFailedPrecondition
	}
	if !c.Removed && (s.Released || r.Cleanup != nil || r.Proof != nil || r.Migration != nil ||
		r.MigrationDestination != nil && (r.MigrationDestination.Prepared != nil || r.MigrationDestination.Restore != nil)) {
		return fmt.Errorf("early peer still owns its cache path: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func (j *runtimeSlotJournal) recordMigrationCapturePeer(request protocol.MigrationCapturePeerRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		r, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Staging.Target.SlotID)))
		if err != nil {
			return err
		}
		s := r.MigrationStaging
		if s == nil || !s.Ready || s.Released || s.Request != request.Staging || r.matchesMigrationStaging(request.Staging) != nil ||
			r.Cleanup != nil || r.Proof != nil {
			return errdefs.ErrFailedPrecondition
		}
		local := protocol.MigrationStagingReserved{RequestDigest: s.RequestDigest, PeerCertificateSHA256: s.PeerCertificateSHA256, Peer: s.Peer}
		if local != request.LocalReceipt() {
			return errdefs.ErrFailedPrecondition
		}
		if c := s.CapturePeer; c != nil {
			if c.RequestDigest != want {
				return errdefs.ErrAlreadyExists
			}
			if c.Removed {
				return errdefs.ErrFailedPrecondition
			}
			return nil
		}
		// A late new grant cannot retroactively acquire an active capture or
		// take over a destination already preparing a different image path.
		if r.Migration != nil || s.Prefetch != nil ||
			r.MigrationDestination != nil && (!request.Staging.IsSource() || !r.MigrationDestination.Adopted()) {
			return errdefs.ErrFailedPrecondition
		}
		// A successfully adopted target becomes the source of a later move.
		// Keep its historical proof and bind the new grant to that exact live
		// runtime, just as the subsequent capture intent must do.
		if request.Staging.IsSource() {
			if err := r.matchesMigration(protocol.MigrationCapture{Request: request.Staging.Source}); err != nil {
				return err
			}
		}
		if err := j.checkMigrationStagingPool(bucket, request.Staging.Target.SlotID); err != nil {
			return err
		}
		c := &MigrationCapturePeerCustody{Request: request, RequestDigest: want}
		if !request.Staging.IsSource() {
			c.ImageDirectory = filepath.Join(j.migrationRoot, "capture-peer-"+want)
		}
		s.CapturePeer = c
		r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, r)
	})
}

// The caller must hold slot reconciliation, join all users of the cache, and
// fsync its physical absence (or transfer into ordinary image custody) first.
func (j *runtimeSlotJournal) recordMigrationCapturePeerRemoved(slot, requestDigest string) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		r, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(slot)))
		if err != nil {
			return err
		}
		if r.MigrationStaging == nil || r.MigrationStaging.CapturePeer == nil {
			return errdefs.ErrFailedPrecondition
		}
		c := r.MigrationStaging.CapturePeer
		if c.RequestDigest != requestDigest || c.Request.Staging.IsSource() {
			return errdefs.ErrFailedPrecondition
		}
		c.Removed = true
		r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, r)
	})
}
