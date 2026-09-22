package nomadruntime

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

// MigrationPrefetchCustody is subordinate to the exact staging reservation.
// Removed records durable cache-path absence, including a rename into normal
// destination custody. It prevents a delayed prefetch from recreating that path.
type MigrationPrefetchCustody struct {
	Request        protocol.MigrationImagePrefetchRequest `json:"request"`
	RequestDigest  string                                 `json:"request_digest"`
	ImageDirectory string                                 `json:"image_directory"`
	Receipt        *protocol.MigrationImagePrefetched     `json:"receipt,omitempty"`
	Removed        bool                                   `json:"removed"`
}

func (r runtimeSlotJournalRecord) hasMigrationPrefetch() bool {
	return r.MigrationStaging != nil && r.MigrationStaging.Prefetch != nil && !r.MigrationStaging.Prefetch.Removed
}

func (r runtimeSlotJournalRecord) validateMigrationPrefetch() error {
	if r.MigrationStaging == nil || r.MigrationStaging.Prefetch == nil {
		return nil
	}
	s, c := r.MigrationStaging, r.MigrationStaging.Prefetch
	want, err := c.Request.Digest()
	if err != nil || want != c.RequestDigest || c.Request.Staging != s.Request || !s.Ready ||
		s.PeerCertificateSHA256 != c.Request.Publication.DestinationPeerCertificateSHA256 ||
		!filepath.IsAbs(c.ImageDirectory) || filepath.Clean(c.ImageDirectory) != c.ImageDirectory ||
		filepath.Base(c.ImageDirectory) != "prefetch-"+want || s.Released && !c.Removed {
		return fmt.Errorf("invalid migration prefetch custody: %w", errdefs.ErrFailedPrecondition)
	}
	if c.Receipt != nil && c.Receipt.ValidateFor(c.Request) != nil {
		return errdefs.ErrFailedPrecondition
	}
	if !c.Removed && (r.Cleanup != nil || r.Proof != nil || r.Migration != nil ||
		r.MigrationDestination != nil && (r.MigrationDestination.Prepared != nil || r.MigrationDestination.Restore != nil)) {
		return fmt.Errorf("prefetch cache still owns its private path: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}

func (j *runtimeSlotJournal) recordMigrationPrefetch(request protocol.MigrationImagePrefetchRequest, receipt *protocol.MigrationImagePrefetched) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if receipt != nil {
		if err := receipt.ValidateFor(request); err != nil {
			return err
		}
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
			s.PeerCertificateSHA256 != request.Publication.DestinationPeerCertificateSHA256 ||
			r.Cleanup != nil || r.Proof != nil || r.Migration != nil || r.MigrationDestination != nil {
			return errdefs.ErrFailedPrecondition
		}
		if c := s.Prefetch; c != nil {
			if c.RequestDigest != want {
				return errdefs.ErrAlreadyExists
			}
			if c.Removed {
				return errdefs.ErrFailedPrecondition
			}
			if c.Receipt != nil {
				if receipt != nil && *receipt != *c.Receipt {
					return errdefs.ErrAlreadyExists
				}
				return nil
			}
		} else {
			if receipt != nil {
				return errdefs.ErrFailedPrecondition
			}
			if err := j.checkMigrationStagingPool(bucket, request.Staging.Target.SlotID); err != nil {
				return err
			}
			s.Prefetch = &MigrationPrefetchCustody{Request: request, RequestDigest: want,
				ImageDirectory: filepath.Join(j.migrationRoot, "prefetch-"+want)}
		}
		s.Prefetch.Receipt = receipt
		r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, r)
	})
}

// Called only while holding slot reconciliation after synchronizing physical
// cache-path absence. It does not release the enclosing staging reservation.
func (j *runtimeSlotJournal) recordMigrationPrefetchRemoved(slot, digest string) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		r, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(slot)))
		if err != nil {
			return err
		}
		if r.MigrationStaging == nil || r.MigrationStaging.Prefetch == nil || r.MigrationStaging.Prefetch.RequestDigest != digest {
			return errdefs.ErrFailedPrecondition
		}
		r.MigrationStaging.Prefetch.Removed = true
		r.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, r)
	})
}
