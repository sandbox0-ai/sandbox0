package nomadruntime

import (
	"context"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationFailureStopCustody struct {
	Request       protocol.MigrationFailureRequest    `json:"request"`
	RequestDigest string                              `json:"request_digest"`
	Proof         *protocol.MigrationFailureStopProof `json:"proof,omitempty"`
	Cleanup       *MigrationFailureCleanupCustody     `json:"cleanup,omitempty"`
}

func (c *MigrationDestinationCustody) validateFailure() error {
	if c == nil || c.Failure == nil {
		return nil
	}
	f := c.Failure
	want, err := f.Request.Digest()
	image, ie := f.Request.Restore.Image.Digest()
	if err != nil || ie != nil || want != f.RequestDigest || image != c.RequestDigest || c.Adoption != nil ||
		c.Prepared == nil || *c.Prepared != f.Request.Restore.Prepared {
		return errdefs.ErrFailedPrecondition
	}
	if c.Restore != nil {
		restore, err := f.Request.Restore.Digest()
		if err != nil || restore != c.Restore.RequestDigest {
			return errdefs.ErrFailedPrecondition
		}
	}
	if f.Proof != nil && f.Proof.ValidateFor(f.Request) != nil {
		return errdefs.ErrFailedPrecondition
	}
	if f.Cleanup != nil {
		want, err := f.Cleanup.Request.Digest()
		if err != nil || want != f.Cleanup.RequestDigest || f.Proof == nil ||
			f.Cleanup.Request.Failure.Proof != *f.Proof {
			return errdefs.ErrFailedPrecondition
		}
		if final := f.Cleanup.Finalization; final != nil {
			want, err := final.Request.Digest()
			if err != nil || want != final.RequestDigest || final.Request.Proof.ValidateFor(f.Cleanup.Request) != nil ||
				(final.Proof != nil && final.Proof.ValidateFor(final.Request) != nil) {
				return errdefs.ErrFailedPrecondition
			}
		}
	}
	return nil
}

// StopFailedMigrationDestination journals the regional failure before touching
// runsc. It never starts a guest, discards target writes, or releases images and
// capacity. The same per-slot lock excludes new restore/adoption observations;
// a request already sent to runsc must be physically removed before proof.
func (d *nodeRuntime) StopFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error) {
	_, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.runner == nil {
		return nil, errdefs.ErrUnavailable
	}
	target := request.Restore.Image.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	return d.stopFailedMigrationDestination(ctx, request)
}

// The recovery fencer already owns the slot's reconciliation boundary.
func (d *nodeRuntime) stopFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureRequest) (*protocol.MigrationFailureStopProof, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	target := request.Restore.Image.Target
	if err := d.journal.recordMigrationFailure(target.SlotID, MigrationFailureStopCustody{Request: request, RequestDigest: want}); err != nil {
		return nil, err
	}
	container := protocol.NomadRunscContainerID(target.SlotID)
	state, err := d.runner.State(ctx, container)
	if !errdefs.IsNotFound(err) {
		if err != nil {
			return nil, err
		}
		if state.ID != container {
			return nil, errdefs.ErrFailedPrecondition
		}
		if err := d.runner.Delete(ctx, container, true); err != nil && !errdefs.IsNotFound(err) {
			return nil, err
		}
		if _, err := d.runner.State(ctx, container); !errdefs.IsNotFound(err) {
			if err != nil {
				return nil, err
			}
			return nil, errdefs.ErrUnavailable
		}
	}
	proof := protocol.MigrationFailureStopProof{RequestDigest: want, ContainerID: container, ContainerAbsent: true}
	if err := d.journal.recordMigrationFailure(target.SlotID, MigrationFailureStopCustody{Request: request, RequestDigest: want, Proof: &proof}); err != nil {
		return nil, err
	}
	return &proof, nil
}

func (j *runtimeSlotJournal) recordMigrationFailure(slot string, next MigrationFailureStopCustody) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(slot)))
		if err != nil {
			return err
		}
		c := record.MigrationDestination
		if c == nil || record.Cleanup != nil || record.Proof != nil || record.Migration != nil || c.Adoption != nil ||
			record.matchesMigrationDestination(next.Request.Restore.Image) != nil {
			return errdefs.ErrFailedPrecondition
		}
		if c.Failure != nil {
			if c.Failure.RequestDigest != next.RequestDigest || (c.Failure.Proof != nil && next.Proof != nil && *c.Failure.Proof != *next.Proof) {
				return errdefs.ErrAlreadyExists
			}
			if c.Failure.Proof != nil {
				return nil // Intent replay cannot erase the durable stop receipt.
			}
		}
		c.Failure = &next
		if err := c.validateFailure(); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}
