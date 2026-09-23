package nomadruntime

import (
	"context"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationFailureCleanupCustody struct {
	Finalization  *MigrationFailureFinalization           `json:"finalization,omitempty"`
	Request       protocol.MigrationFailureCleanupRequest `json:"request"`
	RequestDigest string                                  `json:"request_digest"`
}

func (c *MigrationDestinationCustody) readyForCleanup(request protocol.NodeCleanupControlRequest) bool {
	return c != nil && (c.Adopted() || c.ImageCanceled() && request.Resources == c.Request.Resources ||
		c.Failure != nil && c.Failure.Cleanup != nil && c.Failure.Cleanup.Request.Cleanup == request)
}

// CleanupFailedMigrationDestination reuses physical carrier cleanup but keeps
// the external RootFS crash journal and image until regional writer retirement.
// Ordinary cleanup cannot introduce this immutable failure cleanup authority.
func (d *nodeRuntime) CleanupFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureCleanupRequest) (*protocol.MigrationFailureCleanupProof, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil || d.mounter == nil || d.runtimeSlotNetwork == nil || d.resourceCgroups == nil {
		return nil, errdefs.ErrUnavailable
	}
	target := request.Failure.Request.Restore.Image.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	if err := d.journal.recordMigrationFailureCleanup(request); err != nil {
		return nil, err
	}
	record, err := d.journal.BeginCleanup(request.Cleanup)
	if err != nil {
		return nil, err
	}
	var proof protocol.NodeCleanupControlProof
	if record.Proof != nil {
		proof = *record.Proof
	} else {
		proof, err = d.cleanupWriterRuntimeSlot(ctx, request.Cleanup, &record)
		if err != nil {
			return nil, err
		}
	}
	result := &protocol.MigrationFailureCleanupProof{RequestDigest: want, Cleanup: proof}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	return result, nil
}

func (j *runtimeSlotJournal) recordMigrationFailureCleanup(request protocol.MigrationFailureCleanupRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Cleanup.SlotID)))
		if err != nil {
			return err
		}
		c := record.MigrationDestination
		if c == nil || c.Failure == nil || c.Failure.Proof == nil || c.Adoption != nil || record.Migration != nil ||
			*c.Failure.Proof != request.Failure.Proof || record.matchesCleanup(request.Cleanup) != nil {
			return errdefs.ErrFailedPrecondition
		}
		if c.Failure.Cleanup != nil {
			if c.Failure.Cleanup.RequestDigest != want {
				return errdefs.ErrAlreadyExists
			}
			return nil
		}
		if record.Cleanup != nil || record.Proof != nil {
			return errdefs.ErrFailedPrecondition
		}
		c.Failure.Cleanup = &MigrationFailureCleanupCustody{Request: request, RequestDigest: want}
		if err := c.validateFailure(); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}
