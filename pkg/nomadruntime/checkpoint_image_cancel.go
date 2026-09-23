package nomadruntime

import (
	"context"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

// CheckpointImageCancellation is an irreversible per-carrier tombstone. Its
// proof releases image space, while the ordinary cleanup journal retains the
// carrier until writer, mount, network and resource absence have been proved.
type CheckpointImageCancellation struct {
	RequestDigest string                               `json:"request_digest"`
	Proof         *protocol.CheckpointImageCancelProof `json:"proof,omitempty"`
}

func (c *MigrationDestinationCustody) ImageCanceled() bool {
	return c != nil && c.Cancellation != nil && c.Cancellation.Proof != nil
}

func (c *MigrationDestinationCustody) validateCancellation() error {
	if c == nil || c.Cancellation == nil {
		return nil
	}
	r := protocol.CheckpointImageCancelRequest{Image: c.Request}
	want, err := r.Digest()
	if err != nil || c.Cancellation.RequestDigest != want || c.Restore != nil || c.Failure != nil || c.Adoption != nil {
		return errdefs.ErrFailedPrecondition
	}
	if c.Cancellation.Proof != nil {
		return c.Cancellation.Proof.ValidateFor(r)
	}
	return nil
}

// CancelCheckpointImage fences future prepares before joining a current
// download. Neither a lost reply nor an A/B restart can reopen execution.
func (d *nodeRuntime) CancelCheckpointImage(ctx context.Context, request protocol.CheckpointImageCancelRequest) (*protocol.CheckpointImageCancelProof, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	target := request.Image.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := d.journal.recordCheckpointImageCancellation(request, nil); err != nil {
		return nil, err
	}
	d.mu.Lock()
	worker := d.migrationImagePreparations[target.SlotID]
	if worker != nil {
		worker.cancel()
	}
	d.mu.Unlock()
	if worker != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-worker.done:
		}
	}
	if err := d.beginExternalReconciliation(ctx, target.SlotID); err != nil {
		return nil, err
	}
	defer d.endReconciliation(target.SlotID)
	record, err := d.journal.Get(target.SlotID)
	if err != nil {
		return nil, err
	}
	custody := record.MigrationDestination
	if custody == nil || custody.validateCancellation() != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	if custody.Cancellation.Proof != nil {
		copy := *custody.Cancellation.Proof
		return &copy, nil
	}
	if err := ensureMigrationStagingDirectory(d.journal.migrationRoot); err != nil {
		return nil, err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, custody.ImageDirectory); err != nil {
		return nil, err
	}
	proof := &protocol.CheckpointImageCancelProof{RequestDigest: want, ImageAbsent: true}
	if err := d.journal.recordCheckpointImageCancellation(request, proof); err != nil {
		return nil, err
	}
	return proof, nil
}

func (j *runtimeSlotJournal) recordCheckpointImageCancellation(request protocol.CheckpointImageCancelRequest, proof *protocol.CheckpointImageCancelProof) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if proof != nil && proof.ValidateFor(request) != nil {
		return errdefs.ErrFailedPrecondition
	}
	imageDigest, _ := request.Image.Digest()
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Image.Target.SlotID)))
		if err != nil {
			return err
		}
		if record.matchesMigrationDestination(request.Image) != nil || record.Migration != nil || record.MigrationStaging != nil {
			return errdefs.ErrFailedPrecondition
		}
		c := record.MigrationDestination
		if c == nil {
			if proof != nil || record.Cleanup != nil || record.Proof != nil {
				return errdefs.ErrFailedPrecondition
			}
			// Cancellation grants no download admission and allocates no image
			// bytes; a full staging pool must never prevent a durable tombstone.
			c = &MigrationDestinationCustody{Request: request.Image, RequestDigest: imageDigest,
				ImageDirectory: filepath.Join(j.migrationRoot, "destination-"+imageDigest)}
			record.MigrationDestination = c
		}
		if c.RequestDigest != imageDigest || c.Restore != nil || c.Adoption != nil || c.Failure != nil {
			return errdefs.ErrFailedPrecondition
		}
		if c.Cancellation == nil {
			if proof != nil || record.Cleanup != nil || record.Proof != nil {
				return errdefs.ErrFailedPrecondition
			}
			c.Cancellation = &CheckpointImageCancellation{RequestDigest: want}
		} else if c.Cancellation.RequestDigest != want {
			return errdefs.ErrAlreadyExists
		}
		if c.Cancellation.Proof != nil {
			if proof != nil && *c.Cancellation.Proof != *proof {
				return errdefs.ErrAlreadyExists
			}
			return nil
		}
		c.Cancellation.Proof = proof
		if err := c.validateCancellation(); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}
