package nomadruntime

import (
	"context"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationSourceGC interface {
	AcknowledgeMigrationSourceGC(context.Context, protocol.MigrationSourceGCRequest) (*protocol.MigrationSourceGCAcknowledgement, error)
}

// AcknowledgeMigrationSourceGC is available only on the authenticated regional
// node channel, never the driver's local RPC. Driver reads and bundle deletion
// do not prove that Nomad no longer needs the source receipt after a restart.
func (d *nodeRuntime) AcknowledgeMigrationSourceGC(ctx context.Context, request protocol.MigrationSourceGCRequest) (*protocol.MigrationSourceGCAcknowledgement, error) {
	digest, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.journal.db == nil {
		return nil, errdefs.ErrUnavailable
	}
	if request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	err = d.journal.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		payload := bucket.Get([]byte(request.Target.SlotID))
		// A lost acknowledgement can outlive pruning. Absence satisfies this
		// retention-only request, but must never create a physical-cleanup proof.
		if payload == nil {
			return nil
		}
		record, err := decodeRuntimeSlotJournalRecord(payload)
		if err != nil {
			return err
		}
		if record.Migration.CaptureFailureFinalized() {
			f := record.Migration.Failure.Finalization
			if record.Proof == nil || record.Migration.Capture.Request.Target != request.Target ||
				f.RequestDigest != request.FinalizationDigest || record.Proof.ProofDigest != request.CleanupProofDigest ||
				f.AllocationGC != nil && *f.AllocationGC != request {
				return errdefs.ErrFailedPrecondition
			}
			if f.AllocationGC != nil {
				return nil
			}
			f.AllocationGC = &request
			record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return putRuntimeSlotJournalRecord(bucket, record)
		}
		if record.MigrationDestination.FailureFinalized() {
			if record.matchesMigrationFailureGC(request) != nil {
				return errdefs.ErrFailedPrecondition
			}
			final := record.MigrationDestination.Failure.Cleanup.Finalization
			if final.AllocationGC != nil {
				return nil
			}
			final.AllocationGC = &request
			record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
			return putRuntimeSlotJournalRecord(bucket, record)
		}
		if record.matchesMigrationSourceGC(request) != nil || record.Migration.ExecutionInvalidated {
			return errdefs.ErrFailedPrecondition
		}
		if record.Migration.Finalization.AllocationGC != nil {
			return nil
		}
		record.Migration.Finalization.AllocationGC = &request
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
	if err != nil {
		return nil, err
	}
	return &protocol.MigrationSourceGCAcknowledgement{RequestDigest: digest}, nil
}

func (r runtimeSlotJournalRecord) matchesMigrationSourceGC(request protocol.MigrationSourceGCRequest) error {
	if request.Validate() != nil || r.Migration == nil || r.Migration.Finalization == nil || r.Proof == nil {
		return errdefs.ErrFailedPrecondition
	}
	f := r.Migration.Finalization
	if !f.SessionForgotten || f.RequestDigest != request.FinalizationDigest || r.Proof.ProofDigest != request.CleanupProofDigest ||
		r.Migration.Capture.Request.Target != request.Target {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}
