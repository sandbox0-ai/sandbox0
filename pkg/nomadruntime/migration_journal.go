package nomadruntime

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

// Count unresolved custody, including failed images, rather than active RPCs.
// Reconnection or driver death must not free the node's capture admission.
const maxMigrationImageCustodies = 2

// MigrationCustodian persists source checkpoint custody in ctld's existing
// exclusive node journal. Driver state is a local observation of this custody.
type MigrationCustodian interface {
	GetMigrationCapture(context.Context, string) (*MigrationCaptureCustody, error)
	RecordMigrationCapture(context.Context, protocol.MigrationCapture) error
}

// MigrationCaptureCustody keeps staging outside Nomad's allocation directory.
// Only ctld derives the path; neither the guest nor manager selects host paths.
type MigrationCaptureCustody struct {
	Failure              *MigrationCaptureFailureCustody       `json:"failure,omitempty"`
	Finalization         *MigrationSourceFinalization          `json:"finalization,omitempty"`
	SourceFenceRequest   *protocol.MigrationSourceFenceRequest `json:"source_fence_request,omitempty"`
	SourceFenceProof     *protocol.MigrationSourceFenceProof   `json:"source_fence_proof,omitempty"`
	Capture              protocol.MigrationCapture             `json:"capture"`
	ImageDirectory       string                                `json:"image_directory"`
	PublicationRequest   *protocol.MigrationPublicationRequest `json:"publication_request,omitempty"`
	Publication          *protocol.MigrationPublication        `json:"publication,omitempty"`
	ExecutionInvalidated bool                                  `json:"execution_invalidated,omitempty"`
}

func (d *nodeRuntime) GetMigrationCapture(_ context.Context, slotID string) (*MigrationCaptureCustody, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return nil, fmt.Errorf("migration slot identity: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	if d == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	record, err := d.journal.Get(slotID)
	if err != nil {
		return nil, err
	}
	if record.Migration != nil && record.Migration.ExecutionInvalidated {
		// Preserve the original capture observation in Bolt, but never expose
		// it as usable after execution was observed outside its captured cut.
		record.Migration.Capture.State = protocol.MigrationCaptureUncertain
		record.Migration.Capture.RootFS = nil
		record.Migration.PublicationRequest = nil
		record.Migration.Publication = nil
		record.Migration.SourceFenceRequest = nil
		record.Migration.SourceFenceProof = nil
		record.Migration.Finalization = nil
	}
	return record.Migration, nil
}

func (j *runtimeSlotJournal) invalidateMigrationExecution(slotID string) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(slotID)))
		if err != nil {
			return err
		}
		if current.Migration == nil {
			return errdefs.ErrFailedPrecondition
		}
		current.Migration.ExecutionInvalidated = true
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

func (d *nodeRuntime) RecordMigrationCapture(ctx context.Context, capture protocol.MigrationCapture) error {
	if d == nil || d.journal == nil {
		return errdefs.ErrUnavailable
	}
	if capture.RootFS != nil {
		return fmt.Errorf("only ctld may record the filesystem cut: %w", errdefs.ErrPermissionDenied)
	}
	if err := capture.Validate(); err != nil {
		return err
	}
	if capture.Request.Target.ClusterID != d.clusterID || capture.Request.Target.NodeID != d.nodeID || capture.Request.Target.NodeUID != d.nodeUID {
		return fmt.Errorf("migration custody belongs to another node: %w", errdefs.ErrPermissionDenied)
	}
	if d.hasMigrationCaptureUpload(capture) {
		if capture.State == protocol.MigrationCaptureIntent {
			return nil // An exact lost intent reply joins the existing worker.
		}
		if err := d.beginExternalReconciliation(ctx, capture.Request.Target.SlotID); err != nil {
			return err
		}
	} else if !d.beginReconciliation(capture.Request.Target.SlotID, nil) {
		return fmt.Errorf("source recovery is already in progress: %w", errdefs.ErrUnavailable)
	}
	owned := true
	defer func() {
		if owned {
			d.endReconciliation(capture.Request.Target.SlotID)
		}
	}()
	record, err := d.journal.Get(capture.Request.Target.SlotID)
	if err != nil {
		return err
	}
	// Outcome recording/recovery never needs fresh disk admission. A full
	// pool must not erase an already captured source's completion evidence.
	if record.Migration == nil {
		if err := d.checkMigrationStaging(true); err != nil {
			return err
		}
	}
	if err := d.journal.RecordMigrationCapture(capture); err != nil {
		return err
	}
	if capture.State == protocol.MigrationCaptureIntent {
		record, err := d.journal.Get(capture.Request.Target.SlotID)
		if err != nil {
			return err
		}
		owned = !d.startMigrationCaptureUpload(record)
	}
	return nil
}

func (j *runtimeSlotJournal) RecordMigrationCapture(capture protocol.MigrationCapture) error {
	if j == nil || j.db == nil {
		return errdefs.ErrUnavailable
	}
	if err := capture.Validate(); err != nil {
		return fmt.Errorf("migration custody: %w: %w", err, errdefs.ErrInvalidArgument)
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(capture.Request.Target.SlotID)))
		if err != nil {
			return err
		}
		if err := current.matchesMigration(capture); err != nil {
			return err
		}
		if current.Cleanup != nil || current.Proof != nil || (current.MigrationDestination != nil && !current.MigrationDestination.Adopted()) {
			return fmt.Errorf("migration cannot revive terminal custody: %w", errdefs.ErrFailedPrecondition)
		}
		if custody := current.Migration; custody != nil {
			if custody.Finalization != nil || custody.Failure != nil {
				return errdefs.ErrFailedPrecondition
			}
			prior := custody.Capture
			if prior.Request != capture.Request || prior.RequestDigest != capture.RequestDigest {
				return fmt.Errorf("another migration owns this source: %w", errdefs.ErrAlreadyExists)
			}
			if custody.ExecutionInvalidated && capture.State == protocol.MigrationCaptureUncertain && capture.RootFS == nil {
				return nil
			}
			if prior.State == capture.State {
				if capture.RootFS == nil {
					return nil // Driver retries cannot erase ctld's retained cut.
				}
				if prior.RootFS != nil {
					if prior.RootFS.Digest != capture.RootFS.Digest {
						return errdefs.ErrAlreadyExists
					}
					return nil
				}
				if custody.ExecutionInvalidated {
					return errdefs.ErrFailedPrecondition
				}
			} else if prior.State != protocol.MigrationCaptureIntent || capture.State == protocol.MigrationCaptureIntent {
				return fmt.Errorf("migration capture outcome cannot be rewritten: %w", errdefs.ErrFailedPrecondition)
			}
		} else if capture.State != protocol.MigrationCaptureIntent {
			return fmt.Errorf("migration outcome has no durable intent: %w", errdefs.ErrFailedPrecondition)
		} else {
			if err := j.checkMigrationStagingWrite(bucket, current, &capture.Request, nil); err != nil {
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
				return fmt.Errorf("node migration custody admission is exhausted: %w", errdefs.ErrResourceExhausted)
			}
		}
		if err := os.MkdirAll(j.migrationRoot, 0o700); err != nil {
			return err
		}
		info, err := os.Lstat(j.migrationRoot)
		if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("migration staging root must be a private directory: %w", errdefs.ErrFailedPrecondition)
		}
		if current.Migration == nil {
			current.Migration = &MigrationCaptureCustody{ImageDirectory: filepath.Join(j.migrationRoot, capture.RequestDigest)}
		}
		current.Migration.Capture = capture
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

func (r runtimeSlotJournalRecord) matchesMigration(capture protocol.MigrationCapture) error {
	target, registration := capture.Request.Target, r.Registration
	if c := r.MigrationDestination; c != nil && c.Adopted() {
		adopted := c.Adoption.Request
		revision, _ := c.Restore.Request.Image.RuntimeAssignment().Revision()
		binding, _ := c.Restore.Request.Stage.BindingDigest()
		resources, _ := c.Restore.Request.Image.Resources.Digest()
		if capture.Request.Target != adopted.Target || capture.Request.OperationID == adopted.OperationID ||
			capture.Request.SandboxID != adopted.SandboxID || capture.Request.SourceGeneration != adopted.RuntimeGeneration ||
			capture.Request.ProcdInstanceID != adopted.ProcdInstanceID || capture.Request.AssignmentRevision != revision ||
			capture.Request.BindingDigest != hex.EncodeToString(binding[:]) || capture.Request.ResourceLeaseDigest != strings.TrimPrefix(resources, "sha256:") {
			return fmt.Errorf("subsequent migration changed adopted runtime: %w", errdefs.ErrFailedPrecondition)
		}
	}
	if target.SlotID != registration.SlotID || target.ClusterID != registration.ClusterID || target.NodeID != registration.NodeID ||
		target.AllocationID != registration.AllocationID || target.NodeBootID != registration.NodeBootID {
		return fmt.Errorf("migration capture changed registered incarnation: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}
