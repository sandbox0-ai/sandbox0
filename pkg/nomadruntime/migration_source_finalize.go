package nomadruntime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationSourceFinalizer interface {
	FinalizeMigrationSource(context.Context, protocol.MigrationSourceFinalizeRequest) (*protocol.MigrationSourceFinalizeProof, error)
}

// MigrationSourceFinalizationReader exposes only completed custody to the
// local driver. It cannot authorize or initiate source deletion.
type MigrationSourceFinalizationReader interface {
	GetMigrationSourceFinalization(context.Context, string) (*protocol.MigrationSourceFinalizationReceipt, error)
}

func (d *nodeRuntime) GetMigrationSourceFinalization(ctx context.Context, slot string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	if err := protocol.ValidateSlotID(slot); err != nil {
		return nil, errdefs.ErrInvalidArgument
	}
	if d == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	record, err := d.journal.Get(slot)
	if errdefs.IsNotFound(err) {
		return d.readRegionalMigrationSourceFinalization(ctx, slot)
	}
	if err != nil {
		return nil, err
	}
	c := record.Migration
	if c == nil || c.Finalization == nil || !c.Finalization.SessionForgotten || record.Proof == nil {
		return nil, nil
	}
	if c.ExecutionInvalidated {
		return nil, errdefs.ErrFailedPrecondition
	}
	f := c.Finalization
	receipt := &protocol.MigrationSourceFinalizationReceipt{Request: f.Request,
		Proof: protocol.MigrationSourceFinalizeProof{RequestDigest: f.RequestDigest, RootFS: *f.RootFS, ImageAbsent: f.ImageAbsent, Cleanup: *record.Proof}}
	if err := receipt.Validate(); err != nil {
		return nil, err
	}
	return receipt, nil
}

// Once local proof retention expires, the region's immutable receipt can still
// finish a stale Nomad handle. Never override pending, invalidated, or corrupt
// local custody, and never recreate a journal or an execution/storage grant.
func (d *nodeRuntime) readRegionalMigrationSourceFinalization(ctx context.Context, slot string) (*protocol.MigrationSourceFinalizationReceipt, error) {
	reader, ok := d.registrationAuthority.(MigrationSourceFinalizationReader)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	receipt, err := reader.GetMigrationSourceFinalization(ctx, slot)
	if err != nil || receipt == nil {
		return nil, err
	}
	if receipt.Validate() != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	target := receipt.Request.Fence.PublicationRequest.Capture.Request.Target
	if target.SlotID != slot || target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	return receipt, nil
}

// MigrationSourceFinalization keeps irreversible intent and intermediate
// artifact receipts in the existing slot journal. The ordinary Cleanup/Proof
// fields are the sole source of truth for network and cgroup cleanup.
type MigrationSourceFinalization struct {
	Request          protocol.MigrationSourceFinalizeRequest     `json:"request"`
	RequestDigest    string                                      `json:"request_digest"`
	Stage            rootfshandoff.StageRequest                  `json:"stage"`
	RootFS           *rootfshandoff.MigrationRootFSFinalizeProof `json:"rootfs,omitempty"`
	ImageAbsent      bool                                        `json:"image_absent"`
	SessionForgotten bool                                        `json:"session_forgotten"`
	AllocationGC     *protocol.MigrationSourceGCRequest          `json:"allocation_gc,omitempty"`
}

func (c *MigrationCaptureCustody) readyForCleanup(request protocol.NodeCleanupControlRequest) bool {
	return c.captureFailureReadyForCleanup(request) || c != nil && c.Finalization != nil && c.Finalization.RootFS != nil && c.Finalization.ImageAbsent && c.Finalization.Request.Cleanup == request
}

func (c *MigrationCaptureCustody) validateFinalization() error {
	if c.Finalization == nil {
		return nil
	}
	f := c.Finalization
	// Invalidated execution forbids new handoff, but does not erase historical
	// cleanup intent. Keep that evidence readable for subsequent fencing.
	historical := *c
	historical.ExecutionInvalidated = false
	want, err := f.Request.Digest()
	root, rootErr := f.Request.RootFSRequest()
	if err != nil || rootErr != nil || want != f.RequestDigest || c.SourceFenceProof == nil || c.SourceFenceProof.Digest != f.Request.SourceProof.Digest ||
		historical.matchesFence(f.Request.Fence) != nil || root.ValidateFor(f.Stage, f.Request.SourceProof.RootFS) != nil ||
		f.Stage.Identity.WriterGrantID != f.Request.Cleanup.WriterGrantID || f.Stage.Identity.SlotNonce != f.Request.Cleanup.SlotID ||
		f.Stage.Identity.AllocationID != f.Request.Cleanup.AllocationID || f.Stage.Identity.NodeUID != f.Request.Cleanup.NodeUID ||
		f.Stage.Identity.BootID != f.Request.Cleanup.NodeBootID || f.Stage.ExpectedPolicyToken.NetNSIdentity != f.Request.Cleanup.NetNSIdentity {
		return errdefs.ErrFailedPrecondition
	}
	if f.RootFS != nil && f.RootFS.ValidateFor(f.Stage, f.Request.SourceProof.RootFS, root) != nil {
		return errdefs.ErrFailedPrecondition
	}
	if f.ImageAbsent && f.RootFS == nil || f.SessionForgotten && !f.ImageAbsent {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

func (d *nodeRuntime) FinalizeMigrationSource(ctx context.Context, request protocol.MigrationSourceFinalizeRequest) (*protocol.MigrationSourceFinalizeProof, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil || d.mounter == nil || d.runtimeSlotNetwork == nil || d.resourceCgroups == nil {
		return nil, errdefs.ErrUnavailable
	}
	target := request.Fence.PublicationRequest.Capture.Request.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	runtime, ok := d.runtime.(migrationFinalizeRuntime)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	record, err := d.journal.Get(target.SlotID)
	if err != nil {
		return nil, err
	}
	if record.Migration == nil || record.Migration.matchesFence(request.Fence) != nil || record.Migration.SourceFenceProof == nil || record.Migration.SourceFenceProof.Digest != request.SourceProof.Digest {
		return nil, errdefs.ErrFailedPrecondition
	}
	want, _ := request.Digest()
	if f := record.Migration.Finalization; f != nil {
		if f.RequestDigest != want {
			return nil, errdefs.ErrAlreadyExists
		}
	} else {
		session, err := d.migrationSourceSession(request.Fence.PublicationRequest.Capture.Request)
		if err != nil {
			return nil, err
		}
		if err := d.journal.recordMigrationFinalization(target.SlotID, MigrationSourceFinalization{Request: request, RequestDigest: want, Stage: session.Stage}); err != nil {
			return nil, err
		}
		record, err = d.journal.Get(target.SlotID)
		if err != nil {
			return nil, err
		}
	}
	f := *record.Migration.Finalization
	rootRequest, _ := request.RootFSRequest()
	if record.Proof == nil {
		// A completed fence cannot authorize erasing a newly executing source.
		if _, err := d.runner.State(ctx, record.Registration.RunscContainerID); !errdefs.IsNotFound(err) {
			return nil, errdefs.ErrFailedPrecondition
		}
		if f.RootFS == nil {
			proof, err := runtime.FinalizeMigrationRootFS(ctx, f.Stage, rootRequest)
			if err != nil {
				return nil, err
			}
			if err := proof.ValidateFor(f.Stage, request.SourceProof.RootFS, rootRequest); err != nil {
				return nil, err
			}
			f.RootFS = &proof
			if err := d.journal.recordMigrationFinalization(target.SlotID, f); err != nil {
				return nil, err
			}
		}
		if !f.ImageAbsent {
			if err := removeMigrationImage(d.journal.migrationRoot, record.Migration.ImageDirectory); err != nil {
				return nil, err
			}
			f.ImageAbsent = true
			if err := d.journal.recordMigrationFinalization(target.SlotID, f); err != nil {
				return nil, err
			}
		}
		record, err = d.journal.BeginCleanup(request.Cleanup)
		if err != nil {
			return nil, err
		}
		if record.Proof == nil {
			proof, err := d.cleanupJournaledRuntimeSlot(ctx, request.Cleanup, record, f.RootFS.Digest)
			if err != nil {
				return nil, err
			}
			record.Proof = &proof
		}
	}
	result := &protocol.MigrationSourceFinalizeProof{RequestDigest: want, RootFS: *f.RootFS, ImageAbsent: f.ImageAbsent, Cleanup: *record.Proof}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	// The slot journal now owns the full receipt, so compact session deletion
	// cannot lose response replay. A failure remains retryable via this command.
	if !f.SessionForgotten {
		if err := runtime.ForgetFinalizedMigrationRootFS(f.Stage, rootRequest); err != nil {
			return nil, err
		}
		f.SessionForgotten = true
		if err := d.journal.recordMigrationFinalization(target.SlotID, f); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func (j *runtimeSlotJournal) recordMigrationFinalization(slot string, next MigrationSourceFinalization) error {
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(slot)))
		if err != nil {
			return err
		}
		c := record.Migration
		if c == nil {
			return errdefs.ErrFailedPrecondition
		}
		if prior := c.Finalization; prior != nil {
			before, _ := prior.Stage.BindingDigest()
			after, _ := next.Stage.BindingDigest()
			if prior.RequestDigest != next.RequestDigest || before != after {
				return errdefs.ErrAlreadyExists
			}
			if prior.RootFS != nil && (next.RootFS == nil || *prior.RootFS != *next.RootFS) || prior.ImageAbsent && !next.ImageAbsent || prior.SessionForgotten && !next.SessionForgotten {
				return errdefs.ErrAlreadyExists
			}
			// Only the regional allocation-GC path may introduce this marker.
			// Finalization retries cannot erase or replace its retention barrier.
			if prior.AllocationGC == nil && next.AllocationGC != nil || prior.AllocationGC != nil && (next.AllocationGC == nil || *prior.AllocationGC != *next.AllocationGC) {
				return errdefs.ErrAlreadyExists
			}
		} else if next.RootFS != nil || next.ImageAbsent || next.SessionForgotten || next.AllocationGC != nil || record.Cleanup != nil {
			return errdefs.ErrFailedPrecondition
		}
		if next.SessionForgotten && record.Proof == nil {
			return errdefs.ErrFailedPrecondition
		}
		c.Finalization = &next
		if err := c.validateFinalization(); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}

// removeMigrationImage deletes only a ctld-derived immediate child of its
// private staging root and durably records directory-entry removal.
func removeMigrationImage(root, path string) error {
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 || filepath.Dir(path) != root {
		return errdefs.ErrFailedPrecondition
	}
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	parent, err := os.Open(root)
	if err != nil {
		return err
	}
	err = parent.Sync()
	closeErr := parent.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		return fmt.Errorf("migration image absence is unproven: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
}
