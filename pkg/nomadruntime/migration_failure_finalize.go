package nomadruntime

import (
	"context"
	"fmt"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationFailureFinalization struct {
	Request       protocol.MigrationFailureFinalizeRequest `json:"request"`
	RequestDigest string                                   `json:"request_digest"`
	Proof         *protocol.MigrationFailureFinalizeProof  `json:"proof,omitempty"`
	AllocationGC  *protocol.MigrationSourceGCRequest       `json:"allocation_gc,omitempty"`
}

type migrationFailureFinalizeRuntime interface {
	FinalizeFailedMigrationRootFS(context.Context, rootfshandoff.StageRequest) error
}

// FinalizeFailedMigrationRootFS is reachable only after ctld has persisted the
// regional acknowledgement and exact physical cleanup proof. Regional terminal
// verification remains mandatory before destroying an unpublished dirty tail.
func (r *rootfsRuntime) FinalizeFailedMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest) error {
	if r.authority == nil {
		return errdefs.ErrUnavailable
	}
	if err := r.authority.VerifyTerminalWriterGrant(ctx, stage); err != nil {
		return fmt.Errorf("verify failed target writer retirement: %w", err)
	}
	if err := r.sessions.ReclaimTerminalArtifacts(stage.Parent, stage.Identity); err != nil && !errdefs.IsNotFound(err) {
		return fmt.Errorf("reclaim failed target RootFS artifacts: %w", err)
	}
	// The enclosing slot journal retains the cleanup receipt across this
	// compact-record deletion and a lost response. No new writer is authorized.
	return r.sessions.ForgetVerifiedTerminal(stage.Parent, stage.Identity)
}

func (d *nodeRuntime) FinalizeFailedMigrationDestination(ctx context.Context, request protocol.MigrationFailureFinalizeRequest) (*protocol.MigrationFailureFinalizeProof, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.runner == nil {
		return nil, errdefs.ErrUnavailable
	}
	runtime, ok := d.runtime.(migrationFailureFinalizeRuntime)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	target := request.Request.Failure.Request.Restore.Image.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	f, err := d.journal.recordMigrationFailureFinalization(request, nil)
	if err != nil {
		return nil, err
	}
	if f.Proof != nil {
		copy := *f.Proof
		return &copy, nil
	}
	if _, err := d.runner.State(ctx, request.Request.Cleanup.RunscContainerID); !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("failed target execution absence changed: %w", errdefs.ErrFailedPrecondition)
	}
	if err := runtime.FinalizeFailedMigrationRootFS(ctx, request.Request.Failure.Request.Restore.Stage); err != nil {
		return nil, err
	}
	record, err := d.journal.Get(target.SlotID)
	if err != nil {
		return nil, err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, record.MigrationDestination.ImageDirectory); err != nil {
		return nil, err
	}
	proof := &protocol.MigrationFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}
	if _, err := d.journal.recordMigrationFailureFinalization(request, proof); err != nil {
		return nil, err
	}
	return proof, nil
}

func (j *runtimeSlotJournal) recordMigrationFailureFinalization(request protocol.MigrationFailureFinalizeRequest, proof *protocol.MigrationFailureFinalizeProof) (*MigrationFailureFinalization, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if proof != nil && proof.ValidateFor(request) != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	var result *MigrationFailureFinalization
	err = j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Request.Cleanup.SlotID)))
		if err != nil {
			return err
		}
		c := record.MigrationDestination
		if c == nil || c.Failure == nil || c.Failure.Cleanup == nil || record.Proof == nil ||
			*record.Proof != request.Proof.Cleanup || request.Proof.ValidateFor(c.Failure.Cleanup.Request) != nil {
			return errdefs.ErrFailedPrecondition
		}
		f := c.Failure.Cleanup.Finalization
		if f != nil {
			if f.RequestDigest != want || f.Proof != nil && proof != nil && *f.Proof != *proof {
				return errdefs.ErrAlreadyExists
			}
			if f.Proof != nil || proof == nil {
				result = f
				return nil
			}
		} else {
			if proof != nil {
				return errdefs.ErrFailedPrecondition
			}
			f = &MigrationFailureFinalization{Request: request, RequestDigest: want}
			c.Failure.Cleanup.Finalization = f
		}
		f.Proof = proof
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		result = f
		return putRuntimeSlotJournalRecord(bucket, record)
	})
	return result, err
}

// FailureFinalized proves artifact removal without pretending that the guest
// was adopted. Allocation-GC acknowledgement remains a separate custody gate.
func (c *MigrationDestinationCustody) FailureFinalized() bool {
	return c != nil && c.Failure != nil && c.Failure.Cleanup != nil && c.Failure.Cleanup.Finalization != nil && c.Failure.Cleanup.Finalization.Proof != nil
}

func (r runtimeSlotJournalRecord) matchesMigrationFailureGC(request protocol.MigrationSourceGCRequest) error {
	c := r.MigrationDestination
	if request.Validate() != nil || !c.FailureFinalized() || r.Proof == nil {
		return errdefs.ErrFailedPrecondition
	}
	f := c.Failure.Cleanup.Finalization
	if f.RequestDigest != request.FinalizationDigest || r.Proof.ProofDigest != request.CleanupProofDigest ||
		f.Request.Request.Failure.Request.Restore.Image.Target != request.Target ||
		f.AllocationGC != nil && *f.AllocationGC != request {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}
