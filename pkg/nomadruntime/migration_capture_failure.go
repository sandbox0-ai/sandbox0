package nomadruntime

import (
	"context"
	"encoding/hex"
	"strconv"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

// MigrationCaptureFailureCustody is deliberately distinct from target failure:
// it never asserts that an image was published or a destination was stopped.
// Stage comes from the local RootFS journal, without a writer grant token.
type MigrationCaptureFailureCustody struct {
	Request       protocol.MigrationCaptureFailureRequest `json:"request"`
	RequestDigest string                                  `json:"request_digest"`
	Stage         rootfshandoff.StageRequest              `json:"stage"`
	Finalization  *MigrationCaptureFailureFinalization    `json:"finalization,omitempty"`
}

type MigrationCaptureFailureFinalization struct {
	Request       protocol.MigrationCaptureFailureFinalizeRequest `json:"request"`
	RequestDigest string                                          `json:"request_digest"`
	Proof         *protocol.MigrationCaptureFailureFinalizeProof  `json:"proof,omitempty"`
	AllocationGC  *protocol.MigrationSourceGCRequest              `json:"allocation_gc,omitempty"`
}

func (c *MigrationCaptureCustody) captureFailureReadyForCleanup(request protocol.NodeCleanupControlRequest) bool {
	return c != nil && c.Failure != nil && c.Failure.Request.Cleanup == request
}

func (c *MigrationCaptureCustody) CaptureFailureFinalized() bool {
	return c != nil && c.Failure != nil && c.Failure.Finalization != nil && c.Failure.Finalization.Proof != nil
}

func (c *MigrationCaptureCustody) validateCaptureFailure() error {
	if c.Failure == nil {
		return nil
	}
	f := c.Failure
	want, err := f.Request.Digest()
	s := f.Stage
	binding, bindingErr := s.BindingDigest()
	r := f.Request.Capture.Request
	if err != nil || f.RequestDigest != want || !c.ExecutionInvalidated || c.Capture.Request != r || c.Capture.RootFS != nil ||
		c.PublicationRequest != nil || c.Publication != nil || c.SourceFenceRequest != nil || c.SourceFenceProof != nil || c.Finalization != nil ||
		bindingErr != nil || s.ValidateDurableBinding() != nil || s.Identity.WriterGrantToken != "" ||
		hex.EncodeToString(binding[:]) != r.BindingDigest || s.Identity.SlotNonce != r.Target.SlotID ||
		s.Identity.AllocationID != r.Target.AllocationID || s.Identity.NodeUID != r.Target.NodeUID || s.Identity.BootID != r.Target.NodeBootID ||
		s.Identity.RuntimeGeneration != strconv.FormatInt(r.SourceGeneration, 10) || s.Identity.WriterGrantID != f.Request.Cleanup.WriterGrantID || s.Identity.ClaimID != f.Request.Cleanup.Resources.ClaimID ||
		s.ExpectedPolicyToken.NetNSIdentity != f.Request.Cleanup.NetNSIdentity {
		return errdefs.ErrFailedPrecondition
	}
	if final := f.Finalization; final != nil {
		digest, err := final.Request.Digest()
		cleanupDigest, cleanupErr := final.Request.Request.Digest()
		if err != nil || cleanupErr != nil || cleanupDigest != want || final.RequestDigest != digest ||
			final.Proof != nil && final.Proof.ValidateFor(final.Request) != nil {
			return errdefs.ErrFailedPrecondition
		}
		if final.AllocationGC != nil && (final.Proof == nil || final.AllocationGC.Validate() != nil ||
			final.AllocationGC.Target != r.Target || final.AllocationGC.FinalizationDigest != digest ||
			final.AllocationGC.CleanupProofDigest != final.Request.Proof.Cleanup.ProofDigest) {
			return errdefs.ErrFailedPrecondition
		}
	}
	return nil
}

type migrationLostCutRuntime interface {
	AbandonLostMigrationCut(context.Context, rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSCutRequest, string) error
}

func (r *rootfsRuntime) AbandonLostMigrationCut(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSCutRequest, writerOperationID string) error {
	return r.sessions.AbandonLostMigrationCut(ctx, stage, request, writerOperationID)
}

// CleanupFailedMigrationCapture persists the exact regional fence before
// allowing physical cleanup. It cannot be reached through ordinary driver RPC.
// An unsealed cut can enter abandonment only after its local owner is lost;
// sealed or still-owned cuts retain custody and cannot use this failure lane.
func (d *nodeRuntime) CleanupFailedMigrationCapture(ctx context.Context, request protocol.MigrationCaptureFailureRequest) (*protocol.MigrationCaptureFailureProof, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil || d.mounter == nil || d.runtimeSlotNetwork == nil || d.resourceCgroups == nil {
		return nil, errdefs.ErrUnavailable
	}
	target := request.Capture.Request.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	record, err := d.journal.Get(target.SlotID)
	if err != nil {
		return nil, err
	}
	if record.Migration == nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	if record.Migration.Failure == nil {
		session, err := d.migrationSourceSession(request.Capture.Request)
		if err != nil {
			return nil, err
		}
		if session.Kind == rootfssession.RecoveryMigration && (!session.MigrationCutOwnerLost || session.Live) || session.CrashOperationID != "" {
			return nil, errdefs.ErrFailedPrecondition
		}
		if session.Kind == rootfssession.RecoveryMigration {
			if _, ok := d.runtime.(migrationLostCutRuntime); !ok {
				return nil, errdefs.ErrUnavailable
			}
		}
		if err := d.journal.recordMigrationCaptureFailure(request, session.Stage); err != nil {
			return nil, err
		}
	} else if record.Migration.Failure.RequestDigest != want {
		return nil, errdefs.ErrAlreadyExists
	}
	if record.Proof == nil {
		// Repeat this handoff after node/process restart, including the interval
		// after the failure fence was persisted but before RootFS custody changed.
		session, err := d.migrationSourceSession(request.Capture.Request)
		if err != nil {
			return nil, err
		}
		if session.Kind == rootfssession.RecoveryMigration {
			if !session.MigrationCutOwnerLost || session.Live {
				return nil, errdefs.ErrFailedPrecondition
			}
			runtime, ok := d.runtime.(migrationLostCutRuntime)
			if !ok {
				return nil, errdefs.ErrUnavailable
			}
			cut := rootfshandoff.MigrationRootFSCutRequest{OperationID: request.Capture.Request.OperationID,
				CaptureRequestDigest: request.Capture.RequestDigest, SourceBindingDigest: request.Capture.Request.BindingDigest,
				GenerationID: "migration-" + request.Capture.RequestDigest}
			if err := runtime.AbandonLostMigrationCut(ctx, session.Stage, cut, request.Cleanup.WriterOperationID); err != nil {
				return nil, err
			}
		}
	}
	record, err = d.journal.BeginCleanup(request.Cleanup)
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
	result := &protocol.MigrationCaptureFailureProof{RequestDigest: want, Cleanup: proof}
	if err := result.ValidateFor(request); err != nil {
		return nil, err
	}
	return result, nil
}

func (j *runtimeSlotJournal) recordMigrationCaptureFailure(request protocol.MigrationCaptureFailureRequest, stage rootfshandoff.StageRequest) error {
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
		c := record.Migration
		if c == nil || c.Capture.Request != request.Capture.Request || c.Capture.RootFS != nil ||
			c.PublicationRequest != nil || c.Publication != nil || c.SourceFenceRequest != nil || c.SourceFenceProof != nil || c.Finalization != nil ||
			(!c.ExecutionInvalidated && c.Capture.State != protocol.MigrationCaptureUncertain) || record.matchesCleanup(request.Cleanup) != nil ||
			record.MigrationDestination != nil && !record.MigrationDestination.Adopted() {
			return errdefs.ErrFailedPrecondition
		}
		if c.Failure != nil {
			if c.Failure.RequestDigest != want {
				return errdefs.ErrAlreadyExists
			}
			return nil
		}
		if record.Cleanup != nil || record.Proof != nil {
			return errdefs.ErrFailedPrecondition
		}
		c.ExecutionInvalidated = true
		c.Failure = &MigrationCaptureFailureCustody{Request: request, RequestDigest: want, Stage: stage}
		if err := c.validateCaptureFailure(); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}

func (d *nodeRuntime) FinalizeFailedMigrationCapture(ctx context.Context, request protocol.MigrationCaptureFailureFinalizeRequest) (*protocol.MigrationCaptureFailureFinalizeProof, error) {
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
	target := request.Request.Capture.Request.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	f, err := d.journal.recordMigrationCaptureFailureFinalization(request, nil)
	if err != nil {
		return nil, err
	}
	if f.Proof != nil {
		copy := *f.Proof
		return &copy, nil
	}
	if _, err := d.runner.State(ctx, request.Request.Cleanup.RunscContainerID); !errdefs.IsNotFound(err) {
		return nil, errdefs.ErrFailedPrecondition
	}
	record, err := d.journal.Get(target.SlotID)
	if err != nil {
		return nil, err
	}
	if err := runtime.FinalizeFailedMigrationRootFS(ctx, record.Migration.Failure.Stage); err != nil {
		return nil, err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, record.Migration.ImageDirectory); err != nil {
		return nil, err
	}
	proof := &protocol.MigrationCaptureFailureFinalizeProof{RequestDigest: want, RootFSArtifactsAbsent: true, ImageAbsent: true}
	if _, err := d.journal.recordMigrationCaptureFailureFinalization(request, proof); err != nil {
		return nil, err
	}
	return proof, nil
}

func (j *runtimeSlotJournal) recordMigrationCaptureFailureFinalization(request protocol.MigrationCaptureFailureFinalizeRequest, proof *protocol.MigrationCaptureFailureFinalizeProof) (*MigrationCaptureFailureFinalization, error) {
	want, err := request.Digest()
	if err != nil {
		return nil, err
	}
	if proof != nil && proof.ValidateFor(request) != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	var result *MigrationCaptureFailureFinalization
	err = j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Request.Cleanup.SlotID)))
		if err != nil {
			return err
		}
		c := record.Migration
		if c == nil || c.Failure == nil || record.Proof == nil || *record.Proof != request.Proof.Cleanup ||
			request.Proof.ValidateFor(c.Failure.Request) != nil {
			return errdefs.ErrFailedPrecondition
		}
		f := c.Failure.Finalization
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
			f = &MigrationCaptureFailureFinalization{Request: request, RequestDigest: want}
			c.Failure.Finalization = f
		}
		f.Proof = proof
		if err := c.validateCaptureFailure(); err != nil {
			return err
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		result = f
		return putRuntimeSlotJournalRecord(bucket, record)
	})
	return result, err
}
