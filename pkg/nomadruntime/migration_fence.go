package nomadruntime

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/containerd/errdefs"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshandoff"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationSourceFencer interface {
	FenceMigrationSource(context.Context, protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error)
}

type migrationDetachRuntime interface {
	DetachMigrationRootFS(context.Context, rootfshandoff.StageRequest, rootfshandoff.MigrationRootFSDetachRequest) (rootfshandoff.MigrationRootFSDetachProof, error)
}

func (r *rootfsRuntime) DetachMigrationRootFS(ctx context.Context, stage rootfshandoff.StageRequest, request rootfshandoff.MigrationRootFSDetachRequest) (rootfshandoff.MigrationRootFSDetachProof, error) {
	r.stopRenewal(stage.Parent)
	return r.sessions.DetachMigrationRootFS(ctx, stage, request)
}

// FenceMigrationSource destroys the old execution instance and attests its
// exact writer attachment absent. It retains image custody, networking and
// resource leases: those need the later regional handoff/cleanup transaction.
func (d *nodeRuntime) FenceMigrationSource(ctx context.Context, request protocol.MigrationSourceFenceRequest) (*protocol.MigrationSourceFenceProof, error) {
	digest, err := request.Digest()
	if err != nil {
		return nil, err
	}
	capture := request.PublicationRequest.Capture.Request
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil || d.mounter == nil {
		return nil, errdefs.ErrUnavailable
	}
	if capture.Target.ClusterID != d.clusterID || capture.Target.NodeID != d.nodeID || capture.Target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	runtime, ok := d.runtime.(migrationDetachRuntime)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	if !d.beginReconciliation(capture.Target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(capture.Target.SlotID)
	record, err := d.journal.Get(capture.Target.SlotID)
	if err != nil {
		return nil, err
	}
	if err := record.Migration.matchesFence(request); err != nil {
		return nil, err
	}
	if record.Migration.SourceFenceProof != nil {
		return record.Migration.SourceFenceProof, nil
	}
	session, err := d.migrationSourceSession(capture)
	if err != nil {
		return nil, err
	}
	namespace, err := os.Readlink("/proc/self/ns/mnt")
	if err != nil {
		return nil, err
	}
	if namespace != record.Registration.MountNamespaceID || session.Consumer == nil ||
		session.Consumer.HostMountNamespace != namespace || session.Consumer.StableMount != record.Registration.StableMount {
		return nil, errdefs.ErrFailedPrecondition
	}
	stable, err := d.runtimeSlotStableMountPath(record.Registration)
	if err != nil {
		return nil, err
	}
	if err := d.stopMigrationSource(ctx, *session, false); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationFence(request, nil); err != nil {
		return nil, err
	}
	container := record.Registration.RunscContainerID
	// Checkpoint already stopped execution. Do not KILL a newly executing
	// source to make an old image appear valid; the check above invalidates it.
	if err := d.runner.Delete(ctx, container, true); err != nil && !errdefs.IsNotFound(err) {
		return nil, err
	}
	if _, err := d.runner.State(ctx, container); !errdefs.IsNotFound(err) {
		return nil, fmt.Errorf("source gVisor absence is unproven: %w", errdefs.ErrFailedPrecondition)
	}
	if stable != "" {
		if err := d.mounter.Unmount(stable); err != nil {
			return nil, err
		}
	}
	attached, err := hostMountAttached(record.Registration.StableMount)
	if err != nil {
		return nil, err
	}
	if attached {
		return nil, errdefs.ErrFailedPrecondition
	}
	detachRequest, err := request.RootFSRequest()
	if err != nil {
		return nil, err
	}
	rootfs, err := runtime.DetachMigrationRootFS(ctx, session.Stage, detachRequest)
	if err != nil {
		return nil, err
	}
	if err := rootfs.ValidateFor(session.Stage, *request.PublicationRequest.Capture.RootFS, detachRequest); err != nil {
		return nil, err
	}
	// Detachment can outlive an RPC. Recheck execution at the proof boundary;
	// an unexpected replacement instance invalidates the captured temporal cut.
	if _, err := d.runner.State(ctx, container); !errdefs.IsNotFound(err) {
		if err == nil {
			if invalidateErr := d.journal.invalidateMigrationExecution(capture.Target.SlotID); invalidateErr != nil {
				return nil, invalidateErr
			}
		}
		return nil, fmt.Errorf("source execution reappeared or absence is uncertain: %w", errdefs.ErrFailedPrecondition)
	}
	proof := &protocol.MigrationSourceFenceProof{RequestDigest: digest, RootFS: rootfs, ContainerID: container,
		MountNamespaceID: namespace, ContainerAbsent: true, StableMountAbsent: true}
	proof.Digest, err = proof.ProofDigest()
	if err != nil {
		return nil, err
	}
	if err := proof.ValidateFor(request); err != nil {
		return nil, err
	}
	if err := d.journal.recordMigrationFence(request, proof); err != nil {
		return nil, err
	}
	return proof, nil
}

func (c *MigrationCaptureCustody) matchesFence(request protocol.MigrationSourceFenceRequest) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if c == nil || c.ExecutionInvalidated || c.PublicationRequest == nil || c.Publication == nil ||
		*c.Publication != request.Publication || c.Capture.State != protocol.MigrationCaptureComplete {
		return errdefs.ErrFailedPrecondition
	}
	if err := request.Publication.ValidateFor(*c.PublicationRequest); err != nil {
		return err
	}
	if c.SourceFenceRequest != nil {
		prior, err := c.SourceFenceRequest.Digest()
		if err != nil || prior != want {
			return errdefs.ErrAlreadyExists
		}
	}
	return nil
}

func (j *runtimeSlotJournal) recordMigrationFence(request protocol.MigrationSourceFenceRequest, proof *protocol.MigrationSourceFenceProof) error {
	if _, err := request.Digest(); err != nil {
		return err
	}
	if proof != nil {
		if err := proof.ValidateFor(request); err != nil {
			return err
		}
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.PublicationRequest.Capture.Request.Target.SlotID)))
		if err != nil {
			return err
		}
		if err := current.Migration.matchesFence(request); err != nil {
			return err
		}
		if prior := current.Migration.SourceFenceProof; prior != nil {
			if proof != nil && prior.Digest != proof.Digest {
				return errdefs.ErrAlreadyExists
			}
			return nil
		}
		current.Migration.SourceFenceRequest, current.Migration.SourceFenceProof = &request, proof
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

func (c MigrationCaptureCustody) validateSourceFence() error {
	if c.SourceFenceRequest == nil {
		if c.SourceFenceProof != nil {
			return errdefs.ErrFailedPrecondition
		}
		return nil
	}
	// Preserve evidence even if later execution invalidated the source image;
	// callers use matchesFence to reject that invalidated custody for handoff.
	copy := c
	copy.ExecutionInvalidated = false
	if err := copy.matchesFence(*c.SourceFenceRequest); err != nil {
		return err
	}
	if c.SourceFenceProof != nil {
		return c.SourceFenceProof.ValidateFor(*c.SourceFenceRequest)
	}
	return nil
}
