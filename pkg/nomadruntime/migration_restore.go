package nomadruntime

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/containerd/errdefs"
	rootfssession "github.com/sandbox0-ai/sandbox0/pkg/rootfssession"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationRestoreCustodian interface {
	GetMigrationDestination(context.Context, string) (*MigrationDestinationCustody, error)
	RecordMigrationRestore(context.Context, protocol.MigrationRestoreObservation) error
}

func (d *nodeRuntime) GetMigrationDestination(_ context.Context, slotID string) (*MigrationDestinationCustody, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil {
		return nil, errdefs.ErrUnavailable
	}
	record, err := d.journal.Get(slotID)
	if err != nil {
		return nil, err
	}
	return record.MigrationDestination, nil
}

// RecordMigrationRestore is root-only local control. Intent verifies the
// retained image; restoring requires the exact created container and attached
// writer; restored requires that same container to be running. No path here
// creates or resumes execution on behalf of the driver.
func (d *nodeRuntime) RecordMigrationRestore(ctx context.Context, observation protocol.MigrationRestoreObservation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil {
		return errdefs.ErrUnavailable
	}
	target := observation.Request.Image.Target
	if target.ClusterID != d.clusterID || target.NodeID != d.nodeID || target.NodeUID != d.nodeUID {
		return errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(target.SlotID, nil) {
		return errdefs.ErrUnavailable
	}
	defer d.endReconciliation(target.SlotID)
	record, err := d.journal.Get(target.SlotID)
	if err != nil {
		return err
	}
	custody := record.MigrationDestination
	imageDigest, _ := observation.Request.Image.Digest()
	if custody == nil || custody.Adoption != nil || custody.Failure != nil || custody.RequestDigest != imageDigest || custody.Prepared == nil || *custody.Prepared != observation.Request.Prepared {
		return errdefs.ErrFailedPrecondition
	}
	switch observation.State {
	case protocol.MigrationRestoreIntent:
		if custody.Restore != nil && custody.Restore.State != protocol.MigrationRestoreIntent {
			return errdefs.ErrFailedPrecondition
		}
		runtime, ok := d.runtime.(migrationImageDownloadRuntime)
		if !ok {
			return errdefs.ErrUnavailable
		}
		if _, err := runtime.PrepareMigrationImageFiles(ctx, custody.Request.Receipt.Binding, custody.Request.Receipt.Reference, custody.ImageDirectory, true, nil); err != nil {
			return err
		}
	case protocol.MigrationRestoreExecuting, protocol.MigrationRestoreComplete:
		matched := false
		sessions, err := d.runtime.RecoverySessions()
		if err != nil {
			return err
		}
		want, _ := observation.Request.Stage.BindingDigest()
		for _, session := range sessions {
			actual, err := session.Stage.BindingDigest()
			if err == nil && actual == want && session.Stage.Identity.SlotNonce == target.SlotID && session.Live {
				matched = true
			}
		}
		if !matched {
			return errdefs.ErrFailedPrecondition
		}
		state, err := d.runner.State(ctx, record.Registration.RunscContainerID)
		if err != nil {
			return err
		}
		expected := "created"
		if observation.State == protocol.MigrationRestoreComplete {
			expected = "running"
		}
		if state.ID != record.Registration.RunscContainerID || state.Status != expected {
			return errdefs.ErrFailedPrecondition
		}
	}
	return d.journal.recordMigrationRestore(observation)
}

func (j *runtimeSlotJournal) recordMigrationRestore(observation protocol.MigrationRestoreObservation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		current, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(observation.Request.Image.Target.SlotID)))
		if err != nil {
			return err
		}
		custody := current.MigrationDestination
		digest, _ := observation.Request.Image.Digest()
		if custody == nil || custody.Adoption != nil || custody.Failure != nil || custody.Prepared == nil || custody.RequestDigest != digest || *custody.Prepared != observation.Request.Prepared || current.Cleanup != nil {
			return errdefs.ErrFailedPrecondition
		}
		prior := custody.Restore
		if prior == nil {
			if observation.State != protocol.MigrationRestoreIntent {
				return errdefs.ErrFailedPrecondition
			}
		} else {
			if prior.RequestDigest != observation.RequestDigest {
				return errdefs.ErrAlreadyExists
			}
			if prior.State == observation.State {
				return nil
			}
			allowed := observation.State == protocol.MigrationRestoreUncertain ||
				(prior.State == protocol.MigrationRestoreIntent && observation.State == protocol.MigrationRestoreExecuting) ||
				(prior.State == protocol.MigrationRestoreExecuting && observation.State == protocol.MigrationRestoreComplete)
			if prior.State == protocol.MigrationRestoreUncertain || !allowed {
				return errdefs.ErrFailedPrecondition
			}
		}
		custody.Restore = &observation
		current.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, current)
	})
}

// fenceMigrationDestination retains both the image and any post-restore dirty
// filesystem. Generic crash recovery must not publish/discard this ambiguous
// execution history while the regional migration still owns it.
func (d *nodeRuntime) fenceMigrationDestination(ctx context.Context, session rootfssession.RecoverySession) (bool, error) {
	if d.journal == nil {
		return false, nil
	}
	record, err := d.journal.Get(session.Stage.Identity.SlotNonce)
	if errdefs.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if record.MigrationDestination == nil {
		return false, nil
	}
	custody := record.MigrationDestination
	if custody.Failure != nil {
		want, err := custody.Failure.Request.Restore.Stage.BindingDigest()
		actual, ae := session.Stage.BindingDigest()
		if err != nil || ae != nil || want != actual {
			return true, errdefs.ErrFailedPrecondition
		}
		_, err = d.stopFailedMigrationDestination(ctx, custody.Failure.Request)
		return true, err
	}
	if custody.Restore == nil {
		return true, errdefs.ErrFailedPrecondition
	}
	want, _ := custody.Restore.Request.Stage.BindingDigest()
	actual, err := session.Stage.BindingDigest()
	if err != nil || actual != want {
		return true, errdefs.ErrFailedPrecondition
	}
	if custody.Adoption != nil {
		// Finish a durable handoff before ordinary recovery may touch target writes.
		if _, err := d.adoptMigrationDestination(ctx, custody.Adoption.Request); err != nil {
			return true, err
		}
		return false, nil
	}
	observation := *custody.Restore
	observation.State = protocol.MigrationRestoreUncertain
	if err := d.journal.recordMigrationRestore(observation); err != nil {
		return true, err
	}
	state, err := d.runner.State(ctx, record.Registration.RunscContainerID)
	if errdefs.IsNotFound(err) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	if state.ID != record.Registration.RunscContainerID {
		return true, errdefs.ErrFailedPrecondition
	}
	if state.Status != "stopped" {
		if err := d.runner.Kill(ctx, state.ID, "KILL"); err != nil {
			return true, err
		}
		state, err = d.runner.State(ctx, state.ID)
		if errdefs.IsNotFound(err) {
			return true, nil
		}
		if err != nil {
			return true, err
		}
		if state.ID != record.Registration.RunscContainerID || state.Status != "stopped" {
			return true, errdefs.ErrUnavailable
		}
	}
	return true, nil
}

func (c *Client) GetMigrationDestination(ctx context.Context, slotID string) (*MigrationDestinationCustody, error) {
	if err := protocol.ValidateSlotID(slotID); err != nil {
		return nil, err
	}
	var response nodeRuntimeRPCResponse
	if err := c.call(ctx, runtimeMigrationDestinationGetPath, nodeRuntimeRPCRequest{MigrationSlotID: slotID}, &response); err != nil {
		return nil, err
	}
	if custody := response.MigrationDestination; custody != nil {
		want, err := custody.Request.Digest()
		if err != nil || want != custody.RequestDigest || custody.Request.Target.SlotID != slotID ||
			!filepath.IsAbs(custody.ImageDirectory) || filepath.Clean(custody.ImageDirectory) != custody.ImageDirectory || filepath.Base(custody.ImageDirectory) != "destination-"+want {
			return nil, errdefs.ErrUnavailable
		}
		if custody.Prepared != nil && custody.Prepared.ValidateFor(custody.Request) != nil {
			return nil, errdefs.ErrUnavailable
		}
		if err := custody.validateRestore(); err != nil {
			return nil, err
		}
		if err := custody.validateFailure(); err != nil {
			return nil, err
		}
	}
	return response.MigrationDestination, nil
}

func (c *Client) RecordMigrationRestore(ctx context.Context, observation protocol.MigrationRestoreObservation) error {
	if err := observation.Validate(); err != nil {
		return err
	}
	var response nodeRuntimeRPCResponse
	return c.call(ctx, runtimeMigrationRestoreRecordPath, nodeRuntimeRPCRequest{MigrationRestore: &observation}, &response)
}

func (c *MigrationDestinationCustody) validateRestore() error {
	if err := c.validateAdoption(); err != nil {
		return err
	}
	if c.Restore == nil {
		return nil
	}
	if c.Prepared == nil || c.Restore.Validate() != nil || c.Restore.Request.Prepared != *c.Prepared {
		return errdefs.ErrFailedPrecondition
	}
	digest, _ := c.Restore.Request.Image.Digest()
	if digest != c.RequestDigest {
		return fmt.Errorf("restore image custody changed: %w", errdefs.ErrFailedPrecondition)
	}

	return nil
}
