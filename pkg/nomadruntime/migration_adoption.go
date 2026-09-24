package nomadruntime

import (
	"context"
	"time"

	"github.com/containerd/errdefs"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
	bolt "go.etcd.io/bbolt"
)

type MigrationAdopter interface {
	AdoptMigrationDestination(context.Context, protocol.MigrationAdoptionRequest) (*protocol.MigrationAdoptionProof, error)
}

// MigrationAdoptionCustody retains both intent and receipt. Intent is already
// irreversible: recovery must finish it instead of marking restore uncertain.
type MigrationAdoptionCustody struct {
	RegionalAcknowledgement *protocol.MigrationAdoptionAcknowledgement `json:"regional_acknowledgement,omitempty"`
	Request                 protocol.MigrationAdoptionRequest          `json:"request"`
	RequestDigest           string                                     `json:"request_digest"`
	Proof                   *protocol.MigrationAdoptionProof           `json:"proof,omitempty"`
}

func (c *MigrationDestinationCustody) Adopted() bool {
	return c != nil && c.Adoption != nil && c.Adoption.Proof != nil
}

func (c *MigrationDestinationCustody) validateAdoption() error {
	if c.Adoption == nil {
		return nil
	}
	a := c.Adoption
	want, err := a.Request.Digest()
	if err != nil || want != a.RequestDigest || c.Restore == nil || a.Request.ValidateFor(*c.Restore) != nil {
		return errdefs.ErrFailedPrecondition
	}
	if a.Proof != nil && a.Proof.ValidateFor(a.Request) != nil {
		return errdefs.ErrFailedPrecondition
	}
	if a.RegionalAcknowledgement != nil && (a.Proof == nil || a.RegionalAcknowledgement.ValidateFor(protocol.MigrationAdoptionReceipt{Request: a.Request, Proof: *a.Proof}) != nil) {
		return errdefs.ErrFailedPrecondition
	}
	return nil
}

// pendingCustody includes unacknowledged historical receipts in the existing
// bounded migration admission budget, even after image bytes have been removed.
func (c *MigrationDestinationCustody) pendingCustody() bool {
	if c.ImageCanceled() {
		return false
	}
	if c.FailureFinalized() {
		return c.Failure.Cleanup.Finalization.AllocationGC == nil
	}
	return c != nil && (!c.Adopted() || c.Adoption.RegionalAcknowledgement == nil)
}

// AdoptMigrationDestination is root-only control. The regional command must
// come from authenticated command readiness or command recovery after generation CAS.
// The per-slot lock serializes intent with restore fencing and generic cleanup.
func (d *nodeRuntime) AdoptMigrationDestination(ctx context.Context, request protocol.MigrationAdoptionRequest) (*protocol.MigrationAdoptionProof, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	if d == nil || d.journal == nil || d.runtime == nil || d.runner == nil {
		return nil, errdefs.ErrUnavailable
	}
	if request.Target.ClusterID != d.clusterID || request.Target.NodeID != d.nodeID || request.Target.NodeUID != d.nodeUID {
		return nil, errdefs.ErrPermissionDenied
	}
	if !d.beginReconciliation(request.Target.SlotID, nil) {
		return nil, errdefs.ErrUnavailable
	}
	defer d.endReconciliation(request.Target.SlotID)
	return d.adoptMigrationDestination(ctx, request, false)
}

// A committed checkpoint resume retains an immutable regional image. Once the
// region publishes its exact command-ready adoption request, a later stop must
// not strand local image custody before ordinary crash cleanup can run.
func (d *nodeRuntime) committedCheckpointAdoption(ctx context.Context, custody *MigrationDestinationCustody) (*protocol.MigrationAdoptionRequest, error) {
	if custody == nil || custody.Restore == nil || custody.Request.Checkpoint == nil ||
		(custody.Restore.State != protocol.MigrationRestoreComplete && custody.Restore.State != protocol.MigrationRestoreUncertain) {
		return nil, nil
	}
	reader, ok := d.registrationAuthority.(migrationAdoptionCommandReader)
	if !ok {
		return nil, errdefs.ErrUnavailable
	}
	command, err := reader.GetMigrationAdoptionCommand(ctx, custody.Request.Target.SlotID)
	if err != nil || command == nil {
		return command, err
	}
	restored := *custody.Restore
	restored.State = protocol.MigrationRestoreComplete
	if command.CheckpointRestoreDigest == "" || command.ValidateFor(restored) != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	return command, nil
}

func (d *nodeRuntime) adoptMigrationDestination(ctx context.Context, request protocol.MigrationAdoptionRequest, committedCheckpoint bool) (*protocol.MigrationAdoptionProof, error) {
	record, err := d.journal.Get(request.Target.SlotID)
	if err != nil {
		return nil, err
	}
	c := record.MigrationDestination
	if c == nil || c.Restore == nil || c.Failure != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	restored := *c.Restore
	if committedCheckpoint && request.CheckpointRestoreDigest != "" && c.Request.Checkpoint != nil {
		restored.State = protocol.MigrationRestoreComplete
	}
	if request.ValidateFor(restored) != nil {
		return nil, errdefs.ErrFailedPrecondition
	}
	want, _ := request.Digest()
	if c.Adoption != nil {
		if c.Adoption.RequestDigest != want {
			return nil, errdefs.ErrAlreadyExists
		}
		if c.Adopted() {
			proof := *c.Adoption.Proof
			return &proof, nil
		}
	} else if !committedCheckpoint {
		// First authorization requires the restored writer and process to be
		// alive. Once intent is durable, retry must finish even after a crash.
		sessions, err := d.runtime.RecoverySessions()
		if err != nil {
			return nil, err
		}
		matched := false
		binding, _ := c.Restore.Request.Stage.BindingDigest()
		for _, session := range sessions {
			actual, err := session.Stage.BindingDigest()
			if err == nil && actual == binding && session.Live {
				matched = true
			}
		}
		if !matched {
			return nil, errdefs.ErrFailedPrecondition
		}
		state, err := d.runner.State(ctx, record.Registration.RunscContainerID)
		if err != nil {
			return nil, err
		}
		if state.ID != record.Registration.RunscContainerID || state.Status != "running" {
			return nil, errdefs.ErrFailedPrecondition
		}
		if err := d.journal.recordMigrationAdoption(request, nil); err != nil {
			return nil, err
		}
	} else {
		if request.CheckpointRestoreDigest == "" {
			return nil, errdefs.ErrFailedPrecondition
		}
		if err := d.journal.recordCommittedCheckpointAdoption(request); err != nil {
			return nil, err
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := removeMigrationImage(d.journal.migrationRoot, c.ImageDirectory); err != nil {
		return nil, err
	}
	proof := protocol.MigrationAdoptionProof{RequestDigest: want, ImageAbsent: true}
	if err := d.journal.recordMigrationAdoption(request, &proof); err != nil {
		return nil, err
	}
	return &proof, nil
}

func (j *runtimeSlotJournal) recordMigrationAdoption(request protocol.MigrationAdoptionRequest, proof *protocol.MigrationAdoptionProof) error {
	return j.recordMigrationAdoptionWithCommit(request, proof, false)
}

func (j *runtimeSlotJournal) recordCommittedCheckpointAdoption(request protocol.MigrationAdoptionRequest) error {
	return j.recordMigrationAdoptionWithCommit(request, nil, true)
}

func (j *runtimeSlotJournal) recordMigrationAdoptionWithCommit(request protocol.MigrationAdoptionRequest, proof *protocol.MigrationAdoptionProof, committedCheckpoint bool) error {
	want, err := request.Digest()
	if err != nil {
		return err
	}
	if proof != nil && proof.ValidateFor(request) != nil {
		return errdefs.ErrInvalidArgument
	}
	return j.db.Update(func(tx *bolt.Tx) error {
		bucket, err := runtimeSlotJournalBucketFrom(tx)
		if err != nil {
			return err
		}
		record, err := decodeRuntimeSlotJournalRecord(bucket.Get([]byte(request.Target.SlotID)))
		if err != nil {
			return err
		}
		c := record.MigrationDestination
		if c == nil || c.Restore == nil {
			return errdefs.ErrFailedPrecondition
		}
		restored := *c.Restore
		if committedCheckpoint && request.CheckpointRestoreDigest != "" && c.Request.Checkpoint != nil && c.Adoption == nil {
			restored.State = protocol.MigrationRestoreComplete
		}
		if request.ValidateFor(restored) != nil {
			return errdefs.ErrFailedPrecondition
		}
		if c.Adoption == nil {
			if proof != nil || record.Cleanup != nil {
				return errdefs.ErrFailedPrecondition
			}
			if committedCheckpoint {
				c.Restore = &restored
			}
			c.Adoption = &MigrationAdoptionCustody{Request: request, RequestDigest: want}
		} else {
			if c.Adoption.RequestDigest != want {
				return errdefs.ErrAlreadyExists
			}
			if c.Adoption.Proof != nil || proof == nil {
				return nil
			}
			copy := *proof
			c.Adoption.Proof = &copy
		}
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		return putRuntimeSlotJournalRecord(bucket, record)
	})
}

func (c *Client) AdoptMigrationDestination(ctx context.Context, request protocol.MigrationAdoptionRequest) (*protocol.MigrationAdoptionProof, error) {
	if err := request.Validate(); err != nil {
		return nil, err
	}
	var response nodeRuntimeRPCResponse
	if err := c.call(ctx, runtimeMigrationAdoptPath, nodeRuntimeRPCRequest{MigrationAdoption: &request}, &response); err != nil {
		return nil, err
	}
	if response.MigrationAdoption == nil || response.MigrationAdoption.ValidateFor(request) != nil {
		return nil, errdefs.ErrUnavailable
	}
	return response.MigrationAdoption, nil
}
