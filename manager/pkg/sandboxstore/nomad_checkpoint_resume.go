package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"maps"
	"reflect"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

// ErrNomadCheckpointNotRetained means an explicit memory resume has no image
// owned by this paused sandbox. Admission must fail before reserving capacity.
var ErrNomadCheckpointNotRetained = errors.New("memory checkpoint is not retained")

// bindNomadResumeMode runs inside ordinary resume's quota/lifecycle transaction.
// A retry may not turn an admitted memory restore into a cold start. The saved
// assignment and compatibility come from capture, never a mutable template.
func bindNomadResumeMode(ctx context.Context, tx pgx.Tx, record *SandboxRecord, lifecycle *SandboxLifecycleTxn,
	generation string, memory, create bool) (*protocol.CheckpointRestoreAuthority, string, error) {
	var payload []byte
	var compatibility string
	err := tx.QueryRow(ctx, `SELECT authority,compatibility_digest FROM manager.sandbox_runtime_checkpoint_restores
		WHERE operation_id=$1 FOR UPDATE`, lifecycle.ID).Scan(&payload, &compatibility)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, "", err
	}
	if err == nil {
		var authority protocol.CheckpointRestoreAuthority
		if !memory || json.Unmarshal(payload, &authority) != nil {
			return nil, "", ErrNomadCheckpointConflict
		}
		capture, capturedCompatibility, err := loadOwnedNomadCheckpoint(ctx, tx, record, generation)
		if err != nil {
			return nil, "", err
		}
		if authority.Assignment.OperationID != lifecycle.ID || authority.Assignment.Target.SandboxID != record.ID ||
			authority.Assignment.PreviousGeneration() != lifecycle.FromGeneration ||
			authority.Assignment.Target.RuntimeGeneration != lifecycle.ToGeneration || authority.LifecycleEpoch != lifecycle.Epoch ||
			authority.ValidateFor(*capture.Publication, *capture.Published) != nil || compatibility != capturedCompatibility {
			return nil, "", ErrNomadCheckpointConflict
		}
		return &authority, compatibility, nil
	}
	if !memory {
		return nil, "", nil
	}
	if !create {
		return nil, "", ErrNomadCheckpointConflict
	}
	capture, compatibility, err := loadOwnedNomadCheckpoint(ctx, tx, record, generation)
	if err != nil {
		return nil, "", err
	}
	source, err := runtimecontrol.NewCheckpointCaptureAssignment(capture.Preflight.Source.OperationID, capture.Assignment)
	if err != nil {
		return nil, "", err
	}
	target := capture.Assignment
	target.SandboxID, target.RuntimeGeneration = record.ID, lifecycle.ToGeneration
	target.EnvVars = maps.Clone(target.EnvVars)
	if _, exists := target.EnvVars[runtimecontrol.EnvSandboxID]; exists {
		target.EnvVars[runtimecontrol.EnvSandboxID] = target.SandboxID
	}
	kind := runtimecontrol.CheckpointResume
	if target.SandboxID != source.SandboxID {
		kind = runtimecontrol.CheckpointFork
	}
	authority := &protocol.CheckpointRestoreAuthority{Assignment: runtimecontrol.CheckpointRestoreAssignment{
		OperationID: lifecycle.ID, Kind: kind, Capture: source, Target: target},
		LifecycleEpoch: lifecycle.Epoch, Retained: *capture.Retained}
	if authority.Assignment.PreviousGeneration() != lifecycle.FromGeneration {
		authority.Assignment.FromGeneration = lifecycle.FromGeneration
	}
	if err := authority.ValidateFor(*capture.Publication, *capture.Published); err != nil {
		return nil, "", err
	}
	payload, err = json.Marshal(authority)
	if err != nil {
		return nil, "", err
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_checkpoint_restores
		(operation_id,checkpoint_id,compatibility_digest,authority,evidence) VALUES ($1,$2,$3,$4,'{}')`,
		lifecycle.ID, authority.Retained.CheckpointID, compatibility, payload)
	if err != nil {
		return nil, "", err
	}
	return authority, compatibility, nil
}

// loadOwnedNomadCheckpoint requires an owner reference to the exact paused head,
// not merely a historical capture. The caller holds the owner's sandbox lock;
// the reference remains pinned across every admitted restore attempt.
func loadOwnedNomadCheckpoint(ctx context.Context, tx pgx.Tx, record *SandboxRecord, generation string) (*NomadCheckpointEvidence, string, error) {
	var payload []byte
	var compatibility, checkpoint, retainedGeneration, phase string
	var retainedRuntime int64
	err := tx.QueryRow(ctx, `SELECT c.evidence,c.compatibility_digest,c.operation_id,r.generation_id,r.runtime_generation,l.phase
		FROM manager.sandbox_runtime_checkpoint_refs r
		JOIN manager.sandbox_runtime_checkpoints c ON c.operation_id=r.checkpoint_id
		JOIN manager.sandbox_lifecycle_txns l ON l.txn_id=c.operation_id
		WHERE r.sandbox_id=$1 FOR UPDATE OF r`, record.ID).Scan(
		&payload, &compatibility, &checkpoint, &retainedGeneration, &retainedRuntime, &phase)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, "", ErrNomadCheckpointNotRetained
	}
	if err != nil {
		return nil, "", err
	}
	var evidence NomadCheckpointEvidence
	if json.Unmarshal(payload, &evidence) != nil || evidence.validate() != nil || evidence.Finalized == nil ||
		evidence.Retained == nil || evidence.Retained.CheckpointID != checkpoint ||
		evidence.Assignment.TeamID != record.TeamID || phase != SandboxLifecyclePhaseCommitted ||
		retainedGeneration != generation || evidence.Publication.Capture.RootFS.Generation.GenerationID != generation ||
		retainedRuntime != record.RuntimeGeneration {
		return nil, "", ErrNomadCheckpointConflict
	}
	return &evidence, compatibility, nil
}

func nomadResumeCandidateForMode(ctx context.Context, tx pgx.Tx, record *SandboxRecord, lifecycle *SandboxLifecycleTxn,
	filesystem, generation string, resetSessions, memory, create bool) (*NomadSandboxResumeCandidate, error) {
	authority, compatibility, err := bindNomadResumeMode(ctx, tx, record, lifecycle, generation, memory, create)
	if err != nil {
		return nil, err
	}
	candidate := nomadSandboxResumeCandidate(record, lifecycle, filesystem, generation, resetSessions)
	candidate.Checkpoint, candidate.CheckpointCompatibilityDigest = authority, compatibility
	if authority != nil {
		candidate.ResetCopiedSessionState = authority.Assignment.Target.ResetCopiedSessionState
	}
	return candidate, nil
}

// Checkpoint restore commands are immutable JSON evidence in the resume
// lifecycle. Keeping phase solely in that lifecycle avoids a second state machine.
type NomadCheckpointRestoreEvidence struct {
	CancelRequest    *protocol.CheckpointImageCancelRequest     `json:"cancel_request,omitempty"`
	CancelProof      *protocol.CheckpointImageCancelProof       `json:"cancel_proof,omitempty"`
	Failure          *protocol.MigrationFailureRequest          `json:"failure,omitempty"`
	FailureStopped   *protocol.MigrationFailureStopProof        `json:"failure_stopped,omitempty"`
	FailureCleanup   *protocol.MigrationFailureCleanupRequest   `json:"failure_cleanup,omitempty"`
	FailureCleaned   *protocol.MigrationFailureCleanupProof     `json:"failure_cleaned,omitempty"`
	FailureFinalized *protocol.MigrationFailureFinalizeProof    `json:"failure_finalized,omitempty"`
	FailureGC        *protocol.MigrationSourceGCRequest         `json:"failure_gc,omitempty"`
	FailureGCAck     *protocol.MigrationSourceGCAcknowledgement `json:"failure_gc_ack,omitempty"`
	CPURequestedAt   *time.Time                                 `json:"cpu_requested_at,omitempty"`
	CPURequest       *protocol.MigrationCPUPreflightRequest     `json:"cpu_request,omitempty"`
	CPU              *protocol.MigrationCPUPreflight            `json:"cpu,omitempty"`
	Image            *protocol.MigrationImagePrepareRequest     `json:"image,omitempty"`
	Prepared         *protocol.MigrationImagePrepared           `json:"prepared,omitempty"`
	Restore          *protocol.MigrationRestoreRequest          `json:"restore,omitempty"`
	Restored         *protocol.MigrationRestoreObservation      `json:"restored,omitempty"`
	Handover         *procdapi.RuntimeCheckpointRequest         `json:"handover,omitempty"`
	HandedOver       *procdapi.RuntimeCheckpointResponse        `json:"handed_over,omitempty"`
	Adoption         *protocol.MigrationAdoptionRequest         `json:"adoption,omitempty"`
	Adopted          *protocol.MigrationAdoptionProof           `json:"adopted,omitempty"`
}

func (e NomadCheckpointRestoreEvidence) validate(authority protocol.CheckpointRestoreAuthority, capture NomadCheckpointEvidence) error {
	if e.CancelRequest != nil {
		if e.Image == nil || e.Restore != nil || !reflect.DeepEqual(e.CancelRequest.Image, *e.Image) {
			return ErrNomadCheckpointConflict
		}
		if _, err := e.CancelRequest.Digest(); err != nil {
			return err
		}
	}
	if e.CancelProof != nil && (e.CancelRequest == nil || e.CancelProof.ValidateFor(*e.CancelRequest) != nil) {
		return ErrNomadCheckpointConflict
	}
	if err := e.validateFailure(); err != nil {
		return err
	}
	if e.CPURequest != nil {
		r := e.CPURequest
		if e.CPURequestedAt == nil || e.CPURequestedAt.IsZero() || r.Validate() != nil || !reflect.DeepEqual(r.Checkpoint, &authority.Assignment) ||
			r.Source != capture.Preflight.Source || r.SourceResources != capture.Preflight.SourceResources ||
			!reflect.DeepEqual(r.Launch, &capture.CPU.Launch) {
			return ErrNomadCheckpointConflict
		}
	}
	if e.CPURequestedAt != nil && e.CPURequest == nil {
		return ErrNomadCheckpointConflict
	}
	if e.CPU != nil && (e.CPURequest == nil || e.CPU.ValidateFor(*e.CPURequest) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Image != nil && (e.CPU == nil || e.Image.Validate() != nil ||
		!reflect.DeepEqual(e.Image.Checkpoint, &authority) || !reflect.DeepEqual(e.Image.Publication, *capture.Publication) ||
		e.Image.Receipt != *capture.Published || e.Image.Target != e.CPURequest.Target ||
		e.Image.Resources != e.CPURequest.DestinationResources) {
		return ErrNomadCheckpointConflict
	}
	if e.Prepared != nil && (e.Image == nil || e.Prepared.ValidateFor(*e.Image) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Restore != nil && (e.Prepared == nil || e.Restore.Validate() != nil ||
		!reflect.DeepEqual(e.Restore.Image, *e.Image) || e.Restore.Prepared != *e.Prepared ||
		!reflect.DeepEqual(&e.Restore.Fence, capture.SourceFence) || !reflect.DeepEqual(&e.Restore.Proof, capture.Fenced)) {
		return ErrNomadCheckpointConflict
	}
	if e.Restored != nil && (e.Restore == nil || e.Restored.Validate() != nil || e.Restored.State != protocol.MigrationRestoreComplete ||
		!reflect.DeepEqual(e.Restored.Request, *e.Restore)) {
		return ErrNomadCheckpointConflict
	}
	if e.Handover != nil {
		if e.Restored == nil || capture.Preparation == nil || e.Handover.Action != procdapi.MigrationRestore ||
			e.Handover.InstanceID != capture.Preparation.InstanceID || e.Handover.CaptureEpoch != capture.Preparation.CaptureEpoch ||
			e.Handover.LifecycleEpoch != authority.LifecycleEpoch || e.Handover.Capture != authority.Assignment.Capture ||
			!reflect.DeepEqual(e.Handover.Restore, &authority.Assignment) {
			return ErrNomadCheckpointConflict
		}
		if _, err := e.Handover.Digest(); err != nil {
			return ErrNomadCheckpointConflict
		}
	}
	if e.HandedOver != nil && (e.Handover == nil || e.HandedOver.ValidateFor(*e.Handover) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Adoption != nil && (e.HandedOver == nil || e.Adoption.ValidateFor(*e.Restored) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Adopted != nil && (e.Adoption == nil || e.Adopted.ValidateFor(*e.Adoption) != nil) {
		return ErrNomadCheckpointConflict
	}
	return nil
}
