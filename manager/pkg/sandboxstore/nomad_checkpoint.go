package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/nomadmigration"
	"github.com/sandbox0-ai/sandbox0/pkg/procdapi"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecheckpoint"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

var ErrNomadCheckpointConflict = errors.New("memory checkpoint lifecycle conflict")

// NomadCheckpointEvidence is append-only recovery evidence within the existing
// lifecycle transaction. Phase remains owned exclusively by that transaction.
// Preflight.Source identifies a prospective capture; only CaptureAuthorized
// permits dispatch of its execution-changing command.
type NomadCheckpointEvidence struct {
	CaptureFailure                *protocol.MigrationCaptureFailureRequest       `json:"capture_failure,omitempty"`
	CaptureFailureCleanup         *protocol.MigrationCaptureFailureProof         `json:"capture_failure_cleanup,omitempty"`
	CaptureFailureFinalized       *protocol.MigrationCaptureFailureFinalizeProof `json:"capture_failure_finalized,omitempty"`
	CaptureFailureStagingReleased *protocol.MigrationStagingReleased             `json:"capture_failure_staging_released,omitempty"`
	CaptureFailureGC              *protocol.MigrationSourceGCRequest             `json:"capture_failure_gc,omitempty"`
	CaptureFailureGCAck           *protocol.MigrationSourceGCAcknowledgement     `json:"capture_failure_gc_ack,omitempty"`
	CancelAuthorized              bool                                           `json:"cancel_authorized,omitempty"`
	Canceled                      *procdapi.RuntimeCheckpointResponse            `json:"canceled,omitempty"`
	SourceAbsentProof             []byte                                         `json:"source_absent_proof,omitempty"`
	StagingReleased               *protocol.MigrationStagingReleased             `json:"staging_released,omitempty"`
	Assignment                    runtimecontrol.Assignment                      `json:"assignment"`
	Policy                        string                                         `json:"policy"`
	Address                       string                                         `json:"address"`
	Preflight                     protocol.MigrationCPUPreflightRequest          `json:"preflight"`
	CPU                           *protocol.MigrationCPUPreflight                `json:"cpu,omitempty"`
	Staging                       *protocol.MigrationStagingRequest              `json:"staging,omitempty"`
	Staged                        *protocol.MigrationStagingReserved             `json:"staged,omitempty"`
	Preparation                   *procdapi.RuntimeCheckpointRequest             `json:"preparation,omitempty"`
	Prepared                      *procdapi.RuntimeCheckpointResponse            `json:"prepared,omitempty"`
	CaptureAuthorized             bool                                           `json:"capture_authorized,omitempty"`
	Publication                   *protocol.MigrationPublicationRequest          `json:"publication,omitempty"`
	Published                     *protocol.MigrationPublication                 `json:"published,omitempty"`
	Retained                      *protocol.CheckpointRetained                   `json:"retained,omitempty"`
	SourceFence                   *protocol.MigrationSourceFenceRequest          `json:"source_fence,omitempty"`
	Fenced                        *protocol.MigrationSourceFenceProof            `json:"fenced,omitempty"`
	Finalization                  *protocol.MigrationSourceFinalizeRequest       `json:"finalization,omitempty"`
	Finalized                     *protocol.MigrationSourceFinalizeProof         `json:"finalized,omitempty"`
}

type NomadSandboxCheckpoint struct {
	Lifecycle           *SandboxLifecycleTxn
	SourceSlotID        string
	SourceWriterGrantID string
	CompatibilityDigest string
	Evidence            NomadCheckpointEvidence
}

func (e NomadCheckpointEvidence) validate() error {
	if err := e.validateCaptureFailure(); err != nil {
		return err
	}
	if e.CancelAuthorized && e.CaptureAuthorized {
		return ErrNomadCheckpointConflict
	}
	if e.Canceled != nil {
		if !e.CancelAuthorized || e.Preparation == nil {
			return ErrNomadCheckpointConflict
		}
		request := *e.Preparation
		request.Action = procdapi.MigrationCancel
		if e.Canceled.ValidateFor(request) != nil {
			return ErrNomadCheckpointConflict
		}
	}
	if len(e.SourceAbsentProof) != 0 && (!e.CancelAuthorized || e.Preparation == nil || len(e.SourceAbsentProof) != 32) {
		return ErrNomadCheckpointConflict
	}
	if e.StagingReleased != nil && (!e.CancelAuthorized || e.Staging == nil || e.StagingReleased.ValidateFor(*e.Staging) != nil) {
		return ErrNomadCheckpointConflict
	}
	if err := e.Preflight.Validate(); err != nil {
		return err
	}
	source := e.Preflight.Source
	revision, err := e.Assignment.Revision()
	if err != nil || !e.Preflight.CaptureOnly || e.Assignment.TeamID == "" ||
		e.Assignment.SandboxID != source.SandboxID || e.Assignment.RuntimeGeneration != source.SourceGeneration ||
		revision != source.AssignmentRevision || nomadmigration.ValidateAssignmentPolicy(e.Assignment, e.Policy) != nil ||
		protocol.ValidateNomadProcdAddress(e.Address) != nil {
		return ErrNomadCheckpointConflict
	}
	if e.CPU != nil && e.CPU.ValidateFor(e.Preflight) != nil {
		return ErrNomadCheckpointConflict
	}
	if e.Staging != nil && (e.CPU == nil || !e.Staging.CaptureOnly || e.Staging.Source != source || e.Staging.Validate() != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Staged != nil && (e.Staging == nil || e.Staged.ValidateFor(*e.Staging) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Preparation != nil {
		capture, err := runtimecontrol.NewCheckpointCaptureAssignment(source.OperationID, e.Assignment)
		if err != nil || e.Staged == nil || e.Preparation.Action != procdapi.MigrationPrepare ||
			e.Preparation.Capture != capture || e.Preparation.InstanceID != source.ProcdInstanceID ||
			e.Preparation.CaptureEpoch != source.LifecycleEpoch || e.Preparation.LifecycleEpoch != source.LifecycleEpoch {
			return ErrNomadCheckpointConflict
		}
		if _, err := e.Preparation.Digest(); err != nil {
			return err
		}
	}
	if e.Prepared != nil && (e.Preparation == nil || e.Prepared.ValidateFor(*e.Preparation) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.CaptureAuthorized && e.Prepared == nil {
		return ErrNomadCheckpointConflict
	}
	if e.Publication != nil {
		if !e.CaptureAuthorized || e.CPU == nil || e.Publication.CheckpointSource == nil ||
			!reflect.DeepEqual(*e.Publication.CheckpointSource, e.Assignment) ||
			e.Publication.Capture.Request != source || !reflect.DeepEqual(e.Publication.CPULaunch, &e.CPU.Launch) {
			return ErrNomadCheckpointConflict
		}
		if _, err := e.Publication.Digest(); err != nil {
			return err
		}
	}
	if e.Published != nil && (e.Publication == nil || e.Published.ValidateFor(*e.Publication) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Retained != nil && (e.Published == nil || e.Retained.CheckpointID != source.OperationID ||
		e.Retained.ValidateFor(*e.Publication, *e.Published) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.SourceFence != nil && (e.Retained == nil ||
		!reflect.DeepEqual(e.SourceFence.PublicationRequest, *e.Publication) || e.SourceFence.Publication != *e.Published) {
		return ErrNomadCheckpointConflict
	}
	if e.Fenced != nil && (e.SourceFence == nil || e.Fenced.ValidateFor(*e.SourceFence) != nil) {
		return ErrNomadCheckpointConflict
	}
	if e.Finalization != nil {
		if e.Fenced == nil || !reflect.DeepEqual(e.Finalization.Fence, *e.SourceFence) ||
			!reflect.DeepEqual(e.Finalization.SourceProof, *e.Fenced) || !reflect.DeepEqual(e.Finalization.Checkpoint, e.Retained) {
			return ErrNomadCheckpointConflict
		}
		if _, err := e.Finalization.Digest(); err != nil {
			return err
		}
	}
	if e.Finalized != nil && (e.Finalization == nil || e.Finalized.ValidateFor(*e.Finalization) != nil) {
		return ErrNomadCheckpointConflict
	}
	return nil
}

// ReserveNomadSandboxMemoryPause serializes against every existing lifecycle,
// but reserves no destination capacity and does not stop or retire the writer.
// It returns only read-only CPU preflight authority. Callers must use the
// separate durable preparation and capture gates before touching execution.
func (s *PGSandboxStore) ReserveNomadSandboxMemoryPause(ctx context.Context, operation string, assignment runtimecontrol.Assignment, policy string) (*NomadSandboxCheckpoint, error) {
	if _, err := runtimecontrol.NewCheckpointCaptureAssignment(operation, assignment); err != nil {
		return nil, err
	}
	if err := nomadmigration.ValidateAssignmentPolicy(assignment, policy); err != nil {
		return nil, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeSlotClaimOperation(ctx, tx, operation); err != nil {
		return nil, err
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, assignment.SandboxID)
	if err != nil {
		return nil, err
	}
	result, err := reserveNomadSandboxMemoryPauseTx(ctx, tx, record, operation, assignment, policy)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}

// The caller holds the sandbox row lock. Product admission derives its own
// operation identity; the explicit reservation API additionally fences caller-
// supplied operation IDs before taking that lock.
func reserveNomadSandboxMemoryPauseTx(ctx context.Context, tx pgx.Tx, record *SandboxRecord, operation string, assignment runtimecontrol.Assignment, policy string) (*NomadSandboxCheckpoint, error) {
	captureAssignment, err := runtimecontrol.NewCheckpointCaptureAssignment(operation, assignment)
	if err != nil {
		return nil, err
	}
	if err := nomadmigration.ValidateAssignmentPolicy(assignment, policy); err != nil {
		return nil, err
	}
	if record.TeamID != assignment.TeamID || record.DesiredState != SandboxDesiredStateActive ||
		!record.DeletedAt.IsZero() || record.RuntimeGeneration != assignment.RuntimeGeneration {
		return nil, ErrNomadCheckpointConflict
	}
	existing, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, operation))
	if err != nil {
		return nil, err
	}
	if existing != nil {
		result, err := loadNomadCheckpoint(ctx, tx, existing)
		if err != nil {
			return nil, err
		}
		revision, _ := result.Evidence.Assignment.Revision()
		if !nomadCheckpointLifecycleMatches(existing, record) || revision != captureAssignment.Revision || result.Evidence.Policy != policy {
			return nil, ErrNomadCheckpointConflict
		}
		return result, nil
	}
	active, err := getActiveLifecycleTxn(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if active != nil {
		return nil, ErrNomadCheckpointConflict
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, record)
	if err != nil {
		return nil, err
	}
	slot := writer.slot
	imageBytes := (slot.ResourceLease.MemoryBytes + 64<<20 + 4095) / 4096 * 4096
	uploadBytes, err := protocol.MigrationCaptureUploadBytes(imageBytes)
	if err != nil || uploadBytes+uploadBytes/32+2*runtimecheckpoint.MaxManifestBytes > nomadMigrationCaptureUploadRegionBytes {
		return nil, ErrNomadCheckpointConflict
	}
	if slot.ClaimRuntimeAssignmentRevision != captureAssignment.Revision ||
		slot.ClaimNetworkPolicyDigest != protocol.NetworkPolicyDigest(policy) ||
		slot.ResourceLeaseState != RuntimeResourceLeaseActive || slot.ResourceLease.IsZero() ||
		!bytes.Equal(slot.RootFSBindingDigest, writer.grant.BindingDigest) {
		return nil, ErrNomadCheckpointConflict
	}
	lifecycle := &SandboxLifecycleTxn{ID: operation, SandboxID: record.ID,
		Kind: SandboxLifecycleKindPause, Source: SandboxLifecycleSourceManual, Phase: SandboxLifecyclePhasePreparing,
		FromGeneration: record.RuntimeGeneration, FromRuntimeID: record.RuntimeID, FromRuntimeNamespace: record.RuntimeNamespace,
		ExpectedGenerationID: writer.generation.ID}
	if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, lifecycle); err != nil {
		return nil, err
	}
	source := protocol.MigrationCaptureRequest{Target: nomadMigrationSlotTarget(slot), OperationID: operation,
		LifecycleEpoch: lifecycle.Epoch, SandboxID: record.ID, SourceGeneration: record.RuntimeGeneration,
		AssignmentRevision: captureAssignment.Revision, BindingDigest: hex.EncodeToString(writer.grant.BindingDigest),
		ResourceLeaseDigest: hex.EncodeToString(slot.ResourceLeaseDigest), ProcdInstanceID: slot.ProcdInstanceID}
	result := &NomadSandboxCheckpoint{Lifecycle: lifecycle, SourceSlotID: slot.ID, SourceWriterGrantID: writer.grant.ID,
		CompatibilityDigest: slot.CompatibilityDigest, Evidence: NomadCheckpointEvidence{
			Assignment: assignment, Policy: policy, Address: slot.ProcdAddress,
			Preflight: protocol.MigrationCPUPreflightRequest{CaptureOnly: true, Target: source.Target, Source: source, SourceResources: slot.ResourceLease},
		}}
	if err := result.Evidence.validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(result.Evidence)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_checkpoints
		(operation_id,source_slot_id,source_writer_grant_id,compatibility_digest,evidence) VALUES ($1,$2,$3,$4,$5)`,
		operation, slot.ID, writer.grant.ID, slot.CompatibilityDigest, payload)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func loadNomadCheckpoint(ctx context.Context, tx pgx.Tx, lifecycle *SandboxLifecycleTxn) (*NomadSandboxCheckpoint, error) {
	if lifecycle == nil || lifecycle.Kind != SandboxLifecycleKindPause || lifecycle.Source != SandboxLifecycleSourceManual {
		return nil, ErrNomadCheckpointConflict
	}
	result := &NomadSandboxCheckpoint{Lifecycle: lifecycle}
	var payload []byte
	err := tx.QueryRow(ctx, `SELECT source_slot_id,source_writer_grant_id,compatibility_digest,evidence
		FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1 FOR UPDATE`, lifecycle.ID).
		Scan(&result.SourceSlotID, &result.SourceWriterGrantID, &result.CompatibilityDigest, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNomadCheckpointConflict
	}
	if err != nil {
		return nil, err
	}
	if json.Unmarshal(payload, &result.Evidence) != nil || result.Evidence.validate() != nil {
		return nil, ErrNomadCheckpointConflict
	}
	source := result.Evidence.Preflight.Source
	if source.OperationID != lifecycle.ID || source.SandboxID != lifecycle.SandboxID ||
		source.LifecycleEpoch != lifecycle.Epoch || source.SourceGeneration != lifecycle.FromGeneration ||
		source.Target.SlotID != result.SourceSlotID || source.Target.AllocationID != lifecycle.FromRuntimeID {
		return nil, ErrNomadCheckpointConflict
	}
	return result, nil
}

func nomadCheckpointLifecycleMatches(l *SandboxLifecycleTxn, r *SandboxRecord) bool {
	return l != nil && l.SandboxID == r.ID && l.Kind == SandboxLifecycleKindPause && l.Source == SandboxLifecycleSourceManual &&
		!l.Cancelable && l.CancelRequestedAt.IsZero() && l.Epoch == r.LifecycleEpoch &&
		l.FromGeneration == r.RuntimeGeneration && l.ToGeneration == 0 && l.ToRuntimeID == "" && l.ToRuntimeNamespace == "" &&
		l.FromRuntimeID == r.RuntimeID && l.FromRuntimeNamespace == r.RuntimeNamespace &&
		(l.Phase == SandboxLifecyclePhasePreparing || l.Phase == SandboxLifecyclePhaseBarriered ||
			l.Phase == SandboxLifecyclePhasePublishing || l.Phase == SandboxLifecyclePhaseCommitting)
}

// Mutations lock operation, optional team quota, sandbox, lifecycle and then
// checkpoint evidence. Running-fork cleanup carries active quota into the
// parent's resume. No retry can extend the original CPU window.
func (s *PGSandboxStore) mutateNomadCheckpoint(ctx context.Context, operation string,
	fn func(pgx.Tx, *SandboxRecord, *NomadSandboxCheckpoint) error) (*NomadSandboxCheckpoint, error) {
	return s.mutateNomadCheckpointTx(ctx, operation, false, false, fn)
}

// Retirement creates no execution authority. A hard TTL that expires after
// capture must not prevent retaining its image or physically fencing its writer.
// The exact active lifecycle, runtime generation and source remain mandatory.
func (s *PGSandboxStore) mutateNomadCheckpointRetirement(ctx context.Context, operation string,
	fn func(pgx.Tx, *SandboxRecord, *NomadSandboxCheckpoint) error) (*NomadSandboxCheckpoint, error) {
	return s.mutateNomadCheckpointTx(ctx, operation, true, false, fn)
}

// Historical cleanup is bound to the already-fenced source, even after its
// owner expires, terminates, or advances to a later runtime generation.
func (s *PGSandboxStore) mutateNomadCheckpointCleanup(ctx context.Context, operation string,
	fn func(pgx.Tx, *SandboxRecord, *NomadSandboxCheckpoint) error) (*NomadSandboxCheckpoint, error) {
	return s.mutateNomadCheckpointTx(ctx, operation, true, true, fn)
}

func (s *PGSandboxStore) mutateNomadCheckpointTx(ctx context.Context, operation string, retirement, historical bool,
	fn func(pgx.Tx, *SandboxRecord, *NomadSandboxCheckpoint) error) (*NomadSandboxCheckpoint, error) {
	if strings.TrimSpace(operation) != operation || operation == "" || len(operation) > 256 {
		return nil, ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeSlotClaimOperation(ctx, tx, operation); err != nil {
		return nil, err
	}
	var sandbox string
	if err := tx.QueryRow(ctx, `SELECT sandbox_id FROM manager.sandbox_lifecycle_txns WHERE txn_id=$1`, operation).Scan(&sandbox); err != nil {
		return nil, err
	}
	if historical {
		if err := lockRunningMemoryForkQuota(ctx, tx, operation); err != nil {
			return nil, err
		}
	}
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandbox)
	if err != nil {
		return nil, err
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+` WHERE txn_id=$1 FOR UPDATE`, operation))
	if err != nil {
		return nil, err
	}
	if !historical && (!nomadCheckpointLifecycleMatches(lifecycle, record) ||
		(record.DesiredState != SandboxDesiredStateActive && (!retirement || record.DesiredState != SandboxDesiredStateTerminating)) || !record.DeletedAt.IsZero()) {
		return nil, ErrNomadCheckpointConflict
	}
	result, err := loadNomadCheckpoint(ctx, tx, lifecycle)
	if err != nil {
		return nil, err
	}
	if result.Evidence.CancelAuthorized || result.Evidence.CaptureFailure != nil || result.Evidence.Assignment.TeamID != record.TeamID {
		return nil, ErrNomadCheckpointConflict
	}
	if historical && (result.Evidence.Fenced == nil || lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		(lifecycle.Phase != SandboxLifecyclePhaseCommitting && lifecycle.Phase != SandboxLifecyclePhaseCommitted)) {
		return nil, ErrNomadCheckpointConflict
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT hard_expires_at IS NULL OR hard_expires_at>clock_timestamp()
		FROM manager.sandboxes WHERE sandbox_id=$1`, sandbox).Scan(&live); err != nil {
		return nil, err
	}
	if !live && !retirement {
		return nil, ErrNomadSandboxHardTTLExpired
	}
	before, err := json.Marshal(result.Evidence)
	if err != nil {
		return nil, err
	}
	if err := fn(tx, record, result); err != nil {
		return nil, err
	}
	if err := result.Evidence.validate(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(result.Evidence)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(before, payload) {
		if _, err := tx.Exec(ctx, `UPDATE manager.sandbox_runtime_checkpoints SET evidence=$2 WHERE operation_id=$1`, operation, payload); err != nil {
			return nil, err
		}
	}
	return result, tx.Commit(ctx)
}

func validateNomadCheckpointLiveSource(ctx context.Context, tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) (*RuntimeSlot, error) {
	return validateNomadCheckpointSource(ctx, tx, record, c, true)
}

func validateNomadCheckpointSource(ctx context.Context, tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint, requireFresh bool) (*RuntimeSlot, error) {
	if requireFresh {
		var fresh bool
		if err := tx.QueryRow(ctx, `SELECT created_at <= clock_timestamp() AND created_at+INTERVAL '2 minutes'>clock_timestamp()
		FROM manager.sandbox_runtime_checkpoints WHERE operation_id=$1`, c.Lifecycle.ID).Scan(&fresh); err != nil {
			return nil, err
		}
		if !fresh {
			return nil, ErrNomadCheckpointConflict
		}
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, record)
	if err != nil {
		return nil, err
	}
	slot, e := writer.slot, c.Evidence
	source := e.Preflight.Source
	if nomadMigrationSlotTarget(slot) != source.Target || slot.ResourceLease != e.Preflight.SourceResources ||
		slot.ResourceLeaseState != RuntimeResourceLeaseActive || slot.WriterGrantID != c.SourceWriterGrantID ||
		slot.CompatibilityDigest != c.CompatibilityDigest || slot.ProcdInstanceID != source.ProcdInstanceID ||
		slot.ClaimRuntimeAssignmentRevision != source.AssignmentRevision || slot.ProcdAddress != e.Address ||
		slot.ClaimNetworkPolicyDigest != protocol.NetworkPolicyDigest(e.Policy) ||
		hex.EncodeToString(writer.grant.BindingDigest) != source.BindingDigest || writer.generation.ID != c.Lifecycle.ExpectedGenerationID {
		return nil, ErrNomadCheckpointConflict
	}
	return slot, nil
}

func (s *PGSandboxStore) CommitNomadCheckpointCPU(ctx context.Context, request protocol.MigrationCPUPreflightRequest, response protocol.MigrationCPUPreflight) error {
	if err := response.ValidateFor(request); err != nil {
		return err
	}
	_, err := s.mutateNomadCheckpoint(ctx, request.Source.OperationID, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if !reflect.DeepEqual(e.Preflight, request) {
			return ErrNomadCheckpointConflict
		}
		if e.CPU != nil {
			if !reflect.DeepEqual(*e.CPU, response) {
				return ErrNomadCheckpointConflict
			}
			return nil
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhasePreparing {
			return ErrNomadCheckpointConflict
		}
		slot, err := validateNomadCheckpointLiveSource(ctx, tx, record, c)
		if err != nil {
			return err
		}
		if response.Launch.LaunchAttempt != slot.LaunchAttempt {
			return ErrNomadCheckpointConflict
		}
		if err := validateNomadMigrationCPULineage(ctx, tx, slot, response.Launch); err != nil {
			return err
		}
		e.CPU = &response
		return nil
	})
	return err
}

// Staging uses migration's exact node pool, quotas and crash journal. The
// regional growing-upload grant is optional; completed-image publication
// remains available without granting unaccounted tentative object storage.
func (s *PGSandboxStore) AuthorizeNomadCheckpointStaging(ctx context.Context, operation string) (*protocol.MigrationStagingRequest, error) {
	c, err := s.mutateNomadCheckpoint(ctx, operation, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if e.Staging != nil {
			return nil
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhasePreparing || e.CPU == nil {
			return ErrNomadCheckpointConflict
		}
		if _, err := validateNomadCheckpointLiveSource(ctx, tx, record, c); err != nil {
			return err
		}
		source := e.Preflight.Source
		bytes := (e.Preflight.SourceResources.MemoryBytes + 64<<20 + 4095) / 4096 * 4096
		if _, err := protocol.MigrationCaptureUploadBytes(bytes); err != nil {
			return ErrNomadCheckpointConflict
		}
		cpu, err := e.CPU.Launch.GuestCPUProfile().Digest()
		if err != nil {
			return err
		}
		grant, err := reserveNomadCheckpointCaptureUpload(ctx, tx, source, record.TeamID, c.CompatibilityDigest, cpu, bytes)
		if err != nil {
			return err
		}
		e.Staging = &protocol.MigrationStagingRequest{CaptureOnly: true, Target: source.Target, Source: source,
			Bytes: bytes, Inodes: runtimecheckpoint.MaxFiles * 8, CaptureUpload: grant}
		return e.Staging.Validate()
	})
	if err != nil {
		return nil, err
	}
	return c.Evidence.Staging, nil
}

func (s *PGSandboxStore) CommitNomadCheckpointStaging(ctx context.Context, request protocol.MigrationStagingRequest, response protocol.MigrationStagingReserved) error {
	if err := response.ValidateFor(request); err != nil {
		return err
	}
	_, err := s.mutateNomadCheckpoint(ctx, request.Source.OperationID, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if e.Staging == nil || *e.Staging != request {
			return ErrNomadCheckpointConflict
		}
		if e.Staged != nil {
			if *e.Staged != response {
				return ErrNomadCheckpointConflict
			}
			return nil
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhasePreparing {
			return ErrNomadCheckpointConflict
		}
		if _, err := validateNomadCheckpointLiveSource(ctx, tx, record, c); err != nil {
			return err
		}
		e.Staged = &response
		return nil
	})
	return err
}

func (s *PGSandboxStore) AuthorizeNomadCheckpointPreparation(ctx context.Context, operation string) (*procdapi.RuntimeCheckpointRequest, error) {
	c, err := s.mutateNomadCheckpoint(ctx, operation, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if e.Preparation != nil {
			if c.Lifecycle.Phase != SandboxLifecyclePhaseBarriered {
				return ErrNomadCheckpointConflict
			}
			return nil
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhasePreparing || e.Staged == nil {
			return ErrNomadCheckpointConflict
		}
		if _, err := validateNomadCheckpointLiveSource(ctx, tx, record, c); err != nil {
			return err
		}
		capture, err := runtimecontrol.NewCheckpointCaptureAssignment(operation, e.Assignment)
		if err != nil {
			return err
		}
		e.Preparation = &procdapi.RuntimeCheckpointRequest{Action: procdapi.MigrationPrepare, Capture: capture,
			InstanceID: e.Preflight.Source.ProcdInstanceID, CaptureEpoch: c.Lifecycle.Epoch, LifecycleEpoch: c.Lifecycle.Epoch}
		return (sandboxStoreTx{tx: tx}).UpdateLifecycleTxnPhase(ctx, operation, SandboxLifecyclePhaseBarriered)
	})
	if err != nil {
		return nil, err
	}
	return c.Evidence.Preparation, nil
}

func (s *PGSandboxStore) AuthorizeNomadCheckpointCapture(ctx context.Context, request procdapi.RuntimeCheckpointRequest, response procdapi.RuntimeCheckpointResponse) (*protocol.MigrationCaptureRequest, error) {
	if request.Action != procdapi.MigrationPrepare {
		return nil, ErrNomadCheckpointConflict
	}
	if err := response.ValidateFor(request); err != nil {
		return nil, err
	}
	c, err := s.mutateNomadCheckpoint(ctx, request.Capture.OperationID, func(tx pgx.Tx, record *SandboxRecord, c *NomadSandboxCheckpoint) error {
		e := &c.Evidence
		if e.Preparation == nil || !reflect.DeepEqual(*e.Preparation, request) {
			return ErrNomadCheckpointConflict
		}
		if e.CaptureAuthorized {
			if c.Lifecycle.Phase != SandboxLifecyclePhasePublishing || *e.Prepared != response {
				return ErrNomadCheckpointConflict
			}
			return nil
		}
		if c.Lifecycle.Phase != SandboxLifecyclePhaseBarriered {
			return ErrNomadCheckpointConflict
		}
		if _, err := validateNomadCheckpointLiveSource(ctx, tx, record, c); err != nil {
			return err
		}
		e.Prepared, e.CaptureAuthorized = &response, true
		return (sandboxStoreTx{tx: tx}).UpdateLifecycleTxnPhase(ctx, c.Lifecycle.ID, SandboxLifecyclePhasePublishing)
	})
	if err != nil {
		return nil, err
	}
	return &c.Evidence.Preflight.Source, nil
}
