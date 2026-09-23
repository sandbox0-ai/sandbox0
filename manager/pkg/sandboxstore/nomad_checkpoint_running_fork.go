package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// NomadRunningMemoryFork identifies a composed operation. Pause/fork/resume
// lifecycles own its progress; this record never grants execution or publication.
type NomadRunningMemoryFork struct {
	OperationID             string
	SourceSandboxID         string
	CaptureOperationID      string
	Target                  *SandboxRecord
	ParentResumeOperationID string
}

type runningMemoryForkTarget struct {
	Record            *SandboxRecord `json:"record"`
	ResourceMillicpu  int64          `json:"resource_millicpu"`
	ResourceMemoryMiB int64          `json:"resource_memory_mib"`
}

// RequestNomadSandboxRunningMemoryFork reserves source-only capture under the
// ordinary lifecycle lock. Completion later creates the child and the parent's
// independent memory resume atomically, leaving no window for a cold resume.
func (s *PGSandboxStore) RequestNomadSandboxRunningMemoryFork(ctx context.Context, request *NomadSandboxForkRequest) (*NomadRunningMemoryFork, error) {
	normalized, err := normalizeNomadSandboxForkRequest(request)
	if err != nil {
		return nil, err
	}
	if !normalized.Memory {
		return nil, ErrNomadCheckpointConflict
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := lockRuntimeSlotClaimOperation(ctx, tx, normalized.OperationID); err != nil {
		return nil, err
	}
	source, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SourceSandboxID)
	if err != nil {
		return nil, err
	}
	if source.TeamID != normalized.ExpectedTeamID {
		return nil, ErrNomadCheckpointConflict
	}
	existing, digest, err := loadRunningMemoryFork(ctx, tx, "operation_id", normalized.OperationID)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		if existing.SourceSandboxID != source.ID || existing.Target.ID != normalized.Target.ID || !bytes.Equal(digest, normalized.TargetRecordDigest) || existing.Target.ResourceMillicpu != normalized.Target.ResourceMillicpu || existing.Target.ResourceMemoryMiB != normalized.Target.ResourceMemoryMiB {
			return nil, ErrNomadCheckpointConflict
		}
		return existing, tx.Commit(ctx)
	}
	if !nomadForkTargetDerivedFromSource(source, normalized.Target) {
		return nil, ErrNomadCheckpointConflict
	}
	active, err := getActiveLifecycleTxn(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	if active != nil {
		return nil, ErrNomadCheckpointConflict
	}
	var occupied bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.sandboxes WHERE sandbox_id=$1)
        OR EXISTS(SELECT 1 FROM manager.sandbox_lifecycle_txns WHERE txn_id=$2)`, normalized.Target.ID, normalized.OperationID).Scan(&occupied); err != nil {
		return nil, err
	}
	if occupied {
		return nil, ErrNomadCheckpointConflict
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if !normalized.Target.HardExpiresAt.IsZero() && !normalized.Target.HardExpiresAt.After(now) {
		return nil, ErrNomadCheckpointConflict
	}
	writer, err := lockExactNomadLiveWriter(ctx, tx, source)
	if err != nil {
		return nil, err
	}
	inputs, err := getRuntimeSlotClaimInputs(ctx, tx, writer.slot.ID)
	if err != nil {
		return nil, err
	}
	if inputs == nil {
		return nil, ErrNomadCheckpointConflict
	}
	operation := fmt.Sprintf("memory-fork-capture-%x", sha256.Sum256([]byte(normalized.OperationID)))
	checkpoint, err := reserveNomadSandboxMemoryPauseTx(ctx, tx, source, operation, inputs.Runtime, inputs.NetworkPolicy)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(runningMemoryForkTarget{Record: normalized.Target, ResourceMillicpu: normalized.Target.ResourceMillicpu, ResourceMemoryMiB: normalized.Target.ResourceMemoryMiB})
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO manager.sandbox_runtime_running_memory_forks
        (operation_id,source_sandbox_id,capture_operation_id,target_sandbox_id,target_record,target_record_digest,source_expires_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)`, normalized.OperationID, source.ID, checkpoint.Lifecycle.ID, normalized.Target.ID, payload, normalized.TargetRecordDigest, nullableTime(source.ExpiresAt))
	if err != nil {
		return nil, err
	}
	result := &NomadRunningMemoryFork{OperationID: normalized.OperationID, SourceSandboxID: source.ID, CaptureOperationID: checkpoint.Lifecycle.ID, Target: normalized.Target}
	return result, tx.Commit(ctx)
}

func loadRunningMemoryFork(ctx context.Context, tx pgx.Tx, column, id string) (*NomadRunningMemoryFork, []byte, error) {
	// Column names are private constants, never caller input.
	if column != "operation_id" && column != "capture_operation_id" {
		return nil, nil, ErrNomadCheckpointConflict
	}
	result := &NomadRunningMemoryFork{}
	var payload, digest []byte
	var targetID string
	err := tx.QueryRow(ctx, `SELECT operation_id,source_sandbox_id,capture_operation_id,target_record,target_record_digest,target_sandbox_id,
        COALESCE(parent_resume_operation_id,'') FROM manager.sandbox_runtime_running_memory_forks WHERE `+column+`=$1 FOR UPDATE`, id).Scan(
		&result.OperationID, &result.SourceSandboxID, &result.CaptureOperationID, &payload, &digest, &targetID, &result.ParentResumeOperationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var target runningMemoryForkTarget
	if json.Unmarshal(payload, &target) != nil || target.Record == nil || target.Record.ID != targetID {
		return nil, nil, ErrNomadCheckpointConflict
	}
	result.Target = target.Record
	result.Target.ResourceMillicpu, result.Target.ResourceMemoryMiB = target.ResourceMillicpu, target.ResourceMemoryMiB
	actual, err := NomadSandboxForkTargetRecordDigest(result.Target)
	if err != nil || !bytes.Equal(actual, digest) {
		return nil, nil, ErrNomadCheckpointConflict
	}
	return result, digest, nil
}

// lockRunningMemoryForkQuota precedes the owner row lock. The parent's existing
// active quota is carried into its resume in one transaction, never released to
// a competing claim between the capture and restore lifecycle transitions.
func lockRunningMemoryForkQuota(ctx context.Context, tx pgx.Tx, capture string) error {
	var team string
	err := tx.QueryRow(ctx, `SELECT s.team_id FROM manager.sandbox_runtime_running_memory_forks f
        JOIN manager.sandboxes s ON s.sandbox_id=f.source_sandbox_id WHERE f.capture_operation_id=$1`, capture).Scan(&team)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	return lockActiveSandboxQuotaTeam(ctx, tx, team)
}

func completeRunningMemoryForkHandoff(ctx context.Context, tx pgx.Tx, capture string) error {
	intent, _, err := loadRunningMemoryFork(ctx, tx, "capture_operation_id", capture)
	if err != nil || intent == nil || intent.ParentResumeOperationID != "" {
		return err
	}
	source, err := lockNomadSandboxClaimRecord(ctx, tx, intent.SourceSandboxID)
	if err != nil {
		return err
	}
	var live bool
	if err := tx.QueryRow(ctx, `SELECT desired_state='paused' AND deleted_at IS NULL
        AND (hard_expires_at IS NULL OR hard_expires_at>clock_timestamp()) FROM manager.sandboxes WHERE sandbox_id=$1`, source.ID).Scan(&live); err != nil {
		return err
	}
	if !live {
		return nil
	}
	request, err := normalizeNomadSandboxForkRequest(&NomadSandboxForkRequest{OperationID: intent.OperationID, SourceSandboxID: source.ID,
		ExpectedTeamID: source.TeamID, Target: intent.Target, Memory: true})
	if err != nil {
		return err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return err
	}
	if !intent.Target.HardExpiresAt.IsZero() && !intent.Target.HardExpiresAt.After(now) {
		// A child's shorter deadline must not strand its still-live parent. The
		// failed child keeps an ordinary aborted fork lifecycle for exact retry.
		_, head, _, err := lockNomadSandboxResumeHead(ctx, tx, source.ID)
		if err != nil {
			return err
		}
		failed := &SandboxLifecycleTxn{ID: intent.OperationID, SandboxID: source.ID, Kind: SandboxLifecycleKindFork,
			Source: SandboxLifecycleSourceManual, Phase: SandboxLifecyclePhasePublishing, FromGeneration: source.RuntimeGeneration, ToGeneration: source.RuntimeGeneration,
			ExpectedGenerationID: head, TargetGenerationID: head, TargetSandboxID: intent.Target.ID, TargetRecordDigest: request.TargetRecordDigest}
		if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, failed); err != nil {
			return err
		}
		if err := (sandboxStoreTx{tx: tx}).AbortLifecycleTxn(ctx, failed.ID, "memory fork child expired before capture handoff"); err != nil {
			return err
		}
	} else if _, err := forkNomadPausedSandboxTx(ctx, tx, request); err != nil {
		return err
	}
	// The same parent was counted active until this transaction. Child remains
	// paused; continuing the parent does not admit another active sandbox.
	resume, err := requestNomadSandboxResumeTx(ctx, tx, &RequestNomadSandboxResumeRequest{SandboxID: source.ID, ExpectedTeamID: source.TeamID, Memory: true})
	if err != nil {
		return err
	}
	if resume.Checkpoint == nil || resume.Checkpoint.Retained.CheckpointID != capture {
		return ErrNomadCheckpointConflict
	}
	_, err = tx.Exec(ctx, `UPDATE manager.sandbox_runtime_running_memory_forks SET parent_resume_operation_id=$2
        WHERE operation_id=$1 AND parent_resume_operation_id IS NULL`, intent.OperationID, resume.OperationID)
	return err
}

// Preserve a running parent's original soft deadline; a fork is not a user
// request to refresh its lifetime. Ordinary manual resumes retain their policy.
func runningMemoryForkParentExpiration(ctx context.Context, tx pgx.Tx, operation string, fallback time.Time) (time.Time, error) {
	var expires *time.Time
	err := tx.QueryRow(ctx, `SELECT source_expires_at FROM manager.sandbox_runtime_running_memory_forks WHERE parent_resume_operation_id=$1`, operation).Scan(&expires)
	if errors.Is(err, pgx.ErrNoRows) {
		return fallback, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	if expires == nil {
		return time.Time{}, nil
	}
	return *expires, nil
}

// GetNomadSandboxRunningMemoryFork recovers immutable request inputs before a
// product retry reconstructs timestamps or reads mutable source configuration.
func (s *PGSandboxStore) GetNomadSandboxRunningMemoryFork(ctx context.Context, operation string) (*NomadRunningMemoryFork, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	result, _, err := loadRunningMemoryFork(ctx, tx, "operation_id", operation)
	if err != nil {
		return nil, err
	}
	return result, tx.Commit(ctx)
}
