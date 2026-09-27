package sandboxstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	SandboxResourceResizePausing         = "pausing"
	SandboxResourceResizeResuming        = "resuming"
	SandboxResourceResizeApplied         = "applied"
	SandboxResourceResizeCanceled        = "canceled"
	SandboxLifecycleSourceResourceResize = "resource_resize"
)

var (
	ErrSandboxResourceResizeConflict = errors.New("sandbox resource resize conflict")
	ErrSandboxResourceResizePending  = errors.New("sandbox resource resize pending")
)

// SandboxResourceResize is a desired compute replacement, not a second capacity
// ledger. Admission, cgroup enforcement and terminal proofs stay in runtime slots.
type SandboxResourceResize struct {
	SandboxID      string
	OperationID    string
	FromGeneration int64
	WasActive      bool
	Memory         string
	CPUMillicores  int64
	MemoryBytes    int64
	Phase          string
	Error          string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type RequestSandboxResourceResize struct {
	SandboxID              string
	ExpectedTeamID         string
	ExpectedGeneration     int64
	ExpectedMemoryOverride string
	Memory                 string
	CPUMillicores          int64
	MemoryBytes            int64
	// NoChange is computed using the shared resource policy; a pending operation
	// is always recovered before treating an identical config as a no-op.
	NoChange bool
}

func (s *PGSandboxStore) BeginSandboxResourceResize(ctx context.Context, request RequestSandboxResourceResize) (*SandboxResourceResize, error) {
	if request.SandboxID == "" || request.ExpectedTeamID == "" || request.Memory == "" ||
		len(request.Memory) > 128 || request.CPUMillicores <= 0 || request.MemoryBytes <= 0 {
		return nil, fmt.Errorf("valid sandbox identity and resolved resources are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, request.SandboxID)
	if err != nil {
		return nil, err
	}
	if record.TeamID != request.ExpectedTeamID || !record.DeletedAt.IsZero() ||
		(record.DesiredState != SandboxDesiredStateActive && record.DesiredState != SandboxDesiredStatePaused) {
		return nil, fmt.Errorf("%w: sandbox does not accept resource changes", ErrSandboxResourceResizeConflict)
	}
	existing, err := getSandboxResourceResize(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	if existing != nil && existing.pending() {
		if existing.MemoryBytes != request.MemoryBytes || existing.CPUMillicores != request.CPUMillicores {
			return nil, fmt.Errorf("%w: another resource change is pending", ErrSandboxResourceResizeConflict)
		}
		return existing, tx.Commit(ctx)
	}
	if record.RuntimeGeneration != request.ExpectedGeneration || sandboxMemoryOverride(record) != request.ExpectedMemoryOverride {
		return nil, fmt.Errorf("%w: sandbox resources changed while preparing resize", ErrSandboxResourceResizeConflict)
	}
	if active, err := getActiveLifecycleTxn(ctx, tx, record.ID); err != nil {
		return nil, err
	} else if active != nil {
		return nil, fmt.Errorf("%w: lifecycle %s owns the sandbox", ErrSandboxResourceResizeConflict, active.ID)
	}
	var networkPending bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.sandbox_network_mutations WHERE sandbox_id=$1 AND phase='pending')`, record.ID).Scan(&networkPending); err != nil {
		return nil, err
	}
	if networkPending {
		return nil, fmt.Errorf("%w: network update is pending", ErrSandboxResourceResizeConflict)
	}
	var targetLifecyclePending bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM manager.sandbox_lifecycle_txns WHERE target_sandbox_id=$1 AND phase IN ('preparing','barriered','publishing','committing'))`, record.ID).Scan(&targetLifecyclePending); err != nil {
		return nil, err
	}
	if targetLifecyclePending {
		return nil, fmt.Errorf("%w: another lifecycle owns this sandbox as a target", ErrSandboxResourceResizeConflict)
	}
	if request.NoChange {
		return nil, tx.Commit(ctx)
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, err
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: sandbox hard TTL has expired", ErrSandboxResourceResizeConflict)
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return nil, err
	}
	operationID := "resource-resize-" + hex.EncodeToString(nonce[:])
	_, err = tx.Exec(ctx, `INSERT INTO manager.sandbox_resource_resizes
		(sandbox_id, operation_id, from_generation, was_active, memory, cpu_millicores, memory_bytes, phase)
		VALUES ($1,$2,$3,$4,$5,$6,$7,'pausing') ON CONFLICT (sandbox_id) DO UPDATE SET
		operation_id=EXCLUDED.operation_id, from_generation=EXCLUDED.from_generation,
		was_active=EXCLUDED.was_active, memory=EXCLUDED.memory, cpu_millicores=EXCLUDED.cpu_millicores,
		memory_bytes=EXCLUDED.memory_bytes, phase=EXCLUDED.phase, error='', created_at=NOW(), updated_at=NOW()`,
		record.ID, operationID, record.RuntimeGeneration, record.DesiredState == SandboxDesiredStateActive,
		request.Memory, request.CPUMillicores, request.MemoryBytes)
	if err != nil {
		return nil, err
	}
	resize, err := getSandboxResourceResize(ctx, tx, record.ID)
	if err != nil {
		return nil, err
	}
	return resize, tx.Commit(ctx)
}

// PrepareSandboxResourceResize advances only when filesystem pause and physical
// slot cleanup are complete. It never stops or releases a lease itself.
func (s *PGSandboxStore) PrepareSandboxResourceResize(ctx context.Context, sandboxID string) (*SandboxResourceResize, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := lockNomadSandboxClaimRecord(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	r, err := getSandboxResourceResize(ctx, tx, sandboxID)
	if err != nil || r == nil || !r.pending() {
		return r, err
	}
	cancel := func(reason string) (*SandboxResourceResize, error) {
		r.Phase, r.Error = SandboxResourceResizeCanceled, reason
		if err := saveSandboxResourceResizePhase(ctx, tx, r); err != nil {
			return nil, err
		}
		return r, tx.Commit(ctx)
	}
	if !record.DeletedAt.IsZero() || record.DesiredState == SandboxDesiredStateDeleted || record.DesiredState == SandboxDesiredStateTerminating {
		return cancel("sandbox deletion superseded resize")
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, err
	}
	if !record.HardExpiresAt.IsZero() && !record.HardExpiresAt.After(authorityNow) {
		return cancel("sandbox hard TTL expired")
	}
	active, err := getActiveLifecycleTxn(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if active != nil && active.Source != SandboxLifecycleSourceResourceResize {
		return cancel("another lifecycle superseded resize")
	}
	if r.Phase == SandboxResourceResizeResuming && record.DesiredState == SandboxDesiredStateActive && active == nil {
		if record.RuntimeGeneration != r.FromGeneration+1 || record.ResourceMillicpu != r.CPUMillicores ||
			record.ResourceMemoryMiB != (r.MemoryBytes+(1<<20)-1)/(1<<20) || sandboxMemoryOverride(record) != r.Memory {
			return cancel("resumed runtime does not match requested resources")
		}
		if err := validateAlreadyActiveNomadSandbox(ctx, tx, record); err != nil {
			return nil, err
		}
		r.Phase = SandboxResourceResizeApplied
		if err := saveSandboxResourceResizePhase(ctx, tx, r); err != nil {
			return nil, err
		}
		return r, tx.Commit(ctx)
	}
	if record.RuntimeGeneration != r.FromGeneration {
		return cancel("runtime generation changed")
	}
	if r.Phase == SandboxResourceResizePausing && record.DesiredState == SandboxDesiredStatePaused && active == nil {
		if err := ensureNomadResumePhysicalStateTerminal(ctx, tx, sandboxID); err != nil {
			return nil, err
		}
		config := record.Config
		config.Resources = &SandboxResourceConfig{Memory: r.Memory}
		record.Config = config
		if err := (sandboxStoreTx{tx: tx}).SaveSandbox(ctx, record); err != nil {
			return nil, err
		}
		// Resource changes explicitly restart processes. An old retained memory
		// image must not be chosen by automatic resume after this cold resize.
		if _, err := tx.Exec(ctx, `DELETE FROM manager.sandbox_runtime_checkpoint_refs WHERE sandbox_id=$1`, sandboxID); err != nil {
			return nil, err
		}
		r.Phase = SandboxResourceResizeApplied
		if r.WasActive {
			r.Phase = SandboxResourceResizeResuming
		}
		if err := saveSandboxResourceResizePhase(ctx, tx, r); err != nil {
			return nil, err
		}
	}
	return r, tx.Commit(ctx)
}

func (s *PGSandboxStore) ListPendingSandboxResourceResizes(ctx context.Context, afterSandboxID string, limit int) ([]*SandboxResourceResize, error) {
	if limit < 1 || limit > 1000 {
		return nil, fmt.Errorf("resource resize scan limit must be between 1 and 1000")
	}
	rows, err := s.pool.Query(ctx, sandboxResourceResizeSelect+` WHERE phase IN ('pausing','resuming') AND sandbox_id > $1 ORDER BY sandbox_id LIMIT $2`, afterSandboxID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*SandboxResourceResize
	for rows.Next() {
		r, err := scanSandboxResourceResize(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (r *SandboxResourceResize) pending() bool {
	return r.Phase == SandboxResourceResizePausing || r.Phase == SandboxResourceResizeResuming
}

func sandboxMemoryOverride(record *SandboxRecord) string {
	if record.Config.Resources == nil {
		return ""
	}
	return strings.TrimSpace(record.Config.Resources.Memory)
}

const sandboxResourceResizeSelect = `SELECT sandbox_id,operation_id,from_generation,was_active,memory,cpu_millicores,memory_bytes,phase,error,created_at,updated_at FROM manager.sandbox_resource_resizes`

func getSandboxResourceResize(ctx context.Context, tx pgx.Tx, sandboxID string) (*SandboxResourceResize, error) {
	r, err := scanSandboxResourceResize(tx.QueryRow(ctx, sandboxResourceResizeSelect+` WHERE sandbox_id=$1 FOR UPDATE`, sandboxID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return r, err
}

func scanSandboxResourceResize(row runtimeSlotScanner) (*SandboxResourceResize, error) {
	var r SandboxResourceResize
	err := row.Scan(&r.SandboxID, &r.OperationID, &r.FromGeneration, &r.WasActive, &r.Memory, &r.CPUMillicores, &r.MemoryBytes, &r.Phase, &r.Error, &r.CreatedAt, &r.UpdatedAt)
	return &r, err
}

func saveSandboxResourceResizePhase(ctx context.Context, tx pgx.Tx, r *SandboxResourceResize) error {
	_, err := tx.Exec(ctx, `UPDATE manager.sandbox_resource_resizes SET phase=$3,error=$4,updated_at=NOW() WHERE sandbox_id=$1 AND operation_id=$2`, r.SandboxID, r.OperationID, r.Phase, r.Error)
	return err
}

// checkResourceResizeResumeAdmission forbids a resume at the old specification
// during pause. Once configured, ordinary cold access can help finish the exact
// resize; memory restore is forbidden because process restart was requested.
func checkResourceResizeResumeAdmission(ctx context.Context, tx pgx.Tx, sandboxID string, memory bool) (*SandboxResourceResize, error) {
	r, err := getSandboxResourceResize(ctx, tx, sandboxID)
	if err != nil {
		return nil, err
	}
	if r == nil || !r.pending() {
		return nil, nil
	}
	if r.Phase != SandboxResourceResizeResuming || memory {
		return nil, fmt.Errorf("%w: resource resize owns resume", ErrNomadSandboxResumeConflict)
	}
	return r, nil
}
