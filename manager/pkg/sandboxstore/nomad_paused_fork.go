package sandboxstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// ForkNomadPausedSandbox atomically creates a paused logical target and binds
// it to the immutable head of a paused Nomad source. Exact retries return the
// already-committed target without depending on current source configuration.
func (s *PGSandboxStore) ForkNomadPausedSandbox(
	ctx context.Context,
	request *NomadSandboxForkRequest,
) (*SandboxRecord, error) {
	normalized, err := normalizeNomadSandboxForkRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad paused-fork tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	source, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SourceSandboxID)
	if err != nil {
		return nil, err
	}
	if source.TeamID != normalized.ExpectedTeamID {
		return nil, fmt.Errorf("%w: source team identity changed", ErrNomadSandboxForkConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 FOR UPDATE
	`, normalized.OperationID))
	if err != nil {
		return nil, fmt.Errorf("lock Nomad paused-fork lifecycle: %w", err)
	}
	if lifecycle != nil {
		target, retryErr := loadCompletedNomadPausedFork(ctx, tx, source, lifecycle, normalized)
		if retryErr != nil {
			return nil, retryErr
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit Nomad paused-fork retry: %w", err)
		}
		return target, nil
	}
	if err := validateNomadPausedForkSource(source); err != nil {
		return nil, err
	}
	if !nomadForkTargetDerivedFromSource(source, normalized.Target) {
		return nil, fmt.Errorf("%w: target does not inherit the source runtime identity", ErrNomadSandboxForkConflict)
	}
	active, err := getActiveLifecycleTxn(ctx, tx, source.ID)
	if err != nil {
		return nil, fmt.Errorf("load active Nomad paused-fork lifecycle: %w", err)
	}
	if active != nil {
		return nil, fmt.Errorf("%w: lifecycle %s owns the source sandbox", ErrNomadSandboxForkConflict, active.ID)
	}
	if existing, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1 FOR UPDATE
	`, normalized.Target.ID)); err != nil {
		return nil, fmt.Errorf("check Nomad paused-fork target identity: %w", err)
	} else if existing != nil {
		return nil, fmt.Errorf("%w: target sandbox already exists", ErrNomadSandboxForkConflict)
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, fmt.Errorf("read Nomad paused-fork authority time: %w", err)
	}
	if !source.HardExpiresAt.IsZero() && !source.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: paused source hard TTL has expired", ErrNomadSandboxForkNotReady)
	}
	if !normalized.Target.HardExpiresAt.IsZero() &&
		!normalized.Target.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: target hard TTL has expired", ErrNomadSandboxForkConflict)
	}

	args, err := sandboxRecordInsertArgs(normalized.Target)
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, sandboxRecordInsertSQL+` ON CONFLICT (sandbox_id) DO NOTHING`, args...)
	if err != nil {
		return nil, fmt.Errorf("insert Nomad paused-fork target: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: target sandbox was concurrently reserved", ErrNomadSandboxForkConflict)
	}
	credentialBindingDigest, err := cloneNomadSandboxCredentialBindingsTx(
		ctx, tx, source.TeamID, source.ID, normalized.Target.ID,
	)
	if err != nil {
		return nil, err
	}
	claimOperationID := NomadSandboxForkClaimOperationID(normalized.OperationID, normalized.Target.ID)
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.sandbox_runtime_claims (
			sandbox_id, operation_id, phase, lease_expires_at, credential_binding_digest
		) VALUES ($1, $2, $3, NULL, $4)
	`, normalized.Target.ID, claimOperationID, SandboxRuntimeClaimPhaseReady,
		credentialBindingDigest); err != nil {
		return nil, mapSandboxClaimConflict("insert Nomad paused-fork target claim", err)
	}
	filesystem, err := forkRootFSFilesystem(ctx, tx, &ForkRootFSFilesystemRequest{
		SourceSandboxID: source.ID, TargetSandboxID: normalized.Target.ID,
		TargetTeamID: normalized.Target.TeamID,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) || errors.Is(err, ErrRootFSFilesystemNotFound) {
			return nil, fmt.Errorf("%w: paused source RootFS is not forkable", ErrNomadSandboxForkNotReady)
		}
		return nil, fmt.Errorf("fork paused Nomad RootFS: %w", err)
	}
	if filesystem == nil || filesystem.HeadGenerationID == "" {
		return nil, fmt.Errorf("%w: paused source has no durable generation", ErrNomadSandboxForkNotReady)
	}
	lifecycle = &SandboxLifecycleTxn{
		ID: normalized.OperationID, SandboxID: source.ID, Kind: SandboxLifecycleKindFork,
		Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceManual,
		FromGeneration: source.RuntimeGeneration, ToGeneration: source.RuntimeGeneration,
		TargetSandboxID: normalized.Target.ID, TargetGenerationID: filesystem.HeadGenerationID,
		TargetRecordDigest: normalized.TargetRecordDigest, ExpectedGenerationID: filesystem.HeadGenerationID,
	}
	locked := sandboxStoreTx{tx: tx}
	if err := locked.BeginLifecycleTxn(ctx, lifecycle); err != nil {
		return nil, fmt.Errorf("begin Nomad paused-fork lifecycle: %w", err)
	}
	if err := locked.CommitLifecycleTxn(ctx, lifecycle.ID, filesystem.HeadGenerationID); err != nil {
		return nil, fmt.Errorf("commit Nomad paused-fork lifecycle: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad paused-fork: %w", err)
	}
	return normalized.Target, nil
}

func validateNomadPausedForkSource(source *SandboxRecord) error {
	if source == nil ||
		source.DesiredState != SandboxDesiredStatePaused || !source.DeletedAt.IsZero() ||
		source.RuntimeGeneration < 0 || source.RuntimeID != "" || source.RuntimeNamespace != "" {
		return fmt.Errorf("%w: source is not a canonical paused Nomad sandbox", ErrNomadSandboxForkNotReady)
	}
	return nil
}

func loadCompletedNomadPausedFork(
	ctx context.Context,
	tx pgx.Tx,
	source *SandboxRecord,
	lifecycle *SandboxLifecycleTxn,
	request *NomadSandboxForkRequest,
) (*SandboxRecord, error) {
	if lifecycle != nil && (lifecycle.FromRuntimeNamespace != "" || lifecycle.FromRuntimeID != "") {
		return nil, ErrNomadSandboxRunningForkRequired
	}
	var runningForkRecorded bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_running_forks WHERE operation_id = $1)
	`, request.OperationID).Scan(&runningForkRecorded); err != nil {
		return nil, fmt.Errorf("check paused-fork operation kind: %w", err)
	}
	if runningForkRecorded {
		return nil, ErrNomadSandboxRunningForkRequired
	}
	if lifecycle.SandboxID != source.ID || lifecycle.Kind != SandboxLifecycleKindFork ||
		lifecycle.Phase != SandboxLifecyclePhaseCommitted || lifecycle.Source != SandboxLifecycleSourceManual ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		lifecycle.FromRuntimeNamespace != "" || lifecycle.FromRuntimeID != "" ||
		lifecycle.ToRuntimeNamespace != "" || lifecycle.ToRuntimeID != "" ||
		lifecycle.TargetSandboxID != request.Target.ID ||
		lifecycle.TargetGenerationID != lifecycle.PreparedGenerationID ||
		!bytes.Equal(lifecycle.TargetRecordDigest, request.TargetRecordDigest) ||
		lifecycle.ExpectedGenerationID == "" ||
		lifecycle.PreparedGenerationID != lifecycle.ExpectedGenerationID {
		return nil, fmt.Errorf("%w: paused-fork lifecycle changed", ErrNomadSandboxForkConflict)
	}
	target, err := lockNomadForkTarget(ctx, tx, request)
	if err != nil {
		return nil, err
	}
	filesystem, generation, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, target.ID)
	if err != nil {
		return nil, fmt.Errorf("load completed Nomad paused-fork RootFS: %w", err)
	}
	if filesystem.ID != target.ID || filesystem.TeamID != target.TeamID ||
		filesystem.SourceFilesystemID == "" || filesystem.HeadGenerationID != lifecycle.PreparedGenerationID ||
		generation.ID != lifecycle.PreparedGenerationID {
		return nil, fmt.Errorf("%w: paused-fork target RootFS changed", ErrNomadSandboxForkConflict)
	}
	return target, nil
}
