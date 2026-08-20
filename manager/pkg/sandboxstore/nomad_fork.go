package sandboxstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
)

var (
	ErrNomadSandboxForkConflict        = errors.New("nomad sandbox fork conflict")
	ErrNomadSandboxForkNotReady        = errors.New("nomad sandbox fork is not ready")
	ErrNomadSandboxRunningForkRequired = errors.New("nomad sandbox fork requires running-source recovery")
)

// NomadSandboxForkRequest identifies one deterministic paused target for a
// running or paused source filesystem.
type NomadSandboxForkRequest struct {
	OperationID        string
	SourceSandboxID    string
	ExpectedTeamID     string
	Target             *SandboxRecord
	TargetRecordDigest []byte
}

// NomadSandboxRunningForkCandidate is the exact live writer and node
// incarnation authorized by a durable fork operation.
type NomadSandboxRunningForkCandidate struct {
	OperationID         string
	TargetGenerationID  string
	Completed           bool
	Source              *SandboxRecord
	Target              *SandboxRecord
	Slot                *RuntimeSlot
	SourceFilesystemID  string
	SourceGenerationID  string
	SourceWriterGrantID string
	SourceWriterEpoch   int64
	BindingVersion      int
	BindingDigest       []byte
}

// NomadSandboxRunningForkGenerationID derives the immutable checkpoint
// generation chosen before the source node performs any side effect.
func NomadSandboxRunningForkGenerationID(operationID, targetSandboxID string) string {
	payload := operationID + "\x00" + targetSandboxID
	sum := sha256.Sum256([]byte(payload))
	return "nomad-running-fork-" + hex.EncodeToString(sum[:16])
}

// NomadSandboxForkClaimOperationID identifies the ready logical claim
// of a paused target that has never owned a physical runtime.
func NomadSandboxForkClaimOperationID(operationID, targetSandboxID string) string {
	payload := targetSandboxID + "\x00" + operationID
	sum := sha256.Sum256([]byte(payload))
	return "nomad-fork-target-" + hex.EncodeToString(sum[:16])
}

// RequestNomadSandboxRunningFork persists an exact source lifecycle and a
// ready, paused target in one transaction. Exact retries recover either the
// live writer candidate or the already-published target.
func (s *PGSandboxStore) RequestNomadSandboxRunningFork(
	ctx context.Context,
	request *NomadSandboxForkRequest,
) (*NomadSandboxRunningForkCandidate, error) {
	normalized, err := normalizeNomadSandboxForkRequest(request)
	if err != nil {
		return nil, err
	}
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin Nomad running-fork request tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	source, err := lockNomadSandboxClaimRecord(ctx, tx, normalized.SourceSandboxID)
	if err != nil {
		return nil, err
	}
	if source.TeamID != normalized.ExpectedTeamID {
		return nil, fmt.Errorf("%w: source team identity changed", ErrNomadSandboxForkConflict)
	}
	completed, err := loadCompletedNomadSandboxRunningFork(ctx, tx, source, normalized)
	if err != nil {
		return nil, err
	}
	if completed != nil {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit completed Nomad running-fork retry: %w", err)
		}
		return completed, nil
	}
	active, err := getActiveLifecycleTxn(ctx, tx, source.ID)
	if err != nil {
		return nil, fmt.Errorf("load active Nomad running-fork lifecycle: %w", err)
	}
	targetGenerationID := NomadSandboxRunningForkGenerationID(normalized.OperationID, normalized.Target.ID)
	if active != nil {
		if !nomadRunningForkLifecycleMatches(active, source, normalized, targetGenerationID, false) {
			return nil, fmt.Errorf("%w: lifecycle %s owns the source sandbox", ErrNomadSandboxForkConflict, active.ID)
		}
		target, err := lockNomadForkTarget(ctx, tx, normalized)
		if err != nil {
			return nil, err
		}
		candidate, err := lockNomadRunningForkLiveWriter(ctx, tx, source, target, active, targetGenerationID)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit Nomad running-fork request retry: %w", err)
		}
		return candidate, nil
	}
	if !nomadForkTargetDerivedFromSource(source, normalized.Target) {
		return nil, fmt.Errorf("%w: target does not inherit the source runtime identity", ErrNomadSandboxForkConflict)
	}

	if err := validateNomadRunningForkSourceRecord(source); err != nil {
		return nil, err
	}
	if existing, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1 FOR UPDATE
	`, normalized.Target.ID)); err != nil {
		return nil, fmt.Errorf("check Nomad running-fork target identity: %w", err)
	} else if existing != nil {
		return nil, fmt.Errorf("%w: target sandbox already exists", ErrNomadSandboxForkConflict)
	}

	// Lock and validate the source authority before creating the target. All
	// target writes still occur in this transaction and roll back together.
	placeholderLifecycle := &SandboxLifecycleTxn{
		ID: normalized.OperationID, SandboxID: source.ID, Kind: SandboxLifecycleKindFork,
		Phase: SandboxLifecyclePhasePublishing, Source: SandboxLifecycleSourceManual,
		FromGeneration: source.RuntimeGeneration, ToGeneration: source.RuntimeGeneration,
		FromPodNamespace: source.CurrentPodNamespace, FromPodName: source.CurrentPodName,
		TargetSandboxID: normalized.Target.ID, TargetGenerationID: targetGenerationID,
		TargetRecordDigest: normalized.TargetRecordDigest,
	}
	preflight, err := lockNomadRunningForkLiveWriter(
		ctx, tx, source, normalized.Target, placeholderLifecycle, targetGenerationID,
	)
	if err != nil {
		return nil, err
	}
	if !normalized.Target.HardExpiresAt.IsZero() &&
		!normalized.Target.HardExpiresAt.After(preflight.Slot.AuthorityObservedAt) {
		return nil, fmt.Errorf("%w: target hard TTL has expired", ErrNomadSandboxForkConflict)
	}
	placeholderLifecycle.ExpectedHeadLayerID = preflight.SourceGenerationID

	args, err := sandboxRecordInsertArgs(normalized.Target)
	if err != nil {
		return nil, err
	}
	tag, err := tx.Exec(ctx, sandboxRecordInsertSQL+` ON CONFLICT (sandbox_id) DO NOTHING`, args...)
	if err != nil {
		return nil, fmt.Errorf("insert Nomad running-fork target: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: target sandbox was concurrently reserved", ErrNomadSandboxForkConflict)
	}
	claimOperationID := NomadSandboxForkClaimOperationID(normalized.OperationID, normalized.Target.ID)
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.sandbox_runtime_claims (
			sandbox_id, operation_id, phase, lease_expires_at
		) VALUES ($1, $2, $3, NULL)
	`, normalized.Target.ID, claimOperationID, SandboxRuntimeClaimPhaseReady); err != nil {
		return nil, mapSandboxClaimConflict("insert Nomad running-fork target claim", err)
	}
	if err := (sandboxStoreTx{tx: tx}).BeginLifecycleTxn(ctx, placeholderLifecycle); err != nil {
		return nil, fmt.Errorf("begin Nomad running-fork lifecycle: %w", err)
	}
	preflight.Target = normalized.Target
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit Nomad running-fork request: %w", err)
	}
	return preflight, nil
}

// AbortNomadSandboxRunningFork atomically proves that publication has not
// committed, aborts the source operation, and makes its never-run target due
// for logical cleanup. A concurrent successful publication always wins.
func (s *PGSandboxStore) AbortNomadSandboxRunningFork(
	ctx context.Context,
	operationID, sourceSandboxID, targetSandboxID, reason string,
) (bool, error) {
	operationID = strings.TrimSpace(operationID)
	sourceSandboxID = strings.TrimSpace(sourceSandboxID)
	targetSandboxID = strings.TrimSpace(targetSandboxID)
	reason = strings.TrimSpace(reason)
	if operationID == "" || sourceSandboxID == "" || targetSandboxID == "" ||
		len(operationID) > 512 || len(sourceSandboxID) > 512 || len(targetSandboxID) > 512 {
		return false, fmt.Errorf("canonical fork operation, source, and target identities are required")
	}
	if len(reason) > 2_048 {
		return false, fmt.Errorf("fork abort reason exceeds 2048 bytes")
	}
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("sandbox store is not configured")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return false, fmt.Errorf("begin Nomad running-fork abort tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	source, err := lockNomadSandboxClaimRecord(ctx, tx, sourceSandboxID)
	if err != nil {
		return false, err
	}
	var published bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM manager.rootfs_running_forks WHERE operation_id = $1)
	`, operationID).Scan(&published); err != nil {
		return false, fmt.Errorf("check running-fork publication before abort: %w", err)
	}
	if published {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit completed running-fork abort retry: %w", err)
		}
		return false, nil
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
	`, operationID, sourceSandboxID))
	if err != nil {
		return false, fmt.Errorf("lock running-fork lifecycle for abort: %w", err)
	}
	if lifecycle == nil || lifecycle.Phase == SandboxLifecyclePhaseAborted {
		if err := tx.Commit(ctx); err != nil {
			return false, fmt.Errorf("commit absent running-fork abort retry: %w", err)
		}
		return false, nil
	}
	if !nomadRunningForkLifecycleOwnsNeverRunTarget(lifecycle, source) ||
		lifecycle.TargetSandboxID != targetSandboxID {
		return false, fmt.Errorf("%w: running-fork lifecycle changed before abort", ErrNomadSandboxForkConflict)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2, error = $3, aborted_at = NOW(), updated_at = NOW()
		WHERE txn_id = $1 AND phase = $4
	`, operationID, SandboxLifecyclePhaseAborted, reason, SandboxLifecyclePhasePublishing)
	if err != nil {
		return false, fmt.Errorf("abort stale Nomad running-fork lifecycle: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return false, fmt.Errorf("%w: running-fork lifecycle changed during abort", ErrNomadSandboxForkConflict)
	}
	if err := queueNeverRunNomadForkTargetCleanup(ctx, tx, operationID, targetSandboxID, reason); err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit Nomad running-fork abort: %w", err)
	}
	return true, nil
}

func nomadRunningForkLifecycleOwnsNeverRunTarget(
	lifecycle *SandboxLifecycleTxn,
	source *SandboxRecord,
) bool {
	return lifecycle != nil && source != nil && source.RuntimeBackend == SandboxRuntimeBackendNomad &&
		lifecycle.SandboxID == source.ID && lifecycle.Kind == SandboxLifecycleKindFork &&
		lifecycle.Source == SandboxLifecycleSourceManual && !lifecycle.Cancelable &&
		lifecycle.CancelRequestedAt.IsZero() && lifecycle.Phase == SandboxLifecyclePhasePublishing &&
		lifecycle.FromGeneration == source.RuntimeGeneration &&
		lifecycle.FromPodNamespace != "" && lifecycle.FromPodNamespace == source.CurrentPodNamespace &&
		lifecycle.FromPodName != "" && lifecycle.FromPodName == source.CurrentPodName &&
		lifecycle.ToGeneration == source.RuntimeGeneration && lifecycle.ToPodNamespace == "" &&
		lifecycle.ToPodName == "" && lifecycle.TargetSandboxID != "" &&
		lifecycle.TargetGenerationID != "" && len(lifecycle.TargetRecordDigest) == sha256.Size &&
		lifecycle.ExpectedHeadLayerID != "" &&
		lifecycle.PreparedHeadLayerID == ""
}

func queueNeverRunNomadForkTargetCleanup(
	ctx context.Context,
	tx pgx.Tx,
	operationID, targetSandboxID, reason string,
) error {
	target, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1 FOR UPDATE
	`, targetSandboxID))
	if err != nil {
		return fmt.Errorf("lock never-run Nomad fork target for cleanup: %w", err)
	}
	if target == nil || target.RuntimeBackend != SandboxRuntimeBackendNomad ||
		target.RuntimeGeneration != 0 || target.CurrentPodNamespace != "" || target.CurrentPodName != "" ||
		!target.DeletedAt.IsZero() {
		return fmt.Errorf("%w: never-run Nomad fork target changed before cleanup", ErrNomadSandboxForkConflict)
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, targetSandboxID)
	if err != nil {
		return err
	}
	if claim.OperationID != NomadSandboxForkClaimOperationID(operationID, targetSandboxID) ||
		!claim.CompletedAt.IsZero() || !claim.LeaseExpiresAt.IsZero() {
		return fmt.Errorf("%w: never-run Nomad fork target claim changed before cleanup", ErrNomadSandboxForkConflict)
	}
	ready := target.DesiredState == SandboxDesiredStatePaused && claim.Phase == SandboxRuntimeClaimPhaseReady
	pending := target.DesiredState == SandboxDesiredStateTerminating &&
		claim.Phase == SandboxRuntimeClaimPhaseCleanupPending
	if !ready && !pending {
		return fmt.Errorf("%w: never-run Nomad fork target cleanup state changed", ErrNomadSandboxForkConflict)
	}
	var targetBound, targetSlotted bool
	if err := tx.QueryRow(ctx, `
		SELECT
			EXISTS (SELECT 1 FROM manager.sandbox_rootfs_bindings WHERE sandbox_id = $1),
			EXISTS (SELECT 1 FROM manager.runtime_slots WHERE sandbox_id = $1)
	`, targetSandboxID).Scan(&targetBound, &targetSlotted); err != nil {
		return fmt.Errorf("check never-run Nomad fork target side effects before cleanup: %w", err)
	}
	if targetBound || targetSlotted {
		return fmt.Errorf("%w: never-run Nomad fork target acquired physical state", ErrNomadSandboxForkConflict)
	}
	if pending {
		return nil
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2, updated_at = NOW()
		WHERE sandbox_id = $1 AND deleted_at IS NULL AND desired_state = $3
	`, targetSandboxID, SandboxDesiredStateTerminating, SandboxDesiredStatePaused)
	if err != nil {
		return fmt.Errorf("terminate never-run Nomad fork target: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: never-run Nomad fork target changed during cleanup", ErrNomadSandboxForkConflict)
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.sandbox_runtime_claims
		SET phase = $2, lease_expires_at = NULL,
			cleanup_started_at = COALESCE(cleanup_started_at, NOW()), last_error = $3
		WHERE sandbox_id = $1 AND phase = $4
	`, targetSandboxID, SandboxRuntimeClaimPhaseCleanupPending, reason,
		SandboxRuntimeClaimPhaseReady)
	if err != nil {
		return fmt.Errorf("request never-run Nomad fork target cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("%w: never-run Nomad fork target claim changed during cleanup", ErrNomadSandboxForkConflict)
	}
	return nil
}

func normalizeNomadSandboxForkRequest(
	request *NomadSandboxForkRequest,
) (*NomadSandboxForkRequest, error) {
	if request == nil || request.Target == nil {
		return nil, fmt.Errorf("Nomad fork request and target are required")
	}
	normalized := *request
	normalized.OperationID = strings.TrimSpace(request.OperationID)
	normalized.SourceSandboxID = strings.TrimSpace(request.SourceSandboxID)
	normalized.ExpectedTeamID = strings.TrimSpace(request.ExpectedTeamID)
	target := *request.Target
	normalized.Target = &target
	targetDigest, err := NomadSandboxForkTargetRecordDigest(&target)
	if err != nil {
		return nil, err
	}
	normalized.TargetRecordDigest = targetDigest
	for name, value := range map[string]string{
		"operation_id": normalized.OperationID, "source_sandbox_id": normalized.SourceSandboxID,
		"expected_team_id": normalized.ExpectedTeamID, "target_sandbox_id": target.ID,
	} {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 512 {
			return nil, fmt.Errorf("%s must be canonical and at most 512 bytes", name)
		}
	}
	if target.ID == normalized.SourceSandboxID || target.TeamID != normalized.ExpectedTeamID ||
		target.RuntimeBackend != SandboxRuntimeBackendNomad || target.DesiredState != SandboxDesiredStatePaused ||
		target.RuntimeGeneration != 0 || target.CurrentPodName != "" || target.CurrentPodNamespace != "" ||
		!target.DeletedAt.IsZero() {
		return nil, fmt.Errorf("Nomad fork target must be a fresh paused Nomad sandbox")
	}
	if _, err := sandboxRecordInsertArgs(&target); err != nil {
		return nil, err
	}
	return &normalized, nil
}

// NomadSandboxForkTargetRecordDigest hashes the immutable target fields that
// a durable fork operation must preserve across API and controller retries.
func NomadSandboxForkTargetRecordDigest(target *SandboxRecord) ([]byte, error) {
	if target == nil {
		return nil, fmt.Errorf("Nomad fork target is required")
	}
	canonical := *target
	canonical.LifecycleEpoch = 0
	canonical.HotClaimCompletedAt = time.Time{}
	canonical.DeletedAt = time.Time{}
	canonical.UpdatedAt = time.Time{}
	canonicalTime := func(value time.Time) time.Time {
		if value.IsZero() {
			return time.Time{}
		}
		return value.UTC().Truncate(time.Microsecond)
	}
	canonical.ClaimedAt = canonicalTime(canonical.ClaimedAt)
	canonical.ExpiresAt = canonicalTime(canonical.ExpiresAt)
	canonical.HardExpiresAt = canonicalTime(canonical.HardExpiresAt)
	canonical.CreatedAt = canonicalTime(canonical.CreatedAt)
	payload, err := json.Marshal(canonical)
	if err != nil {
		return nil, fmt.Errorf("marshal Nomad fork target identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return digest[:], nil
}

func validateNomadRunningForkSourceRecord(source *SandboxRecord) error {
	if source == nil || source.RuntimeBackend != SandboxRuntimeBackendNomad ||
		source.DesiredState != SandboxDesiredStateActive || !source.DeletedAt.IsZero() ||
		source.RuntimeGeneration <= 0 || source.CurrentPodName == "" || source.CurrentPodNamespace == "" {
		return fmt.Errorf("%w: source is not a canonical active Nomad sandbox", ErrNomadSandboxForkNotReady)
	}
	return nil
}

func lockNomadForkTarget(
	ctx context.Context,
	tx pgx.Tx,
	request *NomadSandboxForkRequest,
) (*SandboxRecord, error) {
	target, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+`
		WHERE sandbox_id = $1 FOR UPDATE
	`, request.Target.ID))
	if err != nil {
		return nil, fmt.Errorf("lock Nomad running-fork target: %w", err)
	}
	if !nomadForkTargetMatches(target, request.Target) {
		return nil, fmt.Errorf("%w: target sandbox identity changed", ErrNomadSandboxForkConflict)
	}
	claim, err := lockSandboxRuntimeClaim(ctx, tx, target.ID)
	if err != nil {
		return nil, err
	}
	if claim.OperationID != NomadSandboxForkClaimOperationID(request.OperationID, target.ID) ||
		claim.Phase != SandboxRuntimeClaimPhaseReady || !claim.CompletedAt.IsZero() ||
		!claim.LeaseExpiresAt.IsZero() {
		return nil, fmt.Errorf("%w: target logical claim changed", ErrNomadSandboxForkConflict)
	}
	return target, nil
}

func nomadForkTargetMatches(actual, expected *SandboxRecord) bool {
	return actual != nil && expected != nil && actual.DeletedAt.IsZero() &&
		actual.ID == expected.ID && actual.TeamID == expected.TeamID && actual.UserID == expected.UserID &&
		actual.TemplateID == expected.TemplateID && actual.TemplateName == expected.TemplateName &&
		actual.TemplateNamespace == expected.TemplateNamespace && actual.ClusterID == expected.ClusterID &&
		actual.RuntimeBackend == SandboxRuntimeBackendNomad && actual.DesiredState == SandboxDesiredStatePaused &&
		actual.RuntimeGeneration == 0 && actual.CurrentPodName == "" && actual.CurrentPodNamespace == "" &&
		apiequality.Semantic.DeepEqual(actual.Config, expected.Config) &&
		apiequality.Semantic.DeepEqual(actual.TemplateSpec, expected.TemplateSpec) &&
		actual.ExpiresAt.Equal(expected.ExpiresAt) && actual.HardExpiresAt.Equal(expected.HardExpiresAt)
}

func nomadForkTargetDerivedFromSource(source, target *SandboxRecord) bool {
	if source == nil || target == nil || source.TeamID != target.TeamID ||
		source.TemplateID != target.TemplateID || source.TemplateName != target.TemplateName ||
		source.TemplateNamespace != target.TemplateNamespace || source.ClusterID != target.ClusterID ||
		source.RuntimeBackend != SandboxRuntimeBackendNomad || target.RuntimeBackend != SandboxRuntimeBackendNomad ||
		!apiequality.Semantic.DeepEqual(source.TemplateSpec, target.TemplateSpec) {
		return false
	}
	sourceConfig := source.Config
	targetConfig := target.Config
	sourceConfig.TTL, sourceConfig.HardTTL = nil, nil
	targetConfig.TTL, targetConfig.HardTTL = nil, nil
	return apiequality.Semantic.DeepEqual(sourceConfig, targetConfig)
}

func lockNomadRunningForkLiveWriter(
	ctx context.Context,
	tx pgx.Tx,
	source *SandboxRecord,
	target *SandboxRecord,
	lifecycle *SandboxLifecycleTxn,
	targetGenerationID string,
) (*NomadSandboxRunningForkCandidate, error) {
	if err := validateNomadRunningForkSourceRecord(source); err != nil {
		return nil, err
	}
	var authorityNow time.Time
	if err := tx.QueryRow(ctx, `SELECT NOW()`).Scan(&authorityNow); err != nil {
		return nil, fmt.Errorf("read Nomad running-fork authority time: %w", err)
	}
	if !source.HardExpiresAt.IsZero() && !source.HardExpiresAt.After(authorityNow) {
		return nil, fmt.Errorf("%w: source hard TTL has expired", ErrNomadSandboxForkNotReady)
	}
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE sandbox_id = $1 AND state <> $2 FOR UPDATE
	`, source.ID, RuntimeSlotStateTerminal))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: source runtime slot is missing", ErrNomadSandboxForkNotReady)
	}
	if err != nil {
		return nil, fmt.Errorf("lock Nomad running-fork source slot: %w", err)
	}
	if slot.State != RuntimeSlotStateActive || !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) ||
		slot.ClusterID != source.ClusterID || slot.AllocationID != source.CurrentPodName ||
		slot.AllocationNamespace != source.CurrentPodNamespace || slot.WriterGrantID == "" ||
		slot.FilesystemID == "" || slot.SourceGenerationID == "" || slot.ProcdInstanceID == "" ||
		len(slot.CommandReadyDigest) != sha256.Size || slot.CommandReadyAt.IsZero() {
		return nil, fmt.Errorf("%w: source runtime slot is not exact command-ready", ErrNomadSandboxForkNotReady)
	}
	filesystem, generation, err := getRootFSFilesystemAndGenerationForUpdate(ctx, tx, source.ID)
	if err != nil {
		return nil, err
	}
	grantRecord, err := getRootFSWriterGrantForUpdate(ctx, tx, slot.WriterGrantID)
	if err != nil {
		return nil, err
	}
	grant := &grantRecord.RootFSWriterGrant
	runtimeGeneration, parseErr := strconv.ParseInt(grant.RuntimeGeneration, 10, 64)
	if filesystem.ID != slot.FilesystemID || filesystem.TeamID != source.TeamID ||
		filesystem.HeadGenerationID != slot.SourceGenerationID || generation.ID != slot.SourceGenerationID ||
		filesystem.WriterEpoch != grant.WriterEpoch || grant.State != RootFSWriterGrantStateConsumed ||
		!grant.LeaseExpiresAt.After(grantRecord.databaseNow) || grant.SandboxID != source.ID ||
		grant.FilesystemID != filesystem.ID || grant.InitialGenerationID != generation.ID ||
		grant.SlotID != slot.ID || grant.ClaimID != slot.ClaimID || grant.NodeUID != slot.NodeUID ||
		grant.NodeBootID != slot.NodeBootID || parseErr != nil || runtimeGeneration != source.RuntimeGeneration {
		return nil, fmt.Errorf("%w: source RootFS writer authority changed", ErrNomadSandboxForkNotReady)
	}
	if lifecycle.ExpectedHeadLayerID != "" && lifecycle.ExpectedHeadLayerID != generation.ID {
		return nil, fmt.Errorf("%w: source lifecycle head changed", ErrNomadSandboxForkConflict)
	}
	return &NomadSandboxRunningForkCandidate{
		OperationID: lifecycle.ID, TargetGenerationID: targetGenerationID,
		Source: source, Target: target, Slot: slot,
		SourceFilesystemID: filesystem.ID, SourceGenerationID: generation.ID,
		SourceWriterGrantID: grant.ID, SourceWriterEpoch: grant.WriterEpoch,
		BindingVersion: grant.BindingVersion, BindingDigest: append([]byte(nil), grant.BindingDigest...),
	}, nil
}

func nomadRunningForkLifecycleMatches(
	lifecycle *SandboxLifecycleTxn,
	source *SandboxRecord,
	request *NomadSandboxForkRequest,
	targetGenerationID string,
	committed bool,
) bool {
	if lifecycle == nil || source == nil || lifecycle.ID != request.OperationID ||
		lifecycle.SandboxID != source.ID || lifecycle.Kind != SandboxLifecycleKindFork ||
		lifecycle.Source != SandboxLifecycleSourceManual || lifecycle.Cancelable ||
		!lifecycle.CancelRequestedAt.IsZero() || lifecycle.ExpectedHeadLayerID == "" ||
		lifecycle.ToPodNamespace != "" || lifecycle.ToPodName != "" ||
		lifecycle.TargetSandboxID != request.Target.ID || lifecycle.TargetGenerationID != targetGenerationID ||
		!bytes.Equal(lifecycle.TargetRecordDigest, request.TargetRecordDigest) {
		return false
	}
	if committed {
		return lifecycle.Phase == SandboxLifecyclePhaseCommitted &&
			lifecycle.PreparedHeadLayerID == targetGenerationID
	}
	return lifecycle.FromGeneration == source.RuntimeGeneration &&
		lifecycle.ToGeneration == source.RuntimeGeneration &&
		lifecycle.FromPodNamespace == source.CurrentPodNamespace &&
		lifecycle.FromPodName == source.CurrentPodName &&
		lifecycle.Phase == SandboxLifecyclePhasePublishing && lifecycle.PreparedHeadLayerID == ""
}

func loadCompletedNomadSandboxRunningFork(
	ctx context.Context,
	tx pgx.Tx,
	source *SandboxRecord,
	request *NomadSandboxForkRequest,
) (*NomadSandboxRunningForkCandidate, error) {
	var sourceSandboxID, sourceGenerationID, targetSandboxID, targetFilesystemID, checkpointGenerationID string
	err := tx.QueryRow(ctx, `
		SELECT source_sandbox_id, source_generation_id, target_sandbox_id,
			target_filesystem_id, checkpoint_generation_id
		FROM manager.rootfs_running_forks WHERE operation_id = $1
	`, request.OperationID).Scan(
		&sourceSandboxID, &sourceGenerationID, &targetSandboxID, &targetFilesystemID, &checkpointGenerationID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load completed Nomad running fork: %w", err)
	}
	targetGenerationID := NomadSandboxRunningForkGenerationID(request.OperationID, request.Target.ID)
	if sourceSandboxID != request.SourceSandboxID || targetSandboxID != request.Target.ID ||
		targetFilesystemID != request.Target.ID || checkpointGenerationID != targetGenerationID {
		return nil, fmt.Errorf("%w: completed operation identity changed", ErrNomadSandboxForkConflict)
	}
	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2 FOR UPDATE
	`, request.OperationID, source.ID))
	if err != nil {
		return nil, fmt.Errorf("lock completed Nomad running-fork lifecycle: %w", err)
	}
	if !nomadRunningForkLifecycleMatches(lifecycle, source, request, targetGenerationID, true) {
		return nil, fmt.Errorf("%w: completed source lifecycle changed", ErrNomadSandboxForkConflict)
	}
	if lifecycle.ExpectedHeadLayerID != sourceGenerationID {
		return nil, fmt.Errorf("%w: completed source generation changed", ErrNomadSandboxForkConflict)
	}
	target, err := lockNomadForkTarget(ctx, tx, request)
	if err != nil {
		return nil, err
	}
	filesystem, err := scanRootFSFilesystem(tx.QueryRow(ctx, `
		SELECT filesystem_id, team_id, source_filesystem_id, head_layer_id,
			writer_epoch, base_image_ref, base_image_digest, storage_format,
			base_artifact_digest, format_generation, head_generation_id,
			created_at, updated_at
		FROM manager.rootfs_filesystems WHERE filesystem_id = $1
		FOR SHARE
	`, target.ID))
	if err != nil {
		return nil, fmt.Errorf("load completed Nomad running-fork target filesystem: %w", err)
	}
	if filesystem.HeadGenerationID != targetGenerationID || filesystem.TeamID != target.TeamID {
		return nil, fmt.Errorf("%w: completed target RootFS changed", ErrNomadSandboxForkConflict)
	}
	return &NomadSandboxRunningForkCandidate{
		OperationID: request.OperationID, TargetGenerationID: targetGenerationID,
		Completed: true, Source: source, Target: target,
	}, nil
}
