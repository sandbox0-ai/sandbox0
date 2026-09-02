package sandboxstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/deletionwebhook"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
)

const sandboxStoreSchemaName = "manager"

var ErrSandboxRecordNotFound = errors.New("sandbox record not found")

const (
	SandboxDesiredStateActive      = "active"
	SandboxDesiredStatePaused      = "paused"
	SandboxDesiredStateTerminating = "terminating"
	SandboxDesiredStateDeleted     = "deleted"
)

// SandboxRecord is the durable runtime-neutral sandbox identity, desired
// lifecycle state, and configuration. Physical runtime registries own
// observed readiness and failure state.
type SandboxRecord struct {
	ID                string
	TeamID            string
	UserID            string
	TemplateID        string
	TemplateName      string
	TemplateNamespace string
	ClusterID         string
	DesiredState      string
	Config            SandboxConfig
	TemplateSpec      v1alpha1.SandboxTemplateSpec
	RuntimeID         string
	RuntimeNamespace  string
	RuntimeGeneration int64
	LifecycleEpoch    int64
	OwnerKind         string
	// Resource fields are excluded from the legacy fork-record digest. Fork
	// validation binds them explicitly while preserving rolling retries.
	ResourceMillicpu    int64 `json:"-"`
	ResourceMemoryMiB   int64 `json:"-"`
	HotClaimCompletedAt time.Time
	ClaimedAt           time.Time
	ExpiresAt           time.Time
	HardExpiresAt       time.Time
	DeletedAt           time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// SandboxExpirationCandidate is the minimal durable state needed by the
// runtime-neutral TTL controller. Hard expiry always takes precedence over a
// soft TTL deadline for the same sandbox.
type SandboxExpirationCandidate struct {
	SandboxID     string
	DesiredState  string
	ExpiresAt     time.Time
	HardExpiresAt time.Time
}

const (
	SandboxLifecycleKindPause    = "pause"
	SandboxLifecycleKindResume   = "resume"
	SandboxLifecycleKindFork     = "fork"
	SandboxLifecycleKindSnapshot = "snapshot"
	SandboxLifecycleKindRebase   = "rebase"

	SandboxLifecycleSourceManual = "manual"
	SandboxLifecycleSourceAuto   = "auto"
	SandboxLifecycleSourceCrash  = "crash"
	SandboxLifecycleSourceHealth = "health"
	SandboxLifecycleSourceLost   = "lost"

	SandboxLifecyclePhasePreparing  = "preparing"
	SandboxLifecyclePhaseBarriered  = "barriered"
	SandboxLifecyclePhasePublishing = "publishing"
	SandboxLifecyclePhaseCommitting = "committing"
	SandboxLifecyclePhaseCommitted  = "committed"
	SandboxLifecyclePhaseAborted    = "aborted"
)

// SandboxLifecycleTxn is the durable prepare/commit record for a sandbox
// runtime or RootFS topology transition.
type SandboxLifecycleTxn struct {
	ID                       string
	SandboxID                string
	Kind                     string
	Phase                    string
	Source                   string
	Cancelable               bool
	Epoch                    int64
	FromGeneration           int64
	ToGeneration             int64
	FromRuntimeNamespace     string
	FromRuntimeID            string
	ToRuntimeNamespace       string
	ToRuntimeID              string
	TargetSandboxID          string
	TargetGenerationID       string
	TargetRecordDigest       []byte
	SourceBaseArtifactDigest string
	TargetBaseArtifactDigest string
	WorkerClusterID          string
	WorkerNodeID             string
	WorkerNodeUID            string
	WorkerProofDigest        []byte
	WorkerAcknowledgedAt     time.Time
	ExpectedGenerationID     string
	PreparedGenerationID     string
	Error                    string
	CancelReason             string
	CreatedAt                time.Time
	UpdatedAt                time.Time
	CancelRequestedAt        time.Time
	CommittedAt              time.Time
	AbortedAt                time.Time
	RollbackExpiresAt        time.Time
}

// SandboxStore persists sandbox identities independently of physical runtimes.
type SandboxStore interface {
	UpsertSandbox(ctx context.Context, record *SandboxRecord) error
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error)
	ListSandboxes(ctx context.Context, req *ListSandboxesRequest) ([]*SandboxRecord, error)
	ListActiveLifecycleTxns(ctx context.Context, kind string, limit int) ([]*SandboxLifecycleTxn, error)
	GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error)
	ListHardExpiredSandboxes(ctx context.Context, now time.Time, limit int) ([]*SandboxRecord, error)
	MarkSandboxDeleted(ctx context.Context, sandboxID string, deletedAt time.Time) error
	WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error
}

// SandboxStoreTx is a locked sandbox store transaction.
type SandboxStoreTx interface {
	SaveSandbox(ctx context.Context, record *SandboxRecord) error
	SaveRuntime(ctx context.Context, sandboxID, runtimeNamespace, runtimeID string, generation int64, expiresAt, hardExpiresAt time.Time, ownerKind string) error
	MarkHotClaimCompleted(ctx context.Context, sandboxID string, completedAt time.Time) error
	MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error
	MarkRuntimeTerminating(ctx context.Context, sandboxID string) error
	GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error)
	BeginLifecycleTxn(ctx context.Context, txn *SandboxLifecycleTxn) error
	SetLifecycleTxnRuntime(ctx context.Context, txnID, runtimeNamespace, runtimeID string) error
	UpdateLifecycleTxnPhase(ctx context.Context, txnID, phase string) error
	SetLifecycleTxnPreparedGeneration(ctx context.Context, txnID, preparedGenerationID string) error
	RequestLifecycleTxnCancel(ctx context.Context, txnID, reason string) (bool, error)
	CommitLifecycleTxn(ctx context.Context, txnID, preparedGenerationID string) error
	AbortLifecycleTxn(ctx context.Context, txnID, reason string) error
}

type PGSandboxStore struct {
	pool *pgxpool.Pool
}

func NewPGSandboxStore(pool *pgxpool.Pool) *PGSandboxStore {
	if pool == nil {
		return nil
	}
	return &PGSandboxStore{pool: pool}
}

type sandboxStoreLogger interface {
	Printf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func RunSandboxStoreMigrations(ctx context.Context, pool *pgxpool.Pool, logger sandboxStoreLogger) error {
	if err := migrate.Up(ctx, pool, ".",
		migrate.WithBaseFS(storemigrations.FS),
		migrate.WithLogger(logger),
		migrate.WithSchema(sandboxStoreSchemaName),
	); err != nil {
		return fmt.Errorf("run sandbox store migrations: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) UpsertSandbox(ctx context.Context, record *SandboxRecord) error {
	if s == nil || s.pool == nil || record == nil {
		return nil
	}
	return upsertSandboxRecord(ctx, s.pool, record)
}

func upsertSandboxRecord(ctx context.Context, exec sqlExecutor, record *SandboxRecord) error {
	if exec == nil || record == nil {
		return nil
	}
	args, err := sandboxRecordInsertArgs(record)
	if err != nil {
		return err
	}
	args = append(args, SandboxDesiredStateTerminating, SandboxDesiredStateDeleted)
	_, err = exec.Exec(ctx, sandboxRecordInsertSQL+`
		ON CONFLICT (sandbox_id) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			template_id = EXCLUDED.template_id,
			template_name = EXCLUDED.template_name,
			template_namespace = EXCLUDED.template_namespace,
			cluster_id = EXCLUDED.cluster_id,
			desired_state = EXCLUDED.desired_state,
			config = EXCLUDED.config,
			template_spec = EXCLUDED.template_spec,
			runtime_id = EXCLUDED.runtime_id,
			runtime_namespace = EXCLUDED.runtime_namespace,
			runtime_generation = EXCLUDED.runtime_generation,
			lifecycle_epoch = GREATEST(manager.sandboxes.lifecycle_epoch, EXCLUDED.lifecycle_epoch),
			owner_kind = EXCLUDED.owner_kind,
			resource_millicpu = EXCLUDED.resource_millicpu,
			resource_memory_mib = EXCLUDED.resource_memory_mib,
			hot_claim_completed_at = COALESCE(EXCLUDED.hot_claim_completed_at, manager.sandboxes.hot_claim_completed_at),
			claimed_at = EXCLUDED.claimed_at,
			expires_at = EXCLUDED.expires_at,
			hard_expires_at = EXCLUDED.hard_expires_at,
			deleted_at = EXCLUDED.deleted_at,
			updated_at = NOW()
		WHERE manager.sandboxes.deleted_at IS NULL
			AND manager.sandboxes.desired_state NOT IN ($24, $25)
	`, args...)
	if err != nil {
		return fmt.Errorf("upsert sandbox: %w", err)
	}
	return nil
}

const sandboxRecordInsertSQL = `
	INSERT INTO manager.sandboxes (
		sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
		cluster_id, desired_state, config, template_spec,
		runtime_id, runtime_namespace, runtime_generation, lifecycle_epoch,
		owner_kind, resource_millicpu, resource_memory_mib, hot_claim_completed_at,
		claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
	)
	VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, COALESCE($23, NOW()), NOW())`

func sandboxRecordInsertArgs(record *SandboxRecord) ([]any, error) {
	if record == nil {
		return nil, fmt.Errorf("sandbox record is required")
	}
	if strings.TrimSpace(record.ID) == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	configJSON, specJSON, err := marshalSandboxRecordJSON(record)
	if err != nil {
		return nil, err
	}
	if record.ResourceMillicpu <= 0 || record.ResourceMemoryMiB <= 0 {
		return nil, fmt.Errorf("sandbox resource lease metering values must be positive")
	}
	return []any{
		record.ID, record.TeamID, record.UserID, record.TemplateID, record.TemplateName, record.TemplateNamespace,
		record.ClusterID, record.DesiredState, configJSON, specJSON,
		record.RuntimeID, record.RuntimeNamespace, record.RuntimeGeneration, record.LifecycleEpoch,
		strings.TrimSpace(record.OwnerKind), record.ResourceMillicpu, record.ResourceMemoryMiB,
		nullableTime(record.HotClaimCompletedAt),
		nullableTime(record.ClaimedAt), nullableTime(record.ExpiresAt), nullableTime(record.HardExpiresAt),
		nullableTime(record.DeletedAt), nullableTime(record.CreatedAt),
	}, nil
}

func (s *PGSandboxStore) GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	return scanSandboxRecord(s.pool.QueryRow(ctx, sandboxRecordSelectSQL()+` WHERE sandbox_id = $1`, sandboxID))
}

func (s *PGSandboxStore) ListSandboxes(ctx context.Context, req *ListSandboxesRequest) ([]*SandboxRecord, error) {
	if s == nil || s.pool == nil || req == nil {
		return nil, nil
	}
	// Public status is projected from the runtime-slot authority after this
	// query, so only durable filters belong in SQL.
	rows, err := s.pool.Query(ctx, sandboxRecordSelectSQL()+`
		WHERE team_id = $1
			AND deleted_at IS NULL
			AND ($2 = '' OR template_id = $2)
		ORDER BY created_at DESC
	`, req.TeamID, req.TemplateID)
	if err != nil {
		return nil, fmt.Errorf("list sandboxes: %w", err)
	}
	defer rows.Close()
	var records []*SandboxRecord
	for rows.Next() {
		record, err := scanSandboxRecordRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sandboxes: %w", err)
	}
	return records, nil
}

// CountActiveSandboxes returns the region-wide operational count used by the
// active_sandboxes quota. All clusters in a region share this store.
func (s *PGSandboxStore) CountActiveSandboxes(ctx context.Context, teamID string) (int64, error) {
	if s == nil || s.pool == nil {
		return 0, nil
	}
	var total int64
	if err := s.pool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.sandboxes
		WHERE team_id = $1
			AND deleted_at IS NULL
			AND desired_state = $2
	`, strings.TrimSpace(teamID), SandboxDesiredStateActive).Scan(&total); err != nil {
		return 0, fmt.Errorf("count active sandboxes: %w", err)
	}
	return total, nil
}

func (s *PGSandboxStore) ListActiveLifecycleTxns(ctx context.Context, kind string, limit int) ([]*SandboxLifecycleTxn, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, lifecycleTxnSelectSQL()+`
		WHERE kind = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
		ORDER BY updated_at ASC
		LIMIT $2
	`, strings.TrimSpace(kind), limit)
	if err != nil {
		return nil, fmt.Errorf("list active lifecycle txns: %w", err)
	}
	defer rows.Close()
	var txns []*SandboxLifecycleTxn
	for rows.Next() {
		txn, err := scanLifecycleTxnRows(rows)
		if err != nil {
			return nil, err
		}
		txns = append(txns, txn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active lifecycle txns: %w", err)
	}
	return txns, nil
}

// ListPendingRuntimeRecoverySandboxIDs returns due, unclaimed paused
// sandboxes whose latest durable lifecycle outcome requires reconstruction.
func (s *PGSandboxStore) ListPendingRuntimeRecoverySandboxIDs(ctx context.Context, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, pendingRuntimeRecoverySandboxSelectSQL()+`
		AND latest.recovery_next_attempt_at <= NOW()
		AND (latest.recovery_claimed_until IS NULL OR latest.recovery_claimed_until <= NOW())
		ORDER BY s.updated_at ASC
		LIMIT $9
	`, pendingRuntimeRecoveryQueryArgs(limit)...)
	if err != nil {
		return nil, fmt.Errorf("list pending runtime recovery sandboxes: %w", err)
	}
	defer rows.Close()
	var sandboxIDs []string
	for rows.Next() {
		var sandboxID string
		if err := rows.Scan(&sandboxID); err != nil {
			return nil, fmt.Errorf("scan pending runtime recovery sandbox: %w", err)
		}
		sandboxIDs = append(sandboxIDs, sandboxID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate pending runtime recovery sandboxes: %w", err)
	}
	return sandboxIDs, nil
}

// IsRuntimeRecoveryPending revalidates one queued recovery against either its
// exact in-flight crash lifecycle or the durable crash-abandon outcome.
func (s *PGSandboxStore) IsRuntimeRecoveryPending(ctx context.Context, sandboxID string) (bool, error) {
	if s == nil || s.pool == nil {
		return false, nil
	}
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || len(sandboxID) > 512 {
		return false, fmt.Errorf("sandbox ID is required and must not exceed 512 bytes")
	}
	args := pendingRuntimeRecoveryQueryArgs(sandboxID)
	var pending bool
	if err := s.pool.QueryRow(ctx, `
		SELECT
			EXISTS (
	`+pendingRuntimeRecoverySandboxSelectSQL()+`
				AND s.sandbox_id = $9
			)
			OR EXISTS (
				SELECT 1
				FROM manager.sandboxes AS s
				JOIN manager.sandbox_lifecycle_txns AS txn
					ON txn.sandbox_id = s.sandbox_id
				WHERE s.sandbox_id = $9
					AND s.deleted_at IS NULL
					AND s.desired_state IN ('active', 'paused')
					AND txn.epoch = s.lifecycle_epoch
					AND txn.kind = $4
					AND txn.source IN ($5, $6, $7)
					AND txn.phase IN ('preparing', 'barriered', 'publishing', 'committing')
			)
	`, args...).Scan(&pending); err != nil {
		return false, fmt.Errorf("check pending runtime recovery sandbox: %w", err)
	}
	return pending, nil
}

// pendingRuntimeRecoverySandboxSelectSQL deliberately treats an exact
// crash-abandon as a durable recovery outcome even though that transaction is
// aborted: aborting discards the failed writer generation, not the obligation
// to reconstruct the runtime. Failed resume attempts are excluded so they do
// not hide that obligation; a later committed resume or manual pause does.
func pendingRuntimeRecoverySandboxSelectSQL() string {
	return `
		SELECT s.sandbox_id
		FROM manager.sandboxes AS s
		JOIN LATERAL (
			SELECT kind, phase, source,
				recovery_next_attempt_at, recovery_claimed_until
			FROM manager.sandbox_lifecycle_txns
			WHERE sandbox_id = s.sandbox_id
				AND (
					phase = $1
					OR (
						phase = $2
						AND kind = $4
						AND source IN ($5, $6, $7)
						AND error = $8
					)
				)
			ORDER BY epoch DESC
			LIMIT 1
		) AS latest ON TRUE
		WHERE s.deleted_at IS NULL
			AND s.desired_state = $3
			AND latest.kind = $4
			AND latest.source IN ($5, $6, $7)
	`
}

func pendingRuntimeRecoveryQueryArgs(last any) []any {
	return []any{
		SandboxLifecyclePhaseCommitted,
		SandboxLifecyclePhaseAborted,
		SandboxDesiredStatePaused,
		SandboxLifecycleKindPause,
		SandboxLifecycleSourceCrash,
		SandboxLifecycleSourceHealth,
		SandboxLifecycleSourceLost,
		RootFSWriterCrashAbandonReason,
		last,
	}
}

func (s *PGSandboxStore) GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	return getActiveLifecycleTxn(ctx, s.pool, sandboxID)
}

// GetLifecycleTxn returns one exact lifecycle operation, including committed
// identity needed to finish node-side acknowledgement after response loss.
func (s *PGSandboxStore) GetLifecycleTxn(ctx context.Context, txnID string) (*SandboxLifecycleTxn, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	txnID = strings.TrimSpace(txnID)
	if txnID == "" || len(txnID) > 512 {
		return nil, fmt.Errorf("lifecycle txn ID is required and must not exceed 512 bytes")
	}
	return scanLifecycleTxn(s.pool.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1
	`, txnID))
}

func (s *PGSandboxStore) ListHardExpiredSandboxes(ctx context.Context, now time.Time, limit int) ([]*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, sandboxRecordSelectSQL()+`
		WHERE deleted_at IS NULL
			AND hard_expires_at IS NOT NULL
			AND hard_expires_at <= $1
		ORDER BY hard_expires_at ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list hard-expired sandboxes: %w", err)
	}
	defer rows.Close()
	var records []*SandboxRecord
	for rows.Next() {
		record, err := scanSandboxRecordRows(rows)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate hard-expired sandboxes: %w", err)
	}
	return records, nil
}

// ListSandboxExpirationCandidates returns sandboxes that need a durable
// hard-delete request or an automatic pause request. Active
// lifecycle transactions suppress only soft expiry; hard expiry must be able
// to preempt a pause, fork, or rebase in progress.
func (s *PGSandboxStore) ListSandboxExpirationCandidates(
	ctx context.Context,
	now time.Time,
	limit int,
) ([]SandboxExpirationCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		WITH expiration_candidates AS (
			SELECT sandbox_id, desired_state, expires_at, hard_expires_at,
				0 AS priority, hard_expires_at AS deadline
			FROM manager.sandboxes
			WHERE deleted_at IS NULL
				AND desired_state IN ($2, $3)
				AND hard_expires_at IS NOT NULL
				AND hard_expires_at <= $1
			UNION ALL
			SELECT sandbox_id, desired_state, expires_at, hard_expires_at,
				1 AS priority, expires_at AS deadline
			FROM manager.sandboxes AS sandbox
			WHERE sandbox.deleted_at IS NULL
				AND sandbox.desired_state = $2
				AND sandbox.expires_at IS NOT NULL
				AND sandbox.expires_at <= $1
				AND (sandbox.hard_expires_at IS NULL OR sandbox.hard_expires_at > $1)
				AND NOT EXISTS (
					SELECT 1
					FROM manager.sandbox_lifecycle_txns AS lifecycle
					WHERE lifecycle.sandbox_id = sandbox.sandbox_id
						AND lifecycle.phase IN ('preparing', 'barriered', 'publishing', 'committing')
				)
		)
		SELECT sandbox_id, desired_state, expires_at, hard_expires_at
		FROM expiration_candidates
		ORDER BY priority, deadline, sandbox_id
		LIMIT $4
	`, now.UTC(), SandboxDesiredStateActive, SandboxDesiredStatePaused, limit)
	if err != nil {
		return nil, fmt.Errorf("list sandbox expiration candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]SandboxExpirationCandidate, 0, limit)
	for rows.Next() {
		var candidate SandboxExpirationCandidate
		var expiresAt, hardExpiresAt *time.Time
		if err := rows.Scan(
			&candidate.SandboxID,
			&candidate.DesiredState,
			&expiresAt,
			&hardExpiresAt,
		); err != nil {
			return nil, fmt.Errorf("scan sandbox expiration candidate: %w", err)
		}
		if expiresAt != nil {
			candidate.ExpiresAt = expiresAt.UTC()
		}
		if hardExpiresAt != nil {
			candidate.HardExpiresAt = hardExpiresAt.UTC()
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sandbox expiration candidates: %w", err)
	}
	return candidates, nil
}

func (s *PGSandboxStore) MarkSandboxDeleted(ctx context.Context, sandboxID string, deletedAt time.Time) error {
	if s == nil || s.pool == nil {
		return nil
	}
	if deletedAt.IsZero() {
		deletedAt = time.Now().UTC()
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin mark sandbox deleted tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentDesiredState, teamID, webhookURL, webhookSecret string
	err = tx.QueryRow(ctx, `
		SELECT desired_state,
			team_id,
			COALESCE(config->'webhook'->>'url', ''),
			COALESCE(config->'webhook'->>'secret', '')
		FROM manager.sandboxes
		WHERE sandbox_id = $1
		FOR UPDATE
	`, sandboxID).Scan(&currentDesiredState, &teamID, &webhookURL, &webhookSecret)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("lock sandbox before marking deleted: %w", err)
	}
	if err == nil {
		blocked, blockErr := sandboxClaimCleanupBlockedByPausedRebase(ctx, tx, sandboxID)
		if blockErr != nil {
			return blockErr
		}
		if blocked {
			return fmt.Errorf("%w: paused RootFS rebase worker outcome is pending", ErrSandboxClaimCleanupPending)
		}
		var materializationPending bool
		if err := tx.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1
				FROM manager.rootfs_generations generation
				JOIN manager.rootfs_materialization_members member
					ON member.generation_id = generation.generation_id
				WHERE member.state = 'uploading'
					AND (
						EXISTS (
							SELECT 1 FROM manager.sandbox_rootfs_bindings binding
							WHERE binding.sandbox_id = $1
								AND binding.filesystem_id = generation.filesystem_id
						)
						OR EXISTS (
							SELECT 1 FROM manager.rootfs_writer_grants writer
							WHERE writer.sandbox_id = $1
								AND writer.filesystem_id = generation.filesystem_id
						)
					)
			)
		`, sandboxID).Scan(&materializationPending); err != nil {
			return fmt.Errorf("check sandbox RootFS materialization cleanup fence: %w", err)
		}
		if materializationPending {
			return fmt.Errorf("%w: RootFS materialization batch is uploading", ErrSandboxClaimCleanupPending)
		}
	}
	if err == nil && currentDesiredState == SandboxDesiredStateTerminating && strings.TrimSpace(webhookURL) != "" {
		if err := deletionwebhook.Enqueue(ctx, tx, sandboxID, teamID, webhookURL, webhookSecret, deletedAt); err != nil {
			return err
		}
	}
	if err == nil {
		if cancelErr := cancelPendingNomadSandboxNetworkMutationForSandbox(
			ctx, tx, sandboxID, "sandbox deleted",
		); cancelErr != nil {
			return cancelErr
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			runtime_id = '',
			runtime_namespace = '',
			deleted_at = $3,
			updated_at = NOW()
		WHERE sandbox_id = $1
	`, sandboxID, SandboxDesiredStateDeleted, deletedAt); err != nil {
		return fmt.Errorf("mark sandbox deleted: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			error = $3,
			aborted_at = NOW(),
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`, sandboxID, SandboxLifecyclePhaseAborted, "sandbox deleted"); err != nil {
		return fmt.Errorf("abort sandbox lifecycle txns for deleted sandbox: %w", err)
	}
	// Node reconcilers retain an external crash proof for 48 hours. Preserve
	// the minimum immutable regional identity before filesystem deletion
	// cascades terminal writer grant history.
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.rootfs_writer_terminal_proofs (
			grant_id, sandbox_id, writer_epoch, binding_version,
			binding_digest, node_uid, state, expires_at
		)
		SELECT grant_id, sandbox_id, writer_epoch, binding_version,
			binding_digest, node_uid, state,
			NOW() + ($4 * INTERVAL '1 millisecond')
		FROM manager.rootfs_writer_grants
		WHERE sandbox_id = $1 AND state IN ($2, $3)
		ON CONFLICT (grant_id) DO NOTHING
	`, sandboxID, RootFSWriterGrantStateRetired, RootFSWriterGrantStateCanceled,
		RootFSWriterTerminalProofRetention.Milliseconds()); err != nil {
		return fmt.Errorf("preserve terminal rootfs writer proofs: %w", err)
	}
	// A terminal slot is durable allocation history, not storage authority. Keep
	// its claim and allocation identity while releasing the references that
	// would otherwise retain an unreferenced filesystem after sandbox deletion.
	if _, err := tx.Exec(ctx, `
		UPDATE manager.runtime_slots
		SET filesystem_id = NULL,
			source_generation_id = NULL,
			writer_grant_id = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1 AND state = $2
	`, sandboxID, RuntimeSlotStateTerminal); err != nil {
		return fmt.Errorf("detach terminal runtime slot storage: %w", err)
	}
	// A completed running fork retains the target checkpoint for idempotent
	// retries while the target exists. Once the target is deleted, its durable
	// sandbox row is enough to reject a replay and the fork record must release
	// its RESTRICT references before the target filesystem can be collected.
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_running_forks
		WHERE target_sandbox_id = $1
	`, sandboxID); err != nil {
		return fmt.Errorf("delete completed running fork target metadata: %w", err)
	}
	filesystemRows, err := tx.Query(ctx, `
		SELECT binding.filesystem_id
		FROM manager.sandbox_rootfs_bindings AS binding
		WHERE binding.sandbox_id = $1
			AND NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_writer_grants AS writer_grant
				WHERE writer_grant.filesystem_id = binding.filesystem_id
					AND writer_grant.state IN ($2, $3, $4)
			)
		UNION
		SELECT writer_grant.filesystem_id
		FROM manager.rootfs_writer_grants AS writer_grant
		WHERE writer_grant.sandbox_id = $1 AND writer_grant.state IN ($5, $6)
	`, sandboxID, RootFSWriterGrantStateIssued, RootFSWriterGrantStateConsumed,
		RootFSWriterGrantStateRetiring, RootFSWriterGrantStateRetired,
		RootFSWriterGrantStateCanceled)
	if err != nil {
		return fmt.Errorf("list deleted sandbox rootfs candidates: %w", err)
	}
	filesystemIDs := make([]string, 0, 1)
	for filesystemRows.Next() {
		var filesystemID string
		if err := filesystemRows.Scan(&filesystemID); err != nil {
			filesystemRows.Close()
			return fmt.Errorf("scan deleted sandbox rootfs candidate: %w", err)
		}
		filesystemIDs = append(filesystemIDs, filesystemID)
	}
	if err := filesystemRows.Err(); err != nil {
		filesystemRows.Close()
		return fmt.Errorf("iterate deleted sandbox rootfs candidates: %w", err)
	}
	filesystemRows.Close()
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.sandbox_rootfs_bindings AS binding
		WHERE binding.sandbox_id = $1
			AND NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_writer_grants AS writer_grant
				WHERE writer_grant.filesystem_id = binding.filesystem_id
					AND writer_grant.state IN ($2, $3, $4)
			)
	`, sandboxID, RootFSWriterGrantStateIssued, RootFSWriterGrantStateConsumed,
		RootFSWriterGrantStateRetiring); err != nil {
		return fmt.Errorf("delete sandbox rootfs binding: %w", err)
	}
	if len(filesystemIDs) > 0 {
		deletableRows, err := tx.Query(ctx, `
				SELECT f.filesystem_id
				FROM manager.rootfs_filesystems AS f
				WHERE f.filesystem_id = ANY($1::text[])
				AND NOT EXISTS (
				SELECT 1
				FROM manager.sandbox_rootfs_bindings b
				WHERE b.filesystem_id = f.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_snapshots s
				WHERE s.filesystem_id = f.filesystem_id
			)
			AND NOT EXISTS (
				SELECT 1
				FROM manager.rootfs_filesystems child
				WHERE child.source_filesystem_id = f.filesystem_id
			)
			FOR UPDATE OF f
			`, filesystemIDs)
		if err != nil {
			return fmt.Errorf("list unreferenced sandbox rootfs filesystems: %w", err)
		}
		deletableFilesystemIDs := make([]string, 0, len(filesystemIDs))
		for deletableRows.Next() {
			var filesystemID string
			if err := deletableRows.Scan(&filesystemID); err != nil {
				deletableRows.Close()
				return fmt.Errorf("scan unreferenced sandbox rootfs filesystem: %w", err)
			}
			deletableFilesystemIDs = append(deletableFilesystemIDs, filesystemID)
		}
		if err := deletableRows.Err(); err != nil {
			deletableRows.Close()
			return fmt.Errorf("iterate unreferenced sandbox rootfs filesystems: %w", err)
		}
		deletableRows.Close()
		if len(deletableFilesystemIDs) == 0 {
			// Snapshots, forks, or another sandbox binding still retain every
			// candidate filesystem.
			deletableFilesystemIDs = nil
		}
		if len(deletableFilesystemIDs) > 0 {
			objectRows, err := tx.Query(ctx, `
				SELECT DISTINCT locator_object.object_key
				FROM manager.rootfs_generation_materialization_objects locator_object
				JOIN manager.rootfs_generations generation USING (generation_id)
				WHERE generation.filesystem_id = ANY($1::text[])
				ORDER BY locator_object.object_key
			`, deletableFilesystemIDs)
			if err != nil {
				return fmt.Errorf("list deleted sandbox materialization objects: %w", err)
			}
			var materializationObjectKeys []string
			for objectRows.Next() {
				var objectKey string
				if err := objectRows.Scan(&objectKey); err != nil {
					objectRows.Close()
					return fmt.Errorf("scan deleted sandbox materialization object: %w", err)
				}
				materializationObjectKeys = append(materializationObjectKeys, objectKey)
			}
			if err := objectRows.Err(); err != nil {
				objectRows.Close()
				return fmt.Errorf("iterate deleted sandbox materialization objects: %w", err)
			}
			objectRows.Close()
			if _, err := tx.Exec(ctx, `
				UPDATE manager.rootfs_filesystems
				SET head_generation_id = NULL, updated_at = NOW()
				WHERE filesystem_id = ANY($1::text[])
			`, deletableFilesystemIDs); err != nil {
				return fmt.Errorf("clear deleted sandbox rootfs generation heads: %w", err)
			}
			if _, err := tx.Exec(ctx, `
				DELETE FROM manager.rootfs_generations
				WHERE filesystem_id = ANY($1::text[])
			`, deletableFilesystemIDs); err != nil {
				return fmt.Errorf("delete sandbox rootfs generations: %w", err)
			}
			for _, objectKey := range materializationObjectKeys {
				if _, err := releaseUnreferencedRootFSMaterializationObject(
					ctx, tx, objectKey, teamID,
				); err != nil {
					return err
				}
			}
			if _, err := tx.Exec(ctx, `
				DELETE FROM manager.rootfs_filesystems
				WHERE filesystem_id = ANY($1::text[])
			`, deletableFilesystemIDs); err != nil {
				return fmt.Errorf("delete unreferenced sandbox rootfs filesystem: %w", err)
			}
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit mark sandbox deleted tx: %w", err)
	}
	return nil
}

// GetRootFSLayerChainByHead returns the immutable ancestor chain ending at
// headLayerID. It is used by point-in-time products that must not follow a
// sandbox head after the source sandbox continues running.
func (s *PGSandboxStore) WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error {
	if s == nil || s.pool == nil || fn == nil {
		return nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin sandbox lock tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := scanSandboxRecord(tx.QueryRow(ctx, sandboxRecordSelectSQL()+` WHERE sandbox_id = $1 FOR UPDATE`, sandboxID))
	if err != nil {
		return err
	}
	if record == nil {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	if err := fn(ctx, sandboxStoreTx{tx: tx}, record); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sandbox lock tx: %w", err)
	}
	return nil
}

type sandboxStoreTx struct {
	tx pgx.Tx
}

func (t sandboxStoreTx) SaveSandbox(ctx context.Context, record *SandboxRecord) error {
	return upsertSandboxRecord(ctx, t.tx, record)
}

func (t sandboxStoreTx) SaveRuntime(ctx context.Context, sandboxID, runtimeNamespace, runtimeID string, generation int64, expiresAt, hardExpiresAt time.Time, ownerKind string) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			runtime_namespace = $3,
			runtime_id = $4,
			runtime_generation = $5,
			expires_at = $6,
			hard_expires_at = $7,
			owner_kind = COALESCE(NULLIF($8, ''), owner_kind),
			deleted_at = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
			AND desired_state NOT IN ($9, $10)
	`, sandboxID, SandboxDesiredStateActive, runtimeNamespace, runtimeID, generation, nullableTime(expiresAt), nullableTime(hardExpiresAt), strings.TrimSpace(ownerKind), SandboxDesiredStateTerminating, SandboxDesiredStateDeleted)
	if err != nil {
		return fmt.Errorf("save sandbox runtime: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	return nil
}

// MarkHotClaimCompleted records a hot-pool handoff inside the caller's locked
// lifecycle transaction.
func (t sandboxStoreTx) MarkHotClaimCompleted(ctx context.Context, sandboxID string, completedAt time.Time) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET hot_claim_completed_at = $2,
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
			AND desired_state NOT IN ($3, $4)
	`, sandboxID, completedAt, SandboxDesiredStateTerminating, SandboxDesiredStateDeleted)
	if err != nil {
		return fmt.Errorf("mark hot claim completed: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	return nil
}

func (t sandboxStoreTx) MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error {
	if err := cancelPendingNomadSandboxNetworkMutationForSandbox(
		ctx, t.tx, sandboxID, "sandbox runtime paused",
	); err != nil {
		return err
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			runtime_namespace = '',
			runtime_id = '',
			runtime_generation = GREATEST(runtime_generation, $3),
			expires_at = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
			AND desired_state NOT IN ($4, $5)
	`, sandboxID, SandboxDesiredStatePaused, generation, SandboxDesiredStateTerminating, SandboxDesiredStateDeleted)
	if err != nil {
		return fmt.Errorf("mark sandbox runtime paused: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	return nil
}

func (t sandboxStoreTx) MarkRuntimeTerminating(ctx context.Context, sandboxID string) error {
	if err := cancelPendingNomadSandboxNetworkMutationForSandbox(
		ctx, t.tx, sandboxID, "sandbox termination requested",
	); err != nil {
		return err
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
	`, sandboxID, SandboxDesiredStateTerminating)
	if err != nil {
		return fmt.Errorf("mark sandbox runtime terminating: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	return nil
}

func (t sandboxStoreTx) GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error) {
	return getActiveLifecycleTxn(ctx, t.tx, sandboxID)
}

func (t sandboxStoreTx) BeginLifecycleTxn(ctx context.Context, txn *SandboxLifecycleTxn) error {
	if txn == nil {
		return nil
	}
	if strings.TrimSpace(txn.ID) == "" {
		return fmt.Errorf("txn_id is required")
	}
	if strings.TrimSpace(txn.SandboxID) == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(txn.Kind) == "" {
		return fmt.Errorf("lifecycle kind is required")
	}
	if err := cancelPendingNomadSandboxNetworkMutationForSandbox(
		ctx, t.tx, txn.SandboxID, "sandbox lifecycle preempted network update",
	); err != nil {
		return err
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET lifecycle_epoch = lifecycle_epoch + 1,
			updated_at = NOW()
		WHERE sandbox_id = $1
	`, txn.SandboxID)
	if err != nil {
		return fmt.Errorf("advance lifecycle epoch: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, txn.SandboxID)
	}
	var epoch int64
	if err := t.tx.QueryRow(ctx, `SELECT lifecycle_epoch FROM manager.sandboxes WHERE sandbox_id = $1`, txn.SandboxID).Scan(&epoch); err != nil {
		return fmt.Errorf("load lifecycle epoch: %w", err)
	}
	txn.Epoch = epoch
	phase := strings.TrimSpace(txn.Phase)
	if phase == "" {
		phase = SandboxLifecyclePhasePreparing
	}
	source := strings.TrimSpace(txn.Source)
	if source == "" {
		source = SandboxLifecycleSourceManual
	}
	_, err = t.tx.Exec(ctx, `
		INSERT INTO manager.sandbox_lifecycle_txns (
			txn_id, sandbox_id, kind, phase, source, cancelable, epoch,
			from_generation, to_generation,
			from_runtime_namespace, from_runtime_id,
			to_runtime_namespace, to_runtime_id,
			target_sandbox_id, target_generation_id, target_record_digest,
			source_base_artifact_digest, target_base_artifact_digest, rollback_expires_at,
			worker_cluster_id, worker_node_id, worker_node_uid, worker_proof_digest,
			expected_generation_id, prepared_generation_id,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
			COALESCE($16, ''::bytea), $17, $18, $19, $20, $21, $22,
			COALESCE($23, ''::bytea), $24, $25, NOW(), NOW())
	`, txn.ID, txn.SandboxID, txn.Kind, phase, source, txn.Cancelable, txn.Epoch,
		txn.FromGeneration, txn.ToGeneration,
		txn.FromRuntimeNamespace, txn.FromRuntimeID,
		txn.ToRuntimeNamespace, txn.ToRuntimeID,
		txn.TargetSandboxID, txn.TargetGenerationID, txn.TargetRecordDigest,
		txn.SourceBaseArtifactDigest, txn.TargetBaseArtifactDigest, nullableTime(txn.RollbackExpiresAt),
		txn.WorkerClusterID, txn.WorkerNodeID, txn.WorkerNodeUID, txn.WorkerProofDigest,
		txn.ExpectedGenerationID, txn.PreparedGenerationID)
	if err != nil {
		return fmt.Errorf("begin lifecycle txn: %w", err)
	}
	txn.Phase = phase
	txn.Source = source
	return nil
}

// BeginOrRestartRootFSWriterCrashLifecycleTxn recovers the deterministic
// runtime-slot writer operation when an earlier pre-fence attempt left the
// same lifecycle ID aborted but the exact writer grant remained live. A
// successfully completed crash-abandon is never reopened.
func (t sandboxStoreTx) BeginOrRestartRootFSWriterCrashLifecycleTxn(
	ctx context.Context,
	txn *SandboxLifecycleTxn,
) error {
	if txn == nil || strings.TrimSpace(txn.ID) == "" || strings.TrimSpace(txn.SandboxID) == "" ||
		txn.Kind != SandboxLifecycleKindPause ||
		!rootFSWriterCrashAbandonSource(txn.Source) || txn.Cancelable ||
		txn.Phase != SandboxLifecyclePhasePublishing || txn.FromGeneration <= 0 || txn.ToGeneration != 0 ||
		strings.TrimSpace(txn.FromRuntimeNamespace) == "" || strings.TrimSpace(txn.FromRuntimeID) == "" ||
		strings.TrimSpace(txn.ToRuntimeNamespace) != "" || strings.TrimSpace(txn.ToRuntimeID) != "" ||
		strings.TrimSpace(txn.TargetSandboxID) != "" || strings.TrimSpace(txn.TargetGenerationID) != "" ||
		len(txn.TargetRecordDigest) != 0 || strings.TrimSpace(txn.SourceBaseArtifactDigest) != "" ||
		strings.TrimSpace(txn.TargetBaseArtifactDigest) != "" || !txn.RollbackExpiresAt.IsZero() ||
		strings.TrimSpace(txn.WorkerClusterID) != "" || strings.TrimSpace(txn.WorkerNodeID) != "" ||
		strings.TrimSpace(txn.WorkerNodeUID) != "" || len(txn.WorkerProofDigest) != 0 ||
		!txn.WorkerAcknowledgedAt.IsZero() || strings.TrimSpace(txn.ExpectedGenerationID) == "" ||
		strings.TrimSpace(txn.PreparedGenerationID) != "" {
		return fmt.Errorf("restartable RootFS writer crash lifecycle is invalid")
	}
	existing, err := scanLifecycleTxn(t.tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1
		FOR UPDATE
	`, txn.ID))
	if err != nil {
		return fmt.Errorf("lock restartable RootFS writer crash lifecycle: %w", err)
	}
	if existing == nil {
		return t.BeginLifecycleTxn(ctx, txn)
	}
	if !restartableRootFSWriterCrashLifecycleMatches(existing, txn) {
		return fmt.Errorf("RootFS writer crash lifecycle %s is not restartable", txn.ID)
	}
	if err := cancelPendingNomadSandboxNetworkMutationForSandbox(
		ctx, t.tx, txn.SandboxID, "sandbox lifecycle preempted network update",
	); err != nil {
		return err
	}
	var epoch int64
	if err := t.tx.QueryRow(ctx, `
		UPDATE manager.sandboxes
		SET lifecycle_epoch = lifecycle_epoch + 1,
			updated_at = NOW()
		WHERE sandbox_id = $1
		RETURNING lifecycle_epoch
	`, txn.SandboxID).Scan(&epoch); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, txn.SandboxID)
		}
		return fmt.Errorf("advance restarted lifecycle epoch: %w", err)
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			epoch = $3,
			error = '',
			cancel_reason = '',
			cancel_requested_at = NULL,
			committed_at = NULL,
			aborted_at = NULL,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase = $4
			AND epoch = $5
	`, txn.ID, SandboxLifecyclePhasePublishing, epoch,
		SandboxLifecyclePhaseAborted, existing.Epoch)
	if err != nil {
		return fmt.Errorf("restart RootFS writer crash lifecycle: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("RootFS writer crash lifecycle %s changed before restart", txn.ID)
	}
	txn.Epoch = epoch
	return nil
}

func restartableRootFSWriterCrashLifecycleMatches(existing, requested *SandboxLifecycleTxn) bool {
	return existing != nil && requested != nil &&
		existing.ID == requested.ID && existing.SandboxID == requested.SandboxID &&
		existing.Kind == requested.Kind && existing.Source == requested.Source &&
		!existing.Cancelable && !requested.Cancelable &&
		existing.Phase == SandboxLifecyclePhaseAborted && existing.CancelRequestedAt.IsZero() &&
		existing.Error != RootFSWriterCrashAbandonReason &&
		existing.FromGeneration == requested.FromGeneration && existing.ToGeneration == requested.ToGeneration &&
		existing.FromRuntimeNamespace == requested.FromRuntimeNamespace && existing.FromRuntimeID == requested.FromRuntimeID &&
		existing.ToRuntimeNamespace == requested.ToRuntimeNamespace && existing.ToRuntimeID == requested.ToRuntimeID &&
		existing.TargetSandboxID == requested.TargetSandboxID && existing.TargetGenerationID == requested.TargetGenerationID &&
		bytes.Equal(existing.TargetRecordDigest, requested.TargetRecordDigest) &&
		existing.SourceBaseArtifactDigest == requested.SourceBaseArtifactDigest &&
		existing.TargetBaseArtifactDigest == requested.TargetBaseArtifactDigest &&
		existing.RollbackExpiresAt.Equal(requested.RollbackExpiresAt) &&
		existing.WorkerClusterID == requested.WorkerClusterID && existing.WorkerNodeID == requested.WorkerNodeID &&
		existing.WorkerNodeUID == requested.WorkerNodeUID &&
		bytes.Equal(existing.WorkerProofDigest, requested.WorkerProofDigest) &&
		existing.WorkerAcknowledgedAt.Equal(requested.WorkerAcknowledgedAt) &&
		existing.ExpectedGenerationID == requested.ExpectedGenerationID &&
		existing.PreparedGenerationID == "" && requested.PreparedGenerationID == ""
}

func (t sandboxStoreTx) SetLifecycleTxnRuntime(ctx context.Context, txnID, runtimeNamespace, runtimeID string) error {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET to_runtime_namespace = $2,
			to_runtime_id = $3,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`, txnID, strings.TrimSpace(runtimeNamespace), strings.TrimSpace(runtimeID))
	if err != nil {
		return fmt.Errorf("set lifecycle txn runtime: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("active lifecycle txn %s not found", txnID)
	}
	return nil
}

func (t sandboxStoreTx) UpdateLifecycleTxnPhase(ctx context.Context, txnID, phase string) error {
	txnID = strings.TrimSpace(txnID)
	phase = strings.TrimSpace(phase)
	if txnID == "" || phase == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, phase)
	if err != nil {
		return fmt.Errorf("update lifecycle txn phase: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("active lifecycle txn %s not found", txnID)
	}
	return nil
}

func (t sandboxStoreTx) SetLifecycleTxnPreparedGeneration(ctx context.Context, txnID, preparedGenerationID string) error {
	txnID = strings.TrimSpace(txnID)
	preparedGenerationID = strings.TrimSpace(preparedGenerationID)
	if txnID == "" || preparedGenerationID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET prepared_generation_id = $2,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, preparedGenerationID)
	if err != nil {
		return fmt.Errorf("set lifecycle txn prepared head: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("active lifecycle txn %s not found", txnID)
	}
	return nil
}

func (t sandboxStoreTx) RequestLifecycleTxnCancel(ctx context.Context, txnID, reason string) (bool, error) {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return false, nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET cancel_requested_at = COALESCE(cancel_requested_at, NOW()),
			cancel_reason = CASE
				WHEN cancel_reason = '' THEN $2
				ELSE cancel_reason
			END,
			updated_at = NOW()
		WHERE txn_id = $1
			AND kind = 'pause'
			AND source = 'auto'
			AND cancelable = TRUE
			AND phase IN ('preparing', 'barriered', 'publishing')
	`, txnID, strings.TrimSpace(reason))
	if err != nil {
		return false, fmt.Errorf("request lifecycle txn cancel: %w", err)
	}
	return tag.RowsAffected() > 0, nil
}

func (t sandboxStoreTx) CommitLifecycleTxn(ctx context.Context, txnID, preparedGenerationID string) error {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			prepared_generation_id = $3,
			committed_at = NOW(),
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, SandboxLifecyclePhaseCommitted, strings.TrimSpace(preparedGenerationID))
	if err != nil {
		return fmt.Errorf("commit lifecycle txn: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("active lifecycle txn %s not found", txnID)
	}
	return nil
}

func (t sandboxStoreTx) AbortLifecycleTxn(ctx context.Context, txnID, reason string) error {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			error = $3,
			aborted_at = NOW(),
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`, txnID, SandboxLifecyclePhaseAborted, strings.TrimSpace(reason))
	if err != nil {
		return fmt.Errorf("abort lifecycle txn: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("active lifecycle txn %s not found", txnID)
	}
	return nil
}

func (t sandboxStoreTx) UpsertSandbox(ctx context.Context, record *SandboxRecord) error {
	return upsertSandboxRecord(ctx, t.tx, record)
}

func sandboxRecordSelectSQL() string {
	return `
		SELECT sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
			cluster_id, desired_state, config, template_spec,
			runtime_id, runtime_namespace, runtime_generation, lifecycle_epoch,
			owner_kind, resource_millicpu, resource_memory_mib, hot_claim_completed_at,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		FROM manager.sandboxes`
}

func lifecycleTxnSelectSQL() string {
	return `
		SELECT txn_id, sandbox_id, kind, phase, source, cancelable, epoch,
			from_generation, to_generation,
			from_runtime_namespace, from_runtime_id,
			to_runtime_namespace, to_runtime_id,
			target_sandbox_id, target_generation_id, target_record_digest,
			source_base_artifact_digest, target_base_artifact_digest, rollback_expires_at,
			worker_cluster_id, worker_node_id, worker_node_uid, worker_proof_digest, worker_acknowledged_at,
			expected_generation_id, prepared_generation_id,
			error, cancel_reason, created_at, updated_at,
			cancel_requested_at, committed_at, aborted_at
		FROM manager.sandbox_lifecycle_txns`
}

func getActiveLifecycleTxn(ctx context.Context, exec interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, sandboxID string) (*SandboxLifecycleTxn, error) {
	return scanLifecycleTxn(exec.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE sandbox_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
		ORDER BY updated_at DESC
		LIMIT 1
	`, sandboxID))
}

type sqlExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

type sandboxRecordScanner interface {
	Scan(dest ...any) error
}

func scanSandboxRecord(row sandboxRecordScanner) (*SandboxRecord, error) {
	record, err := scanSandboxRecordInto(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return record, nil
}

func scanSandboxRecordRows(rows pgx.Rows) (*SandboxRecord, error) {
	record, err := scanSandboxRecordInto(rows)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func scanSandboxRecordInto(scanner sandboxRecordScanner) (*SandboxRecord, error) {
	var record SandboxRecord
	var configJSON, specJSON []byte
	var hotClaimCompletedAt, claimedAt, expiresAt, hardExpiresAt, deletedAt *time.Time
	if err := scanner.Scan(
		&record.ID, &record.TeamID, &record.UserID, &record.TemplateID, &record.TemplateName, &record.TemplateNamespace,
		&record.ClusterID, &record.DesiredState, &configJSON, &specJSON,
		&record.RuntimeID, &record.RuntimeNamespace, &record.RuntimeGeneration, &record.LifecycleEpoch,
		&record.OwnerKind, &record.ResourceMillicpu, &record.ResourceMemoryMiB, &hotClaimCompletedAt,
		&claimedAt, &expiresAt, &hardExpiresAt, &deletedAt, &record.CreatedAt, &record.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(configJSON, &record.Config); err != nil {
		return nil, fmt.Errorf("unmarshal sandbox config: %w", err)
	}
	if err := json.Unmarshal(specJSON, &record.TemplateSpec); err != nil {
		return nil, fmt.Errorf("unmarshal sandbox template spec: %w", err)
	}
	record.ClaimedAt = derefTime(claimedAt)
	record.HotClaimCompletedAt = derefTime(hotClaimCompletedAt)
	record.ExpiresAt = derefTime(expiresAt)
	record.HardExpiresAt = derefTime(hardExpiresAt)
	record.DeletedAt = derefTime(deletedAt)
	return &record, nil
}

func scanLifecycleTxn(row sandboxRecordScanner) (*SandboxLifecycleTxn, error) {
	txn, err := scanLifecycleTxnInto(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	return txn, nil
}

func scanLifecycleTxnRows(rows pgx.Rows) (*SandboxLifecycleTxn, error) {
	return scanLifecycleTxnInto(rows)
}

func scanLifecycleTxnInto(scanner sandboxRecordScanner) (*SandboxLifecycleTxn, error) {
	var txn SandboxLifecycleTxn
	var cancelRequestedAt, committedAt, abortedAt, rollbackExpiresAt, workerAcknowledgedAt *time.Time
	if err := scanner.Scan(
		&txn.ID, &txn.SandboxID, &txn.Kind, &txn.Phase, &txn.Source, &txn.Cancelable, &txn.Epoch,
		&txn.FromGeneration, &txn.ToGeneration,
		&txn.FromRuntimeNamespace, &txn.FromRuntimeID,
		&txn.ToRuntimeNamespace, &txn.ToRuntimeID,
		&txn.TargetSandboxID, &txn.TargetGenerationID, &txn.TargetRecordDigest,
		&txn.SourceBaseArtifactDigest, &txn.TargetBaseArtifactDigest, &rollbackExpiresAt,
		&txn.WorkerClusterID, &txn.WorkerNodeID, &txn.WorkerNodeUID, &txn.WorkerProofDigest, &workerAcknowledgedAt,
		&txn.ExpectedGenerationID, &txn.PreparedGenerationID,
		&txn.Error, &txn.CancelReason, &txn.CreatedAt, &txn.UpdatedAt,
		&cancelRequestedAt, &committedAt, &abortedAt,
	); err != nil {
		return nil, err
	}
	txn.CancelRequestedAt = derefTime(cancelRequestedAt)
	txn.CommittedAt = derefTime(committedAt)
	txn.AbortedAt = derefTime(abortedAt)
	txn.RollbackExpiresAt = derefTime(rollbackExpiresAt)
	txn.WorkerAcknowledgedAt = derefTime(workerAcknowledgedAt)
	return &txn, nil
}

// CloneSandboxLifecycleTxn returns an independent lifecycle transaction value.
func CloneSandboxLifecycleTxn(txn *SandboxLifecycleTxn) *SandboxLifecycleTxn {
	if txn == nil {
		return nil
	}
	clone := *txn
	clone.TargetRecordDigest = append([]byte(nil), txn.TargetRecordDigest...)
	clone.WorkerProofDigest = append([]byte(nil), txn.WorkerProofDigest...)
	return &clone
}

func marshalSandboxRecordJSON(record *SandboxRecord) ([]byte, []byte, error) {
	configJSON, err := json.Marshal(record.Config)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal sandbox config: %w", err)
	}
	specJSON, err := json.Marshal(record.TemplateSpec)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal sandbox template spec: %w", err)
	}
	return configJSON, specJSON, nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
