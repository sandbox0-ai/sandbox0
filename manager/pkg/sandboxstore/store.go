package sandboxstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/deletionwebhook"
	storemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/sandboxstore/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

const sandboxStoreSchemaName = "manager"

var ErrSandboxRecordNotFound = errors.New("sandbox record not found")

const (
	SandboxDesiredStateActive      = "active"
	SandboxDesiredStatePaused      = "paused"
	SandboxDesiredStateTerminating = "terminating"
	SandboxDesiredStateDeleted     = "deleted"
)

// SandboxRecord is the durable sandbox identity, desired lifecycle state, and
// configuration. Kubernetes owns observed runtime readiness and failure state.
type SandboxRecord struct {
	ID                   string
	TeamID               string
	UserID               string
	TemplateID           string
	TemplateName         string
	TemplateNamespace    string
	ClusterID            string
	DesiredState         string
	Config               SandboxConfig
	Mounts               []ClaimMount
	TemplateSpec         v1alpha1.SandboxTemplateSpec
	CurrentPodName       string
	CurrentPodNamespace  string
	RuntimeGeneration    int64
	LifecycleEpoch       int64
	WebhookStateVolumeID string
	OwnerKind            string
	HotClaimCompletedAt  time.Time
	ClaimedAt            time.Time
	ExpiresAt            time.Time
	HardExpiresAt        time.Time
	DeletedAt            time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

const (
	SandboxLifecycleKindPause    = "pause"
	SandboxLifecycleKindResume   = "resume"
	SandboxLifecycleKindFork     = "fork"
	SandboxLifecycleKindSnapshot = "snapshot"

	SandboxLifecycleSourceManual = "manual"
	SandboxLifecycleSourceAuto   = "auto"
	SandboxLifecycleSourceCrash  = "crash"
	SandboxLifecycleSourceHealth = "health"
	SandboxLifecycleSourceLost   = "lost"
	SandboxLifecycleSourceRootFS = "rootfs"

	SandboxLifecyclePhasePreparing  = "preparing"
	SandboxLifecyclePhaseBarriered  = "barriered"
	SandboxLifecyclePhasePublishing = "publishing"
	SandboxLifecyclePhaseCommitting = "committing"
	SandboxLifecyclePhaseCommitted  = "committed"
	SandboxLifecyclePhaseAborted    = "aborted"
)

// SandboxLifecycleTxn is the durable prepare/commit record for a sandbox
// runtime generation transition.
type SandboxLifecycleTxn struct {
	ID                string
	SandboxID         string
	Kind              string
	Phase             string
	Source            string
	Cancelable        bool
	Epoch             int64
	FromGeneration    int64
	ToGeneration      int64
	FromPodNamespace  string
	FromPodName       string
	ToPodNamespace    string
	ToPodName         string
	ExpectedHeadID    string
	PreparedHeadID    string
	Error             string
	CancelReason      string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	CancelRequestedAt time.Time
	CommittedAt       time.Time
	AbortedAt         time.Time
}

// SandboxRuntimeMetadata is durable metadata projected onto a runtime pod.
type SandboxRuntimeMetadata struct {
	WebhookStateVolumeID string
	OwnerKind            string
}

// SandboxRuntimeReconcileCandidate is the durable runtime projection used by
// the anti-entropy controller. Pod fields are hints only; Kubernetes remains
// authoritative for whether the referenced runtime currently exists.
type SandboxRuntimeReconcileCandidate struct {
	SandboxID         string
	DesiredState      string
	PodNamespace      string
	PodName           string
	RuntimeGeneration int64
}

// SandboxStore persists sandbox identities independently of runtime pods.
type SandboxStore interface {
	UpsertSandbox(ctx context.Context, record *SandboxRecord) error
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error)
	ListSandboxes(ctx context.Context, req *ListSandboxesRequest) ([]*SandboxRecord, error)
	ListActiveLifecycleTxns(ctx context.Context, kind string, limit int) ([]*SandboxLifecycleTxn, error)
	GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error)
	ListHardExpiredSandboxes(ctx context.Context, now time.Time, limit int) ([]*SandboxRecord, error)
	MarkSandboxDeleted(ctx context.Context, sandboxID string, deletedAt time.Time) error
	SaveRootFSHead(ctx context.Context, head *SandboxRootFSHead) error
	StageRootFSHead(ctx context.Context, head *SandboxRootFSHead) error
	GetRootFSHead(ctx context.Context, sandboxID string) (*SandboxRootFSHead, error)
	WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error
}

// SandboxStoreTx is a locked sandbox store transaction.
type SandboxStoreTx interface {
	SaveSandbox(ctx context.Context, record *SandboxRecord) error
	SaveRuntime(ctx context.Context, sandboxID, namespace, podName string, generation int64, expiresAt, hardExpiresAt time.Time, metadata SandboxRuntimeMetadata) error
	MarkHotClaimCompleted(ctx context.Context, sandboxID string, completedAt time.Time) error
	MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error
	MarkRuntimeTerminating(ctx context.Context, sandboxID string) error
	SaveRootFSHead(ctx context.Context, head *SandboxRootFSHead) error
	GetRootFSHead(ctx context.Context, sandboxID string) (*SandboxRootFSHead, error)
	GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error)
	BeginLifecycleTxn(ctx context.Context, txn *SandboxLifecycleTxn) error
	SetLifecycleTxnRuntime(ctx context.Context, txnID, namespace, podName string) error
	UpdateLifecycleTxnPhase(ctx context.Context, txnID, phase string) error
	SetLifecycleTxnPreparedHead(ctx context.Context, txnID, preparedHeadID string) error
	RequestLifecycleTxnCancel(ctx context.Context, txnID, reason string) (bool, error)
	CommitLifecycleTxn(ctx context.Context, txnID, preparedHeadID string) error
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

type sandboxStoreExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func upsertSandboxRecord(ctx context.Context, exec sandboxStoreExecutor, record *SandboxRecord) error {
	if exec == nil || record == nil {
		return nil
	}
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	configJSON, mountsJSON, specJSON, err := marshalSandboxRecordJSON(record)
	if err != nil {
		return err
	}
	_, err = exec.Exec(ctx, `
		INSERT INTO manager.sandboxes (
			sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
			cluster_id, desired_state, config, mounts, template_spec,
			current_pod_name, current_pod_namespace, runtime_generation, lifecycle_epoch,
			webhook_state_volume_id, owner_kind, hot_claim_completed_at,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, COALESCE($23, NOW()), NOW())
		ON CONFLICT (sandbox_id) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			template_id = EXCLUDED.template_id,
			template_name = EXCLUDED.template_name,
			template_namespace = EXCLUDED.template_namespace,
			cluster_id = EXCLUDED.cluster_id,
			desired_state = EXCLUDED.desired_state,
			config = EXCLUDED.config,
			mounts = EXCLUDED.mounts,
			template_spec = EXCLUDED.template_spec,
			current_pod_name = EXCLUDED.current_pod_name,
			current_pod_namespace = EXCLUDED.current_pod_namespace,
			runtime_generation = EXCLUDED.runtime_generation,
			lifecycle_epoch = GREATEST(manager.sandboxes.lifecycle_epoch, EXCLUDED.lifecycle_epoch),
			webhook_state_volume_id = EXCLUDED.webhook_state_volume_id,
			owner_kind = EXCLUDED.owner_kind,
			hot_claim_completed_at = COALESCE(EXCLUDED.hot_claim_completed_at, manager.sandboxes.hot_claim_completed_at),
			claimed_at = EXCLUDED.claimed_at,
			expires_at = EXCLUDED.expires_at,
			hard_expires_at = EXCLUDED.hard_expires_at,
			deleted_at = EXCLUDED.deleted_at,
			updated_at = NOW()
		WHERE manager.sandboxes.deleted_at IS NULL
			AND manager.sandboxes.desired_state NOT IN ($24, $25)
	`, record.ID, record.TeamID, record.UserID, record.TemplateID, record.TemplateName, record.TemplateNamespace,
		record.ClusterID, record.DesiredState, configJSON, mountsJSON, specJSON,
		record.CurrentPodName, record.CurrentPodNamespace, record.RuntimeGeneration, record.LifecycleEpoch,
		strings.TrimSpace(record.WebhookStateVolumeID), strings.TrimSpace(record.OwnerKind), nullableTime(record.HotClaimCompletedAt),
		nullableTime(record.ClaimedAt), nullableTime(record.ExpiresAt), nullableTime(record.HardExpiresAt), nullableTime(record.DeletedAt), nullableTime(record.CreatedAt),
		SandboxDesiredStateTerminating, SandboxDesiredStateDeleted)
	if err != nil {
		return fmt.Errorf("upsert sandbox: %w", err)
	}
	return nil
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
	// Public status is projected from the cached Pod after this query, so only
	// durable filters belong in SQL.
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

// ListPendingRuntimeRecoverySandboxIDs returns paused sandboxes whose latest
// committed lifecycle transition requires automatic runtime reconstruction.
func (s *PGSandboxStore) ListPendingRuntimeRecoverySandboxIDs(ctx context.Context, limit int) ([]string, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT s.sandbox_id
		FROM manager.sandboxes s
		JOIN LATERAL (
			SELECT kind, source
			FROM manager.sandbox_lifecycle_txns
			WHERE sandbox_id = s.sandbox_id
				AND phase = $1
			ORDER BY epoch DESC
			LIMIT 1
		) latest ON TRUE
			WHERE s.deleted_at IS NULL
				AND s.desired_state = $2
				AND latest.kind = $3
				AND latest.source IN ($4, $5, $6, $7)
			ORDER BY s.updated_at ASC
			LIMIT $8
		`,
		SandboxLifecyclePhaseCommitted,
		SandboxDesiredStatePaused,
		SandboxLifecycleKindPause,
		SandboxLifecycleSourceCrash,
		SandboxLifecycleSourceHealth,
		SandboxLifecycleSourceLost,
		SandboxLifecycleSourceRootFS,
		limit,
	)
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

// ListRuntimeReconcileCandidates returns a stable page of sandboxes whose
// durable state expects either an active runtime or completion of deletion.
// The controller compares these projections with its synced Pod cache before
// performing a strong Kubernetes API read for suspected mismatches.
func (s *PGSandboxStore) ListRuntimeReconcileCandidates(ctx context.Context, clusterID, afterSandboxID string, limit int) ([]SandboxRuntimeReconcileCandidate, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, `
		SELECT sandbox_id, desired_state, current_pod_namespace, current_pod_name, runtime_generation
		FROM manager.sandboxes
		WHERE deleted_at IS NULL
			AND (
				desired_state IN ($1, $2)
				OR EXISTS (
					SELECT 1
					FROM manager.sandbox_lifecycle_txns txn
					WHERE txn.sandbox_id = manager.sandboxes.sandbox_id
						AND txn.kind = $3
						AND txn.phase IN ('preparing', 'barriered', 'publishing', 'committing')
				)
			)
			AND cluster_id = $4
			AND sandbox_id > $5
		ORDER BY sandbox_id ASC
		LIMIT $6
	`, SandboxDesiredStateActive, SandboxDesiredStateTerminating, SandboxLifecycleKindResume, strings.TrimSpace(clusterID), strings.TrimSpace(afterSandboxID), limit)
	if err != nil {
		return nil, fmt.Errorf("list sandbox runtime reconcile candidates: %w", err)
	}
	defer rows.Close()
	candidates := make([]SandboxRuntimeReconcileCandidate, 0, limit)
	for rows.Next() {
		var candidate SandboxRuntimeReconcileCandidate
		if err := rows.Scan(
			&candidate.SandboxID,
			&candidate.DesiredState,
			&candidate.PodNamespace,
			&candidate.PodName,
			&candidate.RuntimeGeneration,
		); err != nil {
			return nil, fmt.Errorf("scan sandbox runtime reconcile candidate: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sandbox runtime reconcile candidates: %w", err)
	}
	return candidates, nil
}

func (s *PGSandboxStore) GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	return getActiveLifecycleTxn(ctx, s.pool, sandboxID)
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
	if err == nil && currentDesiredState == SandboxDesiredStateTerminating && strings.TrimSpace(webhookURL) != "" {
		if err := deletionwebhook.Enqueue(ctx, tx, sandboxID, teamID, webhookURL, webhookSecret, deletedAt); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			current_pod_name = '',
			current_pod_namespace = '',
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
	var filesystemID string
	err = tx.QueryRow(ctx, `
		DELETE FROM manager.sandbox_rootfs_bindings
		WHERE sandbox_id = $1
		RETURNING filesystem_id
	`, sandboxID).Scan(&filesystemID)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("delete sandbox rootfs binding: %w", err)
	}
	if filesystemID != "" {
		if _, err := tx.Exec(ctx, `
		DELETE FROM manager.rootfs_filesystems f
		WHERE f.filesystem_id = $1
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
		`, filesystemID); err != nil {
			return fmt.Errorf("delete unreferenced sandbox rootfs filesystem: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit mark sandbox deleted tx: %w", err)
	}
	return nil
}

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

func (t sandboxStoreTx) SaveRuntime(ctx context.Context, sandboxID, namespace, podName string, generation int64, expiresAt, hardExpiresAt time.Time, metadata SandboxRuntimeMetadata) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			current_pod_namespace = $3,
			current_pod_name = $4,
			runtime_generation = $5,
			expires_at = $6,
			hard_expires_at = $7,
			webhook_state_volume_id = COALESCE(NULLIF($8, ''), webhook_state_volume_id),
			owner_kind = COALESCE(NULLIF($9, ''), owner_kind),
			deleted_at = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
			AND desired_state NOT IN ($10, $11)
	`, sandboxID, SandboxDesiredStateActive, namespace, podName, generation, nullableTime(expiresAt), nullableTime(hardExpiresAt), strings.TrimSpace(metadata.WebhookStateVolumeID), strings.TrimSpace(metadata.OwnerKind), SandboxDesiredStateTerminating, SandboxDesiredStateDeleted)
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
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET desired_state = $2,
			current_pod_namespace = '',
			current_pod_name = '',
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
			from_pod_namespace, from_pod_name,
			to_pod_namespace, to_pod_name,
			expected_head_id_v3, prepared_head_id_v3,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW(), NOW())
	`, txn.ID, txn.SandboxID, txn.Kind, phase, source, txn.Cancelable, txn.Epoch,
		txn.FromGeneration, txn.ToGeneration,
		txn.FromPodNamespace, txn.FromPodName,
		txn.ToPodNamespace, txn.ToPodName,
		txn.ExpectedHeadID, txn.PreparedHeadID)
	if err != nil {
		return fmt.Errorf("begin lifecycle txn: %w", err)
	}
	txn.Phase = phase
	txn.Source = source
	return nil
}

func (t sandboxStoreTx) SetLifecycleTxnRuntime(ctx context.Context, txnID, namespace, podName string) error {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET to_pod_namespace = $2,
			to_pod_name = $3,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`, txnID, strings.TrimSpace(namespace), strings.TrimSpace(podName))
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

func (t sandboxStoreTx) SetLifecycleTxnPreparedHead(ctx context.Context, txnID, preparedHeadID string) error {
	txnID = strings.TrimSpace(txnID)
	preparedHeadID = strings.TrimSpace(preparedHeadID)
	if txnID == "" || preparedHeadID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET prepared_head_id_v3 = $2,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, preparedHeadID)
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

func (t sandboxStoreTx) CommitLifecycleTxn(ctx context.Context, txnID, preparedHeadID string) error {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			prepared_head_id_v3 = $3,
			committed_at = NOW(),
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, SandboxLifecyclePhaseCommitted, strings.TrimSpace(preparedHeadID))
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
			cluster_id, desired_state, config, mounts, template_spec,
			current_pod_name, current_pod_namespace, runtime_generation, lifecycle_epoch,
			webhook_state_volume_id, owner_kind, hot_claim_completed_at,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		FROM manager.sandboxes`
}

func lifecycleTxnSelectSQL() string {
	return `
		SELECT txn_id, sandbox_id, kind, phase, source, cancelable, epoch,
			from_generation, to_generation,
			from_pod_namespace, from_pod_name,
			to_pod_namespace, to_pod_name,
			expected_head_id_v3, prepared_head_id_v3,
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
	var configJSON, mountsJSON, specJSON []byte
	var hotClaimCompletedAt, claimedAt, expiresAt, hardExpiresAt, deletedAt *time.Time
	if err := scanner.Scan(
		&record.ID, &record.TeamID, &record.UserID, &record.TemplateID, &record.TemplateName, &record.TemplateNamespace,
		&record.ClusterID, &record.DesiredState, &configJSON, &mountsJSON, &specJSON,
		&record.CurrentPodName, &record.CurrentPodNamespace, &record.RuntimeGeneration, &record.LifecycleEpoch,
		&record.WebhookStateVolumeID, &record.OwnerKind, &hotClaimCompletedAt,
		&claimedAt, &expiresAt, &hardExpiresAt, &deletedAt, &record.CreatedAt, &record.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(configJSON, &record.Config); err != nil {
		return nil, fmt.Errorf("unmarshal sandbox config: %w", err)
	}
	if err := json.Unmarshal(mountsJSON, &record.Mounts); err != nil {
		return nil, fmt.Errorf("unmarshal sandbox mounts: %w", err)
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
	var cancelRequestedAt, committedAt, abortedAt *time.Time
	if err := scanner.Scan(
		&txn.ID, &txn.SandboxID, &txn.Kind, &txn.Phase, &txn.Source, &txn.Cancelable, &txn.Epoch,
		&txn.FromGeneration, &txn.ToGeneration,
		&txn.FromPodNamespace, &txn.FromPodName,
		&txn.ToPodNamespace, &txn.ToPodName,
		&txn.ExpectedHeadID, &txn.PreparedHeadID,
		&txn.Error, &txn.CancelReason, &txn.CreatedAt, &txn.UpdatedAt,
		&cancelRequestedAt, &committedAt, &abortedAt,
	); err != nil {
		return nil, err
	}
	txn.CancelRequestedAt = derefTime(cancelRequestedAt)
	txn.CommittedAt = derefTime(committedAt)
	txn.AbortedAt = derefTime(abortedAt)
	return &txn, nil
}

// CloneSandboxLifecycleTxn returns an independent lifecycle transaction value.
func CloneSandboxLifecycleTxn(txn *SandboxLifecycleTxn) *SandboxLifecycleTxn {
	if txn == nil {
		return nil
	}
	clone := *txn
	return &clone
}

func marshalSandboxRecordJSON(record *SandboxRecord) ([]byte, []byte, []byte, error) {
	configJSON, err := json.Marshal(record.Config)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal sandbox config: %w", err)
	}
	mountsJSON, err := json.Marshal(record.Mounts)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal sandbox mounts: %w", err)
	}
	specJSON, err := json.Marshal(record.TemplateSpec)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("marshal sandbox template spec: %w", err)
	}
	return configJSON, mountsJSON, specJSON, nil
}

func nullableTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nullableText(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
