package service

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
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	servicemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/service/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

const sandboxStoreSchemaName = "manager"

var ErrSandboxRecordNotFound = errors.New("sandbox record not found")

const (
	SandboxStatusDeleted = "deleted"
)

// SandboxRecord is the durable sandbox identity and configuration.
type SandboxRecord struct {
	ID                   string
	TeamID               string
	UserID               string
	TemplateID           string
	TemplateName         string
	TemplateNamespace    string
	ClusterID            string
	Status               string
	Config               SandboxConfig
	Mounts               []ClaimMount
	TemplateSpec         v1alpha1.SandboxTemplateSpec
	CurrentPodName       string
	CurrentPodNamespace  string
	RuntimeGeneration    int64
	LifecycleEpoch       int64
	WebhookStateVolumeID string
	OwnerKind            string
	ClaimedAt            time.Time
	ExpiresAt            time.Time
	HardExpiresAt        time.Time
	DeletedAt            time.Time
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

// SandboxRootFSState is manager-internal metadata for one persisted sandbox
// writable rootfs diff.
type SandboxRootFSState struct {
	LayerID       string
	ParentLayerID string
	// ExpectedHeadLayerID overrides ParentLayerID as the head CAS precondition.
	ExpectedHeadLayerID  string
	SandboxID            string
	TeamID               string
	RuntimeGeneration    int64
	Runtime              string
	RuntimeHandler       string
	BaseImageRef         string
	BaseImageDigest      string
	PlatformOS           string
	PlatformArchitecture string
	PlatformVariant      string
	Snapshotter          string
	SnapshotParent       string
	SnapshotParentChain  []string
	DiffDigest           string
	DiffID               string
	DiffMediaType        string
	DiffSize             int64
	DiffObjectKey        string
	HeadObjectDigest     string
	HeadObjectMediaType  string
	HeadObjectSize       int64
	HeadObjectKey        string
	HeadImageRef         string
	HeadImageDigest      string
	Objects              []rootfshead.Object
	CreatedAt            time.Time
	UpdatedAt            time.Time
	LayerChain           []*SandboxRootFSLayer
}

// SandboxRootFSLayer is one immutable OCI diff layer in a sandbox rootfs chain.
type SandboxRootFSLayer struct {
	ID                   string
	ParentLayerID        string
	SourceSandboxID      string
	TeamID               string
	RuntimeGeneration    int64
	Runtime              string
	RuntimeHandler       string
	BaseImageRef         string
	BaseImageDigest      string
	PlatformOS           string
	PlatformArchitecture string
	PlatformVariant      string
	Snapshotter          string
	SnapshotParent       string
	SnapshotParentChain  []string
	DiffDigest           string
	DiffID               string
	DiffMediaType        string
	DiffSize             int64
	DiffObjectKey        string
	HeadObjectDigest     string
	HeadObjectMediaType  string
	HeadObjectSize       int64
	HeadObjectKey        string
	HeadImageRef         string
	HeadImageDigest      string
	CreatedAt            time.Time
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
	ID                  string
	SandboxID           string
	Kind                string
	Phase               string
	Source              string
	Cancelable          bool
	Epoch               int64
	FromGeneration      int64
	ToGeneration        int64
	FromPodNamespace    string
	FromPodName         string
	ToPodNamespace      string
	ToPodName           string
	ExpectedHeadLayerID string
	PreparedHeadLayerID string
	Error               string
	CancelReason        string
	CreatedAt           time.Time
	UpdatedAt           time.Time
	CancelRequestedAt   time.Time
	CommittedAt         time.Time
	AbortedAt           time.Time
}

// SandboxRuntimeMetadata is durable metadata projected onto a runtime pod.
type SandboxRuntimeMetadata struct {
	WebhookStateVolumeID string
	OwnerKind            string
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
	SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error
	GetLatestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error)
	WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error
}

// SandboxStoreTx is a locked sandbox store transaction.
type SandboxStoreTx interface {
	SaveRuntime(ctx context.Context, sandboxID, namespace, podName, status string, generation int64, expiresAt, hardExpiresAt time.Time, metadata SandboxRuntimeMetadata) error
	MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error
	SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error
	GetActiveLifecycleTxn(ctx context.Context, sandboxID string) (*SandboxLifecycleTxn, error)
	BeginLifecycleTxn(ctx context.Context, txn *SandboxLifecycleTxn) error
	SetLifecycleTxnRuntime(ctx context.Context, txnID, namespace, podName string) error
	UpdateLifecycleTxnPhase(ctx context.Context, txnID, phase string) error
	SetLifecycleTxnPreparedHead(ctx context.Context, txnID, preparedHeadLayerID string) error
	RequestLifecycleTxnCancel(ctx context.Context, txnID, reason string) (bool, error)
	CommitLifecycleTxn(ctx context.Context, txnID, preparedHeadLayerID string) error
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
		migrate.WithBaseFS(servicemigrations.FS),
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

func upsertSandboxRecord(ctx context.Context, exec rootFSStateExecutor, record *SandboxRecord) error {
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
			cluster_id, status, config, mounts, template_spec,
			current_pod_name, current_pod_namespace, runtime_generation, lifecycle_epoch,
			webhook_state_volume_id, owner_kind,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, COALESCE($22, NOW()), NOW())
		ON CONFLICT (sandbox_id) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			template_id = EXCLUDED.template_id,
			template_name = EXCLUDED.template_name,
			template_namespace = EXCLUDED.template_namespace,
			cluster_id = EXCLUDED.cluster_id,
			status = EXCLUDED.status,
			config = EXCLUDED.config,
			mounts = EXCLUDED.mounts,
			template_spec = EXCLUDED.template_spec,
			current_pod_name = EXCLUDED.current_pod_name,
			current_pod_namespace = EXCLUDED.current_pod_namespace,
			runtime_generation = EXCLUDED.runtime_generation,
			lifecycle_epoch = GREATEST(manager.sandboxes.lifecycle_epoch, EXCLUDED.lifecycle_epoch),
			webhook_state_volume_id = EXCLUDED.webhook_state_volume_id,
			owner_kind = EXCLUDED.owner_kind,
			claimed_at = EXCLUDED.claimed_at,
			expires_at = EXCLUDED.expires_at,
			hard_expires_at = EXCLUDED.hard_expires_at,
			deleted_at = EXCLUDED.deleted_at,
			updated_at = NOW()
	`, record.ID, record.TeamID, record.UserID, record.TemplateID, record.TemplateName, record.TemplateNamespace,
		record.ClusterID, record.Status, configJSON, mountsJSON, specJSON,
		record.CurrentPodName, record.CurrentPodNamespace, record.RuntimeGeneration, record.LifecycleEpoch,
		strings.TrimSpace(record.WebhookStateVolumeID), strings.TrimSpace(record.OwnerKind),
		nullableTime(record.ClaimedAt), nullableTime(record.ExpiresAt), nullableTime(record.HardExpiresAt), nullableTime(record.DeletedAt), nullableTime(record.CreatedAt))
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
	rows, err := s.pool.Query(ctx, sandboxRecordSelectSQL()+`
		WHERE team_id = $1
			AND deleted_at IS NULL
			AND ($2 = '' OR status = $2)
			AND ($3 = '' OR template_id = $3)
		ORDER BY created_at DESC
	`, req.TeamID, req.Status, req.TemplateID)
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
			AND status IN ($2, $3)
	`, strings.TrimSpace(teamID), SandboxStatusStarting, SandboxStatusRunning).Scan(&total); err != nil {
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
				AND s.status = $2
				AND latest.kind = $3
				AND latest.source IN ($4, $5)
			ORDER BY s.updated_at ASC
			LIMIT $6
		`,
		SandboxLifecyclePhaseCommitted,
		SandboxStatusPaused,
		SandboxLifecycleKindPause,
		SandboxLifecycleSourceCrash,
		SandboxLifecycleSourceHealth,
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
	if _, err := tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET status = $2,
			current_pod_name = '',
			current_pod_namespace = '',
			deleted_at = $3,
			updated_at = NOW()
		WHERE sandbox_id = $1
	`, sandboxID, SandboxStatusDeleted, deletedAt); err != nil {
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
	if _, err := tx.Exec(ctx, `
		WITH removed AS (
			DELETE FROM manager.sandbox_rootfs_bindings
			WHERE sandbox_id = $1
			RETURNING filesystem_id
		)
		DELETE FROM manager.rootfs_filesystems f
		USING removed r
		WHERE f.filesystem_id = r.filesystem_id
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
	`, sandboxID); err != nil {
		return fmt.Errorf("delete sandbox rootfs binding: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM manager.sandbox_rootfs_states WHERE sandbox_id = $1`, sandboxID); err != nil {
		return fmt.Errorf("delete sandbox rootfs states: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM manager.sandbox_rootfs_heads WHERE sandbox_id = $1`, sandboxID); err != nil {
		return fmt.Errorf("delete sandbox rootfs head: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit mark sandbox deleted tx: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error {
	if s == nil || s.pool == nil || state == nil {
		return nil
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin rootfs state tx: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := saveRootFSState(ctx, tx, state); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit rootfs state tx: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) GetLatestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	chain, err := s.GetRootFSLayerChain(ctx, sandboxID)
	if err != nil {
		return nil, err
	}
	if len(chain) > 0 {
		return rootFSStateFromLayerChain(sandboxID, chain), nil
	}
	return nil, nil
}

func (s *PGSandboxStore) GetRootFSLayerChain(ctx context.Context, sandboxID string) ([]*SandboxRootFSLayer, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(sandboxID) == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, rootFSLayerChainSQL(), sandboxID)
	if err != nil {
		return nil, fmt.Errorf("get rootfs layer chain: %w", err)
	}
	return scanRootFSLayerChain(rows)
}

// GetRootFSLayerChainByHead returns the immutable ancestor chain ending at
// headLayerID. It is used by point-in-time products that must not follow a
// sandbox head after the source sandbox continues running.
func (s *PGSandboxStore) GetRootFSLayerChainByHead(ctx context.Context, teamID, headLayerID string) ([]*SandboxRootFSLayer, error) {
	if s == nil || s.pool == nil || strings.TrimSpace(headLayerID) == "" {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, rootFSLayerChainByHeadSQL(), strings.TrimSpace(headLayerID), strings.TrimSpace(teamID))
	if err != nil {
		return nil, fmt.Errorf("get rootfs layer chain by head: %w", err)
	}
	return scanRootFSLayerChain(rows)
}

func scanRootFSLayerChain(rows pgx.Rows) ([]*SandboxRootFSLayer, error) {
	defer rows.Close()
	var layers []*SandboxRootFSLayer
	for rows.Next() {
		layer, err := scanRootFSLayerRows(rows)
		if err != nil {
			return nil, err
		}
		layers = append(layers, layer)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate rootfs layer chain: %w", err)
	}
	return layers, nil
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

func (t sandboxStoreTx) SaveRuntime(ctx context.Context, sandboxID, namespace, podName, status string, generation int64, expiresAt, hardExpiresAt time.Time, metadata SandboxRuntimeMetadata) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET status = $2,
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
	`, sandboxID, status, namespace, podName, generation, nullableTime(expiresAt), nullableTime(hardExpiresAt), strings.TrimSpace(metadata.WebhookStateVolumeID), strings.TrimSpace(metadata.OwnerKind))
	if err != nil {
		return fmt.Errorf("save sandbox runtime: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	return nil
}

func (t sandboxStoreTx) MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET status = $2,
			current_pod_namespace = '',
			current_pod_name = '',
			runtime_generation = GREATEST(runtime_generation, $3),
			expires_at = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1
			AND deleted_at IS NULL
	`, sandboxID, SandboxStatusPaused, generation)
	if err != nil {
		return fmt.Errorf("mark sandbox runtime paused: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrSandboxRecordNotFound, sandboxID)
	}
	return nil
}

func (t sandboxStoreTx) SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error {
	return saveRootFSState(ctx, t.tx, state)
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
			expected_head_layer_id, prepared_head_layer_id,
			created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NOW(), NOW())
	`, txn.ID, txn.SandboxID, txn.Kind, phase, source, txn.Cancelable, txn.Epoch,
		txn.FromGeneration, txn.ToGeneration,
		txn.FromPodNamespace, txn.FromPodName,
		txn.ToPodNamespace, txn.ToPodName,
		txn.ExpectedHeadLayerID, txn.PreparedHeadLayerID)
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

func (t sandboxStoreTx) SetLifecycleTxnPreparedHead(ctx context.Context, txnID, preparedHeadLayerID string) error {
	txnID = strings.TrimSpace(txnID)
	preparedHeadLayerID = strings.TrimSpace(preparedHeadLayerID)
	if txnID == "" || preparedHeadLayerID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET prepared_head_layer_id = $2,
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, preparedHeadLayerID)
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

func (t sandboxStoreTx) CommitLifecycleTxn(ctx context.Context, txnID, preparedHeadLayerID string) error {
	txnID = strings.TrimSpace(txnID)
	if txnID == "" {
		return nil
	}
	tag, err := t.tx.Exec(ctx, `
		UPDATE manager.sandbox_lifecycle_txns
		SET phase = $2,
			prepared_head_layer_id = $3,
			committed_at = NOW(),
			updated_at = NOW()
		WHERE txn_id = $1
			AND phase IN ('preparing', 'barriered', 'publishing', 'committing')
			AND cancel_requested_at IS NULL
	`, txnID, SandboxLifecyclePhaseCommitted, strings.TrimSpace(preparedHeadLayerID))
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
			cluster_id, status, config, mounts, template_spec,
			current_pod_name, current_pod_namespace, runtime_generation, lifecycle_epoch,
			webhook_state_volume_id, owner_kind,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		FROM manager.sandboxes`
}

func lifecycleTxnSelectSQL() string {
	return `
		SELECT txn_id, sandbox_id, kind, phase, source, cancelable, epoch,
			from_generation, to_generation,
			from_pod_namespace, from_pod_name,
			to_pod_namespace, to_pod_name,
			expected_head_layer_id, prepared_head_layer_id,
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

type rootFSStateExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func saveRootFSState(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState) error {
	if exec == nil || state == nil {
		return nil
	}
	if err := validateRootFSState(state); err != nil {
		return err
	}
	if err := saveRootFSLayer(ctx, exec, state); err != nil {
		return err
	}
	return advanceSandboxRootFSFilesystemHead(ctx, exec, state)
}

func validateRootFSState(state *SandboxRootFSState) error {
	if state == nil {
		return nil
	}
	if strings.TrimSpace(state.SandboxID) == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(state.TeamID) == "" {
		return fmt.Errorf("team_id is required")
	}
	if strings.TrimSpace(state.LayerID) == "" {
		return fmt.Errorf("layer_id is required")
	}
	headObjectFields := 0
	if strings.TrimSpace(state.HeadObjectDigest) != "" {
		headObjectFields++
	}
	if strings.TrimSpace(state.HeadObjectMediaType) != "" {
		headObjectFields++
	}
	if state.HeadObjectSize > 0 {
		headObjectFields++
	}
	if strings.TrimSpace(state.HeadObjectKey) != "" {
		headObjectFields++
	}
	if headObjectFields != 0 && headObjectFields != 4 {
		return fmt.Errorf("rootfs head object metadata must be complete")
	}
	if headObjectFields == 4 {
		if _, err := rootFSHeadReferenceFromState(state); err != nil {
			return err
		}
		if _, err := rootFSStateObjects(state, true); err != nil {
			return err
		}
		return nil
	}
	if strings.TrimSpace(state.DiffDigest) == "" {
		return fmt.Errorf("legacy rootfs diff_digest is required")
	}
	if strings.TrimSpace(state.DiffObjectKey) == "" {
		return fmt.Errorf("legacy rootfs diff_object_key is required")
	}
	return nil
}

func saveRootFSLayer(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState) error {
	if exec == nil || state == nil {
		return nil
	}
	if strings.TrimSpace(state.LayerID) == "" {
		return fmt.Errorf("layer_id is required")
	}
	if strings.TrimSpace(state.ParentLayerID) == strings.TrimSpace(state.LayerID) {
		return fmt.Errorf("parent_layer_id cannot reference layer_id")
	}
	parentLayerID := nullableText(state.ParentLayerID)
	parentChainJSON, err := json.Marshal(state.SnapshotParentChain)
	if err != nil {
		return fmt.Errorf("marshal rootfs layer snapshot parent chain: %w", err)
	}
	_, err = exec.Exec(ctx, `
		INSERT INTO manager.rootfs_layers (
			layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
				runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
				snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
				diff_size, diff_object_key, head_object_digest, head_object_media_type,
				head_object_size, head_object_key, head_image_ref, head_image_digest, platform_os,
				platform_architecture, platform_variant, created_at
			)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, COALESCE($27, NOW()))
		ON CONFLICT (layer_id) DO NOTHING
	`, state.LayerID, parentLayerID, state.SandboxID, state.TeamID, state.RuntimeGeneration,
		state.Runtime, state.RuntimeHandler, state.BaseImageRef, state.BaseImageDigest, state.Snapshotter,
		state.SnapshotParent, parentChainJSON, state.DiffDigest, state.DiffID, state.DiffMediaType,
		state.DiffSize, state.DiffObjectKey, state.HeadObjectDigest, state.HeadObjectMediaType,
		state.HeadObjectSize, state.HeadObjectKey, state.HeadImageRef, state.HeadImageDigest, state.PlatformOS,
		state.PlatformArchitecture, state.PlatformVariant, nullableTime(state.CreatedAt))
	if err != nil {
		return fmt.Errorf("save rootfs layer: %w", err)
	}
	return saveRootFSObject(ctx, exec, state)
}

func saveRootFSObject(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState) error {
	if exec == nil || state == nil {
		return nil
	}
	objects, err := rootFSStateObjects(state, true)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return nil
	}
	objectsJSON, err := json.Marshal(objects)
	if err != nil {
		return fmt.Errorf("marshal rootfs object inventory: %w", err)
	}
	var inputCount int64
	var upsertedCount int64
	err = exec.QueryRow(ctx, `
		WITH input AS MATERIALIZED (
			SELECT object.key AS object_key,
				object.digest AS object_digest,
				object.media_type,
				object.size AS object_size
			FROM jsonb_to_recordset($1::jsonb) AS object(
				key TEXT,
				digest TEXT,
				size BIGINT,
				media_type TEXT
			)
		),
		upserted AS (
			INSERT INTO manager.rootfs_objects (
				object_key, team_id, diff_digest, diff_media_type, diff_size,
				first_layer_id, last_referenced_at, missing_at, deleted_at,
				last_error, created_at, updated_at
			)
			SELECT object_key, $2, object_digest, media_type, object_size,
				$3, COALESCE($4, NOW()), NULL, NULL,
				'', COALESCE($4, NOW()), NOW()
			FROM input
			ON CONFLICT (object_key) DO UPDATE SET
				first_layer_id = CASE
					WHEN EXISTS (
						SELECT 1 FROM manager.rootfs_layers owner
						WHERE owner.layer_id = manager.rootfs_objects.first_layer_id
					) THEN manager.rootfs_objects.first_layer_id
					ELSE EXCLUDED.first_layer_id
				END,
				last_referenced_at = NOW(),
				missing_at = NULL,
				deleted_at = NULL,
				last_error = '',
				updated_at = NOW()
			WHERE manager.rootfs_objects.team_id = EXCLUDED.team_id
				AND manager.rootfs_objects.diff_digest = EXCLUDED.diff_digest
				AND manager.rootfs_objects.diff_media_type = EXCLUDED.diff_media_type
				AND manager.rootfs_objects.diff_size = EXCLUDED.diff_size
			RETURNING object_key
		),
		linked AS (
			INSERT INTO manager.rootfs_layer_objects (layer_id, object_key, created_at)
			SELECT $3, object_key, COALESCE($4, NOW())
			FROM upserted
			ON CONFLICT (layer_id, object_key) DO NOTHING
		),
		cleared AS (
			DELETE FROM manager.rootfs_object_deletions deletion
			USING upserted
			WHERE deletion.object_key = upserted.object_key
		)
		SELECT
			(SELECT COUNT(*) FROM input),
			(SELECT COUNT(*) FROM upserted)
	`, objectsJSON, state.TeamID, state.LayerID, nullableTime(state.CreatedAt)).Scan(&inputCount, &upsertedCount)
	if err != nil {
		return fmt.Errorf("save rootfs object inventory: %w", err)
	}
	if inputCount != int64(len(objects)) || upsertedCount != inputCount {
		return fmt.Errorf("%w: accepted %d of %d objects", ErrRootFSObjectConflict, upsertedCount, inputCount)
	}
	return nil
}

func rootFSStateObjects(state *SandboxRootFSState, includeMarker bool) ([]rootfshead.Object, error) {
	if state == nil {
		return nil, nil
	}
	objects := append([]rootfshead.Object(nil), state.Objects...)
	objects = append(objects,
		rootfshead.Object{Key: state.DiffObjectKey, Digest: state.DiffDigest, MediaType: state.DiffMediaType, Size: state.DiffSize},
		rootfshead.Object{Key: state.HeadObjectKey, Digest: state.HeadObjectDigest, MediaType: state.HeadObjectMediaType, Size: state.HeadObjectSize},
	)
	if includeMarker && strings.TrimSpace(state.HeadObjectKey) != "" {
		marker, err := rootFSHeadMarkerObjectFromState(state)
		if err != nil {
			return nil, fmt.Errorf("resolve rootfs head marker object: %w", err)
		}
		objects = append(objects, marker)
	}
	seen := make(map[string]struct{}, len(objects))
	result := make([]rootfshead.Object, 0, len(objects))
	for _, object := range objects {
		if strings.TrimSpace(object.Key) == "" {
			continue
		}
		if !supportedRootFSObjectMediaType(object.MediaType) {
			return nil, fmt.Errorf("invalid rootfs object %s: unsupported media type %q", object.Key, object.MediaType)
		}
		if err := object.Validate(object.MediaType); err != nil {
			return nil, fmt.Errorf("invalid rootfs object %s: %w", object.Key, err)
		}
		if _, ok := seen[object.Key]; ok {
			continue
		}
		seen[object.Key] = struct{}{}
		result = append(result, object)
	}
	return result, nil
}

func supportedRootFSObjectMediaType(mediaType string) bool {
	switch strings.TrimSpace(mediaType) {
	case rootfshead.HeadMediaType,
		rootfshead.DirectoryIndexMediaType,
		rootfshead.DirectoryShardMediaType,
		rootfshead.FileMediaType,
		rootfshead.ChunkMediaType,
		rootfshead.MarkerMediaType,
		rootfshead.ImageEnvelopeMediaType,
		ocispec.MediaTypeImageLayer,
		ocispec.MediaTypeImageLayerGzip,
		ocispec.MediaTypeImageLayerZstd,
		ocispec.MediaTypeImageLayerNonDistributable,
		ocispec.MediaTypeImageLayerNonDistributableGzip,
		ocispec.MediaTypeImageLayerNonDistributableZstd:
		return true
	default:
		return false
	}
}

func advanceSandboxRootFSFilesystemHead(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState) error {
	expectedHeadLayerID := state.ParentLayerID
	if strings.TrimSpace(state.ExpectedHeadLayerID) != "" {
		expectedHeadLayerID = state.ExpectedHeadLayerID
	}
	return advanceRootFSFilesystemHead(ctx, exec, state, nullableText(expectedHeadLayerID))
}

func advanceRootFSFilesystemHead(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState, expectedHeadLayerID any) error {
	if exec == nil || state == nil {
		return nil
	}
	tag, err := exec.Exec(ctx, `
		WITH binding AS (
			SELECT filesystem_id
			FROM manager.sandbox_rootfs_bindings
			WHERE sandbox_id = $1
			UNION ALL
			SELECT $1
			WHERE NOT EXISTS (
				SELECT 1
				FROM manager.sandbox_rootfs_bindings
				WHERE sandbox_id = $1
			)
			LIMIT 1
		),
		advanced AS (
			INSERT INTO manager.rootfs_filesystems (
				filesystem_id, team_id, head_layer_id, base_image_ref,
				base_image_digest, created_at, updated_at
			)
			SELECT
				binding.filesystem_id,
				$2,
				$3,
				$5,
				$6,
				COALESCE($7, NOW()),
				NOW()
			FROM binding
			WHERE $4::text IS NULL OR EXISTS (
				SELECT 1
				FROM manager.rootfs_filesystems current
				WHERE current.filesystem_id = binding.filesystem_id
					AND current.head_layer_id IS NOT DISTINCT FROM $4
			)
			ON CONFLICT (filesystem_id) DO UPDATE SET
				team_id = EXCLUDED.team_id,
				head_layer_id = EXCLUDED.head_layer_id,
				base_image_ref = EXCLUDED.base_image_ref,
				base_image_digest = EXCLUDED.base_image_digest,
				updated_at = NOW()
			WHERE manager.rootfs_filesystems.head_layer_id IS NOT DISTINCT FROM $4
			RETURNING filesystem_id
		),
		ensured_binding AS (
			INSERT INTO manager.sandbox_rootfs_bindings (
				sandbox_id, filesystem_id, team_id, created_at, updated_at
			)
			SELECT $1, filesystem_id, $2, NOW(), NOW()
			FROM advanced
			ON CONFLICT (sandbox_id) DO UPDATE SET
				team_id = EXCLUDED.team_id
			RETURNING filesystem_id
		)
		SELECT filesystem_id FROM ensured_binding
	`, state.SandboxID, state.TeamID, state.LayerID, expectedHeadLayerID,
		state.BaseImageRef, state.BaseImageDigest, nullableTime(state.CreatedAt))
	if err != nil {
		return fmt.Errorf("advance rootfs filesystem head: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: sandbox %s", ErrRootFSHeadConflict, state.SandboxID)
	}
	return nil
}

func rootFSLayerChainSQL() string {
	return `
		WITH RECURSIVE head AS (
			SELECT f.head_layer_id
			FROM manager.sandbox_rootfs_bindings b
			JOIN manager.rootfs_filesystems f ON f.filesystem_id = b.filesystem_id
			WHERE b.sandbox_id = $1
				AND f.head_layer_id IS NOT NULL
			UNION ALL
			SELECT h.head_layer_id
			FROM manager.sandbox_rootfs_heads h
			WHERE h.sandbox_id = $1
				AND NOT EXISTS (
					SELECT 1
					FROM manager.sandbox_rootfs_bindings b
					WHERE b.sandbox_id = $1
				)
		),
		chain AS (
			SELECT
				l.layer_id, l.parent_layer_id, l.source_sandbox_id, l.team_id,
				l.runtime_generation, l.runtime, l.runtime_handler, l.base_image_ref,
					l.base_image_digest, l.snapshotter, l.snapshot_parent,
					l.snapshot_parent_chain, l.diff_digest, l.diff_id, l.diff_media_type,
					l.diff_size, l.diff_object_key, l.head_object_digest,
				l.head_object_media_type, l.head_object_size, l.head_object_key,
				l.head_image_ref, l.head_image_digest,
				l.platform_os, l.platform_architecture, l.platform_variant,
				l.created_at, 0 AS depth
			FROM head h
			JOIN manager.rootfs_layers l ON l.layer_id = h.head_layer_id
			UNION ALL
			SELECT
				p.layer_id, p.parent_layer_id, p.source_sandbox_id, p.team_id,
				p.runtime_generation, p.runtime, p.runtime_handler, p.base_image_ref,
					p.base_image_digest, p.snapshotter, p.snapshot_parent,
					p.snapshot_parent_chain, p.diff_digest, p.diff_id, p.diff_media_type,
					p.diff_size, p.diff_object_key, p.head_object_digest,
				p.head_object_media_type, p.head_object_size, p.head_object_key,
				p.head_image_ref, p.head_image_digest,
				p.platform_os, p.platform_architecture, p.platform_variant,
				p.created_at, c.depth + 1 AS depth
			FROM manager.rootfs_layers p
			JOIN chain c ON p.layer_id = c.parent_layer_id
			WHERE c.head_object_key = ''
		)
		SELECT layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
				runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
				snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
				diff_size, diff_object_key, head_object_digest, head_object_media_type,
			head_object_size, head_object_key, head_image_ref, head_image_digest, platform_os,
			platform_architecture, platform_variant, created_at
		FROM chain
		ORDER BY depth DESC`
}

func rootFSLayerChainByHeadSQL() string {
	return `WITH RECURSIVE chain AS (
			SELECT
				l.layer_id, l.parent_layer_id, l.source_sandbox_id, l.team_id,
				l.runtime_generation, l.runtime, l.runtime_handler, l.base_image_ref,
					l.base_image_digest, l.snapshotter, l.snapshot_parent,
					l.snapshot_parent_chain, l.diff_digest, l.diff_id, l.diff_media_type,
					l.diff_size, l.diff_object_key, l.head_object_digest,
				l.head_object_media_type, l.head_object_size, l.head_object_key,
				l.head_image_ref, l.head_image_digest,
				l.platform_os, l.platform_architecture, l.platform_variant,
				l.created_at, 0 AS depth
			FROM manager.rootfs_layers l
			WHERE l.layer_id = $1
				AND ($2 = '' OR l.team_id = $2)
			UNION ALL
			SELECT
				p.layer_id, p.parent_layer_id, p.source_sandbox_id, p.team_id,
				p.runtime_generation, p.runtime, p.runtime_handler, p.base_image_ref,
					p.base_image_digest, p.snapshotter, p.snapshot_parent,
					p.snapshot_parent_chain, p.diff_digest, p.diff_id, p.diff_media_type,
					p.diff_size, p.diff_object_key, p.head_object_digest,
				p.head_object_media_type, p.head_object_size, p.head_object_key,
				p.head_image_ref, p.head_image_digest,
				p.platform_os, p.platform_architecture, p.platform_variant,
				p.created_at, c.depth + 1 AS depth
			FROM manager.rootfs_layers p
			JOIN chain c ON p.layer_id = c.parent_layer_id
				AND p.team_id = c.team_id
			WHERE c.head_object_key = ''
		)
		SELECT layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
				runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
				snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
				diff_size, diff_object_key, head_object_digest, head_object_media_type,
			head_object_size, head_object_key, head_image_ref, head_image_digest, platform_os,
			platform_architecture, platform_variant, created_at
		FROM chain
		ORDER BY depth DESC`
}

func scanRootFSLayerRows(rows pgx.Rows) (*SandboxRootFSLayer, error) {
	var layer SandboxRootFSLayer
	var parentLayerID *string
	var parentChainJSON []byte
	if err := rows.Scan(
		&layer.ID, &parentLayerID, &layer.SourceSandboxID, &layer.TeamID, &layer.RuntimeGeneration,
		&layer.Runtime, &layer.RuntimeHandler, &layer.BaseImageRef, &layer.BaseImageDigest, &layer.Snapshotter,
		&layer.SnapshotParent, &parentChainJSON, &layer.DiffDigest, &layer.DiffID, &layer.DiffMediaType,
		&layer.DiffSize, &layer.DiffObjectKey, &layer.HeadObjectDigest,
		&layer.HeadObjectMediaType, &layer.HeadObjectSize, &layer.HeadObjectKey,
		&layer.HeadImageRef, &layer.HeadImageDigest,
		&layer.PlatformOS, &layer.PlatformArchitecture, &layer.PlatformVariant, &layer.CreatedAt,
	); err != nil {
		return nil, err
	}
	if parentLayerID != nil {
		layer.ParentLayerID = *parentLayerID
	}
	if len(parentChainJSON) > 0 {
		if err := json.Unmarshal(parentChainJSON, &layer.SnapshotParentChain); err != nil {
			return nil, fmt.Errorf("unmarshal rootfs layer snapshot parent chain: %w", err)
		}
	}
	return &layer, nil
}

func rootFSStateFromLayerChain(sandboxID string, chain []*SandboxRootFSLayer) *SandboxRootFSState {
	if len(chain) == 0 {
		return nil
	}
	head := chain[len(chain)-1]
	return &SandboxRootFSState{
		LayerID:              head.ID,
		ParentLayerID:        head.ParentLayerID,
		SandboxID:            sandboxID,
		TeamID:               head.TeamID,
		RuntimeGeneration:    head.RuntimeGeneration,
		Runtime:              head.Runtime,
		RuntimeHandler:       head.RuntimeHandler,
		BaseImageRef:         head.BaseImageRef,
		BaseImageDigest:      head.BaseImageDigest,
		PlatformOS:           head.PlatformOS,
		PlatformArchitecture: head.PlatformArchitecture,
		PlatformVariant:      head.PlatformVariant,
		Snapshotter:          head.Snapshotter,
		SnapshotParent:       head.SnapshotParent,
		SnapshotParentChain:  append([]string(nil), head.SnapshotParentChain...),
		DiffDigest:           head.DiffDigest,
		DiffID:               head.DiffID,
		DiffMediaType:        head.DiffMediaType,
		DiffSize:             head.DiffSize,
		DiffObjectKey:        head.DiffObjectKey,
		HeadObjectDigest:     head.HeadObjectDigest,
		HeadObjectMediaType:  head.HeadObjectMediaType,
		HeadObjectSize:       head.HeadObjectSize,
		HeadObjectKey:        head.HeadObjectKey,
		HeadImageRef:         head.HeadImageRef,
		HeadImageDigest:      head.HeadImageDigest,
		CreatedAt:            head.CreatedAt,
		LayerChain:           cloneSandboxRootFSLayers(chain),
	}
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
	var claimedAt, expiresAt, hardExpiresAt, deletedAt *time.Time
	if err := scanner.Scan(
		&record.ID, &record.TeamID, &record.UserID, &record.TemplateID, &record.TemplateName, &record.TemplateNamespace,
		&record.ClusterID, &record.Status, &configJSON, &mountsJSON, &specJSON,
		&record.CurrentPodName, &record.CurrentPodNamespace, &record.RuntimeGeneration, &record.LifecycleEpoch,
		&record.WebhookStateVolumeID, &record.OwnerKind,
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
		&txn.ExpectedHeadLayerID, &txn.PreparedHeadLayerID,
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

func cloneSandboxLifecycleTxn(txn *SandboxLifecycleTxn) *SandboxLifecycleTxn {
	if txn == nil {
		return nil
	}
	clone := *txn
	return &clone
}

func sandboxLifecyclePhaseActive(phase string) bool {
	switch phase {
	case SandboxLifecyclePhasePreparing, SandboxLifecyclePhaseBarriered, SandboxLifecyclePhasePublishing, SandboxLifecyclePhaseCommitting:
		return true
	default:
		return false
	}
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

func cloneSandboxRootFSLayers(layers []*SandboxRootFSLayer) []*SandboxRootFSLayer {
	if len(layers) == 0 {
		return nil
	}
	out := make([]*SandboxRootFSLayer, 0, len(layers))
	for _, layer := range layers {
		if layer == nil {
			out = append(out, nil)
			continue
		}
		clone := *layer
		clone.SnapshotParentChain = append([]string(nil), layer.SnapshotParentChain...)
		out = append(out, &clone)
	}
	return out
}
