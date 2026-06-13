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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	servicemigrations "github.com/sandbox0-ai/sandbox0/manager/pkg/service/migrations"
	"github.com/sandbox0-ai/sandbox0/pkg/migrate"
)

const sandboxStoreSchemaName = "manager"

var ErrSandboxRecordNotFound = errors.New("sandbox record not found")

const (
	SandboxStatusDeleted = "deleted"
)

// SandboxRecord is the durable sandbox identity and configuration.
type SandboxRecord struct {
	ID                  string
	TeamID              string
	UserID              string
	TemplateID          string
	TemplateName        string
	TemplateNamespace   string
	ClusterID           string
	Status              string
	Config              SandboxConfig
	Mounts              []ClaimMount
	TemplateSpec        v1alpha1.SandboxTemplateSpec
	CurrentPodName      string
	CurrentPodNamespace string
	RuntimeGeneration   int64
	ClaimedAt           time.Time
	ExpiresAt           time.Time
	HardExpiresAt       time.Time
	DeletedAt           time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// SandboxRootFSState is manager-internal metadata for one persisted sandbox
// writable rootfs diff.
type SandboxRootFSState struct {
	SandboxID           string
	TeamID              string
	RuntimeGeneration   int64
	Runtime             string
	RuntimeHandler      string
	BaseImageRef        string
	BaseImageDigest     string
	Snapshotter         string
	SnapshotParent      string
	SnapshotParentChain []string
	DiffDigest          string
	DiffMediaType       string
	DiffSize            int64
	DiffObjectKey       string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// SandboxStore persists sandbox identities independently of runtime pods.
type SandboxStore interface {
	UpsertSandbox(ctx context.Context, record *SandboxRecord) error
	GetSandbox(ctx context.Context, sandboxID string) (*SandboxRecord, error)
	ListSandboxes(ctx context.Context, req *ListSandboxesRequest) ([]*SandboxRecord, error)
	ListPausingSandboxes(ctx context.Context, limit int) ([]*SandboxRecord, error)
	ListHardExpiredSandboxes(ctx context.Context, now time.Time, limit int) ([]*SandboxRecord, error)
	MarkSandboxDeleted(ctx context.Context, sandboxID string, deletedAt time.Time) error
	SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error
	GetLatestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error)
	WithSandboxLock(ctx context.Context, sandboxID string, fn func(context.Context, SandboxStoreTx, *SandboxRecord) error) error
}

// SandboxStoreTx is a locked sandbox store transaction.
type SandboxStoreTx interface {
	SaveRuntime(ctx context.Context, sandboxID, namespace, podName, status string, generation int64, expiresAt, hardExpiresAt time.Time) error
	MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error
	SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error
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
	if strings.TrimSpace(record.ID) == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	configJSON, mountsJSON, specJSON, err := marshalSandboxRecordJSON(record)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.sandboxes (
			sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
			cluster_id, status, config, mounts, template_spec,
			current_pod_name, current_pod_namespace, runtime_generation,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, COALESCE($19, NOW()), NOW())
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
			claimed_at = EXCLUDED.claimed_at,
			expires_at = EXCLUDED.expires_at,
			hard_expires_at = EXCLUDED.hard_expires_at,
			deleted_at = EXCLUDED.deleted_at,
			updated_at = NOW()
	`, record.ID, record.TeamID, record.UserID, record.TemplateID, record.TemplateName, record.TemplateNamespace,
		record.ClusterID, record.Status, configJSON, mountsJSON, specJSON,
		record.CurrentPodName, record.CurrentPodNamespace, record.RuntimeGeneration,
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

func (s *PGSandboxStore) ListPausingSandboxes(ctx context.Context, limit int) ([]*SandboxRecord, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	if limit <= 0 {
		limit = 500
	}
	rows, err := s.pool.Query(ctx, sandboxRecordSelectSQL()+`
		WHERE deleted_at IS NULL
			AND status = $1
		ORDER BY updated_at ASC
		LIMIT $2
	`, SandboxStatusPausing, limit)
	if err != nil {
		return nil, fmt.Errorf("list pausing sandboxes: %w", err)
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
		return nil, fmt.Errorf("iterate pausing sandboxes: %w", err)
	}
	return records, nil
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
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.sandboxes
		SET status = $2,
			current_pod_name = '',
			current_pod_namespace = '',
			deleted_at = $3,
			updated_at = NOW()
		WHERE sandbox_id = $1
	`, sandboxID, SandboxStatusDeleted, deletedAt)
	if err != nil {
		return fmt.Errorf("mark sandbox deleted: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM manager.sandbox_rootfs_states WHERE sandbox_id = $1`, sandboxID); err != nil {
		return fmt.Errorf("delete sandbox rootfs states: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error {
	if s == nil || s.pool == nil || state == nil {
		return nil
	}
	return saveRootFSState(ctx, s.pool, state)
}

func (s *PGSandboxStore) GetLatestRootFSState(ctx context.Context, sandboxID string) (*SandboxRootFSState, error) {
	if s == nil || s.pool == nil {
		return nil, nil
	}
	return scanRootFSState(s.pool.QueryRow(ctx, rootFSStateSelectSQL()+`
		WHERE sandbox_id = $1
		ORDER BY runtime_generation DESC, updated_at DESC
		LIMIT 1
	`, sandboxID))
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

func (t sandboxStoreTx) SaveRuntime(ctx context.Context, sandboxID, namespace, podName, status string, generation int64, expiresAt, hardExpiresAt time.Time) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET status = $2,
			current_pod_namespace = $3,
			current_pod_name = $4,
			runtime_generation = $5,
			expires_at = $6,
			hard_expires_at = $7,
			deleted_at = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1
	`, sandboxID, status, namespace, podName, generation, nullableTime(expiresAt), nullableTime(hardExpiresAt))
	if err != nil {
		return fmt.Errorf("save sandbox runtime: %w", err)
	}
	return nil
}

func (t sandboxStoreTx) MarkRuntimePaused(ctx context.Context, sandboxID string, generation int64, pausedAt time.Time) error {
	_, err := t.tx.Exec(ctx, `
		UPDATE manager.sandboxes
		SET status = $2,
			current_pod_namespace = '',
			current_pod_name = '',
			runtime_generation = GREATEST(runtime_generation, $3),
			expires_at = NULL,
			updated_at = NOW()
		WHERE sandbox_id = $1
	`, sandboxID, SandboxStatusPaused, generation)
	if err != nil {
		return fmt.Errorf("mark sandbox runtime paused: %w", err)
	}
	return nil
}

func (t sandboxStoreTx) SaveRootFSState(ctx context.Context, state *SandboxRootFSState) error {
	return saveRootFSState(ctx, t.tx, state)
}

func sandboxRecordSelectSQL() string {
	return `
		SELECT sandbox_id, team_id, user_id, template_id, template_name, template_namespace,
			cluster_id, status, config, mounts, template_spec,
			current_pod_name, current_pod_namespace, runtime_generation,
			claimed_at, expires_at, hard_expires_at, deleted_at, created_at, updated_at
		FROM manager.sandboxes`
}

type rootFSStateExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func saveRootFSState(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState) error {
	if exec == nil || state == nil {
		return nil
	}
	if strings.TrimSpace(state.SandboxID) == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if strings.TrimSpace(state.TeamID) == "" {
		return fmt.Errorf("team_id is required")
	}
	if strings.TrimSpace(state.DiffDigest) == "" {
		return fmt.Errorf("diff_digest is required")
	}
	if strings.TrimSpace(state.DiffObjectKey) == "" {
		return fmt.Errorf("diff_object_key is required")
	}
	parentChainJSON, err := json.Marshal(state.SnapshotParentChain)
	if err != nil {
		return fmt.Errorf("marshal rootfs snapshot parent chain: %w", err)
	}
	_, err = exec.Exec(ctx, `
		INSERT INTO manager.sandbox_rootfs_states (
			sandbox_id, team_id, runtime_generation, runtime, runtime_handler,
			base_image_ref, base_image_digest, snapshotter, snapshot_parent,
			snapshot_parent_chain, diff_digest, diff_media_type, diff_size,
			diff_object_key, created_at, updated_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, COALESCE($15, NOW()), NOW())
		ON CONFLICT (sandbox_id, runtime_generation) DO UPDATE SET
			team_id = EXCLUDED.team_id,
			runtime = EXCLUDED.runtime,
			runtime_handler = EXCLUDED.runtime_handler,
			base_image_ref = EXCLUDED.base_image_ref,
			base_image_digest = EXCLUDED.base_image_digest,
			snapshotter = EXCLUDED.snapshotter,
			snapshot_parent = EXCLUDED.snapshot_parent,
			snapshot_parent_chain = EXCLUDED.snapshot_parent_chain,
			diff_digest = EXCLUDED.diff_digest,
			diff_media_type = EXCLUDED.diff_media_type,
			diff_size = EXCLUDED.diff_size,
			diff_object_key = EXCLUDED.diff_object_key,
			updated_at = NOW()
	`, state.SandboxID, state.TeamID, state.RuntimeGeneration, state.Runtime, state.RuntimeHandler,
		state.BaseImageRef, state.BaseImageDigest, state.Snapshotter, state.SnapshotParent,
		parentChainJSON, state.DiffDigest, state.DiffMediaType, state.DiffSize,
		state.DiffObjectKey, nullableTime(state.CreatedAt))
	if err != nil {
		return fmt.Errorf("save sandbox rootfs state: %w", err)
	}
	return nil
}

func rootFSStateSelectSQL() string {
	return `
		SELECT sandbox_id, team_id, runtime_generation, runtime, runtime_handler,
			base_image_ref, base_image_digest, snapshotter, snapshot_parent,
			snapshot_parent_chain, diff_digest, diff_media_type, diff_size,
			diff_object_key, created_at, updated_at
		FROM manager.sandbox_rootfs_states`
}

func scanRootFSState(row sandboxRecordScanner) (*SandboxRootFSState, error) {
	var state SandboxRootFSState
	var parentChainJSON []byte
	if err := row.Scan(
		&state.SandboxID, &state.TeamID, &state.RuntimeGeneration, &state.Runtime, &state.RuntimeHandler,
		&state.BaseImageRef, &state.BaseImageDigest, &state.Snapshotter, &state.SnapshotParent,
		&parentChainJSON, &state.DiffDigest, &state.DiffMediaType, &state.DiffSize,
		&state.DiffObjectKey, &state.CreatedAt, &state.UpdatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if len(parentChainJSON) > 0 {
		if err := json.Unmarshal(parentChainJSON, &state.SnapshotParentChain); err != nil {
			return nil, fmt.Errorf("unmarshal rootfs snapshot parent chain: %w", err)
		}
	}
	return &state, nil
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
		&record.CurrentPodName, &record.CurrentPodNamespace, &record.RuntimeGeneration,
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

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
