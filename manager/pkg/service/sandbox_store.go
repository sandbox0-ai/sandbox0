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
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
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
// writable rootfs checkpoint.
type SandboxRootFSState struct {
	LayerID       string
	ParentLayerID string
	// ExpectedHeadLayerID overrides ParentLayerID as the head CAS precondition.
	ExpectedHeadLayerID string
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
	StorageEngine       string
	DiffDigest          string
	DiffMediaType       string
	DiffSize            int64
	DiffObjectKey       string
	S0FSVolumeID        string
	S0FSManifestKey     string
	S0FSManifestSeq     uint64
	S0FSCheckpointSeq   uint64
	CreatedAt           time.Time
	UpdatedAt           time.Time
	LayerChain          []*SandboxRootFSLayer
}

// SandboxRootFSLayer is one immutable rootfs checkpoint record in a sandbox
// rootfs chain.
type SandboxRootFSLayer struct {
	ID                  string
	ParentLayerID       string
	SourceSandboxID     string
	TeamID              string
	RuntimeGeneration   int64
	Runtime             string
	RuntimeHandler      string
	BaseImageRef        string
	BaseImageDigest     string
	Snapshotter         string
	SnapshotParent      string
	SnapshotParentChain []string
	StorageEngine       string
	DiffDigest          string
	DiffID              string
	DiffMediaType       string
	DiffSize            int64
	DiffObjectKey       string
	S0FSVolumeID        string
	S0FSManifestKey     string
	S0FSManifestSeq     uint64
	S0FSCheckpointSeq   uint64
	CreatedAt           time.Time
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
	if _, err := s.pool.Exec(ctx, `
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
	if _, err := s.pool.Exec(ctx, `DELETE FROM manager.sandbox_rootfs_states WHERE sandbox_id = $1`, sandboxID); err != nil {
		return fmt.Errorf("delete sandbox rootfs states: %w", err)
	}
	if _, err := s.pool.Exec(ctx, `DELETE FROM manager.sandbox_rootfs_heads WHERE sandbox_id = $1`, sandboxID); err != nil {
		return fmt.Errorf("delete sandbox rootfs head: %w", err)
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

func (t sandboxStoreTx) UpsertSandbox(ctx context.Context, record *SandboxRecord) error {
	return upsertSandboxRecord(ctx, t.tx, record)
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
	switch rootFSStorageEngine(state.StorageEngine) {
	case ctldapi.RootFSStorageEngineS0FS:
		if strings.TrimSpace(state.S0FSVolumeID) == "" {
			return fmt.Errorf("s0fs_volume_id is required")
		}
		if strings.TrimSpace(state.S0FSManifestKey) == "" {
			return fmt.Errorf("s0fs_manifest_key is required")
		}
		if state.S0FSManifestSeq == 0 {
			return fmt.Errorf("s0fs_manifest_seq is required")
		}
	default:
		return fmt.Errorf("unsupported rootfs storage_engine %q", state.StorageEngine)
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
	storageEngine := rootFSStorageEngine(state.StorageEngine)
	if storageEngine != ctldapi.RootFSStorageEngineS0FS {
		return fmt.Errorf("unsupported rootfs storage_engine %q", state.StorageEngine)
	}
	if state.DiffMediaType == "" && storageEngine == ctldapi.RootFSStorageEngineS0FS {
		state.DiffMediaType = "application/vnd.sandbox0.rootfs.s0fs.v1+json"
	}
	if state.DiffDigest == "" && state.S0FSManifestKey != "" {
		state.DiffDigest = "s0fs:" + state.S0FSManifestKey
	}
	if state.DiffObjectKey == "" && state.S0FSManifestKey != "" {
		state.DiffObjectKey = state.S0FSManifestKey
	}
	if state.DiffSize < 0 {
		state.DiffSize = 0
	}
	if state.DiffDigest == "" {
		state.DiffDigest = "s0fs:" + state.LayerID
	}
	if state.DiffObjectKey == "" {
		state.DiffObjectKey = state.LayerID
	}
	parentLayerID := nullableText(state.ParentLayerID)
	parentChainJSON, err := json.Marshal(state.SnapshotParentChain)
	if err != nil {
		return fmt.Errorf("marshal rootfs layer snapshot parent chain: %w", err)
	}
	if err := saveRootFSLayerRow(ctx, exec, state, storageEngine, parentLayerID, parentChainJSON); err != nil {
		return err
	}
	return nil
}

func saveRootFSLayerRow(ctx context.Context, exec rootFSStateExecutor, state *SandboxRootFSState, storageEngine string, parentLayerID any, parentChainJSON []byte) error {
	_, err := exec.Exec(ctx, `
		INSERT INTO manager.rootfs_layers (
			layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
			runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
			snapshot_parent, snapshot_parent_chain, diff_digest, diff_id, diff_media_type,
			diff_size, diff_object_key, storage_engine, s0fs_volume_id, s0fs_manifest_key,
			s0fs_manifest_seq, s0fs_checkpoint_seq, created_at
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, COALESCE($23, NOW()))
		ON CONFLICT (layer_id) DO NOTHING
	`, state.LayerID, parentLayerID, state.SandboxID, state.TeamID, state.RuntimeGeneration,
		state.Runtime, state.RuntimeHandler, state.BaseImageRef, state.BaseImageDigest, state.Snapshotter,
		state.SnapshotParent, parentChainJSON, state.DiffDigest, "", state.DiffMediaType,
		state.DiffSize, state.DiffObjectKey, storageEngine, state.S0FSVolumeID, state.S0FSManifestKey,
		int64(state.S0FSManifestSeq), int64(state.S0FSCheckpointSeq), nullableTime(state.CreatedAt))
	if err != nil {
		return fmt.Errorf("save rootfs layer: %w", err)
	}
	return nil
}

func rootFSStorageEngine(raw string) string {
	switch strings.TrimSpace(raw) {
	case "", ctldapi.RootFSStorageEngineS0FS:
		return ctldapi.RootFSStorageEngineS0FS
	default:
		return strings.TrimSpace(raw)
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
				l.snapshot_parent_chain, l.storage_engine, l.diff_digest, l.diff_id, l.diff_media_type,
				l.diff_size, l.diff_object_key, l.s0fs_volume_id, l.s0fs_manifest_key,
				l.s0fs_manifest_seq, l.s0fs_checkpoint_seq, l.created_at, 0 AS depth
			FROM head h
			JOIN manager.rootfs_layers l ON l.layer_id = h.head_layer_id
			UNION ALL
			SELECT
				p.layer_id, p.parent_layer_id, p.source_sandbox_id, p.team_id,
				p.runtime_generation, p.runtime, p.runtime_handler, p.base_image_ref,
				p.base_image_digest, p.snapshotter, p.snapshot_parent,
				p.snapshot_parent_chain, p.storage_engine, p.diff_digest, p.diff_id, p.diff_media_type,
				p.diff_size, p.diff_object_key, p.s0fs_volume_id, p.s0fs_manifest_key,
				p.s0fs_manifest_seq, p.s0fs_checkpoint_seq, p.created_at, c.depth + 1 AS depth
			FROM manager.rootfs_layers p
			JOIN chain c ON p.layer_id = c.parent_layer_id
		)
		SELECT layer_id, parent_layer_id, source_sandbox_id, team_id, runtime_generation,
			runtime, runtime_handler, base_image_ref, base_image_digest, snapshotter,
			snapshot_parent, snapshot_parent_chain, storage_engine, diff_digest, diff_id,
			diff_media_type, diff_size, diff_object_key, s0fs_volume_id, s0fs_manifest_key,
			s0fs_manifest_seq, s0fs_checkpoint_seq, created_at
		FROM chain
		ORDER BY depth DESC`
}

func scanRootFSLayerRows(rows pgx.Rows) (*SandboxRootFSLayer, error) {
	var layer SandboxRootFSLayer
	var parentLayerID *string
	var parentChainJSON []byte
	var s0fsManifestSeq int64
	var s0fsCheckpointSeq int64
	if err := rows.Scan(
		&layer.ID, &parentLayerID, &layer.SourceSandboxID, &layer.TeamID, &layer.RuntimeGeneration,
		&layer.Runtime, &layer.RuntimeHandler, &layer.BaseImageRef, &layer.BaseImageDigest, &layer.Snapshotter,
		&layer.SnapshotParent, &parentChainJSON, &layer.StorageEngine, &layer.DiffDigest, &layer.DiffID,
		&layer.DiffMediaType, &layer.DiffSize, &layer.DiffObjectKey, &layer.S0FSVolumeID,
		&layer.S0FSManifestKey, &s0fsManifestSeq, &s0fsCheckpointSeq, &layer.CreatedAt,
	); err != nil {
		return nil, err
	}
	if s0fsManifestSeq > 0 {
		layer.S0FSManifestSeq = uint64(s0fsManifestSeq)
	}
	if s0fsCheckpointSeq > 0 {
		layer.S0FSCheckpointSeq = uint64(s0fsCheckpointSeq)
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
		LayerID:             head.ID,
		ParentLayerID:       head.ParentLayerID,
		SandboxID:           sandboxID,
		TeamID:              head.TeamID,
		RuntimeGeneration:   head.RuntimeGeneration,
		Runtime:             head.Runtime,
		RuntimeHandler:      head.RuntimeHandler,
		BaseImageRef:        head.BaseImageRef,
		BaseImageDigest:     head.BaseImageDigest,
		Snapshotter:         head.Snapshotter,
		SnapshotParent:      head.SnapshotParent,
		SnapshotParentChain: append([]string(nil), head.SnapshotParentChain...),
		StorageEngine:       rootFSStorageEngine(head.StorageEngine),
		DiffDigest:          head.DiffDigest,
		DiffMediaType:       head.DiffMediaType,
		DiffSize:            head.DiffSize,
		DiffObjectKey:       head.DiffObjectKey,
		S0FSVolumeID:        head.S0FSVolumeID,
		S0FSManifestKey:     head.S0FSManifestKey,
		S0FSManifestSeq:     head.S0FSManifestSeq,
		S0FSCheckpointSeq:   head.S0FSCheckpointSeq,
		CreatedAt:           head.CreatedAt,
		LayerChain:          cloneSandboxRootFSLayers(chain),
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
