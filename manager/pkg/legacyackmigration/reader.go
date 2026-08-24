package legacyackmigration

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ReadCatalog captures one repeatable-read, read-only view of the final ACK
// manager schema. It does not accept schema drift or mutate the source.
func ReadCatalog(ctx context.Context, pool *pgxpool.Pool) (*Catalog, error) {
	if pool == nil {
		return nil, fmt.Errorf("legacy manager database pool is required")
	}
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin legacy manager snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	catalog := &Catalog{}
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(version_id) FILTER (WHERE is_applied), 0)
		FROM manager.goose_db_version
	`).Scan(&catalog.ManagerSchemaVersion); err != nil {
		return nil, fmt.Errorf("read legacy manager schema version: %w", err)
	}
	if catalog.ManagerSchemaVersion != LegacyManagerSchemaVersion {
		return nil, fmt.Errorf("legacy manager schema version is %d, expected %d", catalog.ManagerSchemaVersion, LegacyManagerSchemaVersion)
	}
	if err := tx.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM manager.sandbox_lifecycle_txns
		WHERE phase IN ('preparing', 'barriered', 'publishing', 'committing')
	`).Scan(&catalog.ActiveLifecycleTxns); err != nil {
		return nil, fmt.Errorf("count active legacy lifecycle transactions: %w", err)
	}

	if catalog.Sandboxes, err = readSandboxes(ctx, tx); err != nil {
		return nil, err
	}
	if catalog.Layers, err = readLayers(ctx, tx); err != nil {
		return nil, err
	}
	if catalog.Filesystems, err = readFilesystems(ctx, tx); err != nil {
		return nil, err
	}
	if catalog.Bindings, err = readBindings(ctx, tx); err != nil {
		return nil, err
	}
	if catalog.Snapshots, err = readSnapshots(ctx, tx); err != nil {
		return nil, err
	}
	if catalog.SourceSandboxes, err = readSourceSandboxes(ctx, tx); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit legacy manager read-only snapshot: %w", err)
	}
	return catalog, nil
}

func readSourceSandboxes(ctx context.Context, tx pgx.Tx) ([]SourceSandbox, error) {
	rows, err := tx.Query(ctx, `
		WITH referenced AS (
			SELECT source_sandbox_id AS sandbox_id FROM manager.rootfs_layers
			UNION
			SELECT source_sandbox_id FROM manager.rootfs_snapshots
		)
		SELECT sandbox.sandbox_id, sandbox.team_id, sandbox.template_spec
		FROM referenced
		JOIN manager.sandboxes sandbox USING (sandbox_id)
		WHERE referenced.sandbox_id <> ''
		ORDER BY sandbox.sandbox_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query legacy source sandbox templates: %w", err)
	}
	defer rows.Close()
	var result []SourceSandbox
	for rows.Next() {
		var item SourceSandbox
		if err := rows.Scan(&item.ID, &item.TeamID, &item.TemplateSpec); err != nil {
			return nil, fmt.Errorf("scan legacy source sandbox template: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy source sandbox templates: %w", err)
	}
	return result, nil
}

func readSandboxes(ctx context.Context, tx pgx.Tx) ([]Sandbox, error) {
	rows, err := tx.Query(ctx, `
		SELECT sandbox_id, team_id, user_id, template_id, template_name,
			template_namespace, cluster_id, desired_state, config, template_spec,
			runtime_generation, lifecycle_epoch, owner_kind, hot_claim_completed_at,
			claimed_at, expires_at, hard_expires_at, created_at, updated_at
		FROM manager.sandboxes
		WHERE deleted_at IS NULL AND desired_state <> 'deleted'
		ORDER BY sandbox_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query legacy sandboxes: %w", err)
	}
	defer rows.Close()
	var result []Sandbox
	for rows.Next() {
		var item Sandbox
		var hot, claimed, expires, hardExpires sql.NullTime
		if err := rows.Scan(
			&item.ID, &item.TeamID, &item.UserID, &item.TemplateID, &item.TemplateName,
			&item.TemplateNamespace, &item.ClusterID, &item.DesiredState, &item.Config, &item.TemplateSpec,
			&item.RuntimeGeneration, &item.LifecycleEpoch, &item.OwnerKind, &hot,
			&claimed, &expires, &hardExpires, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan legacy sandbox: %w", err)
		}
		item.HotClaimCompletedAt = nullableTimeValue(hot)
		item.ClaimedAt = nullableTimeValue(claimed)
		item.ExpiresAt = nullableTimeValue(expires)
		item.HardExpiresAt = nullableTimeValue(hardExpires)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy sandboxes: %w", err)
	}
	return result, nil
}

func readLayers(ctx context.Context, tx pgx.Tx) ([]Layer, error) {
	rows, err := tx.Query(ctx, `
		SELECT layer_id, parent_layer_id, source_sandbox_id, team_id,
			runtime_generation, runtime, runtime_handler, base_image_ref,
			base_image_digest, snapshotter, snapshot_parent, snapshot_parent_chain,
			diff_digest, diff_id, diff_media_type, diff_size, diff_object_key,
			platform_os, platform_architecture, platform_variant, created_at
		FROM manager.rootfs_layers
		ORDER BY created_at, layer_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query legacy rootfs layers: %w", err)
	}
	defer rows.Close()
	var result []Layer
	for rows.Next() {
		var item Layer
		var parent sql.NullString
		if err := rows.Scan(
			&item.ID, &parent, &item.SourceSandboxID, &item.TeamID,
			&item.RuntimeGeneration, &item.Runtime, &item.RuntimeHandler,
			&item.BaseImageRef, &item.BaseImageDigest, &item.Snapshotter,
			&item.SnapshotParent, &item.SnapshotParentChain, &item.DiffDigest,
			&item.DiffID, &item.DiffMediaType, &item.DiffSize, &item.DiffObjectKey,
			&item.PlatformOS, &item.PlatformArchitecture, &item.PlatformVariant,
			&item.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan legacy rootfs layer: %w", err)
		}
		item.ParentID = nullableStringValue(parent)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy rootfs layers: %w", err)
	}
	return result, nil
}

func readFilesystems(ctx context.Context, tx pgx.Tx) ([]Filesystem, error) {
	rows, err := tx.Query(ctx, `
		SELECT filesystem_id, team_id, source_filesystem_id, head_layer_id,
			base_image_ref, base_image_digest, created_at, updated_at
		FROM manager.rootfs_filesystems
		ORDER BY filesystem_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query legacy rootfs filesystems: %w", err)
	}
	defer rows.Close()
	var result []Filesystem
	for rows.Next() {
		var item Filesystem
		var source, head sql.NullString
		if err := rows.Scan(
			&item.ID, &item.TeamID, &source, &head, &item.BaseImageRef,
			&item.BaseImageDigest, &item.CreatedAt, &item.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan legacy rootfs filesystem: %w", err)
		}
		item.SourceFilesystemID = nullableStringValue(source)
		item.HeadLayerID = nullableStringValue(head)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy rootfs filesystems: %w", err)
	}
	return result, nil
}

func readBindings(ctx context.Context, tx pgx.Tx) ([]Binding, error) {
	rows, err := tx.Query(ctx, `
		SELECT sandbox_id, filesystem_id, team_id, created_at, updated_at
		FROM manager.sandbox_rootfs_bindings
		ORDER BY sandbox_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query legacy rootfs bindings: %w", err)
	}
	defer rows.Close()
	var result []Binding
	for rows.Next() {
		var item Binding
		if err := rows.Scan(&item.SandboxID, &item.FilesystemID, &item.TeamID, &item.CreatedAt, &item.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan legacy rootfs binding: %w", err)
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy rootfs bindings: %w", err)
	}
	return result, nil
}

func readSnapshots(ctx context.Context, tx pgx.Tx) ([]Snapshot, error) {
	rows, err := tx.Query(ctx, `
		SELECT snapshot_id, team_id, source_sandbox_id, head_layer_id,
			filesystem_id, name, description, created_at, expires_at
		FROM manager.rootfs_snapshots
		ORDER BY snapshot_id
	`)
	if err != nil {
		return nil, fmt.Errorf("query legacy rootfs snapshots: %w", err)
	}
	defer rows.Close()
	var result []Snapshot
	for rows.Next() {
		var item Snapshot
		var filesystem sql.NullString
		var expires sql.NullTime
		if err := rows.Scan(
			&item.ID, &item.TeamID, &item.SourceSandboxID, &item.HeadLayerID,
			&filesystem, &item.Name, &item.Description, &item.CreatedAt, &expires,
		); err != nil {
			return nil, fmt.Errorf("scan legacy rootfs snapshot: %w", err)
		}
		item.FilesystemID = nullableStringValue(filesystem)
		item.ExpiresAt = nullableTimeValue(expires)
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate legacy rootfs snapshots: %w", err)
	}
	return result, nil
}

func nullableStringValue(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}

func nullableTimeValue(value sql.NullTime) (result time.Time) {
	if value.Valid {
		return value.Time
	}
	return result
}
