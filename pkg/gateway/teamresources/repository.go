package teamresources

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SchemaConfig names the module schemas that may hold team-owned resources.
type SchemaConfig struct {
	Scheduler    string
	Manager      string
	StorageProxy string
}

// DefaultSchemaConfig returns the deployment defaults used by sandbox0 services.
func DefaultSchemaConfig() SchemaConfig {
	return SchemaConfig{
		Scheduler:    "scheduler",
		Manager:      "manager",
		StorageProxy: "storage_proxy",
	}
}

// Repository builds a conservative team-owned resource inventory from region PostgreSQL.
type Repository struct {
	pool    *pgxpool.Pool
	schemas SchemaConfig
}

// Option configures a Repository.
type Option func(*Repository)

// WithSchemaConfig overrides module schema names.
func WithSchemaConfig(schemas SchemaConfig) Option {
	return func(r *Repository) {
		r.schemas = schemas
	}
}

// NewRepository creates a team resource inventory repository.
func NewRepository(pool *pgxpool.Pool, opts ...Option) *Repository {
	r := &Repository{
		pool:    pool,
		schemas: DefaultSchemaConfig(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// GetTeamResourceInventory returns resource counts that should block or survive team deletion.
func (r *Repository) GetTeamResourceInventory(ctx context.Context, teamID string) (*Inventory, error) {
	inventory := &Inventory{
		TeamID:          strings.TrimSpace(teamID),
		RetentionPolicy: MeteringRetentionPolicy,
	}
	if r == nil || r.pool == nil || inventory.TeamID == "" {
		return inventory, nil
	}

	blockingQueries := r.blockingQueries()
	retainedQueries := r.retainedQueries()

	for _, query := range blockingQueries {
		count, err := r.countOptional(ctx, query.table, query.sql, inventory.TeamID)
		if err != nil {
			return nil, err
		}
		inventory.AddBlocking(query.category, count)
	}

	blockingDiscoverySchemas, err := r.blockingDiscoverySchemas(ctx)
	if err != nil {
		return nil, err
	}
	discoveredBlocking, err := r.discoverTeamIDQueries(ctx, blockingQueries, blockingDiscoverySchemas)
	if err != nil {
		return nil, err
	}
	for _, query := range discoveredBlocking {
		count, err := r.countOptional(ctx, query.table, query.sql, inventory.TeamID)
		if err != nil {
			return nil, err
		}
		inventory.AddBlocking(query.category, count)
	}

	for _, query := range retainedQueries {
		count, err := r.countOptional(ctx, query.table, query.sql, inventory.TeamID)
		if err != nil {
			return nil, err
		}
		inventory.AddRetained(query.category, count)
	}

	discoveredRetained, err := r.discoverTeamIDQueries(ctx, append(blockingQueries, retainedQueries...), r.retainedDiscoverySchemas())
	if err != nil {
		return nil, err
	}
	for _, query := range discoveredRetained {
		count, err := r.countOptional(ctx, query.table, query.sql, inventory.TeamID)
		if err != nil {
			return nil, err
		}
		inventory.AddRetained(query.category, count)
	}

	return inventory, nil
}

type countQuery struct {
	category string
	table    string
	sql      string
}

func (r *Repository) blockingQueries() []countQuery {
	apiKeys := tableRef("", "api_keys")
	sshKeys := tableRef("", "user_ssh_public_keys")

	schedulerTemplates := tableRef(r.schemas.Scheduler, "scheduler_templates")
	schedulerAllocations := tableRef(r.schemas.Scheduler, "scheduler_template_allocations")
	schedulerTemplateBuilds := tableRef(r.schemas.Scheduler, "scheduler_template_builds")
	credentialSources := tableRef(r.schemas.Scheduler, "credential_sources")
	credentialSourceVersions := tableRef(r.schemas.Scheduler, "credential_source_versions")
	credentialBindings := tableRef(r.schemas.Scheduler, "sandbox_egress_credential_bindings")

	managerSandboxes := tableRef(r.schemas.Manager, "sandboxes")
	managerLifecycleTxns := tableRef(r.schemas.Manager, "sandbox_lifecycle_txns")
	managerRootFSStates := tableRef(r.schemas.Manager, "sandbox_rootfs_states")
	managerRootFSHeads := tableRef(r.schemas.Manager, "sandbox_rootfs_heads")
	managerRootFSBindings := tableRef(r.schemas.Manager, "sandbox_rootfs_bindings")
	managerRootFSFilesystems := tableRef(r.schemas.Manager, "rootfs_filesystems")
	managerRootFSSnapshots := tableRef(r.schemas.Manager, "rootfs_snapshots")
	managerRootFSLayers := tableRef(r.schemas.Manager, "rootfs_layers")
	managerRootFSObjects := tableRef(r.schemas.Manager, "rootfs_objects")
	managerRootFSObjectDeletions := tableRef(r.schemas.Manager, "rootfs_object_deletions")

	storageVolumes := tableRef(r.schemas.StorageProxy, "sandbox_volumes")
	storageSnapshots := tableRef(r.schemas.StorageProxy, "sandbox_volume_snapshots")
	storageMounts := tableRef(r.schemas.StorageProxy, "sandbox_volume_mounts")
	storageCoordinations := tableRef(r.schemas.StorageProxy, "snapshot_coordinations")
	storageFlushResponses := tableRef(r.schemas.StorageProxy, "snapshot_flush_responses")
	storageOwners := tableRef(r.schemas.StorageProxy, "sandbox_volume_owners")
	storageS0FSHeads := tableRef(r.schemas.StorageProxy, "sandbox_volume_s0fs_heads")
	storageHandoffs := tableRef(r.schemas.StorageProxy, "sandbox_volume_handoffs")
	storageSyncReplicas := tableRef(r.schemas.StorageProxy, "sandbox_volume_sync_replicas")
	storageSyncJournal := tableRef(r.schemas.StorageProxy, "sandbox_volume_sync_journal")
	storageSyncConflicts := tableRef(r.schemas.StorageProxy, "sandbox_volume_sync_conflicts")
	storageSyncRequests := tableRef(r.schemas.StorageProxy, "sandbox_volume_sync_requests")
	storageSyncRetention := tableRef(r.schemas.StorageProxy, "sandbox_volume_sync_retention")
	storageSyncNamespacePolicy := tableRef(r.schemas.StorageProxy, "sandbox_volume_sync_namespace_policy")

	return []countQuery{
		{
			category: "api_keys",
			table:    apiKeys,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id::text = $1`, apiKeys),
		},
		{
			category: "ssh_public_keys",
			table:    sshKeys,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE COALESCE(team_id, '') = $1`, sshKeys),
		},
		{
			category: "scheduler_templates",
			table:    schedulerTemplates,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE scope = 'team' AND team_id = $1`, schedulerTemplates),
		},
		{
			category: "scheduler_template_allocations",
			table:    schedulerAllocations,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE scope = 'team' AND team_id = $1`, schedulerAllocations),
		},
		{
			category: "scheduler_template_builds",
			table:    schedulerTemplateBuilds,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE scope = 'team' AND team_id = $1`, schedulerTemplateBuilds),
		},
		{
			category: "credential_sources",
			table:    credentialSources,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, credentialSources),
		},
		{
			category: "credential_source_versions",
			table:    credentialSourceVersions,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s v
				JOIN %s s ON s.id = v.source_id
				WHERE s.team_id = $1
			`, credentialSourceVersions, credentialSources),
		},
		{
			category: "sandbox_egress_credential_bindings",
			table:    credentialBindings,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, credentialBindings),
		},
		{
			category: "sandboxes",
			table:    managerSandboxes,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1 AND deleted_at IS NULL`, managerSandboxes),
		},
		{
			category: "sandbox_cleanup_pending",
			table:    managerSandboxes,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s
				WHERE team_id = $1
				  AND deleted_at IS NOT NULL
				  AND cleanup_completed_at IS NULL
			`, managerSandboxes),
		},
		{
			category: "sandbox_lifecycle_transactions",
			table:    managerLifecycleTxns,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s tx
				JOIN %s s ON s.sandbox_id = tx.sandbox_id
				WHERE s.team_id = $1
				  AND tx.phase IN ('preparing', 'barriered', 'publishing', 'committing')
			`, managerLifecycleTxns, managerSandboxes),
		},
		{
			category: "sandbox_rootfs_states",
			table:    managerRootFSStates,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSStates),
		},
		{
			category: "sandbox_rootfs_heads",
			table:    managerRootFSHeads,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSHeads),
		},
		{
			category: "sandbox_rootfs_bindings",
			table:    managerRootFSBindings,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSBindings),
		},
		{
			category: "rootfs_filesystems",
			table:    managerRootFSFilesystems,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSFilesystems),
		},
		{
			category: "rootfs_snapshots",
			table:    managerRootFSSnapshots,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSSnapshots),
		},
		{
			category: "rootfs_layers",
			table:    managerRootFSLayers,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSLayers),
		},
		{
			category: "rootfs_objects",
			table:    managerRootFSObjects,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1 AND deleted_at IS NULL`, managerRootFSObjects),
		},
		{
			category: "rootfs_object_deletions",
			table:    managerRootFSObjectDeletions,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, managerRootFSObjectDeletions),
		},
		{
			category: "sandbox_volumes",
			table:    storageVolumes,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageVolumes),
		},
		{
			category: "sandbox_volume_snapshots",
			table:    storageSnapshots,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageSnapshots),
		},
		{
			category: "sandbox_volume_mounts",
			table:    storageMounts,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s m
				JOIN %s v ON v.id = m.volume_id
				WHERE v.team_id = $1
			`, storageMounts, storageVolumes),
		},
		{
			category: "snapshot_coordinations",
			table:    storageCoordinations,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s c
				JOIN %s v ON v.id = c.volume_id
				WHERE v.team_id = $1
			`, storageCoordinations, storageVolumes),
		},
		{
			category: "snapshot_flush_responses",
			table:    storageFlushResponses,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s r
				JOIN %s c ON c.id = r.coord_id
				JOIN %s v ON v.id = c.volume_id
				WHERE v.team_id = $1
			`, storageFlushResponses, storageCoordinations, storageVolumes),
		},
		{
			category: "sandbox_volume_owners",
			table:    storageOwners,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s o
				JOIN %s v ON v.id = o.volume_id
				WHERE v.team_id = $1
			`, storageOwners, storageVolumes),
		},
		{
			category: "sandbox_volume_s0fs_heads",
			table:    storageS0FSHeads,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s h
				JOIN %s v ON v.id = h.volume_id
				WHERE v.team_id = $1
			`, storageS0FSHeads, storageVolumes),
		},
		{
			category: "sandbox_volume_handoffs",
			table:    storageHandoffs,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s h
				JOIN %s v ON v.id = h.volume_id
				WHERE v.team_id = $1
			`, storageHandoffs, storageVolumes),
		},
		{
			category: "sandbox_volume_sync_replicas",
			table:    storageSyncReplicas,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageSyncReplicas),
		},
		{
			category: "sandbox_volume_sync_journal",
			table:    storageSyncJournal,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageSyncJournal),
		},
		{
			category: "sandbox_volume_sync_conflicts",
			table:    storageSyncConflicts,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageSyncConflicts),
		},
		{
			category: "sandbox_volume_sync_requests",
			table:    storageSyncRequests,
			sql: fmt.Sprintf(`
				SELECT COUNT(*)
				FROM %s r
				JOIN %s v ON v.id = r.volume_id
				WHERE v.team_id = $1
			`, storageSyncRequests, storageVolumes),
		},
		{
			category: "sandbox_volume_sync_retention",
			table:    storageSyncRetention,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageSyncRetention),
		},
		{
			category: "sandbox_volume_sync_namespace_policy",
			table:    storageSyncNamespacePolicy,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id = $1`, storageSyncNamespacePolicy),
		},
	}
}

func (r *Repository) retainedQueries() []countQuery {
	return nil
}

func (r *Repository) countOptional(ctx context.Context, table, query, teamID string) (int64, error) {
	var tableName sql.NullString
	if err := r.pool.QueryRow(ctx, `SELECT to_regclass($1)::text`, table).Scan(&tableName); err != nil {
		return 0, fmt.Errorf("check resource table %s: %w", table, err)
	}
	if !tableName.Valid {
		return 0, nil
	}

	var count int64
	if err := r.pool.QueryRow(ctx, query, teamID).Scan(&count); err != nil {
		if isUndefinedRelation(err) {
			return 0, nil
		}
		return 0, fmt.Errorf("count team resources in %s: %w", table, err)
	}
	return count, nil
}

func (r *Repository) discoverTeamIDQueries(ctx context.Context, known []countQuery, schemas []string) ([]countQuery, error) {
	schemas = compactSchemas(schemas)
	if len(schemas) == 0 {
		return nil, nil
	}

	knownTables, err := r.resolveKnownTables(ctx, known)
	if err != nil {
		return nil, err
	}

	rows, err := r.pool.Query(ctx, `
		SELECT table_schema, table_name
		FROM information_schema.columns
		WHERE column_name = 'team_id'
		  AND table_schema = ANY($1::text[])
		GROUP BY table_schema, table_name
		ORDER BY table_schema, table_name
	`, schemas)
	if err != nil {
		return nil, fmt.Errorf("discover team-scoped resource tables: %w", err)
	}
	defer rows.Close()

	var queries []countQuery
	for rows.Next() {
		var schema, tableName string
		if err := rows.Scan(&schema, &tableName); err != nil {
			return nil, fmt.Errorf("scan team-scoped resource table: %w", err)
		}
		if isIgnoredDiscoveredTable(schema, tableName) || knownTables[tableKey(schema, tableName)] {
			continue
		}
		table := tableRef(schema, tableName)
		queries = append(queries, countQuery{
			category: discoveredCategory(schema, tableName),
			table:    table,
			sql:      fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE team_id::text = $1`, table),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate team-scoped resource tables: %w", err)
	}
	return queries, nil
}

func (r *Repository) blockingDiscoverySchemas(ctx context.Context) ([]string, error) {
	gatewaySchema, err := r.currentSchema(ctx)
	if err != nil {
		return nil, err
	}
	return []string{
		gatewaySchema,
		r.schemas.Scheduler,
		r.schemas.Manager,
		r.schemas.StorageProxy,
	}, nil
}

func (r *Repository) retainedDiscoverySchemas() []string {
	return nil
}

func (r *Repository) currentSchema(ctx context.Context) (string, error) {
	var schema string
	if err := r.pool.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema); err != nil {
		return "", fmt.Errorf("get current schema: %w", err)
	}
	return schema, nil
}

func (r *Repository) resolveKnownTables(ctx context.Context, queries []countQuery) (map[string]bool, error) {
	known := make(map[string]bool, len(queries))
	for _, query := range queries {
		var schema, table string
		err := r.pool.QueryRow(ctx, `
			SELECT n.nspname, c.relname
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE c.oid = to_regclass($1)
		`, query.table).Scan(&schema, &table)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("resolve resource table %s: %w", query.table, err)
		}
		known[tableKey(schema, table)] = true
	}
	return known, nil
}

func compactSchemas(schemas []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(schemas))
	for _, schema := range schemas {
		schema = strings.TrimSpace(schema)
		if schema == "" || seen[schema] {
			continue
		}
		seen[schema] = true
		out = append(out, schema)
	}
	return out
}

func isIgnoredDiscoveredTable(schema string, table string) bool {
	if schema == "quota" {
		switch table {
		case "region_default_policies",
			"team_policies",
			"team_states",
			"team_usage",
			"allocations",
			"allocation_items",
			"transfer_operations",
			"transfer_items":
			return true
		}
	}
	switch table {
	case "team_members":
		return true
	default:
		return false
	}
}

func discoveredCategory(schema, table string) string {
	if strings.TrimSpace(schema) == "" {
		return table
	}
	return schema + "." + table
}

func tableKey(schema, table string) string {
	return schema + "." + table
}

func tableRef(schema, table string) string {
	if strings.TrimSpace(schema) == "" {
		return pgx.Identifier{table}.Sanitize()
	}
	return pgx.Identifier{schema, table}.Sanitize()
}

func isUndefinedRelation(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == "42P01" || pgErr.Code == "3F000"
}
