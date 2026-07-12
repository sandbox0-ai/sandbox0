package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type OpenConfig struct {
	DSN                string
	Schema             Config
	Migrate            bool
	RequireAuditSchema bool
}

func Open(ctx context.Context, cfg OpenConfig) (*sql.DB, *Repository, error) {
	dsn := strings.TrimSpace(cfg.DSN)
	if dsn == "" {
		return nil, nil, fmt.Errorf("clickhouse dsn is required")
	}

	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("open clickhouse: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = db.Close()
		}
	}()

	if err := db.PingContext(ctx); err != nil {
		return nil, nil, fmt.Errorf("ping clickhouse: %w", err)
	}
	if cfg.Migrate {
		if err := EnsureSchema(ctx, db, cfg.Schema); err != nil {
			return nil, nil, err
		}
	}
	if cfg.RequireAuditSchema {
		if err := validateAuditEventTable(ctx, db, cfg.Schema); err != nil {
			return nil, nil, err
		}
	}
	repo, err := NewRepository(db, cfg.Schema)
	if err != nil {
		return nil, nil, err
	}
	cleanup = false
	return db, repo, nil
}

func validateAuditEventTable(ctx context.Context, db *sql.DB, cfg Config) error {
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return err
	}
	var engine, sortingKey, partitionKey string
	if err := db.QueryRowContext(ctx,
		"SELECT engine, sorting_key, partition_key FROM system.tables WHERE database = ? AND name = ?",
		normalized.Database, normalized.EventsTable,
	).Scan(&engine, &sortingKey, &partitionKey); err != nil {
		return fmt.Errorf("inspect ClickHouse audit event table: %w", err)
	}
	rows, err := db.QueryContext(ctx,
		"SELECT name FROM system.columns WHERE database = ? AND table = ?",
		normalized.Database, normalized.EventsTable,
	)
	if err != nil {
		return fmt.Errorf("inspect ClickHouse audit event columns: %w", err)
	}
	defer rows.Close()
	columns := make([]string, 0, 40)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return fmt.Errorf("scan ClickHouse audit event column: %w", err)
		}
		columns = append(columns, name)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan ClickHouse audit event columns: %w", err)
	}
	if err := validateAuditEventTableMetadata(engine, sortingKey, partitionKey, columns); err != nil {
		return fmt.Errorf("ClickHouse table %s.%s is not the canonical audit v2 schema: %w", normalized.Database, normalized.EventsTable, err)
	}
	return nil
}

func validateAuditEventTableMetadata(engine, sortingKey, partitionKey string, columns []string) error {
	if strings.TrimSpace(engine) != "ReplacingMergeTree" {
		return fmt.Errorf("engine is %q, want ReplacingMergeTree", engine)
	}
	normalizeExpression := func(value string) string {
		replacer := strings.NewReplacer("`", "", " ", "", "\n", "", "\t", "", "(", "", ")", "")
		return strings.ToLower(replacer.Replace(value))
	}
	if got, want := normalizeExpression(sortingKey), "team_id,sandbox_id,occurred_at,event_id,payload_hash"; got != want {
		return fmt.Errorf("sorting key is %q, want %q", sortingKey, want)
	}
	if got, want := normalizeExpression(partitionKey), "toyyyymmingested_at"; got != want {
		return fmt.Errorf("partition key is %q, want toYYYYMM(ingested_at)", partitionKey)
	}
	required := []string{
		"event_id", "schema_version", "team_id", "sandbox_id", "region_id", "cluster_id",
		"occurred_at", "ingested_at", "source", "event_type", "phase", "outcome",
		"actor_kind", "actor_id", "actor_user_id", "actor_api_key_id", "actor_auth_method",
		"action", "resource_type", "resource_id", "resource_subresource", "operation_id",
		"parent_event_id", "producer_service", "producer_instance", "producer_sequence",
		"request_id", "trace_id", "source_ip", "user_agent", "http_method", "route",
		"status_code", "cursor", "watermark", "attributes", "integrity_algorithm",
		"payload_hash", "signature", "signing_key_id", "version",
	}
	available := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		available[column] = struct{}{}
	}
	for _, column := range required {
		if _, ok := available[column]; !ok {
			return fmt.Errorf("required column %q is missing", column)
		}
	}
	return nil
}
