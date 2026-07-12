package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"

	_ "github.com/ClickHouse/clickhouse-go/v2"
)

type OpenConfig struct {
	DSN                string
	Schema             Config
	Migrate            bool
	RequireAuditSchema bool
}

type auditTableMetadata struct {
	Engine           string
	EngineFull       string
	SortingKey       string
	PartitionKey     string
	CreateTableQuery string
}

type auditColumnMetadata struct {
	Name              string
	Type              string
	DefaultKind       string
	DefaultExpression string
}

var canonicalAuditColumns = []auditColumnMetadata{
	{Name: "event_id", Type: "String"},
	{Name: "schema_version", Type: "UInt16"},
	{Name: "team_id", Type: "String"},
	{Name: "sandbox_id", Type: "String"},
	{Name: "region_id", Type: "LowCardinality(String)"},
	{Name: "cluster_id", Type: "LowCardinality(String)"},
	{Name: "occurred_at", Type: "DateTime64(9, 'UTC')"},
	{Name: "ingested_at", Type: "DateTime64(9, 'UTC')"},
	{Name: "source", Type: "LowCardinality(String)"},
	{Name: "event_type", Type: "LowCardinality(String)"},
	{Name: "phase", Type: "LowCardinality(String)"},
	{Name: "outcome", Type: "LowCardinality(String)"},
	{Name: "actor_kind", Type: "LowCardinality(String)"},
	{Name: "actor_id", Type: "String"},
	{Name: "actor_user_id", Type: "String"},
	{Name: "actor_api_key_id", Type: "String"},
	{Name: "actor_auth_method", Type: "LowCardinality(String)"},
	{Name: "action", Type: "LowCardinality(String)"},
	{Name: "resource_type", Type: "LowCardinality(String)"},
	{Name: "resource_id", Type: "String"},
	{Name: "resource_subresource", Type: "String"},
	{Name: "operation_id", Type: "String"},
	{Name: "parent_event_id", Type: "String"},
	{Name: "producer_service", Type: "LowCardinality(String)"},
	{Name: "producer_instance", Type: "String"},
	{Name: "producer_sequence", Type: "UInt64"},
	{Name: "request_id", Type: "String"},
	{Name: "trace_id", Type: "String"},
	{Name: "source_ip", Type: "String"},
	{Name: "user_agent", Type: "String"},
	{Name: "http_method", Type: "LowCardinality(String)"},
	{Name: "route", Type: "String"},
	{Name: "status_code", Type: "UInt16"},
	{Name: "cursor", Type: "String"},
	{Name: "watermark", Type: "String"},
	{Name: "attributes", Type: "String"},
	{Name: "integrity_algorithm", Type: "LowCardinality(String)"},
	{Name: "payload_hash", Type: "FixedString(64)"},
	{Name: "signature", Type: "String"},
	{Name: "signing_key_id", Type: "FixedString(64)"},
	{Name: "version", Type: "UInt64", DefaultKind: "MATERIALIZED", DefaultExpression: "toUnixTimestamp64Nano(ingested_at)"},
}

var (
	auditTTLKeywordPattern      = regexp.MustCompile(`(?i)\bTTL\b`)
	auditSettingsKeywordPattern = regexp.MustCompile(`(?i)\bSETTINGS\b`)
)

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
	var table auditTableMetadata
	if err := db.QueryRowContext(ctx,
		"SELECT engine, engine_full, sorting_key, partition_key, create_table_query FROM system.tables WHERE database = ? AND name = ?",
		normalized.Database, normalized.EventsTable,
	).Scan(&table.Engine, &table.EngineFull, &table.SortingKey, &table.PartitionKey, &table.CreateTableQuery); err != nil {
		return fmt.Errorf("inspect ClickHouse audit event table: %w", err)
	}
	rows, err := db.QueryContext(ctx,
		"SELECT name, type, default_kind, default_expression FROM system.columns WHERE database = ? AND table = ?",
		normalized.Database, normalized.EventsTable,
	)
	if err != nil {
		return fmt.Errorf("inspect ClickHouse audit event columns: %w", err)
	}
	defer rows.Close()
	columns := make([]auditColumnMetadata, 0, len(canonicalAuditColumns))
	for rows.Next() {
		var column auditColumnMetadata
		if err := rows.Scan(&column.Name, &column.Type, &column.DefaultKind, &column.DefaultExpression); err != nil {
			return fmt.Errorf("scan ClickHouse audit event column: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan ClickHouse audit event columns: %w", err)
	}
	if err := validateCanonicalAuditEventTableMetadata(table, columns, normalized.RetentionDays); err != nil {
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
	if got, want := normalizeExpression(partitionKey), "toyyyymmoccurred_at"; got != want {
		return fmt.Errorf("partition key is %q, want toYYYYMM(occurred_at)", partitionKey)
	}
	available := make(map[string]struct{}, len(columns))
	for _, column := range columns {
		available[column] = struct{}{}
	}
	for _, column := range canonicalAuditColumns {
		if _, ok := available[column.Name]; !ok {
			return fmt.Errorf("required column %q is missing", column.Name)
		}
	}
	return nil
}

func validateCanonicalAuditEventTableMetadata(table auditTableMetadata, columns []auditColumnMetadata, retentionDays int) error {
	names := make([]string, 0, len(columns))
	available := make(map[string]auditColumnMetadata, len(columns))
	expectedNames := make(map[string]struct{}, len(canonicalAuditColumns))
	for _, expected := range canonicalAuditColumns {
		expectedNames[expected.Name] = struct{}{}
	}
	for _, column := range columns {
		if _, ok := expectedNames[column.Name]; !ok {
			return fmt.Errorf("unexpected column %q is present", column.Name)
		}
		if _, ok := available[column.Name]; ok {
			return fmt.Errorf("column %q is duplicated in metadata", column.Name)
		}
		names = append(names, column.Name)
		available[column.Name] = column
	}
	if err := validateAuditEventTableMetadata(table.Engine, table.SortingKey, table.PartitionKey, names); err != nil {
		return err
	}

	engineFull := normalizeAuditSchemaExpression(table.EngineFull)
	const expectedEngine = "replacingmergetree(version)"
	if engineFull != expectedEngine && !strings.HasPrefix(engineFull, expectedEngine+"partitionby") {
		return fmt.Errorf("engine_full is %q, want ReplacingMergeTree(version)", table.EngineFull)
	}

	for _, expected := range canonicalAuditColumns {
		actual, ok := available[expected.Name]
		if !ok {
			continue
		}
		if normalizeAuditSchemaExpression(actual.Type) != normalizeAuditSchemaExpression(expected.Type) {
			return fmt.Errorf("column %q type is %q, want %q", expected.Name, actual.Type, expected.Type)
		}
		if !strings.EqualFold(strings.TrimSpace(actual.DefaultKind), expected.DefaultKind) {
			return fmt.Errorf("column %q default kind is %q, want %q", expected.Name, actual.DefaultKind, expected.DefaultKind)
		}
		if normalizeAuditSchemaExpression(actual.DefaultExpression) != normalizeAuditSchemaExpression(expected.DefaultExpression) {
			return fmt.Errorf("column %q default expression is %q, want %q", expected.Name, actual.DefaultExpression, expected.DefaultExpression)
		}
	}

	if err := validateCanonicalAuditTTL(table.CreateTableQuery, retentionDays); err != nil {
		return err
	}
	return nil
}

func validateCanonicalAuditTTL(createTableQuery string, retentionDays int) error {
	ttlLocation := auditTTLKeywordPattern.FindStringIndex(createTableQuery)
	if ttlLocation == nil {
		return fmt.Errorf("table TTL is missing")
	}
	ttlClause := createTableQuery[ttlLocation[1]:]
	if settingsLocation := auditSettingsKeywordPattern.FindStringIndex(ttlClause); settingsLocation != nil {
		ttlClause = ttlClause[:settingsLocation[0]]
	}
	ttl := normalizeAuditSchemaExpression(ttlClause)
	ttl = strings.TrimSuffix(ttl, "delete")
	wants := []string{
		fmt.Sprintf("todatetime(ingested_at)+interval%dday", retentionDays),
		fmt.Sprintf("todatetime(ingested_at)+tointervalday(%d)", retentionDays),
	}
	for _, want := range wants {
		if ttl == want {
			return nil
		}
	}
	return fmt.Errorf("table TTL is %q, want retention from ingested_at for %d days", ttl, retentionDays)
}

func normalizeAuditSchemaExpression(value string) string {
	replacer := strings.NewReplacer("`", "", " ", "", "\n", "", "\r", "", "\t", "")
	return strings.ToLower(replacer.Replace(strings.TrimSpace(value)))
}
