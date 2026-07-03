package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
)

type schemaExecer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func SchemaStatements(cfg Config) ([]string, error) {
	cfg, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}

	database := quoteIdentifier(cfg.Database)
	eventsTable := qualifiedEventsTable(cfg)
	logsTable := qualifiedLogsTable(cfg)
	metricsTable := qualifiedMetricsTable(cfg)
	eventsTTL := fmt.Sprintf("toDateTime(occurred_at) + INTERVAL %d DAY DELETE", cfg.RetentionDays)
	logsTTL := fmt.Sprintf("toDateTime(occurred_at) + INTERVAL %d DAY DELETE", cfg.LogsRetentionDays)
	metricsTTL := fmt.Sprintf("toDateTime(occurred_at) + INTERVAL %d DAY DELETE", cfg.MetricsRetentionDays)

	return []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	team_id String,
	sandbox_id String,
	region_id LowCardinality(String),
	cluster_id LowCardinality(String),
	occurred_at DateTime64(9, 'UTC'),
	ingested_at DateTime64(9, 'UTC'),
	source LowCardinality(String),
	event_type LowCardinality(String),
	outcome LowCardinality(String),
	cursor String,
	watermark String,
	attributes String,
	version UInt64 MATERIALIZED toUnixTimestamp64Nano(ingested_at)
)
	ENGINE = ReplacingMergeTree(version)
	PARTITION BY toYYYYMM(occurred_at)
	ORDER BY (team_id, sandbox_id, occurred_at, source, event_type, cursor)
	TTL %s
	SETTINGS index_granularity = 8192`, eventsTable, eventsTTL),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	team_id String,
	sandbox_id String,
	region_id LowCardinality(String),
	cluster_id LowCardinality(String),
	context_id String,
	process_id String,
	occurred_at DateTime64(9, 'UTC'),
	ingested_at DateTime64(9, 'UTC'),
	stream LowCardinality(String),
	message String,
	cursor String,
	attributes String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (team_id, sandbox_id, occurred_at, stream, cursor)
TTL %s
SETTINGS index_granularity = 8192`, logsTable, logsTTL),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	team_id String,
	sandbox_id String,
	region_id LowCardinality(String),
	cluster_id LowCardinality(String),
	context_id String,
	occurred_at DateTime64(9, 'UTC'),
	ingested_at DateTime64(9, 'UTC'),
	name LowCardinality(String),
	unit LowCardinality(String),
	value Float64,
	cursor String,
	attributes String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (team_id, sandbox_id, name, occurred_at, context_id, cursor)
TTL %s
SETTINGS index_granularity = 8192`, metricsTable, metricsTTL),
		fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", eventsTable, eventsTTL),
		fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", logsTable, logsTTL),
		fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", metricsTable, metricsTTL),
	}, nil
}

func EnsureSchema(ctx context.Context, db schemaExecer, cfg Config) error {
	if db == nil {
		return fmt.Errorf("clickhouse db is nil")
	}
	statements, err := SchemaStatements(cfg)
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply clickhouse schema: %w", err)
		}
	}
	return nil
}
