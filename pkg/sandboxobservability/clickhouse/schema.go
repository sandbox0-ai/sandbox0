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
	runtimeSamplesTable := qualifiedRuntimeSamplesTable(cfg)
	eventsTTL := fmt.Sprintf("toDateTime(ingested_at) + INTERVAL %d DAY DELETE", cfg.RetentionDays)
	logsTTL := fmt.Sprintf("toDateTime(occurred_at) + INTERVAL %d DAY DELETE", cfg.LogsRetentionDays)
	runtimeSamplesTTL := fmt.Sprintf("toDateTime(observed_at) + INTERVAL %d DAY DELETE", cfg.RuntimeSamplesRetentionDays)

	return []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", database),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	event_id String,
	schema_version UInt16,
	team_id String,
	sandbox_id String,
	region_id LowCardinality(String),
	cluster_id LowCardinality(String),
	occurred_at DateTime64(9, 'UTC'),
	ingested_at DateTime64(9, 'UTC'),
	source LowCardinality(String),
	event_type LowCardinality(String),
	phase LowCardinality(String),
	outcome LowCardinality(String),
	actor_kind LowCardinality(String),
	actor_id String,
	actor_user_id String,
	actor_api_key_id String,
	actor_auth_method LowCardinality(String),
	action LowCardinality(String),
	resource_type LowCardinality(String),
	resource_id String,
	resource_subresource String,
	operation_id String,
	parent_event_id String,
	producer_service LowCardinality(String),
	producer_instance String,
	producer_sequence UInt64,
	request_id String,
	trace_id String,
	source_ip String,
	user_agent String,
	http_method LowCardinality(String),
	route String,
	status_code UInt16,
	cursor String,
	watermark String,
	attributes String,
	integrity_algorithm LowCardinality(String),
	payload_hash FixedString(64),
	signature String,
	signing_key_id FixedString(64),
	version UInt64 MATERIALIZED toUnixTimestamp64Nano(ingested_at)
)
	ENGINE = ReplacingMergeTree(version)
	PARTITION BY toYYYYMM(occurred_at)
	ORDER BY (team_id, sandbox_id, occurred_at, event_id, payload_hash)
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
	runtime_generation Int64,
	series_epoch String,
	observed_at DateTime64(9, 'UTC'),
	ingested_at DateTime64(9, 'UTC'),
	sample_id String,
	cpu_utilization Nullable(Float64),
	cpu_usage Nullable(Float64),
	cpu_time_seconds Nullable(Float64),
	cpu_limit_cores Nullable(Float64),
	memory_usage_bytes Nullable(UInt64),
	memory_working_set_bytes Nullable(UInt64),
	memory_available_bytes Nullable(UInt64),
	memory_limit_bytes Nullable(UInt64),
	memory_utilization Nullable(Float64),
	network_receive_bytes Nullable(UInt64),
	network_transmit_bytes Nullable(UInt64),
	network_receive_errors Nullable(UInt64),
	network_transmit_errors Nullable(UInt64),
	process_count Nullable(UInt64),
	rootfs_writable_usage_bytes Nullable(UInt64),
	rootfs_writable_inodes Nullable(UInt64),
	missing String,
	version UInt64 MATERIALIZED toUnixTimestamp64Nano(ingested_at)
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY toYYYYMM(observed_at)
ORDER BY (team_id, sandbox_id, observed_at, runtime_generation, series_epoch, sample_id)
TTL %s
SETTINGS index_granularity = 8192`, runtimeSamplesTable, runtimeSamplesTTL),
		fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", eventsTable, eventsTTL),
		fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", logsTable, logsTTL),
		fmt.Sprintf("ALTER TABLE %s MODIFY TTL %s", runtimeSamplesTable, runtimeSamplesTTL),
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
