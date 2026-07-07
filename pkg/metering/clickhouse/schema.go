package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
)

func ApplySchema(ctx context.Context, db *sql.DB, cfg Config) error {
	if db == nil {
		return fmt.Errorf("clickhouse db is nil")
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return err
	}
	for _, stmt := range schemaStatements(normalized) {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply clickhouse metering schema: %w", err)
		}
	}
	return nil
}

func schemaStatements(cfg Config) []string {
	events := qualified(cfg.Database, cfg.EventsTable)
	windows := qualified(cfg.Database, cfg.WindowsTable)
	watermarks := qualified(cfg.Database, cfg.WatermarksTable)
	sandboxState := qualified(cfg.Database, cfg.SandboxStateTable)
	storageState := qualified(cfg.Database, cfg.StorageStateTable)
	return []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", quoteIdentifier(cfg.Database)),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    event_id String,
    producer String,
    region_id String,
    event_type String,
    subject_type String,
    subject_id String,
    team_id String,
    user_id String,
    sandbox_id String,
    volume_id String,
    snapshot_id String,
    template_id String,
    cluster_id String,
    occurred_at DateTime64(9, 'UTC'),
    recorded_at DateTime64(9, 'UTC'),
    version UInt64,
    data String
) ENGINE = ReplacingMergeTree(version)
ORDER BY (region_id, producer, event_id)
`, events),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    window_id String,
    producer String,
    region_id String,
    window_type String,
    subject_type String,
    subject_id String,
    team_id String,
    user_id String,
    sandbox_id String,
    volume_id String,
    snapshot_id String,
    template_id String,
    cluster_id String,
    window_start DateTime64(9, 'UTC'),
    window_end DateTime64(9, 'UTC'),
    value Int64,
    unit String,
    recorded_at DateTime64(9, 'UTC'),
    version UInt64,
    data String
) ENGINE = ReplacingMergeTree(version)
ORDER BY (region_id, producer, window_id)
`, windows),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    producer String,
    region_id String,
    complete_before DateTime64(9, 'UTC'),
    updated_at DateTime64(9, 'UTC'),
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (region_id, producer)
`, watermarks),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    sandbox_id String,
    namespace String,
    team_id String,
    user_id String,
    template_id String,
    cluster_id String,
    owner_kind String,
    resource_millicpu Int64,
    resource_memory_mib Int64,
    claimed_at Nullable(DateTime64(9, 'UTC')),
    active_since Nullable(DateTime64(9, 'UTC')),
    paused UInt8,
    paused_at Nullable(DateTime64(9, 'UTC')),
    terminated_at Nullable(DateTime64(9, 'UTC')),
    last_observed_at DateTime64(9, 'UTC'),
    last_resource_version String,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (sandbox_id)
`, sandboxState),
		fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    subject_type String,
    subject_id String,
    product String,
    owner_kind String,
    team_id String,
    user_id String,
    sandbox_id String,
    volume_id String,
    snapshot_id String,
    cluster_id String,
    region_id String,
    size_bytes Int64,
    observed_at DateTime64(9, 'UTC'),
    unbilled_byte_nanoseconds Int64,
    deleted UInt8,
    version UInt64
) ENGINE = ReplacingMergeTree(version)
ORDER BY (subject_type, subject_id)
`, storageState),
	}
}
