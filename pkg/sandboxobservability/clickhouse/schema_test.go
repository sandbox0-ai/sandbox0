package clickhouse

import (
	"strings"
	"testing"
)

func TestSchemaStatementsUseReplacingMergeTreeAndRetentionTTL(t *testing.T) {
	statements, err := SchemaStatements(Config{
		RetentionDays:               7,
		LogsRetentionDays:           3,
		RuntimeSamplesRetentionDays: 14,
	})
	if err != nil {
		t.Fatalf("SchemaStatements() error = %v", err)
	}
	if len(statements) != 7 {
		t.Fatalf("statement count = %d, want 7", len(statements))
	}
	createEventsTable := statements[1]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `sandbox0_observability`.`sandbox_audit_events`",
		"ENGINE = ReplacingMergeTree(version)",
		"PARTITION BY toYYYYMM(occurred_at)",
		"ORDER BY (team_id, sandbox_id, occurred_at, event_id, payload_hash)",
		"TTL toDateTime(ingested_at) + INTERVAL 7 DAY DELETE",
	} {
		if !strings.Contains(createEventsTable, want) {
			t.Fatalf("create events table statement missing %q:\n%s", want, createEventsTable)
		}
	}
	createLogsTable := statements[2]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `sandbox0_observability`.`sandbox_logs`",
		"ENGINE = MergeTree",
		"ORDER BY (team_id, sandbox_id, occurred_at, stream, cursor)",
		"TTL toDateTime(occurred_at) + INTERVAL 3 DAY DELETE",
	} {
		if !strings.Contains(createLogsTable, want) {
			t.Fatalf("create logs table statement missing %q:\n%s", want, createLogsTable)
		}
	}
	createRuntimeSamplesTable := statements[3]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `sandbox0_observability`.`sandbox_runtime_samples`",
		"runtime_generation Int64",
		"series_epoch String",
		"cpu_utilization Nullable(Float64)",
		"network_receive_bytes Nullable(UInt64)",
		"missing String",
		"ENGINE = ReplacingMergeTree(version)",
		"ORDER BY (team_id, sandbox_id, observed_at, runtime_generation, series_epoch, sample_id)",
		"TTL toDateTime(observed_at) + INTERVAL 14 DAY DELETE",
	} {
		if !strings.Contains(createRuntimeSamplesTable, want) {
			t.Fatalf("create runtime samples table statement missing %q:\n%s", want, createRuntimeSamplesTable)
		}
	}
	if !strings.Contains(statements[4], "ALTER TABLE `sandbox0_observability`.`sandbox_audit_events` MODIFY TTL toDateTime(ingested_at) + INTERVAL 7 DAY DELETE") {
		t.Fatalf("events alter ttl statement = %q", statements[4])
	}
	if !strings.Contains(statements[5], "ALTER TABLE `sandbox0_observability`.`sandbox_logs` MODIFY TTL toDateTime(occurred_at) + INTERVAL 3 DAY DELETE") {
		t.Fatalf("logs alter ttl statement = %q", statements[5])
	}
	if !strings.Contains(statements[6], "ALTER TABLE `sandbox0_observability`.`sandbox_runtime_samples` MODIFY TTL toDateTime(observed_at) + INTERVAL 14 DAY DELETE") {
		t.Fatalf("runtime samples alter ttl statement = %q", statements[6])
	}
}

func TestAuditPartitionKeyIsStableAcrossDeliveryRetries(t *testing.T) {
	statements, err := SchemaStatements(Config{})
	if err != nil {
		t.Fatalf("SchemaStatements() error = %v", err)
	}
	createEventsTable := statements[1]
	if !strings.Contains(createEventsTable, "PARTITION BY toYYYYMM(occurred_at)") {
		t.Fatalf("audit partition must use signed occurred_at:\n%s", createEventsTable)
	}
	if strings.Contains(createEventsTable, "PARTITION BY toYYYYMM(ingested_at)") {
		t.Fatalf("audit partition must not change when a retry receives a new ingested_at:\n%s", createEventsTable)
	}
}

func TestSchemaStatementsRejectUnsafeIdentifiers(t *testing.T) {
	if _, err := SchemaStatements(Config{Database: "sandbox0;DROP"}); err == nil {
		t.Fatal("SchemaStatements() error = nil, want unsafe database identifier rejection")
	}
	if _, err := SchemaStatements(Config{EventsTable: "events.v2"}); err == nil {
		t.Fatal("SchemaStatements() error = nil, want unsafe table identifier rejection")
	}
	if _, err := SchemaStatements(Config{LogsTable: "logs;DROP"}); err == nil {
		t.Fatal("SchemaStatements() error = nil, want unsafe logs table identifier rejection")
	}
	if _, err := SchemaStatements(Config{RuntimeSamplesTable: "runtime.v2"}); err == nil {
		t.Fatal("SchemaStatements() error = nil, want unsafe runtime samples table identifier rejection")
	}
}

func TestValidateAuditEventTableMetadataRejectsLegacySchema(t *testing.T) {
	legacyColumns := []string{"team_id", "sandbox_id", "occurred_at", "ingested_at", "source", "event_type"}
	if err := validateAuditEventTableMetadata(
		"ReplacingMergeTree",
		"team_id, sandbox_id, occurred_at, source, event_type, cursor",
		"toYYYYMM(occurred_at)",
		legacyColumns,
	); err == nil {
		t.Fatal("validateAuditEventTableMetadata() error = nil, want legacy schema rejection")
	}
}

func TestValidateAuditEventTableMetadataAcceptsCanonicalSchema(t *testing.T) {
	columns := strings.Split("event_id schema_version team_id sandbox_id region_id cluster_id occurred_at ingested_at source event_type phase outcome actor_kind actor_id actor_user_id actor_api_key_id actor_auth_method action resource_type resource_id resource_subresource operation_id parent_event_id producer_service producer_instance producer_sequence request_id trace_id source_ip user_agent http_method route status_code cursor watermark attributes integrity_algorithm payload_hash signature signing_key_id version", " ")
	if err := validateAuditEventTableMetadata(
		"ReplacingMergeTree",
		"team_id, sandbox_id, occurred_at, event_id, payload_hash",
		"toYYYYMM(occurred_at)",
		columns,
	); err != nil {
		t.Fatalf("validateAuditEventTableMetadata() error = %v", err)
	}
}
