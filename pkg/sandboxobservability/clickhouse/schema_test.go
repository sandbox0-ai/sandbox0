package clickhouse

import (
	"strings"
	"testing"
)

func TestSchemaStatementsUseReplacingMergeTreeAndRetentionTTL(t *testing.T) {
	statements, err := SchemaStatements(Config{
		RetentionDays:        7,
		LogsRetentionDays:    3,
		MetricsRetentionDays: 14,
	})
	if err != nil {
		t.Fatalf("SchemaStatements() error = %v", err)
	}
	if len(statements) != 7 {
		t.Fatalf("statement count = %d, want 7", len(statements))
	}
	createEventsTable := statements[1]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `sandbox0_observability`.`sandbox_events`",
		"ENGINE = ReplacingMergeTree(version)",
		"ORDER BY (team_id, sandbox_id, occurred_at, source, event_type, cursor)",
		"TTL toDateTime(occurred_at) + INTERVAL 7 DAY DELETE",
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
	createMetricsTable := statements[3]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `sandbox0_observability`.`sandbox_metric_samples`",
		"ENGINE = MergeTree",
		"ORDER BY (team_id, sandbox_id, name, occurred_at, context_id, cursor)",
		"TTL toDateTime(occurred_at) + INTERVAL 14 DAY DELETE",
	} {
		if !strings.Contains(createMetricsTable, want) {
			t.Fatalf("create metrics table statement missing %q:\n%s", want, createMetricsTable)
		}
	}
	if !strings.Contains(statements[4], "ALTER TABLE `sandbox0_observability`.`sandbox_events` MODIFY TTL toDateTime(occurred_at) + INTERVAL 7 DAY DELETE") {
		t.Fatalf("events alter ttl statement = %q", statements[4])
	}
	if !strings.Contains(statements[5], "ALTER TABLE `sandbox0_observability`.`sandbox_logs` MODIFY TTL toDateTime(occurred_at) + INTERVAL 3 DAY DELETE") {
		t.Fatalf("logs alter ttl statement = %q", statements[5])
	}
	if !strings.Contains(statements[6], "ALTER TABLE `sandbox0_observability`.`sandbox_metric_samples` MODIFY TTL toDateTime(occurred_at) + INTERVAL 14 DAY DELETE") {
		t.Fatalf("metrics alter ttl statement = %q", statements[6])
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
	if _, err := SchemaStatements(Config{MetricsTable: "metrics.v2"}); err == nil {
		t.Fatal("SchemaStatements() error = nil, want unsafe metrics table identifier rejection")
	}
}
