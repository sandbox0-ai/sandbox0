package clickhouse

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

func TestAuditEventColumnSpecMatchesExplicitRowAdapters(t *testing.T) {
	event := sandboxobservability.Event{
		EventID:    "event-1",
		OccurredAt: time.Date(2026, 7, 1, 1, 2, 3, 4, time.UTC),
		IngestedAt: time.Date(2026, 7, 1, 1, 2, 4, 5, time.UTC),
		Attributes: map[string]any{"key": "value"},
	}
	row, err := newAuditEventRow(event)
	if err != nil {
		t.Fatalf("newAuditEventRow() error = %v", err)
	}

	wantCount := auditEventInsertColumnCount()
	seen := make(map[string]struct{}, len(canonicalAuditEventColumns))
	wantBindingNames := make([]string, 0, wantCount)
	for _, column := range canonicalAuditEventColumns {
		if column.name == "" || column.typeName == "" {
			t.Fatalf("audit column has empty name or type: %#v", column)
		}
		if _, ok := seen[column.name]; ok {
			t.Fatalf("audit column %q is duplicated", column.name)
		}
		seen[column.name] = struct{}{}
		if column.insertPlaceholder != "" {
			wantBindingNames = append(wantBindingNames, column.name)
		}
	}
	if wantCount != 42 {
		t.Fatalf("insertable audit column count = %d, want 42", wantCount)
	}
	bindings := row.columnBindings()
	gotBindingNames := make([]string, len(bindings))
	for i, binding := range bindings {
		gotBindingNames[i] = binding.name
	}
	if !reflect.DeepEqual(gotBindingNames, wantBindingNames) {
		t.Fatalf("row binding order = %#v, want canonical column order %#v", gotBindingNames, wantBindingNames)
	}
	if got := len(strings.Split(auditEventSelectColumns, ", ")); got != wantCount {
		t.Fatalf("SELECT column count = %d, want %d", got, wantCount)
	}
	if got := strings.Count(auditEventInsertPlaceholders, "?"); got != wantCount {
		t.Fatalf("insert placeholder count = %d, want %d", got, wantCount)
	}
	if got := len(row.insertValues()); got != wantCount {
		t.Fatalf("insert row value count = %d, want %d", got, wantCount)
	}
	if got := len((&auditEventRow{}).scanDestinations()); got != wantCount {
		t.Fatalf("scan destination count = %d, want %d", got, wantCount)
	}
	for _, name := range strings.Split(auditEventSelectColumns, ", ") {
		if name == "version" {
			t.Fatalf("materialized version column must not be selected into an Event: %s", auditEventSelectColumns)
		}
	}
	if !strings.Contains(auditEventColumnDefinitions(), "version UInt64 MATERIALIZED toUnixTimestamp64Nano(ingested_at)") {
		t.Fatalf("column definitions missing materialized version:\n%s", auditEventColumnDefinitions())
	}
	if strings.Contains(auditEventColumnDefinitions(), "\tcursor ") || strings.Contains(auditEventColumnDefinitions(), "\twatermark ") {
		t.Fatalf("event schema contains query transport columns:\n%s", auditEventColumnDefinitions())
	}
}

func TestAuditEventRowExecutionScopeRoundTrip(t *testing.T) {
	event := sandboxobservability.Event{
		ExecutionScope: &sandboxobservability.ExecutionScope{
			Namespace:   "codex",
			Kind:        "native_session",
			ID:          "thread-1",
			Attribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
		},
	}
	row, err := newAuditEventRow(event)
	if err != nil {
		t.Fatalf("newAuditEventRow() error = %v", err)
	}
	got, err := row.toEvent()
	if err != nil {
		t.Fatalf("toEvent() error = %v", err)
	}
	if got.ExecutionScope == nil || *got.ExecutionScope != *event.ExecutionScope {
		t.Fatalf("execution scope = %#v, want %#v", got.ExecutionScope, event.ExecutionScope)
	}

	legacy, err := (auditEventRow{event: sandboxobservability.Event{SchemaVersion: sandboxobservability.LegacyEventSchemaVersion}}).toEvent()
	if err != nil {
		t.Fatalf("legacy toEvent() error = %v", err)
	}
	if legacy.ExecutionScope != nil {
		t.Fatalf("legacy execution scope = %#v, want nil", legacy.ExecutionScope)
	}
	if legacy.SchemaVersion != sandboxobservability.LegacyEventSchemaVersion {
		t.Fatalf("legacy schema version = %d, want %d", legacy.SchemaVersion, sandboxobservability.LegacyEventSchemaVersion)
	}
}

func TestAppendEventFiltersBuildsCanonicalSharedFilters(t *testing.T) {
	start := time.Date(2026, 7, 1, 1, 2, 3, 4, time.FixedZone("start", 8*60*60))
	end := time.Date(2026, 7, 1, 2, 3, 4, 5, time.FixedZone("end", -5*60*60))
	query := sandboxobservability.EventQuery{
		TeamID:                    "team-1",
		SandboxID:                 "sandbox-1",
		MaxSchemaVersion:          sandboxobservability.CurrentEventSchemaVersion,
		StartTime:                 &start,
		EndTime:                   &end,
		Source:                    sandboxobservability.SourceNetd,
		EventType:                 sandboxobservability.EventTypeNetworkAudit,
		Outcome:                   sandboxobservability.OutcomeDenied,
		ActorKind:                 sandboxobservability.ActorKindSandboxWorkload,
		ActorID:                   "actor-1",
		ExecutionScopeNamespace:   "codex",
		ExecutionScopeKind:        "native_session",
		ExecutionScopeID:          "thread-1",
		ExecutionScopeAttribution: sandboxobservability.ExecutionScopeAttributionProcessEnvironment,
		Action:                    "network.deny",
		ResourceType:              "sandbox_network",
		OperationID:               "operation-1",
	}

	var builder strings.Builder
	args := appendEventFilters(&builder, query)
	wantSQL := "team_id = ? AND sandbox_id = ? AND schema_version <= ?" +
		" AND occurred_at >= " + dateTime64NanoPlaceholder +
		" AND occurred_at <= " + dateTime64NanoPlaceholder +
		" AND source = ?" +
		" AND event_type = ?" +
		" AND outcome = ?" +
		" AND actor_kind = ?" +
		" AND actor_id = ?" +
		" AND execution_scope_namespace = ?" +
		" AND execution_scope_kind = ?" +
		" AND execution_scope_id = ?" +
		" AND execution_scope_attribution = ?" +
		" AND action = ?" +
		" AND resource_type = ?" +
		" AND operation_id = ?"
	if got := builder.String(); got != wantSQL {
		t.Fatalf("appendEventFilters() SQL =\n%s\nwant:\n%s", got, wantSQL)
	}
	wantArgs := []any{
		"team-1",
		"sandbox-1",
		sandboxobservability.CurrentEventSchemaVersion,
		dateTime64NanoArg(start),
		dateTime64NanoArg(end),
		string(sandboxobservability.SourceNetd),
		string(sandboxobservability.EventTypeNetworkAudit),
		string(sandboxobservability.OutcomeDenied),
		string(sandboxobservability.ActorKindSandboxWorkload),
		"actor-1",
		"codex",
		"native_session",
		"thread-1",
		string(sandboxobservability.ExecutionScopeAttributionProcessEnvironment),
		"network.deny",
		"sandbox_network",
		"operation-1",
	}
	if !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("appendEventFilters() args = %#v, want %#v", args, wantArgs)
	}
}
