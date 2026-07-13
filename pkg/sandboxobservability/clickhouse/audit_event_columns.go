package clickhouse

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

// auditEventColumnSpec is the canonical storage contract for one audit event
// column. An empty insertPlaceholder marks a ClickHouse-computed column.
type auditEventColumnSpec struct {
	name              string
	typeName          string
	defaultKind       string
	defaultExpression string
	insertPlaceholder string
}

var canonicalAuditEventColumns = []auditEventColumnSpec{
	{name: "event_id", typeName: "String", insertPlaceholder: "?"},
	{name: "schema_version", typeName: "UInt16", insertPlaceholder: "?"},
	{name: "team_id", typeName: "String", insertPlaceholder: "?"},
	{name: "sandbox_id", typeName: "String", insertPlaceholder: "?"},
	{name: "region_id", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "cluster_id", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "occurred_at", typeName: "DateTime64(9, 'UTC')", insertPlaceholder: dateTime64NanoPlaceholder},
	{name: "ingested_at", typeName: "DateTime64(9, 'UTC')", insertPlaceholder: dateTime64NanoPlaceholder},
	{name: "source", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "event_type", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "phase", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "outcome", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "actor_kind", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "actor_id", typeName: "String", insertPlaceholder: "?"},
	{name: "actor_user_id", typeName: "String", insertPlaceholder: "?"},
	{name: "actor_api_key_id", typeName: "String", insertPlaceholder: "?"},
	{name: "actor_auth_method", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "action", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "resource_type", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "resource_id", typeName: "String", insertPlaceholder: "?"},
	{name: "resource_subresource", typeName: "String", insertPlaceholder: "?"},
	{name: "operation_id", typeName: "String", insertPlaceholder: "?"},
	{name: "parent_event_id", typeName: "String", insertPlaceholder: "?"},
	{name: "producer_service", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "producer_instance", typeName: "String", insertPlaceholder: "?"},
	{name: "producer_sequence", typeName: "Int64", insertPlaceholder: "?"},
	{name: "request_id", typeName: "String", insertPlaceholder: "?"},
	{name: "trace_id", typeName: "String", insertPlaceholder: "?"},
	{name: "source_ip", typeName: "String", insertPlaceholder: "?"},
	{name: "user_agent", typeName: "String", insertPlaceholder: "?"},
	{name: "http_method", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "route", typeName: "String", insertPlaceholder: "?"},
	{name: "status_code", typeName: "UInt16", insertPlaceholder: "?"},
	{name: "attributes", typeName: "String", insertPlaceholder: "?"},
	{name: "integrity_algorithm", typeName: "LowCardinality(String)", insertPlaceholder: "?"},
	{name: "payload_hash", typeName: "FixedString(64)", insertPlaceholder: "?"},
	{name: "signature", typeName: "String", insertPlaceholder: "?"},
	{name: "signing_key_id", typeName: "FixedString(64)", insertPlaceholder: "?"},
	{
		name:              "version",
		typeName:          "UInt64",
		defaultKind:       "MATERIALIZED",
		defaultExpression: "toUnixTimestamp64Nano(ingested_at)",
	},
}

var (
	auditEventSelectColumns      = auditEventInsertColumnNames()
	auditEventInsertPlaceholders = auditEventInsertPlaceholderList()
)

func auditEventInsertColumnNames() string {
	names := make([]string, 0, len(canonicalAuditEventColumns))
	for _, column := range canonicalAuditEventColumns {
		if column.insertPlaceholder != "" {
			names = append(names, column.name)
		}
	}
	return strings.Join(names, ", ")
}

func auditEventInsertPlaceholderList() string {
	placeholders := make([]string, 0, len(canonicalAuditEventColumns))
	for _, column := range canonicalAuditEventColumns {
		if column.insertPlaceholder != "" {
			placeholders = append(placeholders, column.insertPlaceholder)
		}
	}
	return strings.Join(placeholders, ", ")
}

func auditEventInsertColumnCount() int {
	count := 0
	for _, column := range canonicalAuditEventColumns {
		if column.insertPlaceholder != "" {
			count++
		}
	}
	return count
}

func auditEventColumnDefinitions() string {
	var builder strings.Builder
	for i, column := range canonicalAuditEventColumns {
		if i > 0 {
			builder.WriteString(",\n")
		}
		builder.WriteByte('\t')
		builder.WriteString(column.name)
		builder.WriteByte(' ')
		builder.WriteString(column.typeName)
		if column.defaultKind != "" {
			builder.WriteByte(' ')
			builder.WriteString(column.defaultKind)
		}
		if column.defaultExpression != "" {
			builder.WriteByte(' ')
			builder.WriteString(column.defaultExpression)
		}
	}
	return builder.String()
}

// auditEventRow keeps the SQL representation separate from the public event
// model while retaining an explicit, reviewable field order in both directions.
type auditEventRow struct {
	event          sandboxobservability.Event
	source         string
	eventType      string
	phase          string
	outcome        string
	actorKind      string
	attributesJSON string
}

// auditEventColumnBinding couples the read and write adapters for one storage
// column. Keeping both directions in one ordered list prevents a column added
// in the middle of the schema from silently shifting only inserts or scans.
type auditEventColumnBinding struct {
	name            string
	scanDestination any
	insertValue     any
}

func newAuditEventRow(event sandboxobservability.Event) (auditEventRow, error) {
	attributes, err := encodeAttributes(event.Attributes)
	if err != nil {
		return auditEventRow{}, fmt.Errorf("encode attributes: %w", err)
	}
	return auditEventRow{
		event:          event,
		source:         string(event.Source),
		eventType:      string(event.EventType),
		phase:          string(event.Phase),
		outcome:        string(event.Outcome),
		actorKind:      string(event.Actor.Kind),
		attributesJSON: attributes,
	}, nil
}

func (row *auditEventRow) columnBindings() []auditEventColumnBinding {
	return []auditEventColumnBinding{
		{name: "event_id", scanDestination: &row.event.EventID, insertValue: row.event.EventID},
		{name: "schema_version", scanDestination: &row.event.SchemaVersion, insertValue: row.event.SchemaVersion},
		{name: "team_id", scanDestination: &row.event.TeamID, insertValue: row.event.TeamID},
		{name: "sandbox_id", scanDestination: &row.event.SandboxID, insertValue: row.event.SandboxID},
		{name: "region_id", scanDestination: &row.event.RegionID, insertValue: row.event.RegionID},
		{name: "cluster_id", scanDestination: &row.event.ClusterID, insertValue: row.event.ClusterID},
		{name: "occurred_at", scanDestination: &row.event.OccurredAt, insertValue: dateTime64NanoArg(row.event.OccurredAt)},
		{name: "ingested_at", scanDestination: &row.event.IngestedAt, insertValue: dateTime64NanoArg(row.event.IngestedAt)},
		{name: "source", scanDestination: &row.source, insertValue: row.source},
		{name: "event_type", scanDestination: &row.eventType, insertValue: row.eventType},
		{name: "phase", scanDestination: &row.phase, insertValue: row.phase},
		{name: "outcome", scanDestination: &row.outcome, insertValue: row.outcome},
		{name: "actor_kind", scanDestination: &row.actorKind, insertValue: row.actorKind},
		{name: "actor_id", scanDestination: &row.event.Actor.ID, insertValue: row.event.Actor.ID},
		{name: "actor_user_id", scanDestination: &row.event.Actor.UserID, insertValue: row.event.Actor.UserID},
		{name: "actor_api_key_id", scanDestination: &row.event.Actor.APIKeyID, insertValue: row.event.Actor.APIKeyID},
		{name: "actor_auth_method", scanDestination: &row.event.Actor.AuthMethod, insertValue: row.event.Actor.AuthMethod},
		{name: "action", scanDestination: &row.event.Action, insertValue: row.event.Action},
		{name: "resource_type", scanDestination: &row.event.Resource.Type, insertValue: row.event.Resource.Type},
		{name: "resource_id", scanDestination: &row.event.Resource.ID, insertValue: row.event.Resource.ID},
		{name: "resource_subresource", scanDestination: &row.event.Resource.Subresource, insertValue: row.event.Resource.Subresource},
		{name: "operation_id", scanDestination: &row.event.OperationID, insertValue: row.event.OperationID},
		{name: "parent_event_id", scanDestination: &row.event.ParentEventID, insertValue: row.event.ParentEventID},
		{name: "producer_service", scanDestination: &row.event.Producer.Service, insertValue: row.event.Producer.Service},
		{name: "producer_instance", scanDestination: &row.event.Producer.Instance, insertValue: row.event.Producer.Instance},
		{name: "producer_sequence", scanDestination: &row.event.Producer.Sequence, insertValue: row.event.Producer.Sequence},
		{name: "request_id", scanDestination: &row.event.Request.RequestID, insertValue: row.event.Request.RequestID},
		{name: "trace_id", scanDestination: &row.event.Request.TraceID, insertValue: row.event.Request.TraceID},
		{name: "source_ip", scanDestination: &row.event.Request.SourceIP, insertValue: row.event.Request.SourceIP},
		{name: "user_agent", scanDestination: &row.event.Request.UserAgent, insertValue: row.event.Request.UserAgent},
		{name: "http_method", scanDestination: &row.event.Request.HTTPMethod, insertValue: row.event.Request.HTTPMethod},
		{name: "route", scanDestination: &row.event.Request.Route, insertValue: row.event.Request.Route},
		{name: "status_code", scanDestination: &row.event.Request.StatusCode, insertValue: row.event.Request.StatusCode},
		{name: "attributes", scanDestination: &row.attributesJSON, insertValue: row.attributesJSON},
		{name: "integrity_algorithm", scanDestination: &row.event.Integrity.Algorithm, insertValue: row.event.Integrity.Algorithm},
		{name: "payload_hash", scanDestination: &row.event.Integrity.PayloadHash, insertValue: row.event.Integrity.PayloadHash},
		{name: "signature", scanDestination: &row.event.Integrity.Signature, insertValue: row.event.Integrity.Signature},
		{name: "signing_key_id", scanDestination: &row.event.Integrity.SigningKeyID, insertValue: row.event.Integrity.SigningKeyID},
	}
}

func (row *auditEventRow) scanDestinations() []any {
	bindings := row.columnBindings()
	destinations := make([]any, len(bindings))
	for i, binding := range bindings {
		destinations[i] = binding.scanDestination
	}
	return destinations
}

func (row *auditEventRow) insertValues() []any {
	bindings := row.columnBindings()
	values := make([]any, len(bindings))
	for i, binding := range bindings {
		values[i] = binding.insertValue
	}
	return values
}

func (row auditEventRow) toEvent() (sandboxobservability.Event, error) {
	attributes, err := decodeAttributes(row.attributesJSON)
	if err != nil {
		return sandboxobservability.Event{}, err
	}
	event := row.event
	event.OccurredAt = event.OccurredAt.UTC()
	event.IngestedAt = event.IngestedAt.UTC()
	event.Source = sandboxobservability.Source(row.source)
	event.EventType = sandboxobservability.EventType(row.eventType)
	event.Phase = sandboxobservability.EventPhase(row.phase)
	event.Outcome = sandboxobservability.Outcome(row.outcome)
	event.Actor.Kind = sandboxobservability.ActorKind(row.actorKind)
	event.Attributes = attributes
	return event, nil
}
