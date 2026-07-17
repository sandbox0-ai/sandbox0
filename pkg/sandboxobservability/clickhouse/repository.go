package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const maxInsertBatchSize = 500
const dateTime64NanoPlaceholder = "fromUnixTimestamp64Nano(?, 'UTC')"
const auditInsertReliabilitySettings = " SETTINGS async_insert = 0, wait_for_async_insert = 1"

type sqlBackend interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type Repository struct {
	db                  sqlBackend
	cfg                 Config
	eventsTable         string
	logsTable           string
	runtimeSamplesTable string
	runtimeQuerySlots   chan struct{}
	loadRuntimeMetric   runtimeMetricLoader
	now                 func() time.Time
}

func NewRepository(db sqlBackend, cfg Config) (*Repository, error) {
	if db == nil {
		return nil, fmt.Errorf("clickhouse db is nil")
	}
	normalized, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	repository := &Repository{
		db:                  db,
		cfg:                 normalized,
		eventsTable:         qualifiedEventsTable(normalized),
		logsTable:           qualifiedLogsTable(normalized),
		runtimeSamplesTable: qualifiedRuntimeSamplesTable(normalized),
		runtimeQuerySlots:   make(chan struct{}, normalized.RuntimeQueryConcurrency),
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
	repository.loadRuntimeMetric = repository.queryRuntimeMetric
	return repository, nil
}

func (r *Repository) Database() string {
	return r.cfg.Database
}

func (r *Repository) EventsTable() string {
	return r.cfg.EventsTable
}

func (r *Repository) LogsTable() string {
	return r.cfg.LogsTable
}

func (r *Repository) RuntimeSamplesTable() string {
	return r.cfg.RuntimeSamplesTable
}

func (r *Repository) RetentionDays() int {
	return r.cfg.RetentionDays
}

func (r *Repository) LogsRetentionDays() int {
	return r.cfg.LogsRetentionDays
}

func (r *Repository) RuntimeSamplesRetentionDays() int {
	return r.cfg.RuntimeSamplesRetentionDays
}

func (r *Repository) ListEvents(ctx context.Context, query sandboxobservability.EventQuery) (*sandboxobservability.EventListResult, error) {
	return r.listEvents(ctx, query)
}

func (r *Repository) InsertEvents(ctx context.Context, events []sandboxobservability.Event) error {
	if len(events) == 0 {
		return nil
	}

	normalized := make([]sandboxobservability.Event, 0, len(events))
	now := r.now()
	for i, event := range events {
		normalizedEvent, err := normalizeEventForInsert(event, now)
		if err != nil {
			return fmt.Errorf("event %d: %w", i, err)
		}
		normalized = append(normalized, normalizedEvent)
	}

	for len(normalized) > 0 {
		chunkSize := len(normalized)
		if chunkSize > maxInsertBatchSize {
			chunkSize = maxInsertBatchSize
		}
		query, args, err := r.buildInsertSQL(normalized[:chunkSize])
		if err != nil {
			return err
		}
		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("%w: insert events: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
		normalized = normalized[chunkSize:]
	}
	return nil
}

func (r *Repository) listEvents(ctx context.Context, query sandboxobservability.EventQuery) (*sandboxobservability.EventListResult, error) {
	normalized, limit, page, err := normalizeQuery(query)
	if err != nil {
		return nil, err
	}

	sqlQuery, args := r.buildListSQL(normalized, limit+1, page)
	rows, err := r.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: query events: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil {
		return nil, fmt.Errorf("%w: scan events: %v", sandboxobservability.ErrBackendUnavailable, err)
	}

	nextCursor := ""
	if len(events) > limit {
		nextCursor, err = encodePageCursor(events[limit-1], normalized.MaxSchemaVersion)
		if err != nil {
			return nil, fmt.Errorf("%w: encode cursor: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
		events = events[:limit]
	}

	return &sandboxobservability.EventListResult{
		Events:         events,
		NextCursor:     nextCursor,
		Watermark:      lastWatermark(events),
		EffectiveQuery: sandboxobservability.EffectiveEventQuery(normalized),
	}, nil
}

func (r *Repository) buildInsertSQL(events []sandboxobservability.Event) (string, []any, error) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(r.eventsTable)
	builder.WriteString(" (")
	builder.WriteString(auditEventSelectColumns)
	builder.WriteString(")")
	builder.WriteString(auditInsertReliabilitySettings)
	builder.WriteString(" VALUES ")

	columnCount := auditEventInsertColumnCount()
	args := make([]any, 0, len(events)*columnCount)
	for i, event := range events {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(")
		builder.WriteString(auditEventInsertPlaceholders)
		builder.WriteString(")")
		row, err := newAuditEventRow(event)
		if err != nil {
			return "", nil, err
		}
		values := row.insertValues()
		if len(values) != columnCount {
			return "", nil, fmt.Errorf("audit event row has %d values for %d insert columns", len(values), columnCount)
		}
		args = append(args, values...)
	}

	return builder.String(), args, nil
}

func (r *Repository) buildListSQL(query sandboxobservability.EventQuery, limit int, cursor *pageCursor) (string, []any) {
	var builder strings.Builder
	builder.WriteString("SELECT ")
	builder.WriteString(auditEventSelectColumns)
	builder.WriteString(" FROM ")
	builder.WriteString(r.eventsTable)
	builder.WriteString(" FINAL WHERE ")

	args := appendEventFilters(&builder, query)
	if query.EventID != "" {
		builder.WriteString(" AND event_id = ?")
		args = append(args, query.EventID)
	}
	if cursor != nil {
		builder.WriteString(" AND (occurred_at, ingested_at, source, event_type, event_id, payload_hash) > (")
		builder.WriteString(dateTime64NanoPlaceholder + ", " + dateTime64NanoPlaceholder + ", ?, ?, ?, ?)")
		args = append(args, dateTime64NanoArg(cursor.OccurredAt), dateTime64NanoArg(cursor.IngestedAt), cursor.Source, cursor.EventType, cursor.Cursor, cursor.PayloadHash)
	}

	builder.WriteString(" ORDER BY occurred_at ASC, ingested_at ASC, source ASC, event_type ASC, event_id ASC, payload_hash ASC")
	builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return builder.String(), args
}

// appendEventFilters writes the filters shared by historical list and watch
// queries. Exact event ID lookup remains list-only.
func appendEventFilters(builder *strings.Builder, query sandboxobservability.EventQuery) []any {
	builder.WriteString("team_id = ? AND sandbox_id = ? AND schema_version <= ?")
	args := []any{query.TeamID, query.SandboxID, query.MaxSchemaVersion}
	if query.StartTime != nil {
		builder.WriteString(" AND occurred_at >= " + dateTime64NanoPlaceholder)
		args = append(args, dateTime64NanoArg(*query.StartTime))
	}
	if query.EndTime != nil {
		builder.WriteString(" AND occurred_at <= " + dateTime64NanoPlaceholder)
		args = append(args, dateTime64NanoArg(*query.EndTime))
	}
	if query.Source != "" {
		builder.WriteString(" AND source = ?")
		args = append(args, string(query.Source))
	}
	if query.EventType != "" {
		builder.WriteString(" AND event_type = ?")
		args = append(args, string(query.EventType))
	}
	if query.Outcome != "" {
		builder.WriteString(" AND outcome = ?")
		args = append(args, string(query.Outcome))
	}
	if query.ActorKind != "" {
		builder.WriteString(" AND actor_kind = ?")
		args = append(args, string(query.ActorKind))
	}
	if query.ActorID != "" {
		builder.WriteString(" AND actor_id = ?")
		args = append(args, query.ActorID)
	}
	if query.ExecutionScopeNamespace != "" {
		builder.WriteString(" AND execution_scope_namespace = ?")
		args = append(args, query.ExecutionScopeNamespace)
	}
	if query.ExecutionScopeKind != "" {
		builder.WriteString(" AND execution_scope_kind = ?")
		args = append(args, query.ExecutionScopeKind)
	}
	if query.ExecutionScopeID != "" {
		builder.WriteString(" AND execution_scope_id = ?")
		args = append(args, query.ExecutionScopeID)
	}
	if query.ExecutionScopeAttribution != "" {
		builder.WriteString(" AND execution_scope_attribution = ?")
		args = append(args, string(query.ExecutionScopeAttribution))
	}
	if query.Action != "" {
		builder.WriteString(" AND action = ?")
		args = append(args, query.Action)
	}
	if query.ResourceType != "" {
		builder.WriteString(" AND resource_type = ?")
		args = append(args, query.ResourceType)
	}
	if query.OperationID != "" {
		builder.WriteString(" AND operation_id = ?")
		args = append(args, query.OperationID)
	}
	return args
}

func normalizeQuery(query sandboxobservability.EventQuery) (sandboxobservability.EventQuery, int, *pageCursor, error) {
	query.TeamID = strings.TrimSpace(query.TeamID)
	query.SandboxID = strings.TrimSpace(query.SandboxID)
	query.Cursor = strings.TrimSpace(query.Cursor)
	maxSchemaVersionSpecified := query.MaxSchemaVersion != 0
	if query.TeamID == "" {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("team_id is required")
	}
	if query.SandboxID == "" {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("sandbox_id is required")
	}
	effectiveMaxSchemaVersion, ok := sandboxobservability.NormalizeEventMaxSchemaVersion(query.MaxSchemaVersion)
	if !ok {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: max_schema_version must be greater than or equal to 2", sandboxobservability.ErrInvalidQuery)
	}
	query.MaxSchemaVersion = effectiveMaxSchemaVersion
	if query.StartTime != nil {
		start := query.StartTime.UTC()
		if !sandboxobservability.ValidDateTime64Nano(start) {
			return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: start_time is outside the DateTime64(9) range", sandboxobservability.ErrInvalidQuery)
		}
		query.StartTime = &start
	}
	if query.EndTime != nil {
		end := query.EndTime.UTC()
		if !sandboxobservability.ValidDateTime64Nano(end) {
			return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: end_time is outside the DateTime64(9) range", sandboxobservability.ErrInvalidQuery)
		}
		query.EndTime = &end
	}
	if query.StartTime != nil && query.EndTime != nil && query.EndTime.Before(*query.StartTime) {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("end_time must be greater than or equal to start_time")
	}
	if query.Source != "" && !sandboxobservability.ValidSource(query.Source) {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("invalid source")
	}
	if query.EventType != "" && !sandboxobservability.ValidEventType(query.EventType) {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("invalid event_type")
	}
	if query.Outcome != "" && !sandboxobservability.ValidOutcome(query.Outcome) {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("invalid outcome")
	}
	query.ActorID = strings.TrimSpace(query.ActorID)
	query.ExecutionScopeNamespace = strings.TrimSpace(query.ExecutionScopeNamespace)
	query.ExecutionScopeKind = strings.TrimSpace(query.ExecutionScopeKind)
	query.ExecutionScopeID = strings.TrimSpace(query.ExecutionScopeID)
	query.ExecutionScopeAttribution = sandboxobservability.ExecutionScopeAttribution(strings.TrimSpace(string(query.ExecutionScopeAttribution)))
	query.Action = strings.TrimSpace(query.Action)
	query.ResourceType = strings.TrimSpace(query.ResourceType)
	query.OperationID = strings.TrimSpace(query.OperationID)
	query.EventID = strings.TrimSpace(query.EventID)
	if query.ActorKind != "" && !sandboxobservability.ValidActorKind(query.ActorKind) {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("invalid actor_kind")
	}
	if err := sandboxobservability.ValidateExecutionScopeFilter(query.ExecutionScopeNamespace, query.ExecutionScopeKind, query.ExecutionScopeID, query.ExecutionScopeAttribution); err != nil {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: invalid execution scope filter: %v", sandboxobservability.ErrInvalidQuery, err)
	}
	if hasExecutionScopeQueryFilter(query) && !maxSchemaVersionSpecified {
		query.MaxSchemaVersion = sandboxobservability.CurrentEventSchemaVersion
	}
	if hasExecutionScopeQueryFilter(query) && query.MaxSchemaVersion != sandboxobservability.CurrentEventSchemaVersion {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: execution scope filters require max_schema_version=3", sandboxobservability.ErrInvalidQuery)
	}
	if query.EventID != "" && (query.StartTime != nil || query.EndTime != nil || query.Cursor != "" || query.Source != "" || query.EventType != "" || query.Outcome != "" || query.ActorKind != "" || query.ActorID != "" || query.ExecutionScopeNamespace != "" || query.ExecutionScopeKind != "" || query.ExecutionScopeID != "" || query.ExecutionScopeAttribution != "" || query.Action != "" || query.ResourceType != "" || query.OperationID != "") {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("event_id cannot be combined with other event filters or cursor")
	}

	limit := query.Limit
	if query.EventID != "" {
		limit = 2
	} else if limit <= 0 {
		limit = DefaultQueryLimit
	} else if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}

	var cursor *pageCursor
	if query.Cursor != "" {
		decoded, err := decodePageCursor(query.Cursor)
		if err != nil {
			return sandboxobservability.EventQuery{}, 0, nil, err
		}
		if !sandboxobservability.ValidDateTime64Nano(decoded.OccurredAt) || !sandboxobservability.ValidDateTime64Nano(decoded.IngestedAt) {
			return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: timestamp is outside the DateTime64(9) range", sandboxobservability.ErrInvalidCursor)
		}
		cursorMaxSchemaVersion := decoded.MaxSchemaVersion
		if cursorMaxSchemaVersion == 0 {
			cursorMaxSchemaVersion = sandboxobservability.LegacyEventSchemaVersion
		}
		if cursorMaxSchemaVersion != query.MaxSchemaVersion {
			return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: max_schema_version does not match cursor", sandboxobservability.ErrInvalidCursor)
		}
		cursor = decoded
	}

	return query, limit, cursor, nil
}

func hasExecutionScopeQueryFilter(query sandboxobservability.EventQuery) bool {
	return query.ExecutionScopeNamespace != "" ||
		query.ExecutionScopeKind != "" ||
		query.ExecutionScopeID != "" ||
		query.ExecutionScopeAttribution != ""
}

func normalizeEventForInsert(event sandboxobservability.Event, now time.Time) (sandboxobservability.Event, error) {
	if err := sandboxobservability.ValidateSignedEvent(event); err != nil {
		return sandboxobservability.Event{}, err
	}
	event.OccurredAt = event.OccurredAt.UTC()
	if event.IngestedAt.IsZero() {
		event.IngestedAt = now
	}
	event.IngestedAt = event.IngestedAt.UTC()
	return event, nil
}

// dateTime64NanoArg preserves the exact timestamp protected by the audit
// signature. clickhouse-go binds a bare time.Time positional argument as a
// whole-second DateTime; passing signed Unix nanoseconds through
// fromUnixTimestamp64Nano also preserves pre-epoch DateTime64(9) values.
func dateTime64NanoArg(value time.Time) int64 {
	return value.UTC().UnixNano()
}

func scanEvents(rows *sql.Rows) ([]sandboxobservability.Event, error) {
	var events []sandboxobservability.Event
	for rows.Next() {
		var row auditEventRow
		if err := rows.Scan(row.scanDestinations()...); err != nil {
			return nil, err
		}
		event, err := row.toEvent()
		if err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if events == nil {
		events = []sandboxobservability.Event{}
	}
	return events, nil
}

func encodeAttributes(attributes map[string]any) (string, error) {
	if len(attributes) == 0 {
		return "{}", nil
	}
	encoded, err := json.Marshal(attributes)
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeAttributes(value string) (map[string]any, error) {
	if value == "" || value == "{}" {
		return nil, nil
	}
	var attributes map[string]any
	if err := json.Unmarshal([]byte(value), &attributes); err != nil {
		return nil, err
	}
	if len(attributes) == 0 {
		return nil, nil
	}
	return attributes, nil
}

func lastWatermark(events []sandboxobservability.Event) string {
	if len(events) == 0 {
		return ""
	}
	return events[len(events)-1].EventID
}
