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
		nextCursor, err = encodePageCursor(events[limit-1])
		if err != nil {
			return nil, fmt.Errorf("%w: encode cursor: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
		events = events[:limit]
	}

	return &sandboxobservability.EventListResult{
		Events:     events,
		NextCursor: nextCursor,
		Watermark:  lastWatermark(events),
	}, nil
}

func (r *Repository) buildInsertSQL(events []sandboxobservability.Event) (string, []any, error) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(r.eventsTable)
	builder.WriteString(" (team_id, sandbox_id, region_id, cluster_id, occurred_at, ingested_at, source, event_type, outcome, cursor, watermark, attributes) VALUES ")

	args := make([]any, 0, len(events)*12)
	for i, event := range events {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		attributes, err := encodeAttributes(event.Attributes)
		if err != nil {
			return "", nil, fmt.Errorf("encode attributes: %w", err)
		}
		args = append(args,
			event.TeamID,
			event.SandboxID,
			event.RegionID,
			event.ClusterID,
			event.OccurredAt.UTC(),
			event.IngestedAt.UTC(),
			string(event.Source),
			string(event.EventType),
			string(event.Outcome),
			event.Cursor,
			event.Watermark,
			attributes,
		)
	}

	return builder.String(), args, nil
}

func (r *Repository) buildListSQL(query sandboxobservability.EventQuery, limit int, cursor *pageCursor) (string, []any) {
	var builder strings.Builder
	builder.WriteString("SELECT team_id, sandbox_id, region_id, cluster_id, occurred_at, ingested_at, source, event_type, outcome, cursor, watermark, attributes FROM ")
	builder.WriteString(r.eventsTable)
	builder.WriteString(" FINAL WHERE team_id = ? AND sandbox_id = ?")

	args := []any{query.TeamID, query.SandboxID}
	if query.StartTime != nil {
		builder.WriteString(" AND occurred_at >= ?")
		args = append(args, query.StartTime.UTC())
	}
	if query.EndTime != nil {
		builder.WriteString(" AND occurred_at <= ?")
		args = append(args, query.EndTime.UTC())
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
	if cursor != nil {
		builder.WriteString(" AND (occurred_at, ingested_at, source, event_type, cursor) > (?, ?, ?, ?, ?)")
		args = append(args, cursor.OccurredAt, cursor.IngestedAt, cursor.Source, cursor.EventType, cursor.Cursor)
	}

	builder.WriteString(" ORDER BY occurred_at ASC, ingested_at ASC, source ASC, event_type ASC, cursor ASC")
	builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return builder.String(), args
}

func normalizeQuery(query sandboxobservability.EventQuery) (sandboxobservability.EventQuery, int, *pageCursor, error) {
	query.TeamID = strings.TrimSpace(query.TeamID)
	query.SandboxID = strings.TrimSpace(query.SandboxID)
	query.Cursor = strings.TrimSpace(query.Cursor)
	if query.TeamID == "" {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("team_id is required")
	}
	if query.SandboxID == "" {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("sandbox_id is required")
	}
	if query.StartTime != nil {
		start := query.StartTime.UTC()
		query.StartTime = &start
	}
	if query.EndTime != nil {
		end := query.EndTime.UTC()
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

	limit := query.Limit
	if limit <= 0 {
		limit = DefaultQueryLimit
	}
	if limit > MaxQueryLimit {
		limit = MaxQueryLimit
	}

	var cursor *pageCursor
	if query.Cursor != "" {
		decoded, err := decodePageCursor(query.Cursor)
		if err != nil {
			return sandboxobservability.EventQuery{}, 0, nil, err
		}
		cursor = decoded
	}

	return query, limit, cursor, nil
}

func normalizeEventForInsert(event sandboxobservability.Event, now time.Time) (sandboxobservability.Event, error) {
	event.TeamID = strings.TrimSpace(event.TeamID)
	event.SandboxID = strings.TrimSpace(event.SandboxID)
	event.RegionID = strings.TrimSpace(event.RegionID)
	event.ClusterID = strings.TrimSpace(event.ClusterID)
	event.Cursor = strings.TrimSpace(event.Cursor)
	event.Watermark = strings.TrimSpace(event.Watermark)

	if event.TeamID == "" {
		return sandboxobservability.Event{}, fmt.Errorf("team_id is required")
	}
	if event.SandboxID == "" {
		return sandboxobservability.Event{}, fmt.Errorf("sandbox_id is required")
	}
	if event.OccurredAt.IsZero() {
		return sandboxobservability.Event{}, fmt.Errorf("occurred_at is required")
	}
	if !sandboxobservability.ValidSource(event.Source) {
		return sandboxobservability.Event{}, fmt.Errorf("invalid source")
	}
	if !sandboxobservability.ValidEventType(event.EventType) {
		return sandboxobservability.Event{}, fmt.Errorf("invalid event_type")
	}
	if event.Outcome != "" && !sandboxobservability.ValidOutcome(event.Outcome) {
		return sandboxobservability.Event{}, fmt.Errorf("invalid outcome")
	}
	if event.Cursor == "" {
		return sandboxobservability.Event{}, fmt.Errorf("cursor is required")
	}
	event.OccurredAt = event.OccurredAt.UTC()
	if event.IngestedAt.IsZero() {
		event.IngestedAt = now
	}
	event.IngestedAt = event.IngestedAt.UTC()
	return event, nil
}

func scanEvents(rows *sql.Rows) ([]sandboxobservability.Event, error) {
	var events []sandboxobservability.Event
	for rows.Next() {
		var (
			event          sandboxobservability.Event
			source         string
			eventType      string
			outcome        string
			attributesJSON string
		)
		if err := rows.Scan(
			&event.TeamID,
			&event.SandboxID,
			&event.RegionID,
			&event.ClusterID,
			&event.OccurredAt,
			&event.IngestedAt,
			&source,
			&eventType,
			&outcome,
			&event.Cursor,
			&event.Watermark,
			&attributesJSON,
		); err != nil {
			return nil, err
		}
		attributes, err := decodeAttributes(attributesJSON)
		if err != nil {
			return nil, err
		}
		event.OccurredAt = event.OccurredAt.UTC()
		event.IngestedAt = event.IngestedAt.UTC()
		event.Source = sandboxobservability.Source(source)
		event.EventType = sandboxobservability.EventType(eventType)
		event.Outcome = sandboxobservability.Outcome(outcome)
		event.Attributes = attributes
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
	for i := len(events) - 1; i >= 0; i-- {
		if events[i].Watermark != "" {
			return events[i].Watermark
		}
	}
	return ""
}
