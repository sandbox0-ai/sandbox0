package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const (
	eventTailCursorKind = "event"
	logTailCursorKind   = "log"
)

func (r *Repository) WatchEvents(ctx context.Context, query sandboxobservability.EventQuery, opts sandboxobservability.WatchOptions) (*sandboxobservability.EventListResult, error) {
	return r.watchEvents(ctx, query, opts)
}

func (r *Repository) WatchLogs(ctx context.Context, query sandboxobservability.LogQuery, opts sandboxobservability.WatchOptions) (*sandboxobservability.LogListResult, error) {
	normalized, limit, cursor, err := normalizeWatchLogQuery(query, opts)
	if err != nil {
		return nil, err
	}
	sqlQuery, args := r.buildWatchLogsSQL(normalized, limit, cursor)
	rows, err := r.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: watch logs: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	defer rows.Close()

	logs, err := scanLogs(rows)
	if err != nil {
		return nil, fmt.Errorf("%w: scan logs: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	nextCursor := ""
	if len(logs) > 0 {
		last := logs[len(logs)-1]
		nextCursor, err = encodeTailCursor(logTailCursorKind, last.IngestedAt, string(last.Stream), "", last.Cursor, "")
		if err != nil {
			return nil, fmt.Errorf("%w: encode cursor: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
	}
	return &sandboxobservability.LogListResult{
		Logs:       logs,
		NextCursor: nextCursor,
		Watermark:  lastLogWatermark(logs),
	}, nil
}

func (r *Repository) watchEvents(ctx context.Context, query sandboxobservability.EventQuery, opts sandboxobservability.WatchOptions) (*sandboxobservability.EventListResult, error) {
	normalized, limit, cursor, err := normalizeWatchEventQuery(query, opts)
	if err != nil {
		return nil, err
	}
	sqlQuery, args := r.buildWatchEventsSQL(normalized, limit, cursor)
	rows, err := r.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: watch events: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil {
		return nil, fmt.Errorf("%w: scan events: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	nextCursor := ""
	if len(events) > 0 {
		last := events[len(events)-1]
		nextCursor, err = encodeTailCursor(eventTailCursorKind, last.IngestedAt, string(last.Source), string(last.EventType), last.EventID, last.Integrity.PayloadHash)
		if err != nil {
			return nil, fmt.Errorf("%w: encode cursor: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
	}
	return &sandboxobservability.EventListResult{
		Events:     events,
		NextCursor: nextCursor,
		Watermark:  lastWatermark(events),
	}, nil
}

func (r *Repository) buildWatchEventsSQL(query sandboxobservability.EventQuery, limit int, cursor *tailCursor) (string, []any) {
	var builder strings.Builder
	builder.WriteString("SELECT ")
	builder.WriteString(auditEventSelectColumns)
	builder.WriteString(" FROM ")
	builder.WriteString(r.eventsTable)
	builder.WriteString(" FINAL WHERE ")

	args := appendEventFilters(&builder, query)
	if cursor != nil {
		builder.WriteString(" AND (ingested_at, source, event_type, event_id, payload_hash) > (")
		builder.WriteString(dateTime64NanoPlaceholder + ", ?, ?, ?, ?)")
		args = append(args, dateTime64NanoArg(cursor.IngestedAt), cursor.Source, cursor.EventType, cursor.Cursor, cursor.PayloadHash)
	}

	builder.WriteString(" ORDER BY ingested_at ASC, source ASC, event_type ASC, event_id ASC, payload_hash ASC")
	builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return builder.String(), args
}

func (r *Repository) buildWatchLogsSQL(query sandboxobservability.LogQuery, limit int, cursor *tailCursor) (string, []any) {
	var builder strings.Builder
	builder.WriteString("SELECT team_id, sandbox_id, region_id, cluster_id, context_id, process_id, occurred_at, ingested_at, stream, message, cursor, attributes FROM ")
	builder.WriteString(r.logsTable)
	builder.WriteString(" WHERE team_id = ? AND sandbox_id = ?")

	args := []any{query.TeamID, query.SandboxID}
	if query.StartTime != nil {
		builder.WriteString(" AND occurred_at >= ?")
		args = append(args, query.StartTime.UTC())
	}
	if query.EndTime != nil {
		builder.WriteString(" AND occurred_at <= ?")
		args = append(args, query.EndTime.UTC())
	}
	if query.ContextID != "" {
		builder.WriteString(" AND context_id = ?")
		args = append(args, query.ContextID)
	}
	if query.Stream != "" {
		builder.WriteString(" AND stream = ?")
		args = append(args, string(query.Stream))
	}
	if cursor != nil {
		builder.WriteString(" AND (ingested_at, stream, cursor) > (?, ?, ?)")
		args = append(args, cursor.IngestedAt, cursor.Source, cursor.Cursor)
	}

	builder.WriteString(" ORDER BY ingested_at ASC, stream ASC, cursor ASC")
	builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return builder.String(), args
}

func normalizeWatchEventQuery(query sandboxobservability.EventQuery, opts sandboxobservability.WatchOptions) (sandboxobservability.EventQuery, int, *tailCursor, error) {
	if strings.TrimSpace(query.EventID) != "" {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("event_id cannot be combined with watch")
	}
	query.Cursor = ""
	if opts.Limit > 0 {
		query.Limit = opts.Limit
	}
	normalized, limit, _, err := normalizeQuery(query)
	if err != nil {
		return sandboxobservability.EventQuery{}, 0, nil, err
	}
	cursor, err := normalizeWatchCursor(opts, eventTailCursorKind)
	if err != nil {
		return sandboxobservability.EventQuery{}, 0, nil, err
	}
	if cursor != nil && !sandboxobservability.ValidDateTime64Nano(cursor.IngestedAt) {
		return sandboxobservability.EventQuery{}, 0, nil, fmt.Errorf("%w: timestamp is outside the DateTime64(9) range", sandboxobservability.ErrInvalidCursor)
	}
	return normalized, limit, cursor, nil
}

func normalizeWatchLogQuery(query sandboxobservability.LogQuery, opts sandboxobservability.WatchOptions) (sandboxobservability.LogQuery, int, *tailCursor, error) {
	query.Cursor = ""
	if opts.Limit > 0 {
		query.Limit = opts.Limit
	}
	normalized, limit, _, err := normalizeLogQuery(query)
	if err != nil {
		return sandboxobservability.LogQuery{}, 0, nil, err
	}
	cursor, err := normalizeWatchCursor(opts, logTailCursorKind)
	if err != nil {
		return sandboxobservability.LogQuery{}, 0, nil, err
	}
	return normalized, limit, cursor, nil
}

func normalizeWatchCursor(opts sandboxobservability.WatchOptions, expectedKind string) (*tailCursor, error) {
	cursorValue := strings.TrimSpace(opts.Cursor)
	if cursorValue != "" {
		return decodeTailCursor(cursorValue, expectedKind)
	}
	if opts.AfterIngestedAt == nil {
		return nil, nil
	}
	ingestedAt := opts.AfterIngestedAt.UTC()
	return &tailCursor{
		Kind:       expectedKind,
		IngestedAt: ingestedAt,
	}, nil
}
