package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

const logCursorType = "log"

func (r *Repository) InsertLogs(ctx context.Context, logs []sandboxobservability.LogEntry) error {
	if len(logs) == 0 {
		return nil
	}
	normalized := make([]sandboxobservability.LogEntry, 0, len(logs))
	now := r.now()
	for i, entry := range logs {
		normalizedEntry, err := normalizeLogForInsert(entry, now)
		if err != nil {
			return fmt.Errorf("log %d: %w", i, err)
		}
		normalized = append(normalized, normalizedEntry)
	}
	for len(normalized) > 0 {
		chunkSize := len(normalized)
		if chunkSize > maxInsertBatchSize {
			chunkSize = maxInsertBatchSize
		}
		query, args, err := r.buildLogInsertSQL(normalized[:chunkSize])
		if err != nil {
			return err
		}
		if _, err := r.db.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("%w: insert logs: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
		normalized = normalized[chunkSize:]
	}
	return nil
}

func (r *Repository) ListLogs(ctx context.Context, query sandboxobservability.LogQuery) (*sandboxobservability.LogListResult, error) {
	normalized, limit, page, err := normalizeLogQuery(query)
	if err != nil {
		return nil, err
	}
	sqlQuery, args := r.buildListLogsSQL(normalized, limit+1, page)
	rows, err := r.db.QueryContext(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("%w: query logs: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	defer rows.Close()

	logs, err := scanLogs(rows)
	if err != nil {
		return nil, fmt.Errorf("%w: scan logs: %v", sandboxobservability.ErrBackendUnavailable, err)
	}
	nextCursor := ""
	if len(logs) > limit {
		last := logs[limit-1]
		nextCursor, err = encodeGenericPageCursor(last.OccurredAt, last.IngestedAt, string(last.Stream), logCursorType, last.Cursor)
		if err != nil {
			return nil, fmt.Errorf("%w: encode cursor: %v", sandboxobservability.ErrBackendUnavailable, err)
		}
		logs = logs[:limit]
	}
	return &sandboxobservability.LogListResult{
		Logs:       logs,
		NextCursor: nextCursor,
		Watermark:  lastLogWatermark(logs),
	}, nil
}

func (r *Repository) buildLogInsertSQL(logs []sandboxobservability.LogEntry) (string, []any, error) {
	var builder strings.Builder
	builder.WriteString("INSERT INTO ")
	builder.WriteString(r.logsTable)
	builder.WriteString(" (team_id, sandbox_id, region_id, cluster_id, context_id, process_id, occurred_at, ingested_at, stream, message, cursor, attributes) VALUES ")

	args := make([]any, 0, len(logs)*12)
	for i, entry := range logs {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
		attributes, err := encodeAttributes(entry.Attributes)
		if err != nil {
			return "", nil, fmt.Errorf("encode attributes: %w", err)
		}
		args = append(args,
			entry.TeamID,
			entry.SandboxID,
			entry.RegionID,
			entry.ClusterID,
			entry.ContextID,
			entry.ProcessID,
			entry.OccurredAt.UTC(),
			entry.IngestedAt.UTC(),
			string(entry.Stream),
			entry.Message,
			entry.Cursor,
			attributes,
		)
	}
	return builder.String(), args, nil
}

func (r *Repository) buildListLogsSQL(query sandboxobservability.LogQuery, limit int, cursor *pageCursor) (string, []any) {
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
		builder.WriteString(" AND (occurred_at, ingested_at, stream, cursor) > (?, ?, ?, ?)")
		args = append(args, cursor.OccurredAt, cursor.IngestedAt, cursor.Source, cursor.Cursor)
	}
	builder.WriteString(" ORDER BY occurred_at ASC, ingested_at ASC, stream ASC, cursor ASC")
	builder.WriteString(fmt.Sprintf(" LIMIT %d", limit))
	return builder.String(), args
}

func normalizeLogQuery(query sandboxobservability.LogQuery) (sandboxobservability.LogQuery, int, *pageCursor, error) {
	query.TeamID = strings.TrimSpace(query.TeamID)
	query.SandboxID = strings.TrimSpace(query.SandboxID)
	query.Cursor = strings.TrimSpace(query.Cursor)
	query.ContextID = strings.TrimSpace(query.ContextID)
	if query.TeamID == "" {
		return sandboxobservability.LogQuery{}, 0, nil, fmt.Errorf("team_id is required")
	}
	if query.SandboxID == "" {
		return sandboxobservability.LogQuery{}, 0, nil, fmt.Errorf("sandbox_id is required")
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
		return sandboxobservability.LogQuery{}, 0, nil, fmt.Errorf("end_time must be greater than or equal to start_time")
	}
	if query.Stream != "" && !sandboxobservability.ValidLogStream(query.Stream) {
		return sandboxobservability.LogQuery{}, 0, nil, fmt.Errorf("invalid stream")
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
			return sandboxobservability.LogQuery{}, 0, nil, err
		}
		if decoded.EventType != logCursorType {
			return sandboxobservability.LogQuery{}, 0, nil, sandboxobservability.ErrInvalidCursor
		}
		cursor = decoded
	}
	return query, limit, cursor, nil
}

func normalizeLogForInsert(entry sandboxobservability.LogEntry, now time.Time) (sandboxobservability.LogEntry, error) {
	entry.TeamID = strings.TrimSpace(entry.TeamID)
	entry.SandboxID = strings.TrimSpace(entry.SandboxID)
	entry.RegionID = strings.TrimSpace(entry.RegionID)
	entry.ClusterID = strings.TrimSpace(entry.ClusterID)
	entry.ContextID = strings.TrimSpace(entry.ContextID)
	entry.ProcessID = strings.TrimSpace(entry.ProcessID)
	entry.Cursor = strings.TrimSpace(entry.Cursor)
	if entry.TeamID == "" {
		return sandboxobservability.LogEntry{}, fmt.Errorf("team_id is required")
	}
	if entry.SandboxID == "" {
		return sandboxobservability.LogEntry{}, fmt.Errorf("sandbox_id is required")
	}
	if entry.OccurredAt.IsZero() {
		return sandboxobservability.LogEntry{}, fmt.Errorf("occurred_at is required")
	}
	if entry.Stream != "" && !sandboxobservability.ValidLogStream(entry.Stream) {
		return sandboxobservability.LogEntry{}, fmt.Errorf("invalid stream")
	}
	if entry.Cursor == "" {
		return sandboxobservability.LogEntry{}, fmt.Errorf("cursor is required")
	}
	entry.OccurredAt = entry.OccurredAt.UTC()
	if entry.IngestedAt.IsZero() {
		entry.IngestedAt = now
	}
	entry.IngestedAt = entry.IngestedAt.UTC()
	return entry, nil
}

func scanLogs(rows *sql.Rows) ([]sandboxobservability.LogEntry, error) {
	var logs []sandboxobservability.LogEntry
	for rows.Next() {
		var (
			entry          sandboxobservability.LogEntry
			stream         string
			attributesJSON string
		)
		if err := rows.Scan(
			&entry.TeamID,
			&entry.SandboxID,
			&entry.RegionID,
			&entry.ClusterID,
			&entry.ContextID,
			&entry.ProcessID,
			&entry.OccurredAt,
			&entry.IngestedAt,
			&stream,
			&entry.Message,
			&entry.Cursor,
			&attributesJSON,
		); err != nil {
			return nil, err
		}
		attributes, err := decodeAttributes(attributesJSON)
		if err != nil {
			return nil, err
		}
		entry.OccurredAt = entry.OccurredAt.UTC()
		entry.IngestedAt = entry.IngestedAt.UTC()
		entry.Stream = sandboxobservability.LogStream(stream)
		entry.Attributes = attributes
		logs = append(logs, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if logs == nil {
		logs = []sandboxobservability.LogEntry{}
	}
	return logs, nil
}

func lastLogWatermark(logs []sandboxobservability.LogEntry) string {
	for i := len(logs) - 1; i >= 0; i-- {
		if logs[i].Cursor != "" {
			return logs[i].Cursor
		}
	}
	return ""
}
