package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	metering "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

const (
	dateTime64NanoPlaceholder         = "fromUnixTimestamp64Nano(?, 'UTC')"
	nullableDateTime64NanoPlaceholder = "if(toUInt8(?), fromUnixTimestamp64Nano(?, 'UTC'), NULL)"
	maxMeteringInsertBatchSize        = 500
	maxWindowDetailBatchSize          = 200
	meteringInsertReliabilitySettings = " SETTINGS async_insert = 0, wait_for_async_insert = 1"
)

type Repository struct {
	db  *sql.DB
	cfg Config
	now func() time.Time
}

func NewRepository(db *sql.DB, cfg Config) *Repository {
	normalized, _ := normalizeConfig(cfg)
	return &Repository{
		db:  db,
		cfg: normalized,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

// ApplyProjectionBatch writes data and state rows before advancing producer
// watermarks. PostgreSQL remains the durable producer boundary, so any partial
// ClickHouse acceptance is retried safely through versioned rows.
func (r *Repository) ApplyProjectionBatch(ctx context.Context, batch *metering.ProjectionBatch) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	if batch == nil {
		return fmt.Errorf("metering projection batch is nil")
	}

	// Validate the complete batch before the first INSERT. Once validation has
	// succeeded, transient failures may still leave a partial idempotent write.
	for _, event := range batch.Events {
		if _, err := r.eventInsertValues(event); err != nil {
			return err
		}
	}
	for _, window := range batch.Windows {
		if _, err := r.windowInsertValues(window); err != nil {
			return err
		}
	}
	for _, state := range batch.SandboxStates {
		if _, err := r.sandboxStateInsertValues(state); err != nil {
			return err
		}
	}
	for _, mutation := range batch.StorageMutations {
		if mutation == nil {
			return fmt.Errorf("storage projection mutation is nil")
		}
		if _, err := r.storageStateInsertValues(mutation.State, mutation.Deleted, mutation.DeletedAt); err != nil {
			return err
		}
	}
	for _, watermark := range batch.Watermarks {
		if _, err := r.watermarkInsertValues(watermark); err != nil {
			return err
		}
	}

	if err := r.appendEvents(ctx, batch.Events); err != nil {
		return err
	}
	if err := r.appendWindows(ctx, batch.Windows); err != nil {
		return err
	}
	if err := r.upsertSandboxProjectionStates(ctx, batch.SandboxStates); err != nil {
		return err
	}
	if err := r.writeStorageProjectionStates(ctx, batch.StorageMutations); err != nil {
		return err
	}
	return r.upsertProducerWatermarks(ctx, batch.Watermarks)
}

type insertRowsConfig struct {
	table        string
	columns      string
	placeholders string
	description  string
}

func (r *Repository) execInsertRows(ctx context.Context, cfg insertRowsConfig, rows [][]any) error {
	for len(rows) > 0 {
		chunkSize := min(len(rows), maxMeteringInsertBatchSize)
		chunk := rows[:chunkSize]
		var query strings.Builder
		query.WriteString("INSERT INTO ")
		query.WriteString(cfg.table)
		query.WriteString(" (")
		query.WriteString(cfg.columns)
		query.WriteString(")")
		query.WriteString(meteringInsertReliabilitySettings)
		query.WriteString(" VALUES ")

		args := make([]any, 0)
		for index, row := range chunk {
			if index > 0 {
				query.WriteString(", ")
			}
			query.WriteString(cfg.placeholders)
			args = append(args, row...)
		}
		if _, err := r.db.ExecContext(ctx, query.String(), args...); err != nil {
			return fmt.Errorf("%s: %w", cfg.description, err)
		}
		rows = rows[chunkSize:]
	}
	return nil
}

func (r *Repository) AppendEvent(ctx context.Context, event *metering.Event) error {
	return r.appendEvents(ctx, []*metering.Event{event})
}

func (r *Repository) appendEvents(ctx context.Context, events []*metering.Event) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	rows := make([][]any, 0, len(events))
	for _, event := range events {
		row, err := r.eventInsertValues(event)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return r.execInsertRows(ctx, insertRowsConfig{
		table: qualified(r.cfg.Database, r.cfg.EventsTable),
		columns: `
    sequence, event_id, producer, region_id, event_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    occurred_at, recorded_at, version, data`,
		placeholders: fmt.Sprintf(
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s, ?, ?)",
			dateTime64NanoPlaceholder,
			dateTime64NanoPlaceholder,
		),
		description: "insert usage events",
	}, rows)
}

func (r *Repository) eventInsertValues(event *metering.Event) ([]any, error) {
	if event == nil {
		return nil, fmt.Errorf("event is nil")
	}
	if event.EventID == "" {
		return nil, fmt.Errorf("event_id is required")
	}
	if event.Sequence <= 0 {
		return nil, fmt.Errorf("sequence is required")
	}
	if event.Producer == "" {
		return nil, fmt.Errorf("producer is required")
	}
	if event.EventType == "" {
		return nil, fmt.Errorf("event_type is required")
	}
	if event.SubjectType == "" || event.SubjectID == "" {
		return nil, fmt.Errorf("subject_type and subject_id are required")
	}
	if event.OccurredAt.IsZero() {
		return nil, fmt.Errorf("occurred_at is required")
	}
	recordedAt := event.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = r.now()
	}
	data := event.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	return []any{
		event.Sequence, event.EventID, event.Producer, event.RegionID, event.EventType,
		event.SubjectType, event.SubjectID,
		event.TeamID, event.UserID,
		event.SandboxID, event.VolumeID, event.SnapshotID,
		event.TemplateID, event.ClusterID,
		dateTime64NanoArg(event.OccurredAt), dateTime64NanoArg(recordedAt), versionFrom(recordedAt), string(data),
	}, nil
}

func (r *Repository) AppendWindow(ctx context.Context, window *metering.Window) error {
	return r.appendWindows(ctx, []*metering.Window{window})
}

func (r *Repository) appendWindows(ctx context.Context, windows []*metering.Window) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	rows := make([][]any, 0, len(windows))
	for _, window := range windows {
		row, err := r.windowInsertValues(window)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return r.execInsertRows(ctx, insertRowsConfig{
		table: qualified(r.cfg.Database, r.cfg.WindowsTable),
		columns: `
    sequence, window_id, producer, region_id, window_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    window_start, window_end,
    value, unit, recorded_at, version, data`,
		placeholders: fmt.Sprintf(
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s, ?, ?, %s, ?, ?)",
			dateTime64NanoPlaceholder,
			dateTime64NanoPlaceholder,
			dateTime64NanoPlaceholder,
		),
		description: "insert usage windows",
	}, rows)
}

func (r *Repository) windowInsertValues(window *metering.Window) ([]any, error) {
	if window == nil {
		return nil, fmt.Errorf("window is nil")
	}
	if window.WindowID == "" {
		return nil, fmt.Errorf("window_id is required")
	}
	if window.Sequence <= 0 {
		return nil, fmt.Errorf("sequence is required")
	}
	if window.Producer == "" {
		return nil, fmt.Errorf("producer is required")
	}
	if window.WindowType == "" {
		return nil, fmt.Errorf("window_type is required")
	}
	if window.SubjectType == "" || window.SubjectID == "" {
		return nil, fmt.Errorf("subject_type and subject_id are required")
	}
	if window.WindowStart.IsZero() || window.WindowEnd.IsZero() {
		return nil, fmt.Errorf("window_start and window_end are required")
	}
	if window.WindowEnd.Before(window.WindowStart) {
		return nil, fmt.Errorf("window_end must be greater than or equal to window_start")
	}
	if window.Unit == "" {
		return nil, fmt.Errorf("unit is required")
	}
	if window.Value < 0 {
		return nil, fmt.Errorf("value must be non-negative")
	}
	recordedAt := window.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = r.now()
	}
	data := window.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	return []any{
		window.Sequence, window.WindowID, window.Producer, window.RegionID, window.WindowType,
		window.SubjectType, window.SubjectID,
		window.TeamID, window.UserID,
		window.SandboxID, window.VolumeID, window.SnapshotID,
		window.TemplateID, window.ClusterID,
		dateTime64NanoArg(window.WindowStart), dateTime64NanoArg(window.WindowEnd),
		window.Value, window.Unit, dateTime64NanoArg(recordedAt), versionFrom(recordedAt), string(data),
	}, nil
}

func (r *Repository) UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error {
	return r.upsertProducerWatermarks(ctx, []*metering.ProducerWatermark{{
		Producer:       producer,
		RegionID:       regionID,
		CompleteBefore: completeBefore,
	}})
}

func (r *Repository) upsertProducerWatermarks(ctx context.Context, watermarks []*metering.ProducerWatermark) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	rows := make([][]any, 0, len(watermarks))
	for _, watermark := range watermarks {
		row, err := r.watermarkInsertValues(watermark)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return r.execInsertRows(ctx, insertRowsConfig{
		table:        qualified(r.cfg.Database, r.cfg.WatermarksTable),
		columns:      "producer, region_id, complete_before, updated_at, version",
		placeholders: fmt.Sprintf("(?, ?, %s, %s, ?)", dateTime64NanoPlaceholder, dateTime64NanoPlaceholder),
		description:  "upsert producer watermarks",
	}, rows)
}

func (r *Repository) watermarkInsertValues(watermark *metering.ProducerWatermark) ([]any, error) {
	if watermark == nil {
		return nil, fmt.Errorf("producer watermark is nil")
	}
	if watermark.Producer == "" {
		return nil, fmt.Errorf("producer is required")
	}
	if watermark.CompleteBefore.IsZero() {
		return nil, fmt.Errorf("complete_before is required")
	}
	updatedAt := watermark.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = r.now()
	}
	return []any{
		watermark.Producer,
		watermark.RegionID,
		dateTime64NanoArg(watermark.CompleteBefore),
		dateTime64NanoArg(updatedAt),
		versionFrom(watermark.CompleteBefore),
	}, nil
}

func (r *Repository) GetStatus(ctx context.Context, fallbackRegionID string) (*metering.Status, error) {
	status := &metering.Status{RegionID: fallbackRegionID}
	if err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT COALESCE(MAX(sequence), 0)
FROM %s FINAL
`, qualified(r.cfg.Database, r.cfg.EventsTable))).Scan(&status.LatestEventSequence); err != nil {
		return nil, fmt.Errorf("query latest event sequence: %w", err)
	}
	if err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT COALESCE(MAX(sequence), 0)
FROM %s FINAL
`, qualified(r.cfg.Database, r.cfg.WindowsTable))).Scan(&status.LatestWindowSequence); err != nil {
		return nil, fmt.Errorf("query latest window sequence: %w", err)
	}
	if cursor, err := r.latestEventCursor(ctx); err != nil {
		return nil, err
	} else {
		status.LatestEventCursor = cursor
	}
	if cursor, err := r.latestWindowCursor(ctx); err != nil {
		return nil, err
	} else {
		status.LatestWindowCursor = cursor
	}

	var completeBefore sql.NullTime
	var producerCount uint64
	var regionID string
	err := r.db.QueryRowContext(ctx, watermarkStatusQuery(qualified(r.cfg.Database, r.cfg.WatermarksTable))).Scan(&completeBefore, &producerCount, &regionID)
	if err != nil {
		return nil, fmt.Errorf("query producer watermarks: %w", err)
	}
	if completeBefore.Valid {
		value := completeBefore.Time.UTC()
		status.CompleteBefore = &value
	}
	status.ProducerCount = int(producerCount)
	if regionID != "" {
		status.RegionID = regionID
	}
	return status, nil
}

func watermarkStatusQuery(table string) string {
	// The PostgreSQL projector delivers one globally ordered outbox. A
	// projected watermark therefore proves that every older committed batch
	// has reached ClickHouse, regardless of which producer emitted it.
	return fmt.Sprintf(`
SELECT MAX(complete_before), COUNT(), any(region_id)
FROM %s FINAL
`, table)
}

func (r *Repository) latestEventCursor(ctx context.Context) (string, error) {
	var recordedAt time.Time
	var producer, id string
	err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT recorded_at, producer, event_id
FROM %s FINAL
ORDER BY recorded_at DESC, producer DESC, event_id DESC
LIMIT 1
`, qualified(r.cfg.Database, r.cfg.EventsTable))).Scan(&recordedAt, &producer, &id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query latest event cursor: %w", err)
	}
	return encodeCursor(recordedAt, producer, id)
}

func (r *Repository) latestWindowCursor(ctx context.Context) (string, error) {
	var recordedAt time.Time
	var producer, id string
	err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT recorded_at, producer, window_id
FROM %s FINAL
ORDER BY recorded_at DESC, producer DESC, window_id DESC
LIMIT 1
`, qualified(r.cfg.Database, r.cfg.WindowsTable))).Scan(&recordedAt, &producer, &id)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("query latest window cursor: %w", err)
	}
	return encodeCursor(recordedAt, producer, id)
}

func (r *Repository) ListEvents(ctx context.Context, cursor string, limit int) ([]*metering.Event, string, error) {
	if limit <= 0 {
		limit = 100
	}
	decoded, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	where, args := cursorWhere(decoded, "event_id")
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT
    sequence, event_id, producer, region_id, event_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    occurred_at, recorded_at, data
FROM %s FINAL
%s
ORDER BY recorded_at ASC, producer ASC, event_id ASC
LIMIT ?
`, qualified(r.cfg.Database, r.cfg.EventsTable), where), args...)
	if err != nil {
		return nil, "", fmt.Errorf("query usage events: %w", err)
	}
	defer rows.Close()

	events := make([]*metering.Event, 0, limit)
	for rows.Next() {
		event := &metering.Event{}
		var data string
		if err := rows.Scan(
			&event.Sequence, &event.EventID, &event.Producer, &event.RegionID, &event.EventType,
			&event.SubjectType, &event.SubjectID,
			&event.TeamID, &event.UserID,
			&event.SandboxID, &event.VolumeID, &event.SnapshotID,
			&event.TemplateID, &event.ClusterID,
			&event.OccurredAt, &event.RecordedAt, &data,
		); err != nil {
			return nil, "", fmt.Errorf("scan usage event: %w", err)
		}
		event.Data = json.RawMessage(data)
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate usage events: %w", err)
	}
	next, err := nextEventCursor(events)
	if err != nil {
		return nil, "", err
	}
	return events, next, nil
}

func (r *Repository) ListWindows(ctx context.Context, cursor string, limit int) ([]*metering.Window, string, error) {
	return r.listWindows(ctx, "", "", cursor, limit)
}

// ListTeamWindows returns usage windows scoped to exactly one team.
func (r *Repository) ListTeamWindows(ctx context.Context, teamID string, windowType string, cursor string, limit int) ([]*metering.Window, string, error) {
	teamID = strings.TrimSpace(teamID)
	if teamID == "" {
		return nil, "", fmt.Errorf("team_id is required")
	}
	return r.listWindows(ctx, teamID, strings.TrimSpace(windowType), cursor, limit)
}

func (r *Repository) listWindows(ctx context.Context, teamID string, windowType string, cursor string, limit int) ([]*metering.Window, string, error) {
	if limit <= 0 {
		limit = 100
	}
	decoded, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	keys, err := r.listWindowKeys(ctx, teamID, windowType, decoded, limit)
	if err != nil {
		return nil, "", err
	}
	if len(keys) == 0 {
		return []*metering.Window{}, "", nil
	}
	details, err := r.loadWindowDetails(ctx, keys)
	if err != nil {
		return nil, "", err
	}

	windows := make([]*metering.Window, 0, len(keys))
	for _, key := range keys {
		window, ok := details[key.identity()]
		if !ok {
			return nil, "", fmt.Errorf(
				"usage window %q from producer %q disappeared while loading its selected version",
				key.WindowID,
				key.Producer,
			)
		}
		if !window.RecordedAt.Equal(key.RecordedAt) {
			return nil, "", fmt.Errorf(
				"usage window %q from producer %q changed while loading its selected version",
				key.WindowID,
				key.Producer,
			)
		}
		if teamID != "" && window.TeamID != teamID {
			return nil, "", fmt.Errorf(
				"usage window %q from producer %q changed team while loading its selected version",
				key.WindowID,
				key.Producer,
			)
		}
		if windowType != "" && window.WindowType != windowType {
			return nil, "", fmt.Errorf(
				"usage window %q from producer %q changed type while loading its selected version",
				key.WindowID,
				key.Producer,
			)
		}
		windows = append(windows, window)
	}
	next, err := nextWindowCursor(windows)
	if err != nil {
		return nil, "", err
	}
	return windows, next, nil
}

type windowLookupIdentity struct {
	RegionID string
	Producer string
	WindowID string
}

type windowLookupKey struct {
	RecordedAt time.Time
	RegionID   string
	Producer   string
	WindowID   string
	Version    uint64
}

func (k windowLookupKey) identity() windowLookupIdentity {
	return windowLookupIdentity{
		RegionID: k.RegionID,
		Producer: k.Producer,
		WindowID: k.WindowID,
	}
}

// listWindowKeys keeps FINAL limited to the narrow cursor and primary-key
// columns. Selecting the full row under FINAL makes ClickHouse retain hundreds
// of MiB while resolving ReplacingMergeTree versions.
func (r *Repository) listWindowKeys(
	ctx context.Context,
	teamID string,
	windowType string,
	cursor *pageCursor,
	limit int,
) ([]windowLookupKey, error) {
	where, args := windowWhere(teamID, windowType, cursor)
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT
    recorded_at, region_id, producer, window_id, version
FROM %s FINAL
%s
ORDER BY recorded_at ASC, producer ASC, window_id ASC
LIMIT ?
`, qualified(r.cfg.Database, r.cfg.WindowsTable), where), args...)
	if err != nil {
		return nil, fmt.Errorf("query usage window keys: %w", err)
	}
	defer rows.Close()

	keys := make([]windowLookupKey, 0, limit)
	seen := make(map[windowLookupIdentity]struct{}, limit)
	for rows.Next() {
		key := windowLookupKey{}
		if err := rows.Scan(
			&key.RecordedAt,
			&key.RegionID,
			&key.Producer,
			&key.WindowID,
			&key.Version,
		); err != nil {
			return nil, fmt.Errorf("scan usage window key: %w", err)
		}
		identity := key.identity()
		if _, ok := seen[identity]; ok {
			return nil, fmt.Errorf(
				"usage window key %q from producer %q is duplicated after FINAL",
				key.WindowID,
				key.Producer,
			)
		}
		seen[identity] = struct{}{}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage window keys: %w", err)
	}
	return keys, nil
}

// loadWindowDetails reads raw primary-key ranges in bounded batches and
// selects exactly the version chosen by the FINAL key query. Avoiding FINAL on
// the wide row set bounds memory without allowing a concurrent replacement to
// advance the export cursor past an unseen version.
func (r *Repository) loadWindowDetails(
	ctx context.Context,
	keys []windowLookupKey,
) (map[windowLookupIdentity]*metering.Window, error) {
	details := make(map[windowLookupIdentity]*metering.Window, len(keys))
	for offset := 0; offset < len(keys); offset += maxWindowDetailBatchSize {
		end := min(offset+maxWindowDetailBatchSize, len(keys))
		batch := keys[offset:end]
		expectedVersions := make(map[windowLookupIdentity]uint64, len(batch))
		var predicates strings.Builder
		args := make([]any, 0, len(batch)*3)
		for index, key := range batch {
			identity := key.identity()
			expectedVersions[identity] = key.Version
			if index > 0 {
				predicates.WriteString(", ")
			}
			predicates.WriteString("(?, ?, ?)")
			args = append(args, key.RegionID, key.Producer, key.WindowID)
		}

		rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT
    sequence, window_id, producer, region_id, window_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    window_start, window_end,
    value, unit, recorded_at, data, version
FROM %s
WHERE (region_id, producer, window_id) IN (%s)
ORDER BY region_id ASC, producer ASC, window_id ASC, version DESC
`, qualified(r.cfg.Database, r.cfg.WindowsTable), predicates.String()), args...)
		if err != nil {
			return nil, fmt.Errorf("query usage window details: %w", err)
		}

		for rows.Next() {
			window := &metering.Window{}
			var data string
			var version uint64
			if err := rows.Scan(
				&window.Sequence, &window.WindowID, &window.Producer, &window.RegionID, &window.WindowType,
				&window.SubjectType, &window.SubjectID,
				&window.TeamID, &window.UserID,
				&window.SandboxID, &window.VolumeID, &window.SnapshotID,
				&window.TemplateID, &window.ClusterID,
				&window.WindowStart, &window.WindowEnd,
				&window.Value, &window.Unit, &window.RecordedAt, &data, &version,
			); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan usage window detail: %w", err)
			}
			identity := windowLookupIdentity{
				RegionID: window.RegionID,
				Producer: window.Producer,
				WindowID: window.WindowID,
			}
			expectedVersion, ok := expectedVersions[identity]
			if !ok || version != expectedVersion {
				continue
			}
			if _, ok := details[identity]; ok {
				continue
			}
			window.Data = json.RawMessage(data)
			details[identity] = window
		}
		if err := rows.Err(); err != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate usage window details: %w", err)
		}
		if err := rows.Close(); err != nil {
			return nil, fmt.Errorf("close usage window details: %w", err)
		}
	}
	return details, nil
}

func windowWhere(teamID string, windowType string, cursor *pageCursor) (string, []any) {
	clauses := make([]string, 0, 3)
	args := make([]any, 0, 5)
	if teamID != "" {
		clauses = append(clauses, "team_id = ?")
		args = append(args, teamID)
	}
	if windowType != "" {
		clauses = append(clauses, "window_type = ?")
		args = append(args, windowType)
	}
	if cursor != nil {
		clauses = append(clauses, fmt.Sprintf("(recorded_at, producer, window_id) > (%s, ?, ?)", dateTime64NanoPlaceholder))
		args = append(args, dateTime64NanoArg(cursor.RecordedAt), cursor.Producer, cursor.ID)
	}
	if len(clauses) == 0 {
		return "", args
	}
	return "WHERE " + strings.Join(clauses, " AND "), args
}

func nextEventCursor(events []*metering.Event) (string, error) {
	if len(events) == 0 {
		return "", nil
	}
	last := events[len(events)-1]
	if last == nil {
		return "", fmt.Errorf("last usage event is nil")
	}
	return encodeCursor(last.RecordedAt, last.Producer, last.EventID)
}

func nextWindowCursor(windows []*metering.Window) (string, error) {
	if len(windows) == 0 {
		return "", nil
	}
	last := windows[len(windows)-1]
	if last == nil {
		return "", fmt.Errorf("last usage window is nil")
	}
	return encodeCursor(last.RecordedAt, last.Producer, last.WindowID)
}

func cursorWhere(cursor *pageCursor, idColumn string) (string, []any) {
	if cursor == nil {
		return "", nil
	}
	return fmt.Sprintf("WHERE (recorded_at, producer, %s) > (%s, ?, ?)", idColumn, dateTime64NanoPlaceholder), []any{dateTime64NanoArg(cursor.RecordedAt), cursor.Producer, cursor.ID}
}

func (r *Repository) GetSandboxProjectionState(ctx context.Context, sandboxID string) (*metering.SandboxProjectionState, error) {
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	state := &metering.SandboxProjectionState{}
	var paused uint8
	var claimedAt, activeSince, pausedAt, terminatedAt sql.NullTime
	err := r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT
    sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
    owner_kind, resource_millicpu, resource_memory_mib,
    claimed_at, active_since, paused, paused_at, terminated_at,
    last_observed_at, last_resource_version, source_revision, source_lifecycle_epoch
FROM %s FINAL
WHERE sandbox_id = ?
LIMIT 1
`, qualified(r.cfg.Database, r.cfg.SandboxStateTable)), sandboxID).Scan(
		&state.SandboxID, &state.Namespace, &state.TeamID, &state.UserID, &state.TemplateID, &state.ClusterID,
		&state.OwnerKind, &state.ResourceMillicpu, &state.ResourceMemoryMiB,
		&claimedAt, &activeSince, &paused, &pausedAt, &terminatedAt,
		&state.LastObservedAt, &state.LastResourceVer, &state.SourceRevision, &state.SourceLifecycleEpoch,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("query sandbox projection state: %w", err)
	}
	state.Paused = paused != 0
	state.ClaimedAt = nullableTimePtr(claimedAt)
	state.ActiveSince = nullableTimePtr(activeSince)
	state.PausedAt = nullableTimePtr(pausedAt)
	state.TerminatedAt = nullableTimePtr(terminatedAt)
	return state, nil
}

// ListActiveSandboxProjectionStates returns current producer state needed to
// bootstrap the PostgreSQL delivery outbox during an upgrade.
func (r *Repository) ListActiveSandboxProjectionStates(ctx context.Context) ([]*metering.SandboxProjectionState, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT
    sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
    owner_kind, resource_millicpu, resource_memory_mib,
    claimed_at, active_since, paused, paused_at, terminated_at,
    last_observed_at, last_resource_version, source_revision, source_lifecycle_epoch
FROM %s FINAL
WHERE terminated_at IS NULL
`, qualified(r.cfg.Database, r.cfg.SandboxStateTable)))
	if err != nil {
		return nil, fmt.Errorf("query active sandbox projection states: %w", err)
	}
	defer rows.Close()
	states := make([]*metering.SandboxProjectionState, 0)
	for rows.Next() {
		state := &metering.SandboxProjectionState{}
		var paused uint8
		var claimedAt, activeSince, pausedAt, terminatedAt sql.NullTime
		if err := rows.Scan(
			&state.SandboxID, &state.Namespace, &state.TeamID, &state.UserID, &state.TemplateID, &state.ClusterID,
			&state.OwnerKind, &state.ResourceMillicpu, &state.ResourceMemoryMiB,
			&claimedAt, &activeSince, &paused, &pausedAt, &terminatedAt,
			&state.LastObservedAt, &state.LastResourceVer, &state.SourceRevision, &state.SourceLifecycleEpoch,
		); err != nil {
			return nil, fmt.Errorf("scan active sandbox projection state: %w", err)
		}
		state.Paused = paused != 0
		state.ClaimedAt = nullableTimePtr(claimedAt)
		state.ActiveSince = nullableTimePtr(activeSince)
		state.PausedAt = nullableTimePtr(pausedAt)
		state.TerminatedAt = nullableTimePtr(terminatedAt)
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active sandbox projection states: %w", err)
	}
	return states, nil
}

func (r *Repository) UpsertSandboxProjectionState(ctx context.Context, state *metering.SandboxProjectionState) error {
	return r.upsertSandboxProjectionStates(ctx, []*metering.SandboxProjectionState{state})
}

func (r *Repository) upsertSandboxProjectionStates(ctx context.Context, states []*metering.SandboxProjectionState) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	rows := make([][]any, 0, len(states))
	for _, state := range states {
		row, err := r.sandboxStateInsertValues(state)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return r.execInsertRows(ctx, insertRowsConfig{
		table: qualified(r.cfg.Database, r.cfg.SandboxStateTable),
		columns: `
    sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
    owner_kind, resource_millicpu, resource_memory_mib,
    claimed_at, active_since, paused, paused_at, terminated_at,
    last_observed_at, last_resource_version, source_revision, source_lifecycle_epoch, version`,
		placeholders: fmt.Sprintf(
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s, ?, %s, %s, %s, ?, ?, ?, ?)",
			nullableDateTime64NanoPlaceholder,
			nullableDateTime64NanoPlaceholder,
			nullableDateTime64NanoPlaceholder,
			nullableDateTime64NanoPlaceholder,
			dateTime64NanoPlaceholder,
		),
		description: "upsert sandbox projection states",
	}, rows)
}

func (r *Repository) sandboxStateInsertValues(state *metering.SandboxProjectionState) ([]any, error) {
	if state == nil {
		return nil, fmt.Errorf("state is nil")
	}
	if state.SandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	if state.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if state.LastObservedAt.IsZero() {
		state.LastObservedAt = r.now()
	}
	claimedAtPresent, claimedAtNanos := nullableDateTime64NanoArgs(state.ClaimedAt)
	activeSincePresent, activeSinceNanos := nullableDateTime64NanoArgs(state.ActiveSince)
	pausedAtPresent, pausedAtNanos := nullableDateTime64NanoArgs(state.PausedAt)
	terminatedAtPresent, terminatedAtNanos := nullableDateTime64NanoArgs(state.TerminatedAt)
	return []any{
		state.SandboxID, state.Namespace, state.TeamID, state.UserID, state.TemplateID, state.ClusterID,
		state.OwnerKind, state.ResourceMillicpu, state.ResourceMemoryMiB,
		claimedAtPresent, claimedAtNanos,
		activeSincePresent, activeSinceNanos,
		boolUInt8(state.Paused),
		pausedAtPresent, pausedAtNanos,
		terminatedAtPresent, terminatedAtNanos,
		dateTime64NanoArg(state.LastObservedAt), state.LastResourceVer,
		state.SourceRevision, state.SourceLifecycleEpoch, versionFrom(state.LastObservedAt),
	}, nil
}

// UpsertStorageProjectionState applies an idempotent storage-state mutation
// captured by the PostgreSQL metering outbox.
func (r *Repository) UpsertStorageProjectionState(ctx context.Context, state *metering.StorageProjectionState) error {
	return r.writeStorageProjectionStates(ctx, []*metering.StorageProjectionMutation{{
		State: state,
	}})
}

// ListActiveStorageProjectionStates returns current producer state needed to
// bootstrap the PostgreSQL delivery outbox during an upgrade.
func (r *Repository) ListActiveStorageProjectionStates(ctx context.Context) ([]*metering.StorageProjectionState, error) {
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT
    subject_type, subject_id, product, owner_kind,
    team_id, user_id, sandbox_id, volume_id,
    snapshot_id, cluster_id, region_id,
    size_bytes, observed_at, unbilled_byte_nanoseconds
FROM %s FINAL
WHERE deleted = 0
`, qualified(r.cfg.Database, r.cfg.StorageStateTable)))
	if err != nil {
		return nil, fmt.Errorf("query active storage projection states: %w", err)
	}
	defer rows.Close()
	states := make([]*metering.StorageProjectionState, 0)
	for rows.Next() {
		state := &metering.StorageProjectionState{}
		if err := rows.Scan(
			&state.SubjectType, &state.SubjectID, &state.Product, &state.OwnerKind,
			&state.TeamID, &state.UserID, &state.SandboxID, &state.VolumeID,
			&state.SnapshotID, &state.ClusterID, &state.RegionID,
			&state.SizeBytes, &state.ObservedAt, &state.UnbilledByteNanoseconds,
		); err != nil {
			return nil, fmt.Errorf("scan active storage projection state: %w", err)
		}
		states = append(states, state)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active storage projection states: %w", err)
	}
	return states, nil
}

// DeleteStorageProjectionState applies an idempotent storage-state tombstone
// captured by the PostgreSQL metering outbox.
func (r *Repository) DeleteStorageProjectionState(ctx context.Context, state *metering.StorageProjectionState, deletedAt time.Time) error {
	return r.writeStorageProjectionStates(ctx, []*metering.StorageProjectionMutation{{
		State:     state,
		Deleted:   true,
		DeletedAt: deletedAt,
	}})
}

func (r *Repository) writeStorageProjectionStates(
	ctx context.Context,
	mutations []*metering.StorageProjectionMutation,
) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	rows := make([][]any, 0, len(mutations))
	for _, mutation := range mutations {
		if mutation == nil {
			return fmt.Errorf("storage projection mutation is nil")
		}
		row, err := r.storageStateInsertValues(mutation.State, mutation.Deleted, mutation.DeletedAt)
		if err != nil {
			return err
		}
		rows = append(rows, row)
	}
	return r.execInsertRows(ctx, insertRowsConfig{
		table: qualified(r.cfg.Database, r.cfg.StorageStateTable),
		columns: `
    subject_type, subject_id, product, owner_kind,
    team_id, user_id, sandbox_id, volume_id, snapshot_id,
    cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds,
    deleted, version`,
		placeholders: fmt.Sprintf(
			"(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, ?, ?, ?)",
			dateTime64NanoPlaceholder,
		),
		description: "write storage projection states",
	}, rows)
}

func (r *Repository) storageStateInsertValues(
	state *metering.StorageProjectionState,
	deleted bool,
	deletedAt time.Time,
) ([]any, error) {
	if state == nil {
		return nil, fmt.Errorf("storage projection state is nil")
	}
	observedAt := state.ObservedAt
	if deleted {
		observedAt = deletedAt
		if observedAt.IsZero() {
			observedAt = r.now()
		}
	}
	return []any{
		state.SubjectType, state.SubjectID, state.Product, state.OwnerKind,
		state.TeamID, state.UserID, state.SandboxID, state.VolumeID, state.SnapshotID,
		state.ClusterID, state.RegionID, state.SizeBytes, dateTime64NanoArg(observedAt), state.UnbilledByteNanoseconds,
		boolUInt8(deleted), versionFrom(observedAt),
	}, nil
}

func (r *Repository) CurrentUsage(ctx context.Context, teamID string, dimension quota.Dimension) (int64, error) {
	switch dimension {
	case quota.DimensionActiveSandboxes:
		return r.currentScalar(ctx, fmt.Sprintf(`
SELECT toInt64(COUNT())
FROM %s FINAL
WHERE team_id = ? AND claimed_at IS NOT NULL AND terminated_at IS NULL AND paused = 0
`, qualified(r.cfg.Database, r.cfg.SandboxStateTable)), teamID)
	default:
		return 0, fmt.Errorf("unsupported quota usage dimension %q", dimension)
	}
}

func (r *Repository) currentScalar(ctx context.Context, query string, args ...any) (int64, error) {
	var value int64
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func dateTime64NanoArg(value time.Time) int64 {
	return value.UTC().UnixNano()
}

func nullableDateTime64NanoArgs(value *time.Time) (uint8, int64) {
	if value == nil || value.IsZero() {
		return 0, 0
	}
	return 1, dateTime64NanoArg(*value)
}

func nullableTimePtr(value sql.NullTime) *time.Time {
	if !value.Valid {
		return nil
	}
	t := value.Time.UTC()
	return &t
}

func boolUInt8(value bool) uint8 {
	if value {
		return 1
	}
	return 0
}

func versionFrom(value time.Time) uint64 {
	if value.IsZero() {
		value = time.Now().UTC()
	}
	nanos := value.UTC().UnixNano()
	if nanos < 0 {
		return 0
	}
	return uint64(nanos)
}
