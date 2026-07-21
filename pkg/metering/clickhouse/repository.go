package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	metering "github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
)

const (
	dateTime64NanoPlaceholder         = "fromUnixTimestamp64Nano(?, 'UTC')"
	nullableDateTime64NanoPlaceholder = "if(toUInt8(?), fromUnixTimestamp64Nano(?, 'UTC'), NULL)"
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

func (r *Repository) AppendEvent(ctx context.Context, event *metering.Event) error {
	return r.appendEvent(ctx, event)
}

func (r *Repository) appendEvent(ctx context.Context, event *metering.Event) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if event.EventID == "" {
		return fmt.Errorf("event_id is required")
	}
	if event.Sequence <= 0 {
		return fmt.Errorf("sequence is required")
	}
	if event.Producer == "" {
		return fmt.Errorf("producer is required")
	}
	if event.EventType == "" {
		return fmt.Errorf("event_type is required")
	}
	if event.SubjectType == "" || event.SubjectID == "" {
		return fmt.Errorf("subject_type and subject_id are required")
	}
	if event.OccurredAt.IsZero() {
		return fmt.Errorf("occurred_at is required")
	}
	recordedAt := event.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = r.now()
	}
	data := event.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (
    sequence, event_id, producer, region_id, event_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    occurred_at, recorded_at, version, data
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s, ?, ?)
`, qualified(r.cfg.Database, r.cfg.EventsTable), dateTime64NanoPlaceholder, dateTime64NanoPlaceholder),
		event.Sequence, event.EventID, event.Producer, event.RegionID, event.EventType,
		event.SubjectType, event.SubjectID,
		event.TeamID, event.UserID,
		event.SandboxID, event.VolumeID, event.SnapshotID,
		event.TemplateID, event.ClusterID,
		dateTime64NanoArg(event.OccurredAt), dateTime64NanoArg(recordedAt), versionFrom(recordedAt), string(data),
	)
	if err != nil {
		return fmt.Errorf("insert usage event: %w", err)
	}
	return nil
}

func (r *Repository) AppendWindow(ctx context.Context, window *metering.Window) error {
	return r.appendWindow(ctx, window)
}

func (r *Repository) appendWindow(ctx context.Context, window *metering.Window) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	if window == nil {
		return fmt.Errorf("window is nil")
	}
	if window.WindowID == "" {
		return fmt.Errorf("window_id is required")
	}
	if window.Sequence <= 0 {
		return fmt.Errorf("sequence is required")
	}
	if window.Producer == "" {
		return fmt.Errorf("producer is required")
	}
	if window.WindowType == "" {
		return fmt.Errorf("window_type is required")
	}
	if window.SubjectType == "" || window.SubjectID == "" {
		return fmt.Errorf("subject_type and subject_id are required")
	}
	if window.WindowStart.IsZero() || window.WindowEnd.IsZero() {
		return fmt.Errorf("window_start and window_end are required")
	}
	if window.WindowEnd.Before(window.WindowStart) {
		return fmt.Errorf("window_end must be greater than or equal to window_start")
	}
	if window.Unit == "" {
		return fmt.Errorf("unit is required")
	}
	if window.Value < 0 {
		return fmt.Errorf("value must be non-negative")
	}
	recordedAt := window.RecordedAt
	if recordedAt.IsZero() {
		recordedAt = r.now()
	}
	data := window.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (
    sequence, window_id, producer, region_id, window_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    window_start, window_end,
    value, unit, recorded_at, version, data
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s, ?, ?, %s, ?, ?)
`, qualified(r.cfg.Database, r.cfg.WindowsTable), dateTime64NanoPlaceholder, dateTime64NanoPlaceholder, dateTime64NanoPlaceholder),
		window.Sequence, window.WindowID, window.Producer, window.RegionID, window.WindowType,
		window.SubjectType, window.SubjectID,
		window.TeamID, window.UserID,
		window.SandboxID, window.VolumeID, window.SnapshotID,
		window.TemplateID, window.ClusterID,
		dateTime64NanoArg(window.WindowStart), dateTime64NanoArg(window.WindowEnd),
		window.Value, window.Unit, dateTime64NanoArg(recordedAt), versionFrom(recordedAt), string(data),
	)
	if err != nil {
		return fmt.Errorf("insert usage window: %w", err)
	}
	return nil
}

func (r *Repository) UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error {
	return r.upsertProducerWatermark(ctx, producer, regionID, completeBefore)
}

func (r *Repository) upsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error {
	if r == nil || r.db == nil {
		return fmt.Errorf("metering clickhouse repository is not configured")
	}
	if producer == "" {
		return fmt.Errorf("producer is required")
	}
	if completeBefore.IsZero() {
		return fmt.Errorf("complete_before is required")
	}
	updatedAt := r.now()
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (producer, region_id, complete_before, updated_at, version)
VALUES (?, ?, %s, %s, ?)
`, qualified(r.cfg.Database, r.cfg.WatermarksTable), dateTime64NanoPlaceholder, dateTime64NanoPlaceholder),
		producer, regionID, dateTime64NanoArg(completeBefore), dateTime64NanoArg(updatedAt), versionFrom(completeBefore),
	)
	if err != nil {
		return fmt.Errorf("upsert producer watermark: %w", err)
	}
	return nil
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
	if limit <= 0 {
		limit = 100
	}
	decoded, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	where, args := cursorWhere(decoded, "window_id")
	args = append(args, limit)
	rows, err := r.db.QueryContext(ctx, fmt.Sprintf(`
SELECT
    sequence, window_id, producer, region_id, window_type,
    subject_type, subject_id,
    team_id, user_id,
    sandbox_id, volume_id, snapshot_id,
    template_id, cluster_id,
    window_start, window_end,
    value, unit, recorded_at, data
FROM %s FINAL
%s
ORDER BY recorded_at ASC, producer ASC, window_id ASC
LIMIT ?
`, qualified(r.cfg.Database, r.cfg.WindowsTable), where), args...)
	if err != nil {
		return nil, "", fmt.Errorf("query usage windows: %w", err)
	}
	defer rows.Close()

	windows := make([]*metering.Window, 0, limit)
	for rows.Next() {
		window := &metering.Window{}
		var data string
		if err := rows.Scan(
			&window.Sequence, &window.WindowID, &window.Producer, &window.RegionID, &window.WindowType,
			&window.SubjectType, &window.SubjectID,
			&window.TeamID, &window.UserID,
			&window.SandboxID, &window.VolumeID, &window.SnapshotID,
			&window.TemplateID, &window.ClusterID,
			&window.WindowStart, &window.WindowEnd,
			&window.Value, &window.Unit, &window.RecordedAt, &data,
		); err != nil {
			return nil, "", fmt.Errorf("scan usage window: %w", err)
		}
		window.Data = json.RawMessage(data)
		windows = append(windows, window)
	}
	if err := rows.Err(); err != nil {
		return nil, "", fmt.Errorf("iterate usage windows: %w", err)
	}
	next, err := nextWindowCursor(windows)
	if err != nil {
		return nil, "", err
	}
	return windows, next, nil
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
    last_observed_at, last_resource_version
FROM %s FINAL
WHERE sandbox_id = ?
LIMIT 1
`, qualified(r.cfg.Database, r.cfg.SandboxStateTable)), sandboxID).Scan(
		&state.SandboxID, &state.Namespace, &state.TeamID, &state.UserID, &state.TemplateID, &state.ClusterID,
		&state.OwnerKind, &state.ResourceMillicpu, &state.ResourceMemoryMiB,
		&claimedAt, &activeSince, &paused, &pausedAt, &terminatedAt,
		&state.LastObservedAt, &state.LastResourceVer,
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
    last_observed_at, last_resource_version
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
			&state.LastObservedAt, &state.LastResourceVer,
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
	return r.upsertSandboxProjectionState(ctx, state)
}

func (r *Repository) upsertSandboxProjectionState(ctx context.Context, state *metering.SandboxProjectionState) error {
	if state == nil {
		return fmt.Errorf("state is nil")
	}
	if state.SandboxID == "" {
		return fmt.Errorf("sandbox_id is required")
	}
	if state.Namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if state.LastObservedAt.IsZero() {
		state.LastObservedAt = r.now()
	}
	claimedAtPresent, claimedAtNanos := nullableDateTime64NanoArgs(state.ClaimedAt)
	activeSincePresent, activeSinceNanos := nullableDateTime64NanoArgs(state.ActiveSince)
	pausedAtPresent, pausedAtNanos := nullableDateTime64NanoArgs(state.PausedAt)
	terminatedAtPresent, terminatedAtNanos := nullableDateTime64NanoArgs(state.TerminatedAt)
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (
    sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
    owner_kind, resource_millicpu, resource_memory_mib,
    claimed_at, active_since, paused, paused_at, terminated_at,
    last_observed_at, last_resource_version, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, %s, %s, ?, %s, %s, %s, ?, ?)
`,
		qualified(r.cfg.Database, r.cfg.SandboxStateTable),
		nullableDateTime64NanoPlaceholder,
		nullableDateTime64NanoPlaceholder,
		nullableDateTime64NanoPlaceholder,
		nullableDateTime64NanoPlaceholder,
		dateTime64NanoPlaceholder,
	),
		state.SandboxID, state.Namespace, state.TeamID, state.UserID, state.TemplateID, state.ClusterID,
		state.OwnerKind, state.ResourceMillicpu, state.ResourceMemoryMiB,
		claimedAtPresent, claimedAtNanos,
		activeSincePresent, activeSinceNanos,
		boolUInt8(state.Paused),
		pausedAtPresent, pausedAtNanos,
		terminatedAtPresent, terminatedAtNanos,
		dateTime64NanoArg(state.LastObservedAt), state.LastResourceVer, versionFrom(state.LastObservedAt),
	)
	if err != nil {
		return fmt.Errorf("upsert sandbox projection state: %w", err)
	}
	return nil
}

func (r *Repository) upsertStorageProjectionState(ctx context.Context, state *metering.StorageProjectionState) error {
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (
    subject_type, subject_id, product, owner_kind,
    team_id, user_id, sandbox_id, volume_id, snapshot_id,
    cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds,
    deleted, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, ?, 0, ?)
`, qualified(r.cfg.Database, r.cfg.StorageStateTable), dateTime64NanoPlaceholder),
		state.SubjectType, state.SubjectID, state.Product, state.OwnerKind,
		state.TeamID, state.UserID, state.SandboxID, state.VolumeID, state.SnapshotID,
		state.ClusterID, state.RegionID, state.SizeBytes, dateTime64NanoArg(state.ObservedAt), state.UnbilledByteNanoseconds,
		versionFrom(state.ObservedAt),
	)
	if err != nil {
		return fmt.Errorf("upsert storage projection state: %w", err)
	}
	return nil
}

// UpsertStorageProjectionState applies an idempotent storage-state mutation
// captured by the PostgreSQL metering outbox.
func (r *Repository) UpsertStorageProjectionState(ctx context.Context, state *metering.StorageProjectionState) error {
	if state == nil {
		return fmt.Errorf("storage projection state is nil")
	}
	return r.upsertStorageProjectionState(ctx, state)
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

func (r *Repository) deleteStorageProjectionState(ctx context.Context, state *metering.StorageProjectionState, deletedAt time.Time) error {
	if deletedAt.IsZero() {
		deletedAt = r.now()
	}
	_, err := r.db.ExecContext(ctx, fmt.Sprintf(`
INSERT INTO %s (
    subject_type, subject_id, product, owner_kind,
    team_id, user_id, sandbox_id, volume_id, snapshot_id,
    cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds,
    deleted, version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, %s, ?, 1, ?)
`, qualified(r.cfg.Database, r.cfg.StorageStateTable), dateTime64NanoPlaceholder),
		state.SubjectType, state.SubjectID, state.Product, state.OwnerKind,
		state.TeamID, state.UserID, state.SandboxID, state.VolumeID, state.SnapshotID,
		state.ClusterID, state.RegionID, state.SizeBytes, dateTime64NanoArg(deletedAt), state.UnbilledByteNanoseconds,
		versionFrom(deletedAt),
	)
	if err != nil {
		return fmt.Errorf("delete storage projection state: %w", err)
	}
	return nil
}

// DeleteStorageProjectionState applies an idempotent storage-state tombstone
// captured by the PostgreSQL metering outbox.
func (r *Repository) DeleteStorageProjectionState(ctx context.Context, state *metering.StorageProjectionState, deletedAt time.Time) error {
	if state == nil {
		return fmt.Errorf("storage projection state is nil")
	}
	return r.deleteStorageProjectionState(ctx, state, deletedAt)
}

func (r *Repository) CurrentUsage(ctx context.Context, teamID string, dimension quota.Dimension) (int64, error) {
	switch dimension {
	case quota.DimensionActiveSandboxes:
		return r.currentScalar(ctx, fmt.Sprintf(`
SELECT toInt64(COUNT())
FROM %s FINAL
WHERE team_id = ? AND claimed_at IS NOT NULL AND terminated_at IS NULL AND paused = 0
`, qualified(r.cfg.Database, r.cfg.SandboxStateTable)), teamID)
	case quota.DimensionVolumeStorageGB:
		current, err := r.currentStorageUsageBytes(ctx, teamID, metering.SubjectTypeVolume)
		return quota.BytesToGBRoundUp(current), err
	case quota.DimensionSnapshotGB:
		current, err := r.currentStorageUsageBytes(ctx, teamID, metering.SubjectTypeSnapshot)
		return quota.BytesToGBRoundUp(current), err
	default:
		return 0, fmt.Errorf("unsupported quota usage dimension %q", dimension)
	}
}

func (r *Repository) ProjectedStorageUsageGB(ctx context.Context, teamID string, dimension quota.Dimension, subjectType, subjectID string, sizeBytes int64) (int64, error) {
	if teamID == "" {
		return 0, fmt.Errorf("team_id is required")
	}
	if subjectID == "" {
		return 0, fmt.Errorf("subject_id is required")
	}
	if sizeBytes < 0 {
		return 0, fmt.Errorf("size_bytes must be non-negative")
	}
	if !storageDimensionMatchesSubjectType(dimension, subjectType) {
		return 0, fmt.Errorf("quota dimension %q does not match storage subject_type %q", dimension, subjectType)
	}
	otherBytes, err := r.currentScalar(ctx, fmt.Sprintf(`
SELECT COALESCE(SUM(size_bytes), 0)
FROM %s FINAL
WHERE deleted = 0 AND team_id = ? AND subject_type = ? AND subject_id != ?
`, qualified(r.cfg.Database, r.cfg.StorageStateTable)), teamID, subjectType, subjectID)
	if err != nil {
		return 0, fmt.Errorf("query projected storage quota usage: %w", err)
	}
	return quota.BytesToGBRoundUp(otherBytes + sizeBytes), nil
}

func (r *Repository) AdditionalStorageUsageGB(ctx context.Context, teamID string, dimension quota.Dimension, subjectType string, additionalBytes int64) (int64, error) {
	if additionalBytes < 0 {
		return 0, fmt.Errorf("additional_bytes must be non-negative")
	}
	if !storageDimensionMatchesSubjectType(dimension, subjectType) {
		return 0, fmt.Errorf("quota dimension %q does not match storage subject_type %q", dimension, subjectType)
	}
	current, err := r.currentStorageUsageBytes(ctx, teamID, subjectType)
	if err != nil {
		return 0, err
	}
	return quota.BytesToGBRoundUp(current + additionalBytes), nil
}

func (r *Repository) currentStorageUsageBytes(ctx context.Context, teamID, subjectType string) (int64, error) {
	return r.currentScalar(ctx, fmt.Sprintf(`
SELECT COALESCE(SUM(size_bytes), 0)
FROM %s FINAL
WHERE deleted = 0 AND team_id = ? AND subject_type = ?
`, qualified(r.cfg.Database, r.cfg.StorageStateTable)), teamID, subjectType)
}

func (r *Repository) currentScalar(ctx context.Context, query string, args ...any) (int64, error) {
	var value int64
	if err := r.db.QueryRowContext(ctx, query, args...).Scan(&value); err != nil {
		return 0, err
	}
	return value, nil
}

func storageDimensionMatchesSubjectType(dimension quota.Dimension, subjectType string) bool {
	switch dimension {
	case quota.DimensionVolumeStorageGB:
		return subjectType == metering.SubjectTypeVolume
	case quota.DimensionSnapshotGB:
		return subjectType == metering.SubjectTypeSnapshot
	default:
		return false
	}
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
