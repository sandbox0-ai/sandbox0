package metering

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Repository struct {
	db   DB
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{
		db:   pool,
		pool: pool,
	}
}

func (r *Repository) Pool() *pgxpool.Pool {
	return r.pool
}

func (r *Repository) InTx(ctx context.Context, fn func(tx pgx.Tx) error) error {
	if r.pool == nil {
		return fmt.Errorf("pool is nil")
	}
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

func (r *Repository) AppendEvent(ctx context.Context, event *Event) error {
	return r.appendEvent(ctx, r.db, event)
}

func (r *Repository) AppendEventTx(ctx context.Context, tx pgx.Tx, event *Event) error {
	return r.appendEvent(ctx, tx, event)
}

func (r *Repository) appendEvent(ctx context.Context, db DB, event *Event) error {
	if event == nil {
		return fmt.Errorf("event is nil")
	}
	if event.EventID == "" {
		return fmt.Errorf("event_id is required")
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

	data := event.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}

	_, err := db.Exec(ctx, `
		INSERT INTO metering.usage_events (
			event_id, producer, region_id, event_type,
			subject_type, subject_id,
			team_id, user_id,
			sandbox_id, volume_id, snapshot_id,
			template_id, cluster_id,
			occurred_at, data
		) VALUES (
			$1, $2, $3, $4,
			$5, $6,
			$7, $8,
			$9, $10, $11,
			$12, $13,
			$14, $15
		)
		ON CONFLICT (event_id) DO NOTHING
	`,
		event.EventID, event.Producer, event.RegionID, event.EventType,
		event.SubjectType, event.SubjectID,
		event.TeamID, event.UserID,
		nullableString(event.SandboxID), nullableString(event.VolumeID), nullableString(event.SnapshotID),
		nullableString(event.TemplateID), nullableString(event.ClusterID),
		event.OccurredAt, data,
	)
	if err != nil {
		return fmt.Errorf("insert usage event: %w", err)
	}

	return nil
}

func (r *Repository) AppendWindow(ctx context.Context, window *Window) error {
	return r.appendWindow(ctx, r.db, window)
}

func (r *Repository) AppendWindowTx(ctx context.Context, tx pgx.Tx, window *Window) error {
	return r.appendWindow(ctx, tx, window)
}

func (r *Repository) appendWindow(ctx context.Context, db DB, window *Window) error {
	if window == nil {
		return fmt.Errorf("window is nil")
	}
	if window.WindowID == "" {
		return fmt.Errorf("window_id is required")
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

	data := window.Data
	if len(data) == 0 {
		data = json.RawMessage(`{}`)
	}

	_, err := db.Exec(ctx, `
		INSERT INTO metering.usage_windows (
			window_id, producer, region_id, window_type,
			subject_type, subject_id,
			team_id, user_id,
			sandbox_id, volume_id, snapshot_id,
			template_id, cluster_id,
			window_start, window_end,
			value, unit, data
		) VALUES (
			$1, $2, $3, $4,
			$5, $6,
			$7, $8,
			$9, $10, $11,
			$12, $13,
			$14, $15,
			$16, $17, $18
		)
		ON CONFLICT (window_id) DO NOTHING
	`,
		window.WindowID, window.Producer, window.RegionID, window.WindowType,
		window.SubjectType, window.SubjectID,
		window.TeamID, window.UserID,
		nullableString(window.SandboxID), nullableString(window.VolumeID), nullableString(window.SnapshotID),
		nullableString(window.TemplateID), nullableString(window.ClusterID),
		window.WindowStart, window.WindowEnd,
		window.Value, window.Unit, data,
	)
	if err != nil {
		return fmt.Errorf("insert usage window: %w", err)
	}

	return nil
}

func (r *Repository) UpsertProducerWatermark(ctx context.Context, producer string, regionID string, completeBefore time.Time) error {
	return r.upsertProducerWatermark(ctx, r.db, producer, regionID, completeBefore)
}

func (r *Repository) UpsertProducerWatermarkTx(ctx context.Context, tx pgx.Tx, producer string, regionID string, completeBefore time.Time) error {
	return r.upsertProducerWatermark(ctx, tx, producer, regionID, completeBefore)
}

func (r *Repository) upsertProducerWatermark(ctx context.Context, db DB, producer string, regionID string, completeBefore time.Time) error {
	if producer == "" {
		return fmt.Errorf("producer is required")
	}
	if completeBefore.IsZero() {
		return fmt.Errorf("complete_before is required")
	}

	_, err := db.Exec(ctx, `
		INSERT INTO metering.producer_watermarks (
			producer, region_id, complete_before
		) VALUES ($1, $2, $3)
		ON CONFLICT (producer) DO UPDATE
		SET region_id = EXCLUDED.region_id,
			complete_before = GREATEST(metering.producer_watermarks.complete_before, EXCLUDED.complete_before),
			updated_at = NOW()
	`, producer, regionID, completeBefore)
	if err != nil {
		return fmt.Errorf("upsert producer watermark: %w", err)
	}

	return nil
}

func (r *Repository) ListEventsAfter(ctx context.Context, afterSequence int64, limit int) ([]*Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.Query(ctx, `
		SELECT
			sequence, event_id, producer, region_id, event_type,
			subject_type, subject_id,
			team_id, user_id,
			COALESCE(sandbox_id, ''), COALESCE(volume_id, ''), COALESCE(snapshot_id, ''),
			COALESCE(template_id, ''), COALESCE(cluster_id, ''),
			occurred_at, recorded_at, data
		FROM metering.usage_events
		WHERE sequence > $1
		ORDER BY sequence ASC
		LIMIT $2
	`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("query usage events: %w", err)
	}
	defer rows.Close()

	events := make([]*Event, 0, limit)
	for rows.Next() {
		event := &Event{}
		if err := rows.Scan(
			&event.Sequence, &event.EventID, &event.Producer, &event.RegionID, &event.EventType,
			&event.SubjectType, &event.SubjectID,
			&event.TeamID, &event.UserID,
			&event.SandboxID, &event.VolumeID, &event.SnapshotID,
			&event.TemplateID, &event.ClusterID,
			&event.OccurredAt, &event.RecordedAt, &event.Data,
		); err != nil {
			return nil, fmt.Errorf("scan usage event: %w", err)
		}
		events = append(events, event)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage events: %w", err)
	}

	return events, nil
}

func (r *Repository) ListWindowsAfter(ctx context.Context, afterSequence int64, limit int) ([]*Window, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.db.Query(ctx, `
		SELECT
			sequence, window_id, producer, region_id, window_type,
			subject_type, subject_id,
			team_id, user_id,
			COALESCE(sandbox_id, ''), COALESCE(volume_id, ''), COALESCE(snapshot_id, ''),
			COALESCE(template_id, ''), COALESCE(cluster_id, ''),
			window_start, window_end,
			value, unit, recorded_at, data
		FROM metering.usage_windows
		WHERE sequence > $1
		ORDER BY sequence ASC
		LIMIT $2
	`, afterSequence, limit)
	if err != nil {
		return nil, fmt.Errorf("query usage windows: %w", err)
	}
	defer rows.Close()

	windows := make([]*Window, 0, limit)
	for rows.Next() {
		window := &Window{}
		if err := rows.Scan(
			&window.Sequence, &window.WindowID, &window.Producer, &window.RegionID, &window.WindowType,
			&window.SubjectType, &window.SubjectID,
			&window.TeamID, &window.UserID,
			&window.SandboxID, &window.VolumeID, &window.SnapshotID,
			&window.TemplateID, &window.ClusterID,
			&window.WindowStart, &window.WindowEnd,
			&window.Value, &window.Unit, &window.RecordedAt, &window.Data,
		); err != nil {
			return nil, fmt.Errorf("scan usage window: %w", err)
		}
		windows = append(windows, window)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate usage windows: %w", err)
	}

	return windows, nil
}

func (r *Repository) GetStatus(ctx context.Context, fallbackRegionID string) (*Status, error) {
	status := &Status{RegionID: fallbackRegionID}

	if err := r.db.QueryRow(ctx, `
		SELECT COALESCE(MAX(sequence), 0)
		FROM metering.usage_events
	`).Scan(&status.LatestEventSequence); err != nil {
		return nil, fmt.Errorf("query latest event sequence: %w", err)
	}

	if err := r.db.QueryRow(ctx, `
		SELECT COALESCE(MAX(sequence), 0)
		FROM metering.usage_windows
	`).Scan(&status.LatestWindowSequence); err != nil {
		return nil, fmt.Errorf("query latest window sequence: %w", err)
	}

	var completeBefore *time.Time
	if err := r.db.QueryRow(ctx, `
		SELECT MIN(complete_before), COUNT(*)
		FROM metering.producer_watermarks
	`).Scan(&completeBefore, &status.ProducerCount); err != nil {
		return nil, fmt.Errorf("query producer watermarks: %w", err)
	}
	status.CompleteBefore = completeBefore

	var regionID string
	if err := r.db.QueryRow(ctx, `
		SELECT COALESCE(NULLIF(MAX(region_id), ''), '')
		FROM metering.producer_watermarks
	`).Scan(&regionID); err == nil && regionID != "" {
		status.RegionID = regionID
	}

	return status, nil
}

func (r *Repository) GetSandboxProjectionState(ctx context.Context, sandboxID string) (*SandboxProjectionState, error) {
	state := &SandboxProjectionState{}
	err := r.db.QueryRow(ctx, `
		SELECT
			sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
			owner_kind, function_id, function_revision_id, function_runtime_instance_id,
			resource_millicpu, resource_memory_mib,
			claimed_at, active_since, paused, paused_at, terminated_at,
			last_observed_at, last_resource_version
		FROM metering.manager_sandbox_projection_state
		WHERE sandbox_id = $1
	`, sandboxID).Scan(
		&state.SandboxID, &state.Namespace, &state.TeamID, &state.UserID, &state.TemplateID, &state.ClusterID,
		&state.OwnerKind, &state.FunctionID, &state.FunctionRevisionID, &state.FunctionRuntimeInstanceID,
		&state.ResourceMillicpu, &state.ResourceMemoryMiB,
		&state.ClaimedAt, &state.ActiveSince, &state.Paused, &state.PausedAt, &state.TerminatedAt,
		&state.LastObservedAt, &state.LastResourceVer,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query sandbox projection state: %w", err)
	}
	return state, nil
}

func (r *Repository) UpsertSandboxProjectionState(ctx context.Context, state *SandboxProjectionState) error {
	return r.upsertSandboxProjectionState(ctx, r.db, state)
}

func (r *Repository) UpsertSandboxProjectionStateTx(ctx context.Context, tx pgx.Tx, state *SandboxProjectionState) error {
	return r.upsertSandboxProjectionState(ctx, tx, state)
}

func (r *Repository) upsertSandboxProjectionState(ctx context.Context, db DB, state *SandboxProjectionState) error {
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
		state.LastObservedAt = time.Now().UTC()
	}

	_, err := db.Exec(ctx, `
			INSERT INTO metering.manager_sandbox_projection_state (
				sandbox_id, namespace, team_id, user_id, template_id, cluster_id,
				owner_kind, function_id, function_revision_id, function_runtime_instance_id,
				resource_millicpu, resource_memory_mib,
				claimed_at, active_since, paused, paused_at, terminated_at,
			last_observed_at, last_resource_version
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9, $10,
			$11, $12,
			$13, $14, $15, $16, $17,
			$18, $19
		)
		ON CONFLICT (sandbox_id) DO UPDATE
		SET namespace = EXCLUDED.namespace,
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			template_id = EXCLUDED.template_id,
			cluster_id = EXCLUDED.cluster_id,
			owner_kind = EXCLUDED.owner_kind,
			function_id = EXCLUDED.function_id,
			function_revision_id = EXCLUDED.function_revision_id,
			function_runtime_instance_id = EXCLUDED.function_runtime_instance_id,
			resource_millicpu = EXCLUDED.resource_millicpu,
			resource_memory_mib = EXCLUDED.resource_memory_mib,
			claimed_at = EXCLUDED.claimed_at,
			active_since = EXCLUDED.active_since,
			paused = EXCLUDED.paused,
			paused_at = EXCLUDED.paused_at,
			terminated_at = EXCLUDED.terminated_at,
			last_observed_at = EXCLUDED.last_observed_at,
			last_resource_version = EXCLUDED.last_resource_version
	`, state.SandboxID, state.Namespace, state.TeamID, state.UserID, state.TemplateID, state.ClusterID,
		state.OwnerKind, state.FunctionID, state.FunctionRevisionID, state.FunctionRuntimeInstanceID,
		state.ResourceMillicpu, state.ResourceMemoryMiB,
		state.ClaimedAt, state.ActiveSince, state.Paused, state.PausedAt, state.TerminatedAt,
		state.LastObservedAt, state.LastResourceVer,
	)
	if err != nil {
		return fmt.Errorf("upsert sandbox projection state: %w", err)
	}

	return nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
