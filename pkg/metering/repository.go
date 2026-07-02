package metering

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/big"
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

func (r *Repository) AppendEvents(ctx context.Context, events []*Event) error {
	return r.appendEvents(ctx, r.db, events)
}

func (r *Repository) AppendEventsTx(ctx context.Context, tx pgx.Tx, events []*Event) error {
	return r.appendEvents(ctx, tx, events)
}

func (r *Repository) appendEvent(ctx context.Context, db DB, event *Event) error {
	if err := validateEvent(event); err != nil {
		return err
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

func (r *Repository) appendEvents(ctx context.Context, db DB, events []*Event) error {
	if len(events) == 0 {
		return nil
	}
	rows := make([]usageEventRow, 0, len(events))
	for _, event := range events {
		if err := validateEvent(event); err != nil {
			return err
		}
		data := event.Data
		if len(data) == 0 {
			data = json.RawMessage(`{}`)
		}
		rows = append(rows, usageEventRow{
			EventID:     event.EventID,
			Producer:    event.Producer,
			RegionID:    event.RegionID,
			EventType:   event.EventType,
			SubjectType: event.SubjectType,
			SubjectID:   event.SubjectID,
			TeamID:      event.TeamID,
			UserID:      event.UserID,
			SandboxID:   event.SandboxID,
			VolumeID:    event.VolumeID,
			SnapshotID:  event.SnapshotID,
			TemplateID:  event.TemplateID,
			ClusterID:   event.ClusterID,
			OccurredAt:  event.OccurredAt,
			Data:        data,
		})
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshal usage event batch: %w", err)
	}
	_, err = db.Exec(ctx, `
		WITH input AS (
			SELECT *
			FROM jsonb_to_recordset($1::jsonb) AS event(
				event_id text,
				producer text,
				region_id text,
				event_type text,
				subject_type text,
				subject_id text,
				team_id text,
				user_id text,
				sandbox_id text,
				volume_id text,
				snapshot_id text,
				template_id text,
				cluster_id text,
				occurred_at timestamptz,
				data jsonb
			)
		)
		INSERT INTO metering.usage_events (
			event_id, producer, region_id, event_type,
			subject_type, subject_id,
			team_id, user_id,
			sandbox_id, volume_id, snapshot_id,
			template_id, cluster_id,
			occurred_at, data
		)
		SELECT
			event_id, producer, region_id, event_type,
			subject_type, subject_id,
			team_id, user_id,
			NULLIF(sandbox_id, ''), NULLIF(volume_id, ''), NULLIF(snapshot_id, ''),
			NULLIF(template_id, ''), NULLIF(cluster_id, ''),
			occurred_at, data
		FROM input
		ON CONFLICT (event_id) DO NOTHING
	`, payload)
	if err != nil {
		return fmt.Errorf("insert usage events: %w", err)
	}
	return nil
}

func (r *Repository) AppendWindow(ctx context.Context, window *Window) error {
	return r.appendWindow(ctx, r.db, window)
}

func (r *Repository) AppendWindowTx(ctx context.Context, tx pgx.Tx, window *Window) error {
	return r.appendWindow(ctx, tx, window)
}

func (r *Repository) AppendWindows(ctx context.Context, windows []*Window) error {
	return r.appendWindows(ctx, r.db, windows)
}

func (r *Repository) AppendWindowsTx(ctx context.Context, tx pgx.Tx, windows []*Window) error {
	return r.appendWindows(ctx, tx, windows)
}

func (r *Repository) appendWindow(ctx context.Context, db DB, window *Window) error {
	if err := validateWindow(window); err != nil {
		return err
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

func (r *Repository) appendWindows(ctx context.Context, db DB, windows []*Window) error {
	if len(windows) == 0 {
		return nil
	}
	rows := make([]usageWindowRow, 0, len(windows))
	for _, window := range windows {
		if err := validateWindow(window); err != nil {
			return err
		}
		data := window.Data
		if len(data) == 0 {
			data = json.RawMessage(`{}`)
		}
		rows = append(rows, usageWindowRow{
			WindowID:    window.WindowID,
			Producer:    window.Producer,
			RegionID:    window.RegionID,
			WindowType:  window.WindowType,
			SubjectType: window.SubjectType,
			SubjectID:   window.SubjectID,
			TeamID:      window.TeamID,
			UserID:      window.UserID,
			SandboxID:   window.SandboxID,
			VolumeID:    window.VolumeID,
			SnapshotID:  window.SnapshotID,
			TemplateID:  window.TemplateID,
			ClusterID:   window.ClusterID,
			WindowStart: window.WindowStart,
			WindowEnd:   window.WindowEnd,
			Value:       window.Value,
			Unit:        window.Unit,
			Data:        data,
		})
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshal usage window batch: %w", err)
	}
	_, err = db.Exec(ctx, `
		WITH input AS (
			SELECT *
			FROM jsonb_to_recordset($1::jsonb) AS window(
				window_id text,
				producer text,
				region_id text,
				window_type text,
				subject_type text,
				subject_id text,
				team_id text,
				user_id text,
				sandbox_id text,
				volume_id text,
				snapshot_id text,
				template_id text,
				cluster_id text,
				window_start timestamptz,
				window_end timestamptz,
				value bigint,
				unit text,
				data jsonb
			)
		)
		INSERT INTO metering.usage_windows (
			window_id, producer, region_id, window_type,
			subject_type, subject_id,
			team_id, user_id,
			sandbox_id, volume_id, snapshot_id,
			template_id, cluster_id,
			window_start, window_end,
			value, unit, data
		)
		SELECT
			window_id, producer, region_id, window_type,
			subject_type, subject_id,
			team_id, user_id,
			NULLIF(sandbox_id, ''), NULLIF(volume_id, ''), NULLIF(snapshot_id, ''),
			NULLIF(template_id, ''), NULLIF(cluster_id, ''),
			window_start, window_end,
			value, unit, data
		FROM input
		ON CONFLICT (window_id) DO NOTHING
	`, payload)
	if err != nil {
		return fmt.Errorf("insert usage windows: %w", err)
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

func (r *Repository) RecordStorageObservation(ctx context.Context, observation *StorageObservation) error {
	if r.pool == nil {
		return r.recordStorageObservation(ctx, r.db, observation)
	}
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.RecordStorageObservationTx(ctx, tx, observation)
	})
}

func (r *Repository) RecordStorageObservationTx(ctx context.Context, tx pgx.Tx, observation *StorageObservation) error {
	return r.recordStorageObservation(ctx, tx, observation)
}

func (r *Repository) RecordStorageObservations(ctx context.Context, observations []*StorageObservation) error {
	if len(observations) == 0 {
		return nil
	}
	if r.pool == nil {
		for _, observation := range observations {
			if err := r.recordStorageObservation(ctx, r.db, observation); err != nil {
				return err
			}
		}
		return nil
	}
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.recordStorageObservations(ctx, tx, observations)
	})
}

func (r *Repository) RecordStorageObservationsTx(ctx context.Context, tx pgx.Tx, observations []*StorageObservation) error {
	return r.recordStorageObservations(ctx, tx, observations)
}

func (r *Repository) recordStorageObservation(ctx context.Context, db DB, observation *StorageObservation) error {
	state, err := r.normalizeStorageObservation(ctx, db, observation)
	if err != nil {
		return err
	}

	previous, err := r.getStorageProjectionStateForUpdate(ctx, db, state.SubjectType, state.SubjectID)
	if err != nil {
		return err
	}
	if previous != nil {
		if state.ObservedAt.Before(previous.ObservedAt) {
			return nil
		}
		window, remainder := storageWindowFromStateWithRemainder(previous, state.ObservedAt)
		if window != nil {
			if err := r.appendWindow(ctx, db, window); err != nil {
				return err
			}
		}
		state.UnbilledByteNanoseconds = remainder
	}

	return r.upsertStorageProjectionState(ctx, db, state)
}

func (r *Repository) recordStorageObservations(ctx context.Context, db DB, observations []*StorageObservation) error {
	if len(observations) == 0 {
		return nil
	}
	states, err := r.normalizeStorageObservations(ctx, db, observations)
	if err != nil {
		return err
	}
	if len(states) == 0 {
		return nil
	}

	previousStates, err := r.getStorageProjectionStatesForUpdate(ctx, db, storageProjectionKeysFromStates(states))
	if err != nil {
		return err
	}

	windows := make([]*Window, 0, len(states))
	upserts := make([]*StorageProjectionState, 0, len(states))
	for _, state := range states {
		previous := previousStates[storageProjectionKeyString(state.SubjectType, state.SubjectID)]
		if previous != nil {
			if state.ObservedAt.Before(previous.ObservedAt) {
				continue
			}
			window, remainder := storageWindowFromStateWithRemainder(previous, state.ObservedAt)
			if window != nil {
				windows = append(windows, window)
			}
			state.UnbilledByteNanoseconds = remainder
		}
		upserts = append(upserts, state)
	}
	if err := r.appendWindows(ctx, db, windows); err != nil {
		return err
	}
	return r.upsertStorageProjectionStates(ctx, db, upserts)
}

func (r *Repository) CloseStorageObservation(ctx context.Context, observation *StorageObservation) error {
	if r.pool == nil {
		return r.closeStorageObservation(ctx, r.db, observation)
	}
	return r.InTx(ctx, func(tx pgx.Tx) error {
		return r.CloseStorageObservationTx(ctx, tx, observation)
	})
}

func (r *Repository) CloseStorageObservationTx(ctx context.Context, tx pgx.Tx, observation *StorageObservation) error {
	return r.closeStorageObservation(ctx, tx, observation)
}

func (r *Repository) closeStorageObservation(ctx context.Context, db DB, observation *StorageObservation) error {
	state, err := r.normalizeStorageObservation(ctx, db, observation)
	if err != nil {
		return err
	}
	previous, err := r.getStorageProjectionStateForUpdate(ctx, db, state.SubjectType, state.SubjectID)
	if err != nil {
		return err
	}
	if previous == nil {
		if observation.ResourceCreatedAt.IsZero() || !state.ObservedAt.After(observation.ResourceCreatedAt) {
			return nil
		}
		end := state.ObservedAt
		state.ObservedAt = observation.ResourceCreatedAt.UTC()
		if window, _ := storageWindowFromStateWithRemainder(state, end); window != nil {
			return r.appendWindow(ctx, db, window)
		}
		return nil
	}
	if state.ObservedAt.Before(previous.ObservedAt) {
		return r.deleteStorageProjectionState(ctx, db, previous.SubjectType, previous.SubjectID)
	}
	if window, _ := storageWindowFromStateWithRemainder(previous, state.ObservedAt); window != nil {
		if err := r.appendWindow(ctx, db, window); err != nil {
			return err
		}
	}
	return r.deleteStorageProjectionState(ctx, db, previous.SubjectType, previous.SubjectID)
}

func (r *Repository) FlushStorageProjectionWindows(ctx context.Context, before time.Time, limit int) (int, error) {
	if r.pool == nil {
		return 0, fmt.Errorf("pool is nil")
	}
	if before.IsZero() {
		before = time.Now().UTC()
	} else {
		before = before.UTC()
	}
	if limit <= 0 {
		limit = 500
	}

	processed := 0
	err := r.InTx(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `
			SELECT
				subject_type, subject_id, product, owner_kind,
				team_id, user_id, COALESCE(sandbox_id, ''), COALESCE(volume_id, ''),
				COALESCE(snapshot_id, ''), COALESCE(cluster_id, ''), region_id,
				size_bytes, observed_at, unbilled_byte_nanoseconds
			FROM metering.storage_projection_state
			WHERE observed_at < $1
			ORDER BY observed_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		`, before, limit)
		if err != nil {
			return fmt.Errorf("query storage projection states: %w", err)
		}
		defer rows.Close()

		states := make([]*StorageProjectionState, 0, limit)
		for rows.Next() {
			state := &StorageProjectionState{}
			if err := rows.Scan(
				&state.SubjectType, &state.SubjectID, &state.Product, &state.OwnerKind,
				&state.TeamID, &state.UserID, &state.SandboxID, &state.VolumeID,
				&state.SnapshotID, &state.ClusterID, &state.RegionID,
				&state.SizeBytes, &state.ObservedAt, &state.UnbilledByteNanoseconds,
			); err != nil {
				return fmt.Errorf("scan storage projection state: %w", err)
			}
			states = append(states, state)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate storage projection states: %w", err)
		}

		windows := make([]*Window, 0, len(states))
		advances := make([]storageProjectionAdvanceRow, 0, len(states))
		for _, state := range states {
			window, remainder := storageWindowFromStateWithRemainder(state, before)
			if window != nil {
				windows = append(windows, window)
			}
			advances = append(advances, storageProjectionAdvanceRow{
				SubjectType:             state.SubjectType,
				SubjectID:               state.SubjectID,
				UnbilledByteNanoseconds: remainder,
			})
		}
		if err := r.appendWindows(ctx, tx, windows); err != nil {
			return err
		}
		if err := r.advanceStorageProjectionStates(ctx, tx, before, advances); err != nil {
			return err
		}
		processed = len(states)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return processed, nil
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
			owner_kind, resource_millicpu, resource_memory_mib,
			claimed_at, active_since, paused, paused_at, terminated_at,
			last_observed_at, last_resource_version
		FROM metering.manager_sandbox_projection_state
		WHERE sandbox_id = $1
	`, sandboxID).Scan(
		&state.SandboxID, &state.Namespace, &state.TeamID, &state.UserID, &state.TemplateID, &state.ClusterID,
		&state.OwnerKind, &state.ResourceMillicpu, &state.ResourceMemoryMiB,
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
				owner_kind, resource_millicpu, resource_memory_mib,
				claimed_at, active_since, paused, paused_at, terminated_at,
			last_observed_at, last_resource_version
		) VALUES (
			$1, $2, $3, $4, $5, $6,
			$7, $8, $9,
			$10, $11, $12, $13, $14,
			$15, $16
		)
		ON CONFLICT (sandbox_id) DO UPDATE
		SET namespace = EXCLUDED.namespace,
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			template_id = EXCLUDED.template_id,
			cluster_id = EXCLUDED.cluster_id,
			owner_kind = EXCLUDED.owner_kind,
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
		state.OwnerKind, state.ResourceMillicpu, state.ResourceMemoryMiB,
		state.ClaimedAt, state.ActiveSince, state.Paused, state.PausedAt, state.TerminatedAt,
		state.LastObservedAt, state.LastResourceVer,
	)
	if err != nil {
		return fmt.Errorf("upsert sandbox projection state: %w", err)
	}

	return nil
}

func (r *Repository) normalizeStorageObservation(ctx context.Context, db DB, observation *StorageObservation) (*StorageProjectionState, error) {
	state, err := normalizeStorageObservationInput(observation)
	if err != nil {
		return nil, err
	}
	if state.SandboxID != "" {
		if err := r.applySandboxStorageProduct(ctx, db, state); err != nil {
			return nil, err
		}
	}
	return state, nil
}

func (r *Repository) normalizeStorageObservations(ctx context.Context, db DB, observations []*StorageObservation) ([]*StorageProjectionState, error) {
	states := make([]*StorageProjectionState, 0, len(observations))
	seen := make(map[string]struct{}, len(observations))
	sandboxIDs := make([]string, 0, len(observations))
	sandboxSeen := make(map[string]struct{}, len(observations))
	for _, observation := range observations {
		state, err := normalizeStorageObservationInput(observation)
		if err != nil {
			return nil, err
		}
		key := storageProjectionKeyString(state.SubjectType, state.SubjectID)
		if _, ok := seen[key]; ok {
			return nil, fmt.Errorf("duplicate storage observation for %s/%s", state.SubjectType, state.SubjectID)
		}
		seen[key] = struct{}{}
		if state.SandboxID != "" {
			if _, ok := sandboxSeen[state.SandboxID]; !ok {
				sandboxSeen[state.SandboxID] = struct{}{}
				sandboxIDs = append(sandboxIDs, state.SandboxID)
			}
		}
		states = append(states, state)
	}
	if len(sandboxIDs) == 0 {
		return states, nil
	}
	ownerKinds, err := r.sandboxStorageOwnerKinds(ctx, db, sandboxIDs)
	if err != nil {
		return nil, err
	}
	for _, state := range states {
		if state.SandboxID == "" || state.OwnerKind != "" {
			continue
		}
		state.OwnerKind = ownerKinds[state.SandboxID]
	}
	return states, nil
}

func normalizeStorageObservationInput(observation *StorageObservation) (*StorageProjectionState, error) {
	if observation == nil {
		return nil, fmt.Errorf("storage observation is nil")
	}
	if observation.SubjectType == "" || observation.SubjectID == "" {
		return nil, fmt.Errorf("storage subject_type and subject_id are required")
	}
	if observation.SubjectType != SubjectTypeVolume && observation.SubjectType != SubjectTypeSnapshot && observation.SubjectType != SubjectTypeRootFS {
		return nil, fmt.Errorf("unsupported storage subject_type %q", observation.SubjectType)
	}
	if observation.SizeBytes < 0 {
		return nil, fmt.Errorf("storage size_bytes must be non-negative")
	}
	observedAt := observation.ObservedAt
	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	} else {
		observedAt = observedAt.UTC()
	}
	product := observation.Product
	if product == "" {
		product = ProductSandbox
	}
	return &StorageProjectionState{
		SubjectType: observation.SubjectType,
		SubjectID:   observation.SubjectID,
		Product:     product,
		OwnerKind:   observation.OwnerKind,
		TeamID:      observation.TeamID,
		UserID:      observation.UserID,
		SandboxID:   observation.SandboxID,
		VolumeID:    observation.VolumeID,
		SnapshotID:  observation.SnapshotID,
		ClusterID:   observation.ClusterID,
		RegionID:    observation.RegionID,
		SizeBytes:   observation.SizeBytes,
		ObservedAt:  observedAt,
	}, nil
}

func (r *Repository) applySandboxStorageProduct(ctx context.Context, db DB, state *StorageProjectionState) error {
	var ownerKind string
	err := db.QueryRow(ctx, `
		SELECT owner_kind
		FROM metering.manager_sandbox_projection_state
		WHERE sandbox_id = $1
	`, state.SandboxID).Scan(&ownerKind)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil
		}
		return fmt.Errorf("query sandbox storage owner state: %w", err)
	}
	if state.OwnerKind == "" {
		state.OwnerKind = ownerKind
	}
	return nil
}

func (r *Repository) sandboxStorageOwnerKinds(ctx context.Context, db DB, sandboxIDs []string) (map[string]string, error) {
	payload, err := json.Marshal(sandboxIDs)
	if err != nil {
		return nil, fmt.Errorf("marshal sandbox ids: %w", err)
	}
	rows, err := db.Query(ctx, `
		WITH sandbox_ids AS (
			SELECT value AS sandbox_id
			FROM jsonb_array_elements_text($1::jsonb)
		)
		SELECT p.sandbox_id, p.owner_kind
		FROM metering.manager_sandbox_projection_state p
		JOIN sandbox_ids ON sandbox_ids.sandbox_id = p.sandbox_id
	`, payload)
	if err != nil {
		return nil, fmt.Errorf("query sandbox storage owner states: %w", err)
	}
	defer rows.Close()
	out := make(map[string]string, len(sandboxIDs))
	for rows.Next() {
		var sandboxID, ownerKind string
		if err := rows.Scan(&sandboxID, &ownerKind); err != nil {
			return nil, fmt.Errorf("scan sandbox storage owner state: %w", err)
		}
		out[sandboxID] = ownerKind
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sandbox storage owner states: %w", err)
	}
	return out, nil
}

func (r *Repository) getStorageProjectionStateForUpdate(ctx context.Context, db DB, subjectType, subjectID string) (*StorageProjectionState, error) {
	state := &StorageProjectionState{}
	err := db.QueryRow(ctx, `
		SELECT
			subject_type, subject_id, product, owner_kind,
			team_id, user_id, COALESCE(sandbox_id, ''), COALESCE(volume_id, ''),
			COALESCE(snapshot_id, ''), COALESCE(cluster_id, ''), region_id,
			size_bytes, observed_at, unbilled_byte_nanoseconds
		FROM metering.storage_projection_state
		WHERE subject_type = $1 AND subject_id = $2
		FOR UPDATE
	`, subjectType, subjectID).Scan(
		&state.SubjectType, &state.SubjectID, &state.Product, &state.OwnerKind,
		&state.TeamID, &state.UserID, &state.SandboxID, &state.VolumeID,
		&state.SnapshotID, &state.ClusterID, &state.RegionID,
		&state.SizeBytes, &state.ObservedAt, &state.UnbilledByteNanoseconds,
	)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query storage projection state: %w", err)
	}
	return state, nil
}

func (r *Repository) getStorageProjectionStatesForUpdate(ctx context.Context, db DB, keys []storageProjectionKey) (map[string]*StorageProjectionState, error) {
	if len(keys) == 0 {
		return map[string]*StorageProjectionState{}, nil
	}
	payload, err := json.Marshal(keys)
	if err != nil {
		return nil, fmt.Errorf("marshal storage projection keys: %w", err)
	}
	rows, err := db.Query(ctx, `
		WITH input AS (
			SELECT *
			FROM jsonb_to_recordset($1::jsonb) AS key(subject_type text, subject_id text)
		)
		SELECT
			s.subject_type, s.subject_id, s.product, s.owner_kind,
			s.team_id, s.user_id, COALESCE(s.sandbox_id, ''), COALESCE(s.volume_id, ''),
			COALESCE(s.snapshot_id, ''), COALESCE(s.cluster_id, ''), s.region_id,
			s.size_bytes, s.observed_at, s.unbilled_byte_nanoseconds
		FROM metering.storage_projection_state s
		JOIN input ON input.subject_type = s.subject_type AND input.subject_id = s.subject_id
		FOR UPDATE OF s
	`, payload)
	if err != nil {
		return nil, fmt.Errorf("query storage projection states: %w", err)
	}
	defer rows.Close()

	states := make(map[string]*StorageProjectionState, len(keys))
	for rows.Next() {
		state := &StorageProjectionState{}
		if err := rows.Scan(
			&state.SubjectType, &state.SubjectID, &state.Product, &state.OwnerKind,
			&state.TeamID, &state.UserID, &state.SandboxID, &state.VolumeID,
			&state.SnapshotID, &state.ClusterID, &state.RegionID,
			&state.SizeBytes, &state.ObservedAt, &state.UnbilledByteNanoseconds,
		); err != nil {
			return nil, fmt.Errorf("scan storage projection state: %w", err)
		}
		states[storageProjectionKeyString(state.SubjectType, state.SubjectID)] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate storage projection states: %w", err)
	}
	return states, nil
}

func (r *Repository) upsertStorageProjectionState(ctx context.Context, db DB, state *StorageProjectionState) error {
	_, err := db.Exec(ctx, `
		INSERT INTO metering.storage_projection_state (
			subject_type, subject_id, product, owner_kind,
			team_id, user_id, sandbox_id, volume_id, snapshot_id,
			cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds
		) VALUES (
			$1, $2, $3, $4,
			$5, $6, $7, $8, $9,
			$10, $11, $12, $13, $14
		)
		ON CONFLICT (subject_type, subject_id) DO UPDATE
		SET product = EXCLUDED.product,
			owner_kind = EXCLUDED.owner_kind,
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			sandbox_id = EXCLUDED.sandbox_id,
			volume_id = EXCLUDED.volume_id,
			snapshot_id = EXCLUDED.snapshot_id,
			cluster_id = EXCLUDED.cluster_id,
			region_id = EXCLUDED.region_id,
			size_bytes = EXCLUDED.size_bytes,
			observed_at = EXCLUDED.observed_at,
			unbilled_byte_nanoseconds = EXCLUDED.unbilled_byte_nanoseconds,
			updated_at = NOW()
	`, state.SubjectType, state.SubjectID, state.Product, state.OwnerKind,
		state.TeamID, state.UserID, nullableString(state.SandboxID), nullableString(state.VolumeID), nullableString(state.SnapshotID),
		nullableString(state.ClusterID), state.RegionID, state.SizeBytes, state.ObservedAt, state.UnbilledByteNanoseconds,
	)
	if err != nil {
		return fmt.Errorf("upsert storage projection state: %w", err)
	}
	return nil
}

func (r *Repository) upsertStorageProjectionStates(ctx context.Context, db DB, states []*StorageProjectionState) error {
	if len(states) == 0 {
		return nil
	}
	rows := make([]storageProjectionStateRow, 0, len(states))
	for _, state := range states {
		if state == nil {
			return fmt.Errorf("storage projection state is nil")
		}
		rows = append(rows, storageProjectionStateRow{
			SubjectType:             state.SubjectType,
			SubjectID:               state.SubjectID,
			Product:                 state.Product,
			OwnerKind:               state.OwnerKind,
			TeamID:                  state.TeamID,
			UserID:                  state.UserID,
			SandboxID:               state.SandboxID,
			VolumeID:                state.VolumeID,
			SnapshotID:              state.SnapshotID,
			ClusterID:               state.ClusterID,
			RegionID:                state.RegionID,
			SizeBytes:               state.SizeBytes,
			ObservedAt:              state.ObservedAt,
			UnbilledByteNanoseconds: state.UnbilledByteNanoseconds,
		})
	}
	payload, err := json.Marshal(rows)
	if err != nil {
		return fmt.Errorf("marshal storage projection states: %w", err)
	}
	_, err = db.Exec(ctx, `
		WITH input AS (
			SELECT *
			FROM jsonb_to_recordset($1::jsonb) AS state(
				subject_type text,
				subject_id text,
				product text,
				owner_kind text,
				team_id text,
				user_id text,
				sandbox_id text,
				volume_id text,
				snapshot_id text,
				cluster_id text,
				region_id text,
				size_bytes bigint,
				observed_at timestamptz,
				unbilled_byte_nanoseconds bigint
			)
		)
		INSERT INTO metering.storage_projection_state (
			subject_type, subject_id, product, owner_kind,
			team_id, user_id, sandbox_id, volume_id, snapshot_id,
			cluster_id, region_id, size_bytes, observed_at, unbilled_byte_nanoseconds
		)
		SELECT
			subject_type, subject_id, product, owner_kind,
			team_id, user_id, NULLIF(sandbox_id, ''), NULLIF(volume_id, ''), NULLIF(snapshot_id, ''),
			NULLIF(cluster_id, ''), region_id, size_bytes, observed_at, unbilled_byte_nanoseconds
		FROM input
		ON CONFLICT (subject_type, subject_id) DO UPDATE
		SET product = EXCLUDED.product,
			owner_kind = EXCLUDED.owner_kind,
			team_id = EXCLUDED.team_id,
			user_id = EXCLUDED.user_id,
			sandbox_id = EXCLUDED.sandbox_id,
			volume_id = EXCLUDED.volume_id,
			snapshot_id = EXCLUDED.snapshot_id,
			cluster_id = EXCLUDED.cluster_id,
			region_id = EXCLUDED.region_id,
			size_bytes = EXCLUDED.size_bytes,
			observed_at = EXCLUDED.observed_at,
			unbilled_byte_nanoseconds = EXCLUDED.unbilled_byte_nanoseconds,
			updated_at = NOW()
	`, payload)
	if err != nil {
		return fmt.Errorf("upsert storage projection states: %w", err)
	}
	return nil
}

func (r *Repository) advanceStorageProjectionStates(ctx context.Context, db DB, observedAt time.Time, advances []storageProjectionAdvanceRow) error {
	if len(advances) == 0 {
		return nil
	}
	payload, err := json.Marshal(advances)
	if err != nil {
		return fmt.Errorf("marshal storage projection advances: %w", err)
	}
	if _, err := db.Exec(ctx, `
		WITH input AS (
			SELECT *
			FROM jsonb_to_recordset($2::jsonb) AS advance(
				subject_type text,
				subject_id text,
				unbilled_byte_nanoseconds bigint
			)
		)
		UPDATE metering.storage_projection_state state
		SET observed_at = $1,
			unbilled_byte_nanoseconds = input.unbilled_byte_nanoseconds,
			updated_at = NOW()
		FROM input
		WHERE state.subject_type = input.subject_type
			AND state.subject_id = input.subject_id
	`, observedAt, payload); err != nil {
		return fmt.Errorf("advance storage projection states: %w", err)
	}
	return nil
}

func (r *Repository) deleteStorageProjectionState(ctx context.Context, db DB, subjectType, subjectID string) error {
	_, err := db.Exec(ctx, `
		DELETE FROM metering.storage_projection_state
		WHERE subject_type = $1 AND subject_id = $2
	`, subjectType, subjectID)
	if err != nil {
		return fmt.Errorf("delete storage projection state: %w", err)
	}
	return nil
}

type usageEventRow struct {
	EventID     string          `json:"event_id"`
	Producer    string          `json:"producer"`
	RegionID    string          `json:"region_id"`
	EventType   string          `json:"event_type"`
	SubjectType string          `json:"subject_type"`
	SubjectID   string          `json:"subject_id"`
	TeamID      string          `json:"team_id"`
	UserID      string          `json:"user_id"`
	SandboxID   string          `json:"sandbox_id"`
	VolumeID    string          `json:"volume_id"`
	SnapshotID  string          `json:"snapshot_id"`
	TemplateID  string          `json:"template_id"`
	ClusterID   string          `json:"cluster_id"`
	OccurredAt  time.Time       `json:"occurred_at"`
	Data        json.RawMessage `json:"data"`
}

type usageWindowRow struct {
	WindowID    string          `json:"window_id"`
	Producer    string          `json:"producer"`
	RegionID    string          `json:"region_id"`
	WindowType  string          `json:"window_type"`
	SubjectType string          `json:"subject_type"`
	SubjectID   string          `json:"subject_id"`
	TeamID      string          `json:"team_id"`
	UserID      string          `json:"user_id"`
	SandboxID   string          `json:"sandbox_id"`
	VolumeID    string          `json:"volume_id"`
	SnapshotID  string          `json:"snapshot_id"`
	TemplateID  string          `json:"template_id"`
	ClusterID   string          `json:"cluster_id"`
	WindowStart time.Time       `json:"window_start"`
	WindowEnd   time.Time       `json:"window_end"`
	Value       int64           `json:"value"`
	Unit        string          `json:"unit"`
	Data        json.RawMessage `json:"data"`
}

type storageProjectionKey struct {
	SubjectType string `json:"subject_type"`
	SubjectID   string `json:"subject_id"`
}

type storageProjectionStateRow struct {
	SubjectType             string    `json:"subject_type"`
	SubjectID               string    `json:"subject_id"`
	Product                 string    `json:"product"`
	OwnerKind               string    `json:"owner_kind"`
	TeamID                  string    `json:"team_id"`
	UserID                  string    `json:"user_id"`
	SandboxID               string    `json:"sandbox_id"`
	VolumeID                string    `json:"volume_id"`
	SnapshotID              string    `json:"snapshot_id"`
	ClusterID               string    `json:"cluster_id"`
	RegionID                string    `json:"region_id"`
	SizeBytes               int64     `json:"size_bytes"`
	ObservedAt              time.Time `json:"observed_at"`
	UnbilledByteNanoseconds int64     `json:"unbilled_byte_nanoseconds"`
}

type storageProjectionAdvanceRow struct {
	SubjectType             string `json:"subject_type"`
	SubjectID               string `json:"subject_id"`
	UnbilledByteNanoseconds int64  `json:"unbilled_byte_nanoseconds"`
}

func storageProjectionKeysFromStates(states []*StorageProjectionState) []storageProjectionKey {
	keys := make([]storageProjectionKey, 0, len(states))
	for _, state := range states {
		if state == nil {
			continue
		}
		keys = append(keys, storageProjectionKey{
			SubjectType: state.SubjectType,
			SubjectID:   state.SubjectID,
		})
	}
	return keys
}

func storageProjectionKeyString(subjectType, subjectID string) string {
	return subjectType + "\x00" + subjectID
}

func storageWindowFromState(state *StorageProjectionState, end time.Time) *Window {
	window, _ := storageWindowFromStateWithRemainder(state, end)
	return window
}

func storageWindowFromStateWithRemainder(state *StorageProjectionState, end time.Time) (*Window, int64) {
	if state == nil {
		return nil, 0
	}
	remainder := normalizeStorageRemainder(state.UnbilledByteNanoseconds)
	end = end.UTC()
	start := state.ObservedAt.UTC()
	if !end.After(start) {
		return nil, remainder
	}
	value, remainder := storageByteHoursWithRemainder(state.SizeBytes, end.Sub(start), remainder)
	if value <= 0 {
		return nil, remainder
	}
	windowType := WindowTypeSandboxVolumeByteHours
	switch state.SubjectType {
	case SubjectTypeRootFS:
		windowType = WindowTypeSandboxRootFSByteHours
	}
	return &Window{
		WindowID:    fmt.Sprintf("storage/%s/%s/%d/%d", state.SubjectType, state.SubjectID, start.UnixNano(), end.UnixNano()),
		Producer:    ProducerStorage,
		RegionID:    state.RegionID,
		WindowType:  windowType,
		SubjectType: state.SubjectType,
		SubjectID:   state.SubjectID,
		TeamID:      state.TeamID,
		UserID:      state.UserID,
		SandboxID:   state.SandboxID,
		VolumeID:    state.VolumeID,
		SnapshotID:  state.SnapshotID,
		ClusterID:   state.ClusterID,
		WindowStart: start,
		WindowEnd:   end,
		Value:       value,
		Unit:        WindowUnitByteHours,
		Data:        storageWindowData(state, end.Sub(start)),
	}, remainder
}

func storageByteHoursWithRemainder(sizeBytes int64, duration time.Duration, previousRemainder int64) (int64, int64) {
	remainder := normalizeStorageRemainder(previousRemainder)
	if sizeBytes <= 0 || duration <= 0 {
		return 0, remainder
	}
	accumulator := big.NewInt(remainder)
	var usage big.Int
	usage.Mul(big.NewInt(sizeBytes), big.NewInt(duration.Nanoseconds()))
	accumulator.Add(accumulator, &usage)

	hourNanos := big.NewInt(int64(time.Hour))
	var quotient big.Int
	var modulo big.Int
	quotient.QuoRem(accumulator, hourNanos, &modulo)
	if !quotient.IsInt64() {
		return math.MaxInt64, 0
	}
	return quotient.Int64(), modulo.Int64()
}

func normalizeStorageRemainder(value int64) int64 {
	if value <= 0 {
		return 0
	}
	hourNanos := int64(time.Hour)
	return value % hourNanos
}

func storageWindowData(state *StorageProjectionState, duration time.Duration) json.RawMessage {
	data := map[string]any{
		"product":               state.Product,
		"size_bytes":            state.SizeBytes,
		"duration_milliseconds": duration.Milliseconds(),
	}
	if state.OwnerKind != "" {
		data["owner_kind"] = state.OwnerKind
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return raw
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func validateEvent(event *Event) error {
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
	return nil
}

func validateWindow(window *Window) error {
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
	return nil
}
