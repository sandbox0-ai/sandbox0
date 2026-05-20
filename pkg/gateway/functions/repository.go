package functions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNotFound      = errors.New("function not found")
	ErrAlreadyExists = errors.New("function already exists")
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateFunctionWithRevision(ctx context.Context, fn *Function, rev *Revision, createdBy string) (*Function, *Revision, error) {
	if r == nil || r.pool == nil {
		return nil, nil, fmt.Errorf("function repository is not configured")
	}
	now := time.Now().UTC()
	if fn.ID == "" {
		fn.ID = uuid.NewString()
	}
	if rev.ID == "" {
		rev.ID = uuid.NewString()
	}
	fn.CreatedAt = now
	fn.UpdatedAt = now
	fn.CreatedBy = createdBy
	fn.Autoscaling = NormalizeAutoscaling(fn.Autoscaling)
	rev.FunctionID = fn.ID
	rev.TeamID = fn.TeamID
	rev.RevisionNumber = 1
	rev.CreatedAt = now
	rev.CreatedBy = createdBy
	fn.ActiveRevisionID = nil

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	if err := insertFunction(ctx, tx, fn); err != nil {
		if strings.Contains(err.Error(), "duplicate") {
			return nil, nil, ErrAlreadyExists
		}
		return nil, nil, err
	}
	if err := insertRevision(ctx, tx, rev); err != nil {
		return nil, nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE functions
		SET active_revision_id = $2, updated_at = $3
		WHERE id = $1
	`, fn.ID, rev.ID, now); err != nil {
		return nil, nil, err
	}
	fn.ActiveRevisionID = &rev.ID
	if err := upsertAlias(ctx, tx, fn.ID, ProductionAlias, rev.ID, createdBy, now); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}
	return fn, rev, nil
}

func (r *Repository) CreateRevision(ctx context.Context, teamID, functionRef string, rev *Revision, promote bool, createdBy string) (*Revision, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if rev.ID == "" {
		rev.ID = uuid.NewString()
	}
	rev.FunctionID = fn.ID
	rev.TeamID = teamID
	rev.CreatedAt = now
	rev.CreatedBy = createdBy

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(revision_number), 0) + 1
		FROM functions_revisions
		WHERE function_id = $1
	`, fn.ID).Scan(&rev.RevisionNumber); err != nil {
		return nil, err
	}
	if err := insertRevision(ctx, tx, rev); err != nil {
		return nil, err
	}
	if promote {
		if err := upsertAlias(ctx, tx, fn.ID, ProductionAlias, rev.ID, createdBy, now); err != nil {
			return nil, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE functions
			SET active_revision_id = $2, updated_at = $3
			WHERE id = $1
		`, fn.ID, rev.ID, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return rev, nil
}

func (r *Repository) ListFunctions(ctx context.Context, teamID string) ([]*Function, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
		FROM functions
		WHERE team_id = $1 AND deleted_at IS NULL
		ORDER BY updated_at DESC, created_at DESC
	`, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Function
	for rows.Next() {
		fn, err := scanFunction(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, fn)
	}
	return out, rows.Err()
}

func (r *Repository) GetFunction(ctx context.Context, teamID, ref string) (*Function, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	ref = strings.TrimSpace(ref)
	var row pgx.Row
	if id, err := uuid.Parse(ref); err == nil {
		row = r.pool.QueryRow(ctx, `
			SELECT id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
			FROM functions
			WHERE team_id = $1 AND deleted_at IS NULL AND (id = $2 OR slug = $3)
		`, teamID, id, ref)
	} else {
		row = r.pool.QueryRow(ctx, `
			SELECT id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
			FROM functions
			WHERE team_id = $1 AND deleted_at IS NULL AND slug = $2
		`, teamID, ref)
	}
	fn, err := scanFunction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return fn, err
}

func (r *Repository) GetFunctionByDomainLabel(ctx context.Context, domainLabel string) (*Function, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	row := r.pool.QueryRow(ctx, `
		SELECT id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
		FROM functions
		WHERE domain_label = $1 AND deleted_at IS NULL
	`, domainLabel)
	fn, err := scanFunction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return fn, err
}

func (r *Repository) GetActiveRevision(ctx context.Context, fn *Function) (*Revision, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	if fn == nil || fn.ActiveRevisionID == nil || strings.TrimSpace(*fn.ActiveRevisionID) == "" {
		return nil, ErrNotFound
	}
	row := r.pool.QueryRow(ctx, `
		SELECT id, function_id, team_id, revision_number, source_sandbox_id, source_service_id,
			source_template_id, restore_mounts, service_snapshot, runtime_sandbox_id, runtime_context_id,
			runtime_updated_at, created_by, created_at
		FROM functions_revisions
		WHERE function_id = $1 AND id = $2
	`, fn.ID, *fn.ActiveRevisionID)
	rev, err := scanRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rev, err
}

func (r *Repository) ListRevisions(ctx context.Context, teamID, functionRef string) ([]*Revision, error) {
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, function_id, team_id, revision_number, source_sandbox_id, source_service_id,
			source_template_id, restore_mounts, service_snapshot, runtime_sandbox_id, runtime_context_id,
			runtime_updated_at, created_by, created_at
		FROM functions_revisions
		WHERE function_id = $1
		ORDER BY revision_number DESC
	`, fn.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Revision
	for rows.Next() {
		rev, err := scanRevision(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, rev)
	}
	return out, rows.Err()
}

func (r *Repository) GetRevision(ctx context.Context, teamID, functionID, revisionID string) (*Revision, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	row := r.pool.QueryRow(ctx, `
		SELECT id, function_id, team_id, revision_number, source_sandbox_id, source_service_id,
			source_template_id, restore_mounts, service_snapshot, runtime_sandbox_id, runtime_context_id,
			runtime_updated_at, created_by, created_at
		FROM functions_revisions
		WHERE team_id = $1 AND function_id = $2 AND id = $3
	`, teamID, functionID, revisionID)
	rev, err := scanRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rev, err
}

func (r *Repository) GetRevisionByNumber(ctx context.Context, teamID, functionRef string, revisionNumber int) (*Revision, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	row := r.pool.QueryRow(ctx, `
		SELECT id, function_id, team_id, revision_number, source_sandbox_id, source_service_id,
			source_template_id, restore_mounts, service_snapshot, runtime_sandbox_id, runtime_context_id,
			runtime_updated_at, created_by, created_at
		FROM functions_revisions
		WHERE function_id = $1 AND revision_number = $2
	`, fn.ID, revisionNumber)
	rev, err := scanRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rev, err
}

func (r *Repository) SetRevisionRuntime(ctx context.Context, teamID, functionID, revisionID, sandboxID, contextID string) (*Revision, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	row := r.pool.QueryRow(ctx, `
		UPDATE functions_revisions
		SET runtime_sandbox_id = $4, runtime_context_id = $5, runtime_updated_at = NOW()
		WHERE team_id = $1 AND function_id = $2 AND id = $3
		RETURNING id, function_id, team_id, revision_number, source_sandbox_id, source_service_id,
			source_template_id, restore_mounts, service_snapshot, runtime_sandbox_id, runtime_context_id,
			runtime_updated_at, created_by, created_at
	`, teamID, functionID, revisionID, sandboxID, contextID)
	rev, err := scanRevision(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return rev, err
}

func (r *Repository) ClearRevisionRuntime(ctx context.Context, teamID, functionID, revisionID string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("function repository is not configured")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
		UPDATE functions_revisions
		SET runtime_sandbox_id = NULL, runtime_context_id = NULL, runtime_updated_at = NULL
		WHERE team_id = $1 AND function_id = $2 AND id = $3
	`, teamID, functionID, revisionID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM function_runtime_instances
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3
	`, teamID, functionID, revisionID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ListRuntimeInstances(ctx context.Context, teamID, functionID, revisionID string) ([]*RuntimeInstance, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, team_id, function_id, revision_id, sandbox_id, context_id, state,
			readiness_state, startup_duration_ms, last_error, last_error_at,
			ready_at, last_used_at, draining_at, failed_at, created_at, updated_at
		FROM function_runtime_instances
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3
		ORDER BY
			CASE state
				WHEN 'ready' THEN 0
				WHEN 'starting' THEN 1
				WHEN 'draining' THEN 2
				ELSE 3
			END,
			COALESCE(last_used_at, ready_at, created_at) ASC
	`, teamID, functionID, revisionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RuntimeInstance
	for rows.Next() {
		inst, err := scanRuntimeInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func (r *Repository) ListRuntimeEvents(ctx context.Context, teamID, functionID, revisionID string, limit int) ([]*RuntimeEvent, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	if limit <= 0 {
		limit = 20
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, team_id, function_id, revision_id, runtime_instance_id, runtime_sandbox_id,
			runtime_context_id, phase, readiness_state, reason, message, startup_duration_ms, created_at
		FROM function_runtime_events
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3
		ORDER BY created_at DESC
		LIMIT $4
	`, teamID, functionID, revisionID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]*RuntimeEvent, 0, limit)
	for rows.Next() {
		event, err := scanRuntimeEvent(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, event)
	}
	return out, rows.Err()
}

func (r *Repository) AppendRuntimeEvent(ctx context.Context, event *RuntimeEvent) (*RuntimeEvent, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	if event == nil {
		return nil, fmt.Errorf("runtime event is nil")
	}
	if event.ID == "" {
		event.ID = uuid.NewString()
	}
	event.Reason = strings.TrimSpace(event.Reason)
	event.Message = strings.TrimSpace(event.Message)
	event.ReadinessState = normalizeRuntimeReadinessState(event.ReadinessState, RuntimeInstanceState(event.Phase))
	row := r.pool.QueryRow(ctx, `
		INSERT INTO function_runtime_events (
			id, team_id, function_id, revision_id, runtime_instance_id, runtime_sandbox_id,
			runtime_context_id, phase, readiness_state, reason, message, startup_duration_ms
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
		RETURNING id, team_id, function_id, revision_id, runtime_instance_id, runtime_sandbox_id,
			runtime_context_id, phase, readiness_state, reason, message, startup_duration_ms, created_at
	`, event.ID, event.TeamID, event.FunctionID, event.RevisionID, event.RuntimeInstanceID, event.RuntimeSandboxID,
		event.RuntimeContextID, event.Phase, event.ReadinessState, event.Reason, event.Message, event.StartupDurationMS)
	out, err := scanRuntimeEvent(row)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Repository) CreateRuntimeInstance(ctx context.Context, inst *RuntimeInstance) (*RuntimeInstance, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	if inst == nil {
		return nil, fmt.Errorf("runtime instance is nil")
	}
	if inst.ID == "" {
		inst.ID = uuid.NewString()
	}
	if inst.State == "" {
		inst.State = RuntimeInstanceStateStarting
	}
	row := r.pool.QueryRow(ctx, `
		INSERT INTO function_runtime_instances (
			id, team_id, function_id, revision_id, sandbox_id, context_id, state, last_error,
			readiness_state, startup_duration_ms, last_error_at, ready_at, last_used_at, draining_at, failed_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
		RETURNING id, team_id, function_id, revision_id, sandbox_id, context_id, state,
			readiness_state, startup_duration_ms, last_error, last_error_at,
			ready_at, last_used_at, draining_at, failed_at, created_at, updated_at
	`, inst.ID, inst.TeamID, inst.FunctionID, inst.RevisionID, inst.SandboxID, inst.ContextID, inst.State, inst.LastError,
		normalizeRuntimeReadinessState(inst.ReadinessState, inst.State), inst.StartupDurationMS, inst.LastErrorAt,
		inst.ReadyAt, inst.LastUsedAt, inst.DrainingAt, inst.FailedAt)
	created, err := scanRuntimeInstance(row)
	if err != nil {
		return nil, err
	}
	return created, nil
}

func (r *Repository) MarkRuntimeInstanceReady(ctx context.Context, teamID, functionID, revisionID, instanceID, sandboxID, contextID string, startupDurationMS *int) (*RuntimeInstance, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	row := tx.QueryRow(ctx, `
		UPDATE function_runtime_instances
		SET sandbox_id = $5, context_id = $6, state = 'ready', last_error = NULL,
			last_error_at = NULL, readiness_state = 'ready', startup_duration_ms = COALESCE($7, startup_duration_ms),
			ready_at = COALESCE(ready_at, NOW()), last_used_at = NOW(), draining_at = NULL, failed_at = NULL
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3 AND id = $4
		RETURNING id, team_id, function_id, revision_id, sandbox_id, context_id, state,
			readiness_state, startup_duration_ms, last_error, last_error_at,
			ready_at, last_used_at, draining_at, failed_at, created_at, updated_at
	`, teamID, functionID, revisionID, instanceID, sandboxID, contextID, startupDurationMS)
	inst, err := scanRuntimeInstance(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE functions_revisions
		SET runtime_sandbox_id = $4, runtime_context_id = $5, runtime_updated_at = NOW()
		WHERE team_id = $1 AND function_id = $2 AND id = $3
	`, teamID, functionID, revisionID, sandboxID, contextID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return inst, nil
}

func (r *Repository) MarkRuntimeInstanceUsed(ctx context.Context, teamID, functionID, revisionID, instanceID string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("function repository is not configured")
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE function_runtime_instances
		SET last_used_at = NOW()
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3 AND id = $4
	`, teamID, functionID, revisionID, instanceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) MarkRuntimeInstanceDraining(ctx context.Context, teamID, functionID, revisionID, instanceID string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("function repository is not configured")
	}
	tag, err := r.pool.Exec(ctx, `
		UPDATE function_runtime_instances
		SET state = 'draining', draining_at = NOW()
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3 AND id = $4
	`, teamID, functionID, revisionID, instanceID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) MarkRuntimeInstanceFailed(ctx context.Context, teamID, functionID, revisionID, instanceID, message string) error {
	return r.MarkRuntimeInstanceFailedWithDetails(ctx, teamID, functionID, revisionID, instanceID, message, nil)
}

func (r *Repository) MarkRuntimeInstanceFailedWithDetails(ctx context.Context, teamID, functionID, revisionID, instanceID, message string, startupDurationMS *int) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("function repository is not configured")
	}
	message = strings.TrimSpace(message)
	tag, err := r.pool.Exec(ctx, `
		UPDATE function_runtime_instances
		SET state = 'failed', last_error = NULLIF($5, ''), last_error_at = NOW(),
			readiness_state = 'failed', startup_duration_ms = COALESCE($6, startup_duration_ms),
			failed_at = NOW()
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3 AND id = $4
	`, teamID, functionID, revisionID, instanceID, message, startupDurationMS)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) DeleteRuntimeInstance(ctx context.Context, teamID, functionID, revisionID, instanceID string) error {
	if r == nil || r.pool == nil {
		return fmt.Errorf("function repository is not configured")
	}
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var currentSandboxID *string
	if err := tx.QueryRow(ctx, `
		SELECT runtime_sandbox_id
		FROM functions_revisions
		WHERE team_id = $1 AND function_id = $2 AND id = $3
		FOR UPDATE
	`, teamID, functionID, revisionID).Scan(&currentSandboxID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return err
	}

	var deletedSandboxID string
	if err := tx.QueryRow(ctx, `
		DELETE FROM function_runtime_instances
		WHERE team_id = $1 AND function_id = $2 AND revision_id = $3 AND id = $4
		RETURNING sandbox_id
	`, teamID, functionID, revisionID, instanceID).Scan(&deletedSandboxID); errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	} else if err != nil {
		return err
	}

	if currentSandboxID != nil && strings.TrimSpace(*currentSandboxID) == strings.TrimSpace(deletedSandboxID) {
		var replacementSandboxID *string
		var replacementContextID *string
		err := tx.QueryRow(ctx, `
			SELECT sandbox_id, context_id
			FROM function_runtime_instances
			WHERE team_id = $1 AND function_id = $2 AND revision_id = $3 AND state = 'ready'
			ORDER BY COALESCE(last_used_at, ready_at, created_at) DESC
			LIMIT 1
		`, teamID, functionID, revisionID).Scan(&replacementSandboxID, &replacementContextID)
		if errors.Is(err, pgx.ErrNoRows) {
			replacementSandboxID = nil
			replacementContextID = nil
		} else if err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE functions_revisions
			SET runtime_sandbox_id = $4, runtime_context_id = $5,
				runtime_updated_at = CASE WHEN $4::text IS NULL THEN NULL ELSE NOW() END
			WHERE team_id = $1 AND function_id = $2 AND id = $3
		`, teamID, functionID, revisionID, replacementSandboxID, replacementContextID); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *Repository) ListRuntimeScaleDownCandidates(ctx context.Context, limit int) ([]*RuntimeInstance, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `
		WITH ready_instances AS (
			SELECT i.id, i.team_id, i.function_id, i.revision_id, i.sandbox_id, i.context_id, i.state,
				i.readiness_state, i.startup_duration_ms, i.last_error, i.last_error_at,
				i.ready_at, i.last_used_at, i.draining_at, i.failed_at, i.created_at, i.updated_at,
				GREATEST(COALESCE((f.autoscaling->>'min_warm')::int, $2), 0) AS min_warm,
				GREATEST(COALESCE((f.autoscaling->>'scale_down_after_seconds')::int, $3), $4) AS scale_down_after_seconds,
				COUNT(*) OVER (PARTITION BY i.revision_id) AS ready_count,
				ROW_NUMBER() OVER (PARTITION BY i.revision_id ORDER BY COALESCE(i.last_used_at, i.ready_at, i.created_at) ASC) AS idle_rank
			FROM function_runtime_instances i
			JOIN functions f ON f.id = i.function_id
			WHERE i.state = 'ready'
				AND f.deleted_at IS NULL
				AND f.active_revision_id = i.revision_id
		)
		SELECT id, team_id, function_id, revision_id, sandbox_id, context_id, state,
			readiness_state, startup_duration_ms, last_error, last_error_at,
			ready_at, last_used_at, draining_at, failed_at, created_at, updated_at
		FROM ready_instances
		WHERE ready_count > min_warm
			AND idle_rank <= ready_count - min_warm
			AND COALESCE(last_used_at, ready_at, created_at) < NOW() - make_interval(secs => scale_down_after_seconds)
		ORDER BY COALESCE(last_used_at, ready_at, created_at) ASC
		LIMIT $1
	`, limit, DefaultMinWarm, DefaultScaleDownAfterSeconds, MinimumScaleDownAfterSeconds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RuntimeInstance
	for rows.Next() {
		inst, err := scanRuntimeInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func (r *Repository) ListRuntimeCleanupCandidates(ctx context.Context, limit int) ([]*RuntimeInstance, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := r.pool.Query(ctx, `
		WITH candidates AS (
			SELECT i.id, i.team_id, i.function_id, i.revision_id, i.sandbox_id, i.context_id, i.state,
				i.readiness_state, i.startup_duration_ms, i.last_error, i.last_error_at,
				i.ready_at, i.last_used_at, i.draining_at, i.failed_at, i.created_at, i.updated_at
			FROM function_runtime_instances i
			JOIN functions f ON f.id = i.function_id
			JOIN functions_revisions r ON r.id = i.revision_id
			WHERE f.deleted_at IS NOT NULL
				OR f.enabled = FALSE
				OR (
					f.active_revision_id IS DISTINCT FROM i.revision_id
					AND COALESCE(i.last_used_at, i.ready_at, i.updated_at, i.created_at)
						< NOW() - make_interval(secs => GREATEST(COALESCE((f.autoscaling->>'scale_down_after_seconds')::int, $2), $3))
				)
				OR (
					i.state = 'failed'
					AND COALESCE(i.failed_at, i.updated_at, i.created_at)
						< NOW() - make_interval(secs => $4)
				)
				OR (
					i.state = 'draining'
					AND COALESCE(i.draining_at, i.updated_at, i.created_at)
						< NOW() - make_interval(secs => $5)
				)
				OR (
					i.state = 'starting'
					AND COALESCE(i.updated_at, i.created_at)
						< NOW() - make_interval(secs => $6)
				)
		)
		SELECT id, team_id, function_id, revision_id, sandbox_id, context_id, state,
			readiness_state, startup_duration_ms, last_error, last_error_at,
			ready_at, last_used_at, draining_at, failed_at, created_at, updated_at
		FROM candidates
		ORDER BY
			CASE state
				WHEN 'failed' THEN 0
				WHEN 'draining' THEN 1
				WHEN 'starting' THEN 2
				ELSE 3
			END,
			COALESCE(failed_at, draining_at, last_used_at, ready_at, updated_at, created_at) ASC
		LIMIT $1
	`, limit, DefaultScaleDownAfterSeconds, MinimumScaleDownAfterSeconds, DefaultFailedRuntimeRetentionSeconds, DefaultDrainingRuntimeRetentionSeconds, DefaultStartingRuntimeRetentionSeconds)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*RuntimeInstance
	for rows.Next() {
		inst, err := scanRuntimeInstance(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, inst)
	}
	return out, rows.Err()
}

func (r *Repository) UpdateFunction(ctx context.Context, teamID, functionRef string, name *string, enabled *bool, autoscaling *Autoscaling) (*Function, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	nextName := fn.Name
	if name != nil && strings.TrimSpace(*name) != "" {
		nextName = strings.TrimSpace(*name)
	}
	nextEnabled := fn.Enabled
	if enabled != nil {
		nextEnabled = *enabled
	}
	nextAutoscaling := fn.Autoscaling
	if autoscaling != nil {
		nextAutoscaling = NormalizeAutoscaling(*autoscaling)
	}
	autoscalingBytes, err := json.Marshal(nextAutoscaling)
	if err != nil {
		return nil, err
	}
	row := r.pool.QueryRow(ctx, `
		UPDATE functions
		SET name = $3, enabled = $4, autoscaling = $5, updated_at = NOW()
		WHERE team_id = $1 AND id = $2 AND deleted_at IS NULL
		RETURNING id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
	`, teamID, fn.ID, nextName, nextEnabled, autoscalingBytes)
	updated, err := scanFunction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return updated, err
}

func (r *Repository) DeleteFunction(ctx context.Context, teamID, functionRef string) (*Function, []*Revision, error) {
	if r == nil || r.pool == nil {
		return nil, nil, fmt.Errorf("function repository is not configured")
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, nil, err
	}
	revisions, err := r.ListRevisions(ctx, teamID, fn.ID)
	if err != nil {
		return nil, nil, err
	}
	row := r.pool.QueryRow(ctx, `
		UPDATE functions
		SET enabled = FALSE, deleted_at = NOW(), updated_at = NOW()
		WHERE team_id = $1 AND id = $2 AND deleted_at IS NULL
		RETURNING id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
	`, teamID, fn.ID)
	deleted, err := scanFunction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, ErrNotFound
	}
	if err != nil {
		return nil, nil, err
	}
	return deleted, revisions, nil
}

func (r *Repository) ListAliases(ctx context.Context, teamID, functionRef string) ([]*Alias, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `
		SELECT a.function_id, a.alias, a.revision_id, r.revision_number, a.updated_by, a.updated_at
		FROM functions_aliases a
		JOIN functions_revisions r ON r.id = a.revision_id
		WHERE a.function_id = $1
		ORDER BY a.alias
	`, fn.ID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Alias
	for rows.Next() {
		alias, err := scanAlias(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, alias)
	}
	return out, rows.Err()
}

func (r *Repository) GetAlias(ctx context.Context, teamID, functionRef, aliasName string) (*Alias, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	row := r.pool.QueryRow(ctx, `
		SELECT a.function_id, a.alias, a.revision_id, r.revision_number, a.updated_by, a.updated_at
		FROM functions_aliases a
		JOIN functions_revisions r ON r.id = a.revision_id
		WHERE a.function_id = $1 AND a.alias = $2
	`, fn.ID, aliasName)
	out, err := scanAlias(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNotFound
	}
	return out, err
}

func (r *Repository) SetAlias(ctx context.Context, teamID, functionRef, alias string, revisionNumber int, updatedBy string) (*Alias, error) {
	if err := ValidateAlias(alias); err != nil {
		return nil, err
	}
	fn, err := r.GetFunction(ctx, teamID, functionRef)
	if err != nil {
		return nil, err
	}
	var revisionID string
	if err := r.pool.QueryRow(ctx, `
		SELECT id
		FROM functions_revisions
		WHERE function_id = $1 AND revision_number = $2
	`, fn.ID, revisionNumber).Scan(&revisionID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	now := time.Now().UTC()
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if err := upsertAlias(ctx, tx, fn.ID, alias, revisionID, updatedBy, now); err != nil {
		return nil, err
	}
	if alias == ProductionAlias {
		if _, err := tx.Exec(ctx, `
			UPDATE functions
			SET active_revision_id = $2, updated_at = $3
			WHERE id = $1
		`, fn.ID, revisionID, now); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &Alias{FunctionID: fn.ID, Alias: alias, RevisionID: revisionID, RevisionNumber: revisionNumber, UpdatedBy: updatedBy, UpdatedAt: now}, nil
}

func NewFunction(teamID, name, userID string) *Function {
	slug := SlugFromName(name)
	return &Function{
		TeamID:      teamID,
		Name:        strings.TrimSpace(name),
		Slug:        slug,
		DomainLabel: DomainLabel(slug, teamID),
		Enabled:     true,
		Autoscaling: DefaultAutoscaling(),
		CreatedBy:   userID,
	}
}

func NewRevision(teamID, sandboxID, serviceID, templateID string, snapshot any, mounts []RestoreMount, userID string) (*Revision, error) {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}
	return &Revision{
		TeamID:           teamID,
		SourceSandboxID:  sandboxID,
		SourceServiceID:  serviceID,
		SourceTemplateID: templateID,
		RestoreMounts:    append([]RestoreMount(nil), mounts...),
		ServiceSnapshot:  data,
		CreatedBy:        userID,
	}, nil
}

func insertFunction(ctx context.Context, tx pgx.Tx, fn *Function) error {
	fn.Autoscaling = NormalizeAutoscaling(fn.Autoscaling)
	autoscalingBytes, err := json.Marshal(fn.Autoscaling)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO functions (
			id, team_id, name, slug, domain_label, active_revision_id, enabled, autoscaling, created_by, created_at, updated_at, deleted_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	`, fn.ID, fn.TeamID, fn.Name, fn.Slug, fn.DomainLabel, fn.ActiveRevisionID, fn.Enabled, autoscalingBytes, fn.CreatedBy, fn.CreatedAt, fn.UpdatedAt, fn.DeletedAt)
	return err
}

func insertRevision(ctx context.Context, tx pgx.Tx, rev *Revision) error {
	restoreMounts, err := json.Marshal(rev.RestoreMounts)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO functions_revisions (
			id, function_id, team_id, revision_number, source_sandbox_id, source_service_id,
			source_template_id, restore_mounts, service_snapshot, runtime_sandbox_id, runtime_context_id,
			runtime_updated_at, created_by, created_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
	`, rev.ID, rev.FunctionID, rev.TeamID, rev.RevisionNumber, rev.SourceSandboxID, rev.SourceServiceID,
		rev.SourceTemplateID, restoreMounts, rev.ServiceSnapshot, rev.RuntimeSandboxID, rev.RuntimeContextID,
		rev.RuntimeUpdatedAt, rev.CreatedBy, rev.CreatedAt)
	return err
}

func upsertAlias(ctx context.Context, tx pgx.Tx, functionID, alias, revisionID, updatedBy string, updatedAt time.Time) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO functions_aliases (function_id, alias, revision_id, updated_by, updated_at)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (function_id, alias)
		DO UPDATE SET revision_id = EXCLUDED.revision_id, updated_by = EXCLUDED.updated_by, updated_at = EXCLUDED.updated_at
	`, functionID, alias, revisionID, updatedBy, updatedAt)
	return err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanFunction(row rowScanner) (*Function, error) {
	var fn Function
	var autoscalingBytes []byte
	if err := row.Scan(&fn.ID, &fn.TeamID, &fn.Name, &fn.Slug, &fn.DomainLabel, &fn.ActiveRevisionID, &fn.Enabled, &autoscalingBytes, &fn.CreatedBy, &fn.CreatedAt, &fn.UpdatedAt, &fn.DeletedAt); err != nil {
		return nil, err
	}
	fn.Autoscaling = DefaultAutoscaling()
	if len(autoscalingBytes) > 0 {
		if err := json.Unmarshal(autoscalingBytes, &fn.Autoscaling); err != nil {
			return nil, err
		}
	}
	fn.Autoscaling = NormalizeAutoscaling(fn.Autoscaling)
	return &fn, nil
}

func scanAlias(row rowScanner) (*Alias, error) {
	var alias Alias
	if err := row.Scan(&alias.FunctionID, &alias.Alias, &alias.RevisionID, &alias.RevisionNumber, &alias.UpdatedBy, &alias.UpdatedAt); err != nil {
		return nil, err
	}
	return &alias, nil
}

func scanRuntimeInstance(row rowScanner) (*RuntimeInstance, error) {
	var inst RuntimeInstance
	if err := row.Scan(&inst.ID, &inst.TeamID, &inst.FunctionID, &inst.RevisionID, &inst.SandboxID, &inst.ContextID, &inst.State, &inst.ReadinessState, &inst.StartupDurationMS, &inst.LastError, &inst.LastErrorAt, &inst.ReadyAt, &inst.LastUsedAt, &inst.DrainingAt, &inst.FailedAt, &inst.CreatedAt, &inst.UpdatedAt); err != nil {
		return nil, err
	}
	inst.ReadinessState = normalizeRuntimeReadinessState(inst.ReadinessState, inst.State)
	return &inst, nil
}

func scanRuntimeEvent(row rowScanner) (*RuntimeEvent, error) {
	var event RuntimeEvent
	if err := row.Scan(&event.ID, &event.TeamID, &event.FunctionID, &event.RevisionID, &event.RuntimeInstanceID, &event.RuntimeSandboxID, &event.RuntimeContextID, &event.Phase, &event.ReadinessState, &event.Reason, &event.Message, &event.StartupDurationMS, &event.CreatedAt); err != nil {
		return nil, err
	}
	event.ReadinessState = normalizeRuntimeReadinessState(event.ReadinessState, RuntimeInstanceState(event.Phase))
	return &event, nil
}

func normalizeRuntimeReadinessState(value RuntimeReadinessState, state RuntimeInstanceState) RuntimeReadinessState {
	switch value {
	case RuntimeReadinessStateUnknown, RuntimeReadinessStateChecking, RuntimeReadinessStateReady, RuntimeReadinessStateFailed:
		return value
	}
	switch state {
	case RuntimeInstanceStateStarting:
		return RuntimeReadinessStateChecking
	case RuntimeInstanceStateReady:
		return RuntimeReadinessStateReady
	case RuntimeInstanceStateFailed:
		return RuntimeReadinessStateFailed
	default:
		return RuntimeReadinessStateUnknown
	}
}

func scanRevision(row rowScanner) (*Revision, error) {
	var rev Revision
	var restoreMounts []byte
	if err := row.Scan(&rev.ID, &rev.FunctionID, &rev.TeamID, &rev.RevisionNumber, &rev.SourceSandboxID, &rev.SourceServiceID, &rev.SourceTemplateID, &restoreMounts, &rev.ServiceSnapshot, &rev.RuntimeSandboxID, &rev.RuntimeContextID, &rev.RuntimeUpdatedAt, &rev.CreatedBy, &rev.CreatedAt); err != nil {
		return nil, err
	}
	if len(restoreMounts) > 0 {
		if err := json.Unmarshal(restoreMounts, &rev.RestoreMounts); err != nil {
			return nil, err
		}
	}
	return &rev, nil
}
