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
		SELECT id, team_id, name, slug, domain_label, active_revision_id, created_by, created_at, updated_at
		FROM functions
		WHERE team_id = $1
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
			SELECT id, team_id, name, slug, domain_label, active_revision_id, created_by, created_at, updated_at
			FROM functions
			WHERE team_id = $1 AND (id = $2 OR slug = $3)
		`, teamID, id, ref)
	} else {
		row = r.pool.QueryRow(ctx, `
			SELECT id, team_id, name, slug, domain_label, active_revision_id, created_by, created_at, updated_at
			FROM functions
			WHERE team_id = $1 AND slug = $2
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
		SELECT id, team_id, name, slug, domain_label, active_revision_id, created_by, created_at, updated_at
		FROM functions
		WHERE domain_label = $1
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
	tag, err := r.pool.Exec(ctx, `
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
	return nil
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
	_, err := tx.Exec(ctx, `
		INSERT INTO functions (
			id, team_id, name, slug, domain_label, active_revision_id, created_by, created_at, updated_at
		) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
	`, fn.ID, fn.TeamID, fn.Name, fn.Slug, fn.DomainLabel, fn.ActiveRevisionID, fn.CreatedBy, fn.CreatedAt, fn.UpdatedAt)
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
	if err := row.Scan(&fn.ID, &fn.TeamID, &fn.Name, &fn.Slug, &fn.DomainLabel, &fn.ActiveRevisionID, &fn.CreatedBy, &fn.CreatedAt, &fn.UpdatedAt); err != nil {
		return nil, err
	}
	return &fn, nil
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
