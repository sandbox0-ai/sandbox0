package functions

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

type DeployInput struct {
	TeamID   string
	UserID   string
	Name     string
	Slug     string
	Scale    FunctionScalePolicy
	Source   FunctionSource
	Spec     FunctionRevisionSpec
	Activate bool
}

type ActiveRevision struct {
	Function Function
	Revision FunctionRevision
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	if pool == nil {
		return nil
	}
	return &Repository{pool: pool}
}

func (r *Repository) DeployRevision(ctx context.Context, input DeployInput) (*FunctionDeployResult, error) {
	if r == nil || r.pool == nil {
		return nil, fmt.Errorf("function repository is not configured")
	}
	teamID := strings.TrimSpace(input.TeamID)
	if teamID == "" {
		return nil, fmt.Errorf("team_id is required")
	}
	slug, err := NormalizeSlug(input.Slug)
	if err != nil {
		return nil, err
	}
	name := strings.TrimSpace(input.Name)
	if name == "" {
		name = slug
	}
	spec, err := NormalizeRevisionSpec(input.Spec)
	if err != nil {
		return nil, err
	}
	scale := NormalizeScalePolicy(input.Scale)

	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin deploy transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	fn, err := getFunctionBySlugTx(ctx, tx, teamID, slug, true)
	if err != nil {
		return nil, err
	}
	if fn == nil {
		fn, err = createFunctionTx(ctx, tx, teamID, input.UserID, name, slug, scale)
		if err != nil {
			return nil, err
		}
	} else {
		fn.Name = name
		fn.Scale = scale
		if err := updateFunctionMetadataTx(ctx, tx, fn); err != nil {
			return nil, err
		}
	}

	revision, err := createRevisionTx(ctx, tx, fn.ID, teamID, input.Source, spec)
	if err != nil {
		return nil, err
	}
	if input.Activate {
		if err := activateRevisionTx(ctx, tx, fn.ID, revision.ID); err != nil {
			return nil, err
		}
		fn.ActiveRevisionID = revision.ID
		revision.Status = RevisionStatusActive
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit deploy transaction: %w", err)
	}
	return &FunctionDeployResult{Function: *fn, Revision: *revision}, nil
}

func (r *Repository) ListFunctions(ctx context.Context, teamID string) ([]Function, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, team_id, created_by, name, slug, domain_label, active_revision_id, enabled, scale_policy, created_at, updated_at
		FROM functions
		WHERE team_id = $1 AND deleted_at IS NULL
		ORDER BY created_at DESC
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("list functions: %w", err)
	}
	defer rows.Close()
	var out []Function
	for rows.Next() {
		fn, err := scanFunctionRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *fn)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate functions: %w", err)
	}
	return out, nil
}

func (r *Repository) GetFunction(ctx context.Context, teamID, idOrSlug string) (*Function, error) {
	idOrSlug = strings.TrimSpace(idOrSlug)
	row := r.pool.QueryRow(ctx, `
		SELECT id, team_id, created_by, name, slug, domain_label, active_revision_id, enabled, scale_policy, created_at, updated_at
		FROM functions
		WHERE team_id = $1 AND deleted_at IS NULL AND (id::text = $2 OR slug = $2)
	`, teamID, idOrSlug)
	fn, err := scanFunction(row)
	if err != nil {
		return nil, err
	}
	return fn, nil
}

func (r *Repository) UpdateFunction(ctx context.Context, teamID, idOrSlug string, name string, enabled *bool, scale *FunctionScalePolicy) (*Function, error) {
	fn, err := r.GetFunction(ctx, teamID, idOrSlug)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(name) != "" {
		fn.Name = strings.TrimSpace(name)
	}
	if enabled != nil {
		fn.Enabled = *enabled
	}
	if scale != nil {
		fn.Scale = NormalizeScalePolicy(*scale)
	}
	scaleJSON, err := json.Marshal(fn.Scale)
	if err != nil {
		return nil, fmt.Errorf("marshal scale policy: %w", err)
	}
	row := r.pool.QueryRow(ctx, `
		UPDATE functions
		SET name = $3, enabled = $4, scale_policy = $5
		WHERE team_id = $1 AND deleted_at IS NULL AND (id::text = $2 OR slug = $2)
		RETURNING id, team_id, created_by, name, slug, domain_label, active_revision_id, enabled, scale_policy, created_at, updated_at
	`, teamID, idOrSlug, fn.Name, fn.Enabled, scaleJSON)
	return scanFunction(row)
}

func (r *Repository) DeleteFunction(ctx context.Context, teamID, idOrSlug string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE functions
		SET deleted_at = NOW(), enabled = false, active_revision_id = NULL
		WHERE team_id = $1 AND deleted_at IS NULL AND (id::text = $2 OR slug = $2)
	`, teamID, strings.TrimSpace(idOrSlug))
	if err != nil {
		return fmt.Errorf("delete function: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func (r *Repository) ListRevisions(ctx context.Context, teamID, functionIDOrSlug string) ([]FunctionRevision, error) {
	fn, err := r.GetFunction(ctx, teamID, functionIDOrSlug)
	if err != nil {
		return nil, err
	}
	rows, err := r.pool.Query(ctx, `
		SELECT id, function_id, team_id, revision_number, source, spec, status,
		       runtime_sandbox_id, runtime_cluster_id, runtime_context_id, created_at, activated_at
		FROM function_revisions
		WHERE team_id = $1 AND function_id = $2
		ORDER BY revision_number DESC
	`, teamID, fn.ID)
	if err != nil {
		return nil, fmt.Errorf("list revisions: %w", err)
	}
	defer rows.Close()
	var out []FunctionRevision
	for rows.Next() {
		revision, err := scanRevisionRows(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *revision)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate revisions: %w", err)
	}
	return out, nil
}

func (r *Repository) ActivateRevision(ctx context.Context, teamID, functionIDOrSlug, revisionID string) (*FunctionDeployResult, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, fmt.Errorf("begin activate transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	fn, err := getFunctionByIDOrSlugTx(ctx, tx, teamID, functionIDOrSlug, true)
	if err != nil {
		return nil, err
	}
	revision, err := getRevisionTx(ctx, tx, teamID, fn.ID, revisionID, true)
	if err != nil {
		return nil, err
	}
	if err := activateRevisionTx(ctx, tx, fn.ID, revision.ID); err != nil {
		return nil, err
	}
	fn.ActiveRevisionID = revision.ID
	revision.Status = RevisionStatusActive
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit activate transaction: %w", err)
	}
	return &FunctionDeployResult{Function: *fn, Revision: *revision}, nil
}

func (r *Repository) GetActiveRevisionByDomainLabel(ctx context.Context, domainLabel string) (*ActiveRevision, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT f.id, f.team_id, f.created_by, f.name, f.slug, f.domain_label, f.active_revision_id, f.enabled, f.scale_policy, f.created_at, f.updated_at,
		       r.id, r.function_id, r.team_id, r.revision_number, r.source, r.spec, r.status,
		       r.runtime_sandbox_id, r.runtime_cluster_id, r.runtime_context_id, r.created_at, r.activated_at
		FROM functions f
		JOIN function_revisions r ON r.id = f.active_revision_id
		WHERE f.domain_label = $1 AND f.deleted_at IS NULL
	`, strings.TrimSpace(domainLabel))
	fn, revision, err := scanActiveRevision(row)
	if err != nil {
		return nil, err
	}
	return &ActiveRevision{Function: *fn, Revision: *revision}, nil
}

func (r *Repository) WithRevisionLock(ctx context.Context, revisionID string, fn func(context.Context, *FunctionRevision) error) error {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin revision lock transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	revision, err := scanRevision(tx.QueryRow(ctx, `
		SELECT id, function_id, team_id, revision_number, source, spec, status,
		       runtime_sandbox_id, runtime_cluster_id, runtime_context_id, created_at, activated_at
		FROM function_revisions
		WHERE id = $1
		FOR UPDATE
	`, revisionID))
	if err != nil {
		return err
	}
	if err := fn(ctx, revision); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE function_revisions
		SET runtime_sandbox_id = $2, runtime_cluster_id = $3, runtime_context_id = $4
		WHERE id = $1
	`, revision.ID, nullString(revision.RuntimeSandboxID), nullString(revision.RuntimeClusterID), nullString(revision.RuntimeContextID)); err != nil {
		return fmt.Errorf("save locked revision runtime: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit revision lock transaction: %w", err)
	}
	return nil
}

func (r *Repository) SetRevisionRuntime(ctx context.Context, revisionID, sandboxID, clusterID, contextID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE function_revisions
		SET runtime_sandbox_id = $2, runtime_cluster_id = $3, runtime_context_id = $4
		WHERE id = $1
	`, revisionID, nullString(sandboxID), nullString(clusterID), nullString(contextID))
	if err != nil {
		return fmt.Errorf("set revision runtime: %w", err)
	}
	return nil
}

func (r *Repository) ClearRevisionRuntime(ctx context.Context, revisionID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE function_revisions
		SET runtime_sandbox_id = NULL, runtime_cluster_id = NULL, runtime_context_id = NULL
		WHERE id = $1
	`, revisionID)
	if err != nil {
		return fmt.Errorf("clear revision runtime: %w", err)
	}
	return nil
}

func getFunctionBySlugTx(ctx context.Context, tx pgx.Tx, teamID, slug string, lock bool) (*Function, error) {
	query := `
		SELECT id, team_id, created_by, name, slug, domain_label, active_revision_id, enabled, scale_policy, created_at, updated_at
		FROM functions
		WHERE team_id = $1 AND slug = $2 AND deleted_at IS NULL`
	if lock {
		query += " FOR UPDATE"
	}
	fn, err := scanFunction(tx.QueryRow(ctx, query, teamID, slug))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	return fn, err
}

func getFunctionByIDOrSlugTx(ctx context.Context, tx pgx.Tx, teamID, idOrSlug string, lock bool) (*Function, error) {
	query := `
		SELECT id, team_id, created_by, name, slug, domain_label, active_revision_id, enabled, scale_policy, created_at, updated_at
		FROM functions
		WHERE team_id = $1 AND deleted_at IS NULL AND (id::text = $2 OR slug = $2)`
	if lock {
		query += " FOR UPDATE"
	}
	return scanFunction(tx.QueryRow(ctx, query, teamID, strings.TrimSpace(idOrSlug)))
}

func createFunctionTx(ctx context.Context, tx pgx.Tx, teamID, userID, name, slug string, scale FunctionScalePolicy) (*Function, error) {
	scaleJSON, err := json.Marshal(scale)
	if err != nil {
		return nil, fmt.Errorf("marshal scale policy: %w", err)
	}
	for i := 0; i < 3; i++ {
		id := uuid.NewString()
		domainLabel, err := NewDomainLabel(slug)
		if err != nil {
			return nil, err
		}
		fn, err := scanFunction(tx.QueryRow(ctx, `
			INSERT INTO functions (id, team_id, created_by, name, slug, domain_label, enabled, scale_policy)
			VALUES ($1, $2, $3, $4, $5, $6, true, $7)
			RETURNING id, team_id, created_by, name, slug, domain_label, active_revision_id, enabled, scale_policy, created_at, updated_at
		`, id, teamID, nullString(userID), name, slug, domainLabel, scaleJSON))
		if err == nil {
			return fn, nil
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
			return nil, err
		}
	}
	return nil, fmt.Errorf("could not allocate unique function domain")
}

func updateFunctionMetadataTx(ctx context.Context, tx pgx.Tx, fn *Function) error {
	scaleJSON, err := json.Marshal(fn.Scale)
	if err != nil {
		return fmt.Errorf("marshal scale policy: %w", err)
	}
	_, err = tx.Exec(ctx, `
		UPDATE functions
		SET name = $2, scale_policy = $3
		WHERE id = $1
	`, fn.ID, fn.Name, scaleJSON)
	if err != nil {
		return fmt.Errorf("update function metadata: %w", err)
	}
	return nil
}

func createRevisionTx(ctx context.Context, tx pgx.Tx, functionID, teamID string, source FunctionSource, spec FunctionRevisionSpec) (*FunctionRevision, error) {
	sourceJSON, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("marshal source: %w", err)
	}
	specJSON, err := json.Marshal(spec)
	if err != nil {
		return nil, fmt.Errorf("marshal revision spec: %w", err)
	}
	var number int
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(revision_number), 0) + 1
		FROM function_revisions
		WHERE function_id = $1
	`, functionID).Scan(&number); err != nil {
		return nil, fmt.Errorf("next revision number: %w", err)
	}
	return scanRevision(tx.QueryRow(ctx, `
		INSERT INTO function_revisions (id, function_id, team_id, revision_number, source, spec, status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, function_id, team_id, revision_number, source, spec, status,
		          runtime_sandbox_id, runtime_cluster_id, runtime_context_id, created_at, activated_at
	`, uuid.NewString(), functionID, teamID, number, sourceJSON, specJSON, RevisionStatusCreated))
}

func activateRevisionTx(ctx context.Context, tx pgx.Tx, functionID, revisionID string) error {
	_, err := tx.Exec(ctx, `
		UPDATE function_revisions
		SET status = CASE WHEN id = $2 THEN $3 ELSE $4 END,
		    activated_at = CASE WHEN id = $2 THEN COALESCE(activated_at, NOW()) ELSE activated_at END
		WHERE function_id = $1
	`, functionID, revisionID, RevisionStatusActive, RevisionStatusCreated)
	if err != nil {
		return fmt.Errorf("update revision status: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE functions
		SET active_revision_id = $2
		WHERE id = $1 AND deleted_at IS NULL
	`, functionID, revisionID)
	if err != nil {
		return fmt.Errorf("activate revision: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

func getRevisionTx(ctx context.Context, tx pgx.Tx, teamID, functionID, revisionID string, lock bool) (*FunctionRevision, error) {
	query := `
		SELECT id, function_id, team_id, revision_number, source, spec, status,
		       runtime_sandbox_id, runtime_cluster_id, runtime_context_id, created_at, activated_at
		FROM function_revisions
		WHERE team_id = $1 AND function_id = $2 AND id::text = $3`
	if lock {
		query += " FOR UPDATE"
	}
	return scanRevision(tx.QueryRow(ctx, query, teamID, functionID, strings.TrimSpace(revisionID)))
}

func scanFunction(row pgx.Row) (*Function, error) {
	var fn Function
	var createdBy sql.NullString
	var activeRevision sql.NullString
	var scaleJSON []byte
	if err := row.Scan(&fn.ID, &fn.TeamID, &createdBy, &fn.Name, &fn.Slug, &fn.DomainLabel, &activeRevision, &fn.Enabled, &scaleJSON, &fn.CreatedAt, &fn.UpdatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan function: %w", err)
	}
	if createdBy.Valid {
		fn.CreatedBy = createdBy.String
	}
	if activeRevision.Valid {
		fn.ActiveRevisionID = activeRevision.String
	}
	if len(scaleJSON) > 0 {
		if err := json.Unmarshal(scaleJSON, &fn.Scale); err != nil {
			return nil, fmt.Errorf("unmarshal scale policy: %w", err)
		}
	}
	fn.Scale = NormalizeScalePolicy(fn.Scale)
	return &fn, nil
}

func scanFunctionRows(rows pgx.Rows) (*Function, error) {
	return scanFunction(rows)
}

func scanRevision(row pgx.Row) (*FunctionRevision, error) {
	var revision FunctionRevision
	var sourceJSON, specJSON []byte
	var runtimeSandboxID, runtimeClusterID, runtimeContextID sql.NullString
	if err := row.Scan(
		&revision.ID, &revision.FunctionID, &revision.TeamID, &revision.Number,
		&sourceJSON, &specJSON, &revision.Status,
		&runtimeSandboxID, &runtimeClusterID, &runtimeContextID,
		&revision.CreatedAt, &revision.ActivatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("scan function revision: %w", err)
	}
	if len(sourceJSON) > 0 {
		if err := json.Unmarshal(sourceJSON, &revision.Source); err != nil {
			return nil, fmt.Errorf("unmarshal revision source: %w", err)
		}
	}
	if len(specJSON) > 0 {
		if err := json.Unmarshal(specJSON, &revision.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal revision spec: %w", err)
		}
	}
	if runtimeSandboxID.Valid {
		revision.RuntimeSandboxID = runtimeSandboxID.String
	}
	if runtimeClusterID.Valid {
		revision.RuntimeClusterID = runtimeClusterID.String
	}
	if runtimeContextID.Valid {
		revision.RuntimeContextID = runtimeContextID.String
	}
	return &revision, nil
}

func scanRevisionRows(rows pgx.Rows) (*FunctionRevision, error) {
	return scanRevision(rows)
}

func scanActiveRevision(row pgx.Row) (*Function, *FunctionRevision, error) {
	var fn Function
	var revision FunctionRevision
	var createdBy sql.NullString
	var activeRevision sql.NullString
	var scaleJSON, sourceJSON, specJSON []byte
	var runtimeSandboxID, runtimeClusterID, runtimeContextID sql.NullString
	if err := row.Scan(
		&fn.ID, &fn.TeamID, &createdBy, &fn.Name, &fn.Slug, &fn.DomainLabel, &activeRevision, &fn.Enabled, &scaleJSON, &fn.CreatedAt, &fn.UpdatedAt,
		&revision.ID, &revision.FunctionID, &revision.TeamID, &revision.Number, &sourceJSON, &specJSON, &revision.Status,
		&runtimeSandboxID, &runtimeClusterID, &runtimeContextID, &revision.CreatedAt, &revision.ActivatedAt,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, fmt.Errorf("scan active function revision: %w", err)
	}
	if createdBy.Valid {
		fn.CreatedBy = createdBy.String
	}
	if activeRevision.Valid {
		fn.ActiveRevisionID = activeRevision.String
	}
	if len(scaleJSON) > 0 {
		if err := json.Unmarshal(scaleJSON, &fn.Scale); err != nil {
			return nil, nil, fmt.Errorf("unmarshal scale policy: %w", err)
		}
	}
	fn.Scale = NormalizeScalePolicy(fn.Scale)
	if len(sourceJSON) > 0 {
		if err := json.Unmarshal(sourceJSON, &revision.Source); err != nil {
			return nil, nil, fmt.Errorf("unmarshal revision source: %w", err)
		}
	}
	if len(specJSON) > 0 {
		if err := json.Unmarshal(specJSON, &revision.Spec); err != nil {
			return nil, nil, fmt.Errorf("unmarshal revision spec: %w", err)
		}
	}
	if runtimeSandboxID.Valid {
		revision.RuntimeSandboxID = runtimeSandboxID.String
	}
	if runtimeClusterID.Valid {
		revision.RuntimeClusterID = runtimeClusterID.String
	}
	if runtimeContextID.Valid {
		revision.RuntimeContextID = runtimeContextID.String
	}
	return &fn, &revision, nil
}

func nullString(value string) any {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return value
}
