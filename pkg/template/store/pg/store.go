package pg

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

// Store implements template and allocation storage in PostgreSQL.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore creates a new Store.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// Ping checks database connectivity.
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// CreateTemplate creates a new template.
func (s *Store) CreateTemplate(ctx context.Context, tpl *template.Template) error {
	specJSON, err := json.Marshal(tpl.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO scheduler_templates (template_id, scope, team_id, user_id, spec)
		VALUES ($1, $2, $3, $4, $5)
	`, tpl.TemplateID, tpl.Scope, tpl.TeamID, tpl.UserID, specJSON)
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}

// GetTemplate gets a template by primary key (scope, team_id, template_id).
func (s *Store) GetTemplate(ctx context.Context, scope, teamID, templateID string) (*template.Template, error) {
	var tpl template.Template
	var specJSON []byte
	err := s.pool.QueryRow(ctx, `
		SELECT template_id, scope, team_id, user_id, spec, created_at, updated_at
		FROM scheduler_templates
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID).Scan(
		&tpl.TemplateID,
		&tpl.Scope,
		&tpl.TeamID,
		&tpl.UserID,
		&specJSON,
		&tpl.CreatedAt,
		&tpl.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get template: %w", err)
	}

	if err := json.Unmarshal(specJSON, &tpl.Spec); err != nil {
		return nil, fmt.Errorf("unmarshal spec: %w", err)
	}
	return &tpl, nil
}

// GetTemplateForTeam gets a template visible to a team.
// It prefers the team's private template (scope=team) and falls back to public.
func (s *Store) GetTemplateForTeam(ctx context.Context, teamID, templateID string) (*template.Template, error) {
	if teamID != "" {
		privateTpl, err := s.GetTemplate(ctx, "team", teamID, templateID)
		if err != nil {
			return nil, err
		}
		if privateTpl != nil {
			return privateTpl, nil
		}
	}
	return s.GetTemplate(ctx, "public", "", templateID)
}

// ListTemplates lists all templates.
func (s *Store) ListTemplates(ctx context.Context) ([]*template.Template, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_id, scope, team_id, user_id, spec, created_at, updated_at
		FROM scheduler_templates
		ORDER BY scope, team_id, template_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	defer rows.Close()

	var templates []*template.Template
	for rows.Next() {
		var tpl template.Template
		var specJSON []byte
		if err := rows.Scan(
			&tpl.TemplateID,
			&tpl.Scope,
			&tpl.TeamID,
			&tpl.UserID,
			&specJSON,
			&tpl.CreatedAt,
			&tpl.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		if err := json.Unmarshal(specJSON, &tpl.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal spec: %w", err)
		}
		templates = append(templates, &tpl)
	}
	return templates, nil
}

// ListVisibleTemplates lists templates visible to a team (public + that team's private).
func (s *Store) ListVisibleTemplates(ctx context.Context, teamID string) ([]*template.Template, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_id, scope, team_id, user_id, spec, created_at, updated_at
		FROM scheduler_templates
		WHERE scope = 'public' OR (scope = 'team' AND team_id = $1)
		ORDER BY scope, template_id
	`, teamID)
	if err != nil {
		return nil, fmt.Errorf("list visible templates: %w", err)
	}
	defer rows.Close()

	var templates []*template.Template
	for rows.Next() {
		var tpl template.Template
		var specJSON []byte
		if err := rows.Scan(
			&tpl.TemplateID,
			&tpl.Scope,
			&tpl.TeamID,
			&tpl.UserID,
			&specJSON,
			&tpl.CreatedAt,
			&tpl.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		if err := json.Unmarshal(specJSON, &tpl.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal spec: %w", err)
		}
		templates = append(templates, &tpl)
	}
	return templates, nil
}

// UpdateTemplate updates a template.
func (s *Store) UpdateTemplate(ctx context.Context, tpl *template.Template) error {
	specJSON, err := json.Marshal(tpl.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		UPDATE scheduler_templates
		SET spec = $5, user_id = $4
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, tpl.Scope, tpl.TeamID, tpl.TemplateID, tpl.UserID, specJSON)
	if err != nil {
		return fmt.Errorf("update template: %w", err)
	}
	return nil
}

// DeleteTemplate deletes a template.
func (s *Store) DeleteTemplate(ctx context.Context, scope, teamID, templateID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM scheduler_templates WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	return nil
}

// UpsertAllocation creates or updates a template allocation.
func (s *Store) UpsertAllocation(ctx context.Context, alloc *template.TemplateAllocation) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO scheduler_template_allocations (template_id, scope, team_id, cluster_id, min_idle, max_idle, sync_status)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (scope, team_id, template_id, cluster_id)
		DO UPDATE SET min_idle = $5, max_idle = $6, sync_status = $7
	`, alloc.TemplateID, alloc.Scope, alloc.TeamID, alloc.ClusterID, alloc.MinIdle, alloc.MaxIdle, alloc.SyncStatus)
	if err != nil {
		return fmt.Errorf("upsert allocation: %w", err)
	}
	return nil
}

// ListAllocationsByTemplate lists all allocations for a template.
func (s *Store) ListAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) ([]*template.TemplateAllocation, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT template_id, scope, team_id, cluster_id, min_idle, max_idle, last_synced_at, sync_status, sync_error, created_at, updated_at
		FROM scheduler_template_allocations
		WHERE scope = $1 AND team_id = $2 AND template_id = $3
		ORDER BY cluster_id
	`, scope, teamID, templateID)
	if err != nil {
		return nil, fmt.Errorf("list allocations by template: %w", err)
	}
	defer rows.Close()

	var allocations []*template.TemplateAllocation
	for rows.Next() {
		var alloc template.TemplateAllocation
		if err := rows.Scan(
			&alloc.TemplateID,
			&alloc.Scope,
			&alloc.TeamID,
			&alloc.ClusterID,
			&alloc.MinIdle,
			&alloc.MaxIdle,
			&alloc.LastSyncedAt,
			&alloc.SyncStatus,
			&alloc.SyncError,
			&alloc.CreatedAt,
			&alloc.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan allocation: %w", err)
		}
		allocations = append(allocations, &alloc)
	}
	return allocations, nil
}

// UpdateAllocationSyncStatus updates the sync status of an allocation.
func (s *Store) UpdateAllocationSyncStatus(ctx context.Context, scope, teamID, templateID, clusterID, status string, syncError *string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE scheduler_template_allocations
		SET sync_status = $5::text, sync_error = $6, last_synced_at = CASE WHEN $5::text = 'synced' THEN NOW() ELSE last_synced_at END
		WHERE scope = $1 AND team_id = $2 AND template_id = $3 AND cluster_id = $4
	`, scope, teamID, templateID, clusterID, status, syncError)
	if err != nil {
		return fmt.Errorf("update allocation sync status: %w", err)
	}
	return nil
}

// DeleteAllocationsByTemplate deletes all allocations for a template.
func (s *Store) DeleteAllocationsByTemplate(ctx context.Context, scope, teamID, templateID string) error {
	_, err := s.pool.Exec(ctx, `
		DELETE FROM scheduler_template_allocations WHERE scope = $1 AND team_id = $2 AND template_id = $3
	`, scope, teamID, templateID)
	if err != nil {
		return fmt.Errorf("delete allocations by template: %w", err)
	}
	return nil
}
