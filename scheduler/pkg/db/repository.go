package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
)

// Cluster represents a registered data-plane cluster
type Cluster struct {
	ClusterID          string    `json:"cluster_id"`
	InternalGatewayURL string    `json:"internal_gateway_url"`
	Weight             int       `json:"weight"`
	Enabled            bool      `json:"enabled"`
	LastSeenAt         *time.Time `json:"last_seen_at,omitempty"`
	CreatedAt          time.Time `json:"created_at"`
	UpdatedAt          time.Time `json:"updated_at"`
}

// Template represents a SandboxTemplate stored in the scheduler
type Template struct {
	TemplateID string                    `json:"template_id"`
	Namespace  string                    `json:"namespace"`
	Spec       v1alpha1.SandboxTemplateSpec `json:"spec"`
	CreatedAt  time.Time                 `json:"created_at"`
	UpdatedAt  time.Time                 `json:"updated_at"`
}

// TemplateAllocation represents how a template is allocated to a cluster
type TemplateAllocation struct {
	TemplateID   string     `json:"template_id"`
	Namespace    string     `json:"namespace"`
	ClusterID    string     `json:"cluster_id"`
	MinIdle      int32      `json:"min_idle"`
	MaxIdle      int32      `json:"max_idle"`
	LastSyncedAt *time.Time `json:"last_synced_at,omitempty"`
	SyncStatus   string     `json:"sync_status"`
	SyncError    *string    `json:"sync_error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
}

// Repository provides database operations for the scheduler
type Repository struct {
	pool *pgxpool.Pool
}

// NewRepository creates a new Repository
func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// Ping checks database connectivity
func (r *Repository) Ping(ctx context.Context) error {
	return r.pool.Ping(ctx)
}

// === Cluster Operations ===

// CreateCluster creates a new cluster
func (r *Repository) CreateCluster(ctx context.Context, cluster *Cluster) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO scheduler_clusters (cluster_id, internal_gateway_url, weight, enabled)
		VALUES ($1, $2, $3, $4)
	`, cluster.ClusterID, cluster.InternalGatewayURL, cluster.Weight, cluster.Enabled)
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	return nil
}

// GetCluster gets a cluster by ID
func (r *Repository) GetCluster(ctx context.Context, clusterID string) (*Cluster, error) {
	var cluster Cluster
	err := r.pool.QueryRow(ctx, `
		SELECT cluster_id, internal_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
		FROM scheduler_clusters
		WHERE cluster_id = $1
	`, clusterID).Scan(
		&cluster.ClusterID,
		&cluster.InternalGatewayURL,
		&cluster.Weight,
		&cluster.Enabled,
		&cluster.LastSeenAt,
		&cluster.CreatedAt,
		&cluster.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get cluster: %w", err)
	}
	return &cluster, nil
}

// ListClusters lists all clusters
func (r *Repository) ListClusters(ctx context.Context) ([]*Cluster, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT cluster_id, internal_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
		FROM scheduler_clusters
		ORDER BY cluster_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list clusters: %w", err)
	}
	defer rows.Close()

	var clusters []*Cluster
	for rows.Next() {
		var cluster Cluster
		if err := rows.Scan(
			&cluster.ClusterID,
			&cluster.InternalGatewayURL,
			&cluster.Weight,
			&cluster.Enabled,
			&cluster.LastSeenAt,
			&cluster.CreatedAt,
			&cluster.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan cluster: %w", err)
		}
		clusters = append(clusters, &cluster)
	}
	return clusters, nil
}

// ListEnabledClusters lists only enabled clusters
func (r *Repository) ListEnabledClusters(ctx context.Context) ([]*Cluster, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT cluster_id, internal_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
		FROM scheduler_clusters
		WHERE enabled = true
		ORDER BY cluster_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list enabled clusters: %w", err)
	}
	defer rows.Close()

	var clusters []*Cluster
	for rows.Next() {
		var cluster Cluster
		if err := rows.Scan(
			&cluster.ClusterID,
			&cluster.InternalGatewayURL,
			&cluster.Weight,
			&cluster.Enabled,
			&cluster.LastSeenAt,
			&cluster.CreatedAt,
			&cluster.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan cluster: %w", err)
		}
		clusters = append(clusters, &cluster)
	}
	return clusters, nil
}

// UpdateCluster updates a cluster
func (r *Repository) UpdateCluster(ctx context.Context, cluster *Cluster) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_clusters
		SET internal_gateway_url = $2, weight = $3, enabled = $4
		WHERE cluster_id = $1
	`, cluster.ClusterID, cluster.InternalGatewayURL, cluster.Weight, cluster.Enabled)
	if err != nil {
		return fmt.Errorf("update cluster: %w", err)
	}
	return nil
}

// UpdateClusterLastSeen updates the last_seen_at timestamp
func (r *Repository) UpdateClusterLastSeen(ctx context.Context, clusterID string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_clusters
		SET last_seen_at = NOW()
		WHERE cluster_id = $1
	`, clusterID)
	if err != nil {
		return fmt.Errorf("update cluster last seen: %w", err)
	}
	return nil
}

// DeleteCluster deletes a cluster
func (r *Repository) DeleteCluster(ctx context.Context, clusterID string) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM scheduler_clusters WHERE cluster_id = $1
	`, clusterID)
	if err != nil {
		return fmt.Errorf("delete cluster: %w", err)
	}
	return nil
}

// === Template Operations ===

// CreateTemplate creates a new template
func (r *Repository) CreateTemplate(ctx context.Context, template *Template) error {
	specJSON, err := json.Marshal(template.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	_, err = r.pool.Exec(ctx, `
		INSERT INTO scheduler_templates (template_id, namespace, spec)
		VALUES ($1, $2, $3)
	`, template.TemplateID, template.Namespace, specJSON)
	if err != nil {
		return fmt.Errorf("create template: %w", err)
	}
	return nil
}

// GetTemplate gets a template by ID and namespace
func (r *Repository) GetTemplate(ctx context.Context, templateID, namespace string) (*Template, error) {
	var template Template
	var specJSON []byte
	err := r.pool.QueryRow(ctx, `
		SELECT template_id, namespace, spec, created_at, updated_at
		FROM scheduler_templates
		WHERE template_id = $1 AND namespace = $2
	`, templateID, namespace).Scan(
		&template.TemplateID,
		&template.Namespace,
		&specJSON,
		&template.CreatedAt,
		&template.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get template: %w", err)
	}

	if err := json.Unmarshal(specJSON, &template.Spec); err != nil {
		return nil, fmt.Errorf("unmarshal spec: %w", err)
	}
	return &template, nil
}

// ListTemplates lists all templates
func (r *Repository) ListTemplates(ctx context.Context) ([]*Template, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT template_id, namespace, spec, created_at, updated_at
		FROM scheduler_templates
		ORDER BY namespace, template_id
	`)
	if err != nil {
		return nil, fmt.Errorf("list templates: %w", err)
	}
	defer rows.Close()

	var templates []*Template
	for rows.Next() {
		var template Template
		var specJSON []byte
		if err := rows.Scan(
			&template.TemplateID,
			&template.Namespace,
			&specJSON,
			&template.CreatedAt,
			&template.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		if err := json.Unmarshal(specJSON, &template.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal spec: %w", err)
		}
		templates = append(templates, &template)
	}
	return templates, nil
}

// ListTemplatesByNamespace lists templates in a specific namespace
func (r *Repository) ListTemplatesByNamespace(ctx context.Context, namespace string) ([]*Template, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT template_id, namespace, spec, created_at, updated_at
		FROM scheduler_templates
		WHERE namespace = $1
		ORDER BY template_id
	`, namespace)
	if err != nil {
		return nil, fmt.Errorf("list templates by namespace: %w", err)
	}
	defer rows.Close()

	var templates []*Template
	for rows.Next() {
		var template Template
		var specJSON []byte
		if err := rows.Scan(
			&template.TemplateID,
			&template.Namespace,
			&specJSON,
			&template.CreatedAt,
			&template.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan template: %w", err)
		}
		if err := json.Unmarshal(specJSON, &template.Spec); err != nil {
			return nil, fmt.Errorf("unmarshal spec: %w", err)
		}
		templates = append(templates, &template)
	}
	return templates, nil
}

// UpdateTemplate updates a template
func (r *Repository) UpdateTemplate(ctx context.Context, template *Template) error {
	specJSON, err := json.Marshal(template.Spec)
	if err != nil {
		return fmt.Errorf("marshal spec: %w", err)
	}

	_, err = r.pool.Exec(ctx, `
		UPDATE scheduler_templates
		SET spec = $3
		WHERE template_id = $1 AND namespace = $2
	`, template.TemplateID, template.Namespace, specJSON)
	if err != nil {
		return fmt.Errorf("update template: %w", err)
	}
	return nil
}

// DeleteTemplate deletes a template
func (r *Repository) DeleteTemplate(ctx context.Context, templateID, namespace string) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM scheduler_templates WHERE template_id = $1 AND namespace = $2
	`, templateID, namespace)
	if err != nil {
		return fmt.Errorf("delete template: %w", err)
	}
	return nil
}

// === Template Allocation Operations ===

// UpsertAllocation creates or updates a template allocation
func (r *Repository) UpsertAllocation(ctx context.Context, alloc *TemplateAllocation) error {
	_, err := r.pool.Exec(ctx, `
		INSERT INTO scheduler_template_allocations (template_id, namespace, cluster_id, min_idle, max_idle, sync_status)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (template_id, namespace, cluster_id)
		DO UPDATE SET min_idle = $4, max_idle = $5, sync_status = $6
	`, alloc.TemplateID, alloc.Namespace, alloc.ClusterID, alloc.MinIdle, alloc.MaxIdle, alloc.SyncStatus)
	if err != nil {
		return fmt.Errorf("upsert allocation: %w", err)
	}
	return nil
}

// GetAllocation gets an allocation
func (r *Repository) GetAllocation(ctx context.Context, templateID, namespace, clusterID string) (*TemplateAllocation, error) {
	var alloc TemplateAllocation
	err := r.pool.QueryRow(ctx, `
		SELECT template_id, namespace, cluster_id, min_idle, max_idle, last_synced_at, sync_status, sync_error, created_at, updated_at
		FROM scheduler_template_allocations
		WHERE template_id = $1 AND namespace = $2 AND cluster_id = $3
	`, templateID, namespace, clusterID).Scan(
		&alloc.TemplateID,
		&alloc.Namespace,
		&alloc.ClusterID,
		&alloc.MinIdle,
		&alloc.MaxIdle,
		&alloc.LastSyncedAt,
		&alloc.SyncStatus,
		&alloc.SyncError,
		&alloc.CreatedAt,
		&alloc.UpdatedAt,
	)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get allocation: %w", err)
	}
	return &alloc, nil
}

// ListAllocationsByTemplate lists all allocations for a template
func (r *Repository) ListAllocationsByTemplate(ctx context.Context, templateID, namespace string) ([]*TemplateAllocation, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT template_id, namespace, cluster_id, min_idle, max_idle, last_synced_at, sync_status, sync_error, created_at, updated_at
		FROM scheduler_template_allocations
		WHERE template_id = $1 AND namespace = $2
		ORDER BY cluster_id
	`, templateID, namespace)
	if err != nil {
		return nil, fmt.Errorf("list allocations by template: %w", err)
	}
	defer rows.Close()

	var allocations []*TemplateAllocation
	for rows.Next() {
		var alloc TemplateAllocation
		if err := rows.Scan(
			&alloc.TemplateID,
			&alloc.Namespace,
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

// ListAllocationsByCluster lists all allocations for a cluster
func (r *Repository) ListAllocationsByCluster(ctx context.Context, clusterID string) ([]*TemplateAllocation, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT template_id, namespace, cluster_id, min_idle, max_idle, last_synced_at, sync_status, sync_error, created_at, updated_at
		FROM scheduler_template_allocations
		WHERE cluster_id = $1
		ORDER BY namespace, template_id
	`, clusterID)
	if err != nil {
		return nil, fmt.Errorf("list allocations by cluster: %w", err)
	}
	defer rows.Close()

	var allocations []*TemplateAllocation
	for rows.Next() {
		var alloc TemplateAllocation
		if err := rows.Scan(
			&alloc.TemplateID,
			&alloc.Namespace,
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

// UpdateAllocationSyncStatus updates the sync status of an allocation
func (r *Repository) UpdateAllocationSyncStatus(ctx context.Context, templateID, namespace, clusterID, status string, syncError *string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_template_allocations
		SET sync_status = $4, sync_error = $5, last_synced_at = CASE WHEN $4 = 'synced' THEN NOW() ELSE last_synced_at END
		WHERE template_id = $1 AND namespace = $2 AND cluster_id = $3
	`, templateID, namespace, clusterID, status, syncError)
	if err != nil {
		return fmt.Errorf("update allocation sync status: %w", err)
	}
	return nil
}

// DeleteAllocationsByTemplate deletes all allocations for a template
func (r *Repository) DeleteAllocationsByTemplate(ctx context.Context, templateID, namespace string) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM scheduler_template_allocations WHERE template_id = $1 AND namespace = $2
	`, templateID, namespace)
	if err != nil {
		return fmt.Errorf("delete allocations by template: %w", err)
	}
	return nil
}

// DeleteAllocation deletes a specific allocation
func (r *Repository) DeleteAllocation(ctx context.Context, templateID, namespace, clusterID string) error {
	_, err := r.pool.Exec(ctx, `
		DELETE FROM scheduler_template_allocations WHERE template_id = $1 AND namespace = $2 AND cluster_id = $3
	`, templateID, namespace, clusterID)
	if err != nil {
		return fmt.Errorf("delete allocation: %w", err)
	}
	return nil
}
