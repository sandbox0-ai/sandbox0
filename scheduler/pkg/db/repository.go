package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

// Cluster represents a registered data-plane cluster.
type Cluster = template.Cluster

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
		INSERT INTO scheduler_clusters (cluster_id, cluster_name, internal_gateway_url, weight, enabled)
		VALUES ($1, $2, $3, $4, $5)
	`, cluster.ClusterID, cluster.ClusterName, cluster.InternalGatewayURL, cluster.Weight, cluster.Enabled)
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	return nil
}

// GetCluster gets a cluster by ID
func (r *Repository) GetCluster(ctx context.Context, clusterID string) (*Cluster, error) {
	var cluster Cluster
	err := r.pool.QueryRow(ctx, `
		SELECT cluster_id, cluster_name, internal_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
		FROM scheduler_clusters
		WHERE cluster_id = $1
	`, clusterID).Scan(
		&cluster.ClusterID,
		&cluster.ClusterName,
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
		SELECT cluster_id, cluster_name, internal_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
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
			&cluster.ClusterName,
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
		SELECT cluster_id, cluster_name, internal_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
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
			&cluster.ClusterName,
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
		SET cluster_name = $2, internal_gateway_url = $3, weight = $4, enabled = $5
		WHERE cluster_id = $1
	`, cluster.ClusterID, cluster.ClusterName, cluster.InternalGatewayURL, cluster.Weight, cluster.Enabled)
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

// === Template Operations are handled by infra/pkg/template/store ===
