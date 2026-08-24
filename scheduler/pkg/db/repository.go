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

// ClusterCapacity is a point-in-time view of claimable node capacity and
// resource-neutral warm slots for one enabled data-plane cluster.
type ClusterCapacity struct {
	Cluster               Cluster
	ClaimCapacity         int64
	ReadySlots            int64
	EligibleNodes         int64
	FreeCPUMillicores     int64
	FreeMemoryBytes       int64
	LargestNodeFreeCPU    int64
	LargestNodeFreeMemory int64
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
		INSERT INTO scheduler_clusters (cluster_id, cluster_name, cluster_gateway_url, weight, enabled)
		VALUES ($1, $2, $3, $4, $5)
	`, cluster.ClusterID, cluster.ClusterName, cluster.ClusterGatewayURL, cluster.Weight, cluster.Enabled)
	if err != nil {
		return fmt.Errorf("create cluster: %w", err)
	}
	return nil
}

// GetCluster gets a cluster by ID
func (r *Repository) GetCluster(ctx context.Context, clusterID string) (*Cluster, error) {
	var cluster Cluster
	err := r.pool.QueryRow(ctx, `
		SELECT cluster_id, cluster_name, cluster_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
		FROM scheduler_clusters
		WHERE cluster_id = $1
	`, clusterID).Scan(
		&cluster.ClusterID,
		&cluster.ClusterName,
		&cluster.ClusterGatewayURL,
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
		SELECT cluster_id, cluster_name, cluster_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
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
			&cluster.ClusterGatewayURL,
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
		SELECT cluster_id, cluster_name, cluster_gateway_url, weight, enabled, last_seen_at, created_at, updated_at
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
			&cluster.ClusterGatewayURL,
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

// ListSchedulableClusters returns enabled clusters with at least one live warm
// slot on a live node that can satisfy the exact resource request. Manager's
// claim transaction remains the final atomic allocation authority.
func (r *Repository) ListSchedulableClusters(
	ctx context.Context,
	cpuMillicores, memoryBytes int64,
) ([]*ClusterCapacity, error) {
	if cpuMillicores <= 0 || memoryBytes <= 0 {
		return nil, fmt.Errorf("claim resources must be positive")
	}
	rows, err := r.pool.Query(ctx, `
		WITH lease_usage AS (
			SELECT cluster_id, node_id, node_uid, node_boot_id,
				COALESCE(SUM(cpu_millicores), 0)::bigint AS used_cpu,
				COALESCE(SUM(memory_bytes), 0)::bigint AS used_memory
			FROM manager.runtime_resource_leases
			WHERE lease_state = 'active'
			GROUP BY cluster_id, node_id, node_uid, node_boot_id
		), live_nodes AS (
			SELECT capacity.cluster_id, capacity.node_id, capacity.node_uid,
				capacity.node_boot_id,
				GREATEST(capacity.cpu_millicores - COALESCE(usage.used_cpu, 0), 0)::bigint AS free_cpu,
				GREATEST(capacity.memory_bytes - COALESCE(usage.used_memory, 0), 0)::bigint AS free_memory
			FROM manager.runtime_node_capacities AS capacity
			LEFT JOIN lease_usage AS usage USING (cluster_id, node_id, node_uid, node_boot_id)
			WHERE capacity.heartbeat_expires_at > NOW()
		), ready_slots AS (
			SELECT slot.cluster_id, slot.node_id, slot.node_uid, slot.node_boot_id,
				COUNT(*)::bigint AS ready_slots
			FROM manager.runtime_slots AS slot
			JOIN live_nodes AS node USING (cluster_id, node_id, node_uid, node_boot_id)
			WHERE slot.state = 'fastpath_ready'
			  AND slot.heartbeat_expires_at > NOW()
			  AND slot.resource_lease_id IS NULL
			GROUP BY slot.cluster_id, slot.node_id, slot.node_uid, slot.node_boot_id
		), eligible_nodes AS (
			SELECT node.cluster_id, node.free_cpu, node.free_memory, slots.ready_slots,
				LEAST(
					slots.ready_slots,
					node.free_cpu / $1::bigint,
					node.free_memory / $2::bigint
				)::bigint AS claim_capacity
			FROM live_nodes AS node
			JOIN ready_slots AS slots USING (cluster_id, node_id, node_uid, node_boot_id)
			WHERE node.free_cpu >= $1 AND node.free_memory >= $2
		)
		SELECT cluster.cluster_id, cluster.cluster_name, cluster.cluster_gateway_url,
			cluster.weight, cluster.enabled, cluster.last_seen_at,
			cluster.created_at, cluster.updated_at,
			SUM(node.claim_capacity)::bigint,
			SUM(node.ready_slots)::bigint,
			COUNT(*)::bigint,
			SUM(node.free_cpu)::bigint,
			SUM(node.free_memory)::bigint,
			MAX(node.free_cpu)::bigint,
			MAX(node.free_memory)::bigint
		FROM scheduler_clusters AS cluster
		JOIN eligible_nodes AS node ON node.cluster_id = cluster.cluster_id
		WHERE cluster.enabled = TRUE AND node.claim_capacity > 0
		GROUP BY cluster.cluster_id, cluster.cluster_name, cluster.cluster_gateway_url,
			cluster.weight, cluster.enabled, cluster.last_seen_at,
			cluster.created_at, cluster.updated_at
	`, cpuMillicores, memoryBytes)
	if err != nil {
		return nil, fmt.Errorf("list schedulable clusters: %w", err)
	}
	defer rows.Close()

	var capacities []*ClusterCapacity
	for rows.Next() {
		var capacity ClusterCapacity
		if err := rows.Scan(
			&capacity.Cluster.ClusterID,
			&capacity.Cluster.ClusterName,
			&capacity.Cluster.ClusterGatewayURL,
			&capacity.Cluster.Weight,
			&capacity.Cluster.Enabled,
			&capacity.Cluster.LastSeenAt,
			&capacity.Cluster.CreatedAt,
			&capacity.Cluster.UpdatedAt,
			&capacity.ClaimCapacity,
			&capacity.ReadySlots,
			&capacity.EligibleNodes,
			&capacity.FreeCPUMillicores,
			&capacity.FreeMemoryBytes,
			&capacity.LargestNodeFreeCPU,
			&capacity.LargestNodeFreeMemory,
		); err != nil {
			return nil, fmt.Errorf("scan schedulable cluster: %w", err)
		}
		capacities = append(capacities, &capacity)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate schedulable clusters: %w", err)
	}
	return capacities, nil
}

// UpdateCluster updates a cluster
func (r *Repository) UpdateCluster(ctx context.Context, cluster *Cluster) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE scheduler_clusters
		SET cluster_name = $2, cluster_gateway_url = $3, weight = $4, enabled = $5
		WHERE cluster_id = $1
	`, cluster.ClusterID, cluster.ClusterName, cluster.ClusterGatewayURL, cluster.Weight, cluster.Enabled)
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
