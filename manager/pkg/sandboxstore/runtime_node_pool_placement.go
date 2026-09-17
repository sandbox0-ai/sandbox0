package sandboxstore

import (
	"context"
	"fmt"
)

// RuntimeNodePoolDemandShape preserves indivisible requests. Aggregate free
// resources on separate hosts are not interchangeable for one sandbox.
type RuntimeNodePoolDemandShape struct {
	CPUMillicores int64
	MemoryBytes   int64
	Slots         int
}

// RuntimeNodePlacementCapacity is a read-only projection of existing authority,
// not another admission ledger. Physical limits still constrain each request.
type RuntimeNodePlacementCapacity struct {
	PhysicalCPU    int64
	PhysicalMemory int64
	FreeCPU        int64
	FreeMemory     int64
	ReadySlots     int
}

func (s *PGSandboxStore) loadRuntimeNodePoolPlacement(ctx context.Context, snapshot *RuntimeNodePoolSnapshot) error {
	rows, err := s.pool.Query(ctx, `
		WITH live AS MATERIALIZED (
			SELECT DISTINCT ON (node_uid) * FROM manager.runtime_node_capacities
			WHERE cluster_id = $1 AND heartbeat_expires_at > NOW()
			ORDER BY node_uid, updated_at DESC
		), leases AS (
			SELECT node_id, node_uid, node_boot_id, SUM(cpu_millicores) AS cpu,
				SUM(memory_bytes) AS memory, COUNT(*) AS count
			FROM manager.runtime_resource_leases
			WHERE cluster_id = $1 AND lease_state = 'active'
			GROUP BY node_id, node_uid, node_boot_id
		), ready AS (
			SELECT node_id, node_uid, node_boot_id, COUNT(*)::integer AS count
			FROM manager.runtime_slots
			WHERE cluster_id = $1 AND state = 'fastpath_ready' AND heartbeat_expires_at > NOW()
			GROUP BY node_id, node_uid, node_boot_id
		)
		SELECT live.cpu_millicores, live.memory_bytes,
			COALESCE(NULLIF(live.admission_cpu_millicores, 0), live.cpu_millicores),
			COALESCE(NULLIF(live.admission_memory_bytes, 0), live.memory_bytes),
			COALESCE(leases.cpu, 0)::bigint, COALESCE(leases.memory, 0)::bigint,
			COALESCE(ready.count, 0), COALESCE(leases.count, 0),
			EXISTS (SELECT 1 FROM manager.runtime_node_instances AS instance
				WHERE instance.cluster_id = live.cluster_id AND instance.nomad_node_id = live.node_id
					AND instance.node_uid = live.node_uid AND instance.pool_kind = 'elastic'
					AND instance.state <> 'revoked')
		FROM live
		LEFT JOIN leases USING (node_id, node_uid, node_boot_id)
		LEFT JOIN ready USING (node_id, node_uid, node_boot_id)
		WHERE NOT EXISTS (SELECT 1 FROM manager.runtime_node_fences AS fence
			WHERE fence.cluster_id = live.cluster_id AND fence.node_id = live.node_id
				AND fence.node_uid = live.node_uid AND fence.state IN ('warming', 'draining', 'revoked'))
	`, snapshot.State.ClusterID)
	if err != nil {
		return fmt.Errorf("query runtime node placement capacity: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var node RuntimeNodePlacementCapacity
		var admissionCPU, admissionMemory, usedCPU, usedMemory, activeLeases int64
		var elastic bool
		if err := rows.Scan(&node.PhysicalCPU, &node.PhysicalMemory, &admissionCPU, &admissionMemory,
			&usedCPU, &usedMemory, &node.ReadySlots, &activeLeases, &elastic); err != nil {
			return fmt.Errorf("scan runtime node placement capacity: %w", err)
		}
		node.FreeCPU, node.FreeMemory = max(0, admissionCPU-usedCPU), max(0, admissionMemory-usedMemory)
		snapshot.PlacementNodes = append(snapshot.PlacementNodes, node)
		if !elastic && (node.ReadySlots > 0 || activeLeases > 0) {
			snapshot.ClusterFixedCPU += admissionCPU
			snapshot.ClusterFixedMemory += admissionMemory
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan runtime node placement capacity: %w", err)
	}
	// Collapse repeated shapes, while preserving the CPU/memory pair. Combining
	// independently maximal dimensions could invent a request that never existed.
	demands, err := s.pool.Query(ctx, `
		SELECT DISTINCT cpu_millicores, memory_bytes, slots
		FROM manager.runtime_node_pool_demands AS demand
		WHERE pool_id = $1 AND cluster_id = $2 AND expires_at > NOW()
			AND NOT EXISTS (SELECT 1 FROM manager.runtime_slots AS acquired
				WHERE acquired.cluster_id = demand.cluster_id AND acquired.claim_operation_id <> ''
					AND acquired.claim_operation_id = demand.operation_id)
	`, snapshot.State.PoolID, snapshot.State.ClusterID)
	if err != nil {
		return fmt.Errorf("query runtime node pool demand shapes: %w", err)
	}
	defer demands.Close()
	for demands.Next() {
		var demand RuntimeNodePoolDemandShape
		if err := demands.Scan(&demand.CPUMillicores, &demand.MemoryBytes, &demand.Slots); err != nil {
			return fmt.Errorf("scan runtime node pool demand shape: %w", err)
		}
		snapshot.DemandShapes = append(snapshot.DemandShapes, demand)
	}
	return demands.Err()
}
