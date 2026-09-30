package sandboxstore

import (
	"context"
	"fmt"
)

// RuntimeNodePoolDemandShape preserves indivisible requests. Aggregate free
// resources on separate hosts are not interchangeable for one sandbox.
type RuntimeNodePoolDemandShape struct {
	CompatibilityDigest string
	CPUMillicores       int64
	MemoryBytes         int64
	Slots               int
}

// RuntimeNodePlacementCapacity is a read-only projection of existing authority,
// not another admission ledger. Physical limits still constrain each request.
type RuntimeNodePlacementCapacity struct {
	ReadyByCompatibility map[string]int
	PhysicalCPU          int64
	PhysicalMemory       int64
	FreeCPU              int64
	FreeMemory           int64
	ReadySlots           int
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
				AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases reserved WHERE reserved.slot_id=runtime_slots.slot_id)
			GROUP BY node_id, node_uid, node_boot_id
		)
		SELECT live.cpu_millicores, live.memory_bytes,
			COALESCE(NULLIF(live.admission_cpu_millicores, 0), live.cpu_millicores),
			COALESCE(NULLIF(live.admission_memory_bytes, 0), live.memory_bytes),
			COALESCE(leases.cpu, 0)::bigint, COALESCE(leases.memory, 0)::bigint,
			COALESCE(ready.count, 0), COALESCE(leases.count, 0),
			COALESCE((SELECT resize.max_carriers FROM manager.runtime_carrier_resizes resize
				JOIN manager.runtime_carrier_controllers controller USING(cluster_id)
				WHERE resize.cluster_id=live.cluster_id AND resize.node_id=live.node_id
					AND resize.node_uid=live.node_uid AND resize.node_boot_id=live.node_boot_id
					AND controller.heartbeat_expires_at>NOW()
					AND (resize.updated_at>NOW()-INTERVAL '2 minutes' OR (NOT resize.pending AND
						(SELECT COUNT(*) FROM manager.runtime_slots s WHERE s.cluster_id=resize.cluster_id
						 AND s.node_id=resize.node_id AND s.node_uid=resize.node_uid AND s.node_boot_id=resize.node_boot_id
						 AND NOT s.carrier_retired AND s.heartbeat_expires_at>NOW()
						 AND s.state IN ('fastpath_ready','claiming','starting','active'))>=cardinality(resize.allowed_groups)))),0),
			COALESCE((SELECT jsonb_object_agg(compatibility_digest,count) FROM (
				SELECT compatibility_digest,COUNT(*)::integer AS count FROM manager.runtime_slots slot
				WHERE slot.cluster_id=live.cluster_id AND slot.node_id=live.node_id AND slot.node_uid=live.node_uid
					AND slot.node_boot_id=live.node_boot_id AND slot.state='fastpath_ready'
					AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases reserved WHERE reserved.slot_id=slot.slot_id)
					AND slot.heartbeat_expires_at>NOW() AND NOT slot.carrier_retired GROUP BY compatibility_digest
			) compatibility_ready),'{}'::jsonb),
			COALESCE((SELECT resize.compatibility_capacity FROM manager.runtime_carrier_resizes resize
				JOIN manager.runtime_carrier_controllers controller USING(cluster_id)
				WHERE resize.cluster_id=live.cluster_id AND resize.node_id=live.node_id
					AND resize.node_uid=live.node_uid AND resize.node_boot_id=live.node_boot_id
					AND controller.heartbeat_expires_at>NOW()
					AND (resize.updated_at>NOW()-INTERVAL '2 minutes' OR (NOT resize.pending AND
						(SELECT COUNT(*) FROM manager.runtime_slots s WHERE s.cluster_id=resize.cluster_id
						 AND s.node_id=resize.node_id AND s.node_uid=resize.node_uid AND s.node_boot_id=resize.node_boot_id
						 AND NOT s.carrier_retired AND s.heartbeat_expires_at>NOW()
						 AND s.state IN ('fastpath_ready','claiming','starting','active'))>=cardinality(resize.allowed_groups)))),'{}'::jsonb),
			COALESCE((SELECT jsonb_object_agg(compatibility_digest,count) FROM (
				SELECT slot.compatibility_digest,COUNT(*)::integer AS count FROM manager.runtime_resource_leases lease
				JOIN manager.runtime_slots slot ON slot.slot_id=lease.slot_id
				WHERE lease.cluster_id=live.cluster_id AND lease.node_id=live.node_id AND lease.node_uid=live.node_uid
					AND lease.lease_state='active' GROUP BY slot.compatibility_digest
			) compatibility_used),'{}'::jsonb),
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
		var adaptiveMaximum int
		var compatibilityMaximum, compatibilityUsed map[string]int
		if err := rows.Scan(&node.PhysicalCPU, &node.PhysicalMemory, &admissionCPU, &admissionMemory,
			&usedCPU, &usedMemory, &node.ReadySlots, &activeLeases, &adaptiveMaximum, &node.ReadyByCompatibility, &compatibilityMaximum, &compatibilityUsed, &elastic); err != nil {
			return fmt.Errorf("scan runtime node placement capacity: %w", err)
		}
		node.FreeCPU, node.FreeMemory = max(0, admissionCPU-usedCPU), max(0, admissionMemory-usedMemory)
		// Only a live controller and exact-boot provisioned carrier ceiling earn
		// this credit. Requests still must fit indivisible physical resources; a
		// stuck resize loses credit after two minutes instead of hiding demand.
		if adaptiveMaximum > 0 {
			node.ReadySlots = max(node.ReadySlots, adaptiveMaximum-int(activeLeases))
			if !elastic {
				snapshot.ClusterFixedUsableSlots = max(snapshot.ClusterFixedUsableSlots, adaptiveMaximum)
				snapshot.ClusterFixedAdaptiveSlots = max(snapshot.ClusterFixedAdaptiveSlots, adaptiveMaximum)
			}
		}
		for digest, maximum := range compatibilityMaximum {
			node.ReadyByCompatibility[digest] = max(node.ReadyByCompatibility[digest], min(node.ReadySlots, maximum-compatibilityUsed[digest]))
		}
		snapshot.PlacementNodes = append(snapshot.PlacementNodes, node)
		if !elastic && (node.ReadySlots > 0 || activeLeases > 0) {
			snapshot.ClusterFixedCPU += admissionCPU
			snapshot.ClusterFixedMemory += admissionMemory
			snapshot.ClusterFixedPhysicalCPU += node.PhysicalCPU
			snapshot.ClusterFixedPhysicalMemory += node.PhysicalMemory
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("scan runtime node placement capacity: %w", err)
	}
	// Placement checks one indivisible request per distinct CPU/memory pair.
	// The aggregate projection already counts batch quantities; requiring a whole
	// batch on one node would buy extra nodes even when distributed capacity fits.
	demands, err := s.pool.Query(ctx, `
		SELECT DISTINCT cpu_millicores, memory_bytes, 1 AS slots,compatibility_digest
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
		if err := demands.Scan(&demand.CPUMillicores, &demand.MemoryBytes, &demand.Slots, &demand.CompatibilityDigest); err != nil {
			return fmt.Errorf("scan runtime node pool demand shape: %w", err)
		}
		snapshot.DemandShapes = append(snapshot.DemandShapes, demand)
	}
	return demands.Err()
}
