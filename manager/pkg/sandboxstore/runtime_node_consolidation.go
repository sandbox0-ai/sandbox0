package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// RuntimeNodeConsolidationReason identifies a cost-driven drain. Migration
// admission uses it to restrict the destination to the fixed worker.
const RuntimeNodeConsolidationReason = "autoscale-consolidation"

type consolidationLease struct {
	compatibility string
	cpu, memory   int64
	eligible      bool
}

// BeginRuntimeNodeConsolidation fences one lightly occupied elastic node only
// when all of its live leases can fit on the fixed worker. The final migration
// reservation still rechecks exact capacity and compatibility under its own
// transaction; a failed preflight never changes node admission.
func (s *PGSandboxStore) BeginRuntimeNodeConsolidation(ctx context.Context, poolID, instanceID string, maxSandboxes int, headroomCPU, headroomMemory int64, headroomSlots int) (bool, error) {
	if strings.TrimSpace(poolID) == "" || strings.TrimSpace(instanceID) == "" ||
		maxSandboxes < 1 || maxSandboxes > 16 || headroomCPU < 0 || headroomMemory < 0 || headroomSlots < 0 {
		return false, errors.New("invalid runtime node consolidation request")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var clusterID, nodeID, nodeUID string
	var ready bool
	err = tx.QueryRow(ctx, `SELECT cluster_id,nomad_node_id,node_uid,
		state='active' AND provider_ready_at IS NOT NULL
		FROM manager.runtime_node_instances
		WHERE pool_id=$1 AND provider_instance_id=$2 AND pool_kind='elastic' FOR UPDATE`, poolID, instanceID).
		Scan(&clusterID, &nodeID, &nodeUID, &ready)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !ready {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var sourceLive bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_node_capacities capacity
		WHERE capacity.cluster_id=$1 AND capacity.node_id=$2 AND capacity.node_uid=$3
		AND capacity.heartbeat_expires_at>NOW())`, clusterID, nodeID, nodeUID).Scan(&sourceLive); err != nil {
		return false, err
	}
	if !sourceLive {
		return false, nil
	}
	var locked bool
	if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock(hashtextextended($1,0))`,
		"sandbox0-node-consolidation/"+clusterID).Scan(&locked); err != nil {
		return false, err
	}
	if !locked {
		return false, nil
	}
	var competing bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM manager.runtime_node_fences
		WHERE cluster_id=$1 AND state='draining'
	)`, clusterID).Scan(&competing); err != nil {
		return false, err
	}
	if competing {
		return false, nil
	}
	// Normal claim locks a ready slot before taking its resource lease. Holding
	// every source slot prevents a new claim from slipping through this fence.
	rows, err := tx.Query(ctx, `SELECT slot_id FROM manager.runtime_slots
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND state<>'terminal'
		FOR UPDATE`, clusterID, nodeID, nodeUID)
	if err != nil {
		return false, err
	}
	for rows.Next() {
		var slotID string
		if err := rows.Scan(&slotID); err != nil {
			rows.Close()
			return false, err
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return false, err
	}
	var fenced bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (
		SELECT 1 FROM manager.runtime_node_fences
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3
		AND state IN ('warming','draining','revoked')
	)`, clusterID, nodeID, nodeUID).Scan(&fenced); err != nil {
		return false, err
	}
	if fenced {
		return false, nil
	}
	leases, err := consolidationSourceLeases(ctx, tx, clusterID, nodeID, nodeUID)
	if err != nil {
		return false, err
	}
	if len(leases) == 0 || len(leases) > maxSandboxes {
		return false, nil
	}
	for _, lease := range leases {
		if !lease.eligible {
			return false, nil
		}
	}
	fits, err := consolidationFitsFixed(ctx, tx, clusterID, leases, headroomCPU, headroomMemory, headroomSlots)
	if err != nil || !fits {
		return false, err
	}
	_, err = tx.Exec(ctx, `UPDATE manager.runtime_node_instances
		SET state='draining',drain_started_at=NOW(),updated_at=NOW()
		WHERE pool_id=$1 AND provider_instance_id=$2 AND state='active'`, poolID, instanceID)
	if err != nil {
		return false, err
	}
	result, err := tx.Exec(ctx, `INSERT INTO manager.runtime_node_fences
		(cluster_id,node_id,node_uid,state,reason)
		VALUES($1,$2,$3,'draining',$4)
		ON CONFLICT(cluster_id,node_id,node_uid) DO UPDATE
		SET state='draining',reason=EXCLUDED.reason,updated_at=NOW()
		WHERE manager.runtime_node_fences.state NOT IN ('draining','revoked')`,
		clusterID, nodeID, nodeUID, RuntimeNodeConsolidationReason)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() != 1 {
		return false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}

func consolidationSourceLeases(ctx context.Context, tx pgx.Tx, clusterID, nodeID, nodeUID string) ([]consolidationLease, error) {
	rows, err := tx.Query(ctx, `SELECT COALESCE(slot.compatibility_digest,''),lease.cpu_millicores,lease.memory_bytes,
		COALESCE(slot.state='active' AND NOT slot.carrier_retired
			AND slot.heartbeat_expires_at>NOW()
			AND slot.claim_runtime_assignment IS NOT NULL AND slot.claim_network_policy IS NOT NULL
			AND slot.resource_lease_id=lease.lease_id
			AND sandbox.desired_state='active' AND sandbox.deleted_at IS NULL
			AND sandbox.runtime_id=slot.allocation_id
			AND sandbox.runtime_namespace=slot.allocation_namespace
			AND NOT EXISTS(SELECT 1 FROM manager.sandbox_lifecycle_txns life
				WHERE life.sandbox_id=sandbox.sandbox_id
				AND life.phase IN ('preparing','barriered','publishing','committing')),FALSE)
		FROM manager.runtime_resource_leases lease
		LEFT JOIN manager.runtime_slots slot ON slot.slot_id=lease.slot_id
		LEFT JOIN manager.sandboxes sandbox ON sandbox.sandbox_id=slot.sandbox_id
		WHERE lease.cluster_id=$1 AND lease.node_id=$2 AND lease.node_uid=$3
			AND lease.lease_state='active'`, clusterID, nodeID, nodeUID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var leases []consolidationLease
	for rows.Next() {
		var lease consolidationLease
		if err := rows.Scan(&lease.compatibility, &lease.cpu, &lease.memory, &lease.eligible); err != nil {
			return nil, err
		}
		leases = append(leases, lease)
	}
	return leases, rows.Err()
}

func consolidationFitsFixed(ctx context.Context, tx pgx.Tx, clusterID string, leases []consolidationLease, headroomCPU, headroomMemory int64, headroomSlots int) (bool, error) {
	var nodeID, nodeUID, bootID string
	var physicalCPU, physicalMemory, admissionCPU, admissionMemory int64
	err := tx.QueryRow(ctx, `SELECT capacity.node_id,capacity.node_uid,capacity.node_boot_id,
		capacity.cpu_millicores,capacity.memory_bytes,
		COALESCE(NULLIF(capacity.admission_cpu_millicores,0),capacity.cpu_millicores),
		COALESCE(NULLIF(capacity.admission_memory_bytes,0),capacity.memory_bytes)
		FROM manager.runtime_node_capacities capacity
		WHERE capacity.cluster_id=$1 AND capacity.heartbeat_expires_at>NOW()
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_instances elastic
			WHERE elastic.cluster_id=capacity.cluster_id AND elastic.nomad_node_id=capacity.node_id
			AND elastic.node_uid=capacity.node_uid AND elastic.pool_kind='elastic' AND elastic.state<>'revoked')
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_fences fence
			WHERE fence.cluster_id=capacity.cluster_id AND fence.node_id=capacity.node_id
			AND fence.node_uid=capacity.node_uid AND fence.state IN ('warming','draining','revoked'))
		ORDER BY capacity.updated_at DESC LIMIT 1 FOR UPDATE OF capacity`, clusterID).
		Scan(&nodeID, &nodeUID, &bootID, &physicalCPU, &physicalMemory, &admissionCPU, &admissionMemory)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var usedCPU, usedMemory int64
	var activeLeases int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(SUM(cpu_millicores),0)::bigint,
		COALESCE(SUM(memory_bytes),0)::bigint,COUNT(*)::integer FROM manager.runtime_resource_leases
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4
		AND lease_state='active'`, clusterID, nodeID, nodeUID, bootID).Scan(&usedCPU, &usedMemory, &activeLeases); err != nil {
		return false, err
	}
	var totalCPU, totalMemory int64
	needed := map[string]int{}
	for _, lease := range leases {
		if lease.compatibility == "" || lease.cpu <= 0 || lease.memory <= 0 ||
			lease.cpu > physicalCPU || lease.memory > physicalMemory {
			return false, nil
		}
		totalCPU += lease.cpu
		totalMemory += lease.memory
		needed[lease.compatibility]++
	}
	if totalCPU+headroomCPU > admissionCPU-usedCPU || totalMemory+headroomMemory > admissionMemory-usedMemory {
		return false, nil
	}
	rows, err := tx.Query(ctx, `SELECT compatibility_digest,COUNT(*)::integer
		FROM manager.runtime_slots slot
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4
		AND state='fastpath_ready' AND heartbeat_expires_at>NOW() AND NOT carrier_retired
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases reserved WHERE reserved.slot_id=slot.slot_id)
		GROUP BY compatibility_digest`, clusterID, nodeID, nodeUID, bootID)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	available := map[string]int{}
	var totalSlots int
	for rows.Next() {
		var digest string
		var count int
		if err := rows.Scan(&digest, &count); err != nil {
			return false, err
		}
		available[digest] = count
		totalSlots += count
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	// An adaptive fixed worker can replenish its ready buffer. Still require
	// one ready compatible carrier per move now, and enough provisioned carrier
	// capacity to restore the configured spare target after the moves.
	var adaptiveMaximum int
	if err := tx.QueryRow(ctx, `SELECT COALESCE(MAX(resize.max_carriers),0)
		FROM manager.runtime_carrier_resizes resize
		JOIN manager.runtime_carrier_controllers controller USING(cluster_id)
		WHERE resize.cluster_id=$1 AND resize.node_id=$2 AND resize.node_uid=$3
			AND resize.node_boot_id=$4 AND NOT resize.pending
			AND controller.heartbeat_expires_at>NOW()`, clusterID, nodeID, nodeUID, bootID).
		Scan(&adaptiveMaximum); err != nil {
		return false, err
	}
	readyRequirement := len(leases) + headroomSlots
	if adaptiveMaximum > 0 {
		if adaptiveMaximum-activeLeases-len(leases) < headroomSlots {
			return false, nil
		}
		readyRequirement = len(leases)
	}
	if totalSlots < readyRequirement {
		return false, nil
	}
	for digest, count := range needed {
		if available[digest] < count {
			return false, nil
		}
	}
	return true, nil
}

// CancelRuntimeNodeConsolidation reopens a stalled source after a bounded
// interval only when no migration or cleanup still owns either node. Already
// migrated sandboxes remain on their committed destination.
func (s *PGSandboxStore) CancelRuntimeNodeConsolidation(ctx context.Context, poolID, instanceID string, timeout time.Duration) (bool, error) {
	if timeout < time.Minute || strings.TrimSpace(poolID) == "" || strings.TrimSpace(instanceID) == "" {
		return false, fmt.Errorf("invalid consolidation cancellation request")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var clusterID, nodeID, nodeUID string
	var stale bool
	err = tx.QueryRow(ctx, `SELECT cluster_id,nomad_node_id,node_uid,
		drain_started_at < NOW() - ($3::double precision * INTERVAL '1 millisecond')
		FROM manager.runtime_node_instances WHERE pool_id=$1 AND provider_instance_id=$2
		AND state='draining' FOR UPDATE`, poolID, instanceID, float64(timeout.Milliseconds())).
		Scan(&clusterID, &nodeID, &nodeUID, &stale)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !stale {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var activeLeases int
	if err := tx.QueryRow(ctx, `SELECT COUNT(*) FROM manager.runtime_resource_leases
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND lease_state='active'`,
		clusterID, nodeID, nodeUID).Scan(&activeLeases); err != nil {
		return false, err
	}
	if activeLeases == 0 {
		return false, nil
	}
	var sourceLive bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_node_capacities capacity
		WHERE capacity.cluster_id=$1 AND capacity.node_id=$2 AND capacity.node_uid=$3
		AND capacity.heartbeat_expires_at>NOW()
		AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases lease
			WHERE lease.cluster_id=$1 AND lease.node_id=$2 AND lease.node_uid=$3
			AND lease.lease_state='active' AND lease.node_boot_id<>capacity.node_boot_id))`,
		clusterID, nodeID, nodeUID).Scan(&sourceLive); err != nil {
		return false, err
	}
	if !sourceLive {
		return false, nil
	}
	var occupied bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM (`+nomadMigrationOccupiedNodesSQL+`) occupied
		WHERE occupied.cluster_id=$1 AND occupied.node_uid=$2)`, clusterID, nodeUID).Scan(&occupied); err != nil {
		return false, err
	}
	if occupied {
		return false, nil
	}
	result, err := tx.Exec(ctx, `DELETE FROM manager.runtime_node_fences
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3
		AND state='draining' AND reason=$4`, clusterID, nodeID, nodeUID, RuntimeNodeConsolidationReason)
	if err != nil {
		return false, err
	}
	if result.RowsAffected() != 1 {
		return false, nil
	}
	_, err = tx.Exec(ctx, `UPDATE manager.runtime_node_instances
		SET state='active',drain_started_at=NULL,updated_at=NOW()
		WHERE pool_id=$1 AND provider_instance_id=$2 AND state='draining'`, poolID, instanceID)
	if err != nil {
		return false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return true, nil
}
