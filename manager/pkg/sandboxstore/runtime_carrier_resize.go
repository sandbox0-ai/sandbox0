package sandboxstore

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// RuntimeCarrierNode projects capacity and immutable resize intent. Resource
// accounting continues to come exclusively from runtime_resource_leases.
type RuntimeCarrierNode struct {
	ClusterID, NodeID, NodeUID, NodeBootID string
	FreeCPU, FreeMemory                    int64
	PhysicalCPU, PhysicalMemory            int64
	Ready                                  int
	Revision                               int64
	Pending                                bool
	Groups                                 []string
	RetainedAllocations                    []string
	MaxCarriers                            int
	CompletedAt                            *time.Time
	Retiring                               bool
	ReadyByCompatibility                   map[string]int
	CompatibilityCapacity                  map[string]int
	SurplusSince                           *time.Time
	StaleIdentity                          bool
}

// HeartbeatRuntimeCarrierController bounds how long the cloud scaler may credit
// replenishable inventory rather than immediately buying another host.
func (s *PGSandboxStore) HeartbeatRuntimeCarrierController(ctx context.Context, cluster string, ttl time.Duration) error {
	if cluster == "" || ttl < time.Second || ttl > time.Minute {
		return ErrRuntimeSlotInvalid
	}
	_, err := s.pool.Exec(ctx, `INSERT INTO manager.runtime_carrier_controllers(cluster_id,heartbeat_expires_at)
		VALUES($1,NOW()+($2*INTERVAL '1 millisecond')) ON CONFLICT(cluster_id) DO UPDATE
		SET heartbeat_expires_at=EXCLUDED.heartbeat_expires_at`, cluster, ttl.Milliseconds())
	return err
}

// RecordRuntimeCarrierSurplus persists a continuous excess-inventory window.
// A dip below the high watermark or an in-flight resize resets that window.
func (s *PGSandboxStore) RecordRuntimeCarrierSurplus(ctx context.Context, cluster string, high int) error {
	if cluster == "" || high < 1 || high > 576 {
		return ErrRuntimeSlotInvalid
	}
	_, err := s.pool.Exec(ctx, `WITH ready AS(SELECT node_id,node_uid,node_boot_id,COUNT(*) AS count
		FROM manager.runtime_slots WHERE cluster_id=$1 AND state='fastpath_ready' AND NOT carrier_retired
		AND heartbeat_expires_at>NOW() GROUP BY node_id,node_uid,node_boot_id)
		UPDATE manager.runtime_carrier_resizes r SET surplus_since=CASE WHEN NOT r.pending AND
		COALESCE((SELECT count FROM ready WHERE node_id=r.node_id AND node_uid=r.node_uid AND node_boot_id=r.node_boot_id),0)>$2
		THEN COALESCE(surplus_since,NOW()) ELSE NULL END WHERE r.cluster_id=$1`, cluster, high)
	return err
}

// ListRuntimeCarrierNodes returns only live, admitted nodes, including interrupted
// resizes. Retired incarnations never regain admission through this controller.
func (s *PGSandboxStore) ListRuntimeCarrierNodes(ctx context.Context, cluster string) ([]RuntimeCarrierNode, error) {
	rows, err := s.pool.Query(ctx, `
        WITH live AS (SELECT DISTINCT ON (c.node_id) c.* FROM manager.runtime_node_capacities c
            LEFT JOIN manager.runtime_carrier_resizes r USING(cluster_id,node_id)
            WHERE c.cluster_id=$1 AND (c.heartbeat_expires_at>NOW() OR r.pending OR
                (cardinality(r.allowed_groups)>8 AND EXISTS(SELECT 1 FROM manager.runtime_node_fences f
                    WHERE f.cluster_id=c.cluster_id AND f.node_id=c.node_id AND f.node_uid=c.node_uid AND f.state='revoked')))
            ORDER BY c.node_id,(c.heartbeat_expires_at>NOW()) DESC,c.updated_at DESC)
        SELECT c.cluster_id,c.node_id,c.node_uid,c.node_boot_id,c.cpu_millicores,c.memory_bytes,
            GREATEST(0,COALESCE(NULLIF(c.admission_cpu_millicores,0),c.cpu_millicores)-
                (SELECT COALESCE(SUM(cpu_millicores),0) FROM manager.runtime_resource_leases
                 WHERE cluster_id=c.cluster_id AND node_uid=c.node_uid AND lease_state='active'))::bigint,
            GREATEST(0,COALESCE(NULLIF(c.admission_memory_bytes,0),c.memory_bytes)-
                (SELECT COALESCE(SUM(memory_bytes),0) FROM manager.runtime_resource_leases
                 WHERE cluster_id=c.cluster_id AND node_uid=c.node_uid AND lease_state='active'))::bigint,
            (SELECT COUNT(*) FROM manager.runtime_slots WHERE cluster_id=c.cluster_id
                AND node_id=c.node_id AND node_uid=c.node_uid AND node_boot_id=c.node_boot_id
                AND state='fastpath_ready' AND heartbeat_expires_at>NOW())::integer,
            COALESCE(r.revision,0),COALESCE(r.pending,false),r.allowed_groups,r.retained_allocations,
            COALESCE(r.max_carriers,0),r.completed_at,
            EXISTS(SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=c.cluster_id
                AND f.node_id=c.node_id AND f.node_uid=c.node_uid AND f.state='revoked'),
            COALESCE(r.compatibility_capacity,'{}'::jsonb),r.surplus_since,
            COALESCE(r.node_boot_id<>c.node_boot_id OR r.node_uid<>c.node_uid,false),
            COALESCE((SELECT jsonb_object_agg(compatibility_digest,count) FROM (
                SELECT compatibility_digest,COUNT(*)::integer AS count FROM manager.runtime_slots s
                WHERE s.cluster_id=c.cluster_id AND s.node_id=c.node_id AND s.node_uid=c.node_uid
                    AND s.node_boot_id=c.node_boot_id AND s.state='fastpath_ready' AND NOT s.carrier_retired
                    AND s.heartbeat_expires_at>NOW() GROUP BY compatibility_digest) ready),'{}'::jsonb)
        FROM live c LEFT JOIN manager.runtime_carrier_resizes r USING(cluster_id,node_id)
        WHERE r.pending OR (cardinality(r.allowed_groups)>8 AND EXISTS(SELECT 1 FROM manager.runtime_node_fences f
            WHERE f.cluster_id=c.cluster_id AND f.node_id=c.node_id AND f.node_uid=c.node_uid AND f.state='revoked'))
        OR (c.heartbeat_expires_at>NOW() AND NOT EXISTS (SELECT 1 FROM manager.runtime_node_fences f
            WHERE f.cluster_id=c.cluster_id AND f.node_id=c.node_id AND f.node_uid=c.node_uid
                AND f.state IN ('warming','draining','revoked')))
        ORDER BY COALESCE(r.pending,false) DESC,r.completed_at NULLS FIRST,c.node_id LIMIT 300`, cluster)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RuntimeCarrierNode
	for rows.Next() {
		var n RuntimeCarrierNode
		if err := rows.Scan(&n.ClusterID, &n.NodeID, &n.NodeUID, &n.NodeBootID, &n.PhysicalCPU, &n.PhysicalMemory, &n.FreeCPU, &n.FreeMemory,
			&n.Ready, &n.Revision, &n.Pending, &n.Groups, &n.RetainedAllocations, &n.MaxCarriers, &n.CompletedAt, &n.Retiring, &n.CompatibilityCapacity, &n.SurplusSince, &n.StaleIdentity, &n.ReadyByCompatibility); err != nil {
			return nil, err
		}
		result = append(result, n)
	}
	return result, rows.Err()
}

// BeginRuntimeCarrierResize serializes with claim resource allocation. Once it
// commits, a new claim cannot race the controller's subsequent busy-slot read.
func (s *PGSandboxStore) BeginRuntimeCarrierResize(ctx context.Context, n RuntimeCarrierNode, maximum int, groups, retained []string) (int64, error) {
	if maximum < 8 || maximum > 576 || len(groups) < 8 || len(groups) > maximum || len(retained) > 8192 {
		return 0, ErrRuntimeSlotInvalid
	}
	if n.ClusterID == "" || n.NodeID == "" || n.NodeUID == "" || n.NodeBootID == "" || len(n.CompatibilityCapacity) > 2 {
		return 0, ErrRuntimeSlotInvalid
	}
	seen := map[string]bool{}
	for _, group := range groups {
		if group == "" || len(group) > 32 || seen[group] {
			return 0, ErrRuntimeSlotInvalid
		}
		seen[group] = true
	}
	for i := 0; i < 8; i++ {
		if !seen[fmt.Sprintf("warm-%d", i)] {
			return 0, ErrRuntimeSlotInvalid
		}
	}
	for digest, count := range n.CompatibilityCapacity {
		canonical, err := normalizeRuntimeSlotDigest("compatibility_digest", digest)
		if err != nil || canonical != digest || count < 1 || count > maximum {
			return 0, ErrRuntimeSlotInvalid
		}
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var marker int
	err = tx.QueryRow(ctx, `SELECT 1 FROM manager.runtime_node_capacities c
        WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4 AND (
        (heartbeat_expires_at>NOW() AND NOT EXISTS (SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=c.cluster_id
            AND f.node_id=c.node_id AND f.node_uid=c.node_uid AND f.state IN ('warming','draining','revoked')))
        OR (EXISTS (SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=c.cluster_id
            AND f.node_id=c.node_id AND f.node_uid=c.node_uid AND f.state='revoked')
            AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases l WHERE l.cluster_id=c.cluster_id AND l.node_uid=c.node_uid AND l.lease_state='active')))
        FOR UPDATE OF c`, n.ClusterID, n.NodeID, n.NodeUID, n.NodeBootID).Scan(&marker)
	if err != nil {
		return 0, err
	}
	// The caller read Nomad before taking the lock. Revalidate that no claim
	// acquired an allocation which the proposed plan would remove in between.
	var unsafe bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM manager.runtime_slots s
		WHERE cluster_id=$1 AND node_id=$2 AND NOT(allocation_id=ANY($3::text[]))
		AND (state IN ('claiming','starting','active','quiescing','orphaned') OR EXISTS(
			SELECT 1 FROM manager.runtime_resource_leases l WHERE l.slot_id=s.slot_id AND l.lease_state='active')))
	`, n.ClusterID, n.NodeID, retained).Scan(&unsafe); err != nil {
		return 0, err
	}
	if unsafe {
		return 0, ErrRuntimeSlotConflict
	}
	var revision int64
	err = tx.QueryRow(ctx, `INSERT INTO manager.runtime_carrier_resizes
        (cluster_id,node_id,node_uid,node_boot_id,max_carriers,allowed_groups,retained_allocations,compatibility_capacity) VALUES($1,$2,$3,$4,$5,$7,$8,COALESCE($9::jsonb,'{}'::jsonb))
        ON CONFLICT(cluster_id,node_id) DO UPDATE SET revision=nextval('manager.runtime_carrier_revision'),
            node_uid=EXCLUDED.node_uid,node_boot_id=EXCLUDED.node_boot_id,pending=true,surplus_since=NULL,
            allowed_groups=EXCLUDED.allowed_groups,retained_allocations=EXCLUDED.retained_allocations,
            max_carriers=EXCLUDED.max_carriers,compatibility_capacity=EXCLUDED.compatibility_capacity,updated_at=NOW()
		WHERE manager.runtime_carrier_resizes.revision=$6 AND (NOT manager.runtime_carrier_resizes.pending
			OR (manager.runtime_carrier_resizes.node_uid=EXCLUDED.node_uid
				AND manager.runtime_carrier_resizes.node_boot_id<>EXCLUDED.node_boot_id
				AND NOT EXISTS(SELECT 1 FROM manager.runtime_resource_leases l WHERE l.cluster_id=$1 AND l.node_uid=$3
					AND l.node_boot_id=manager.runtime_carrier_resizes.node_boot_id AND l.lease_state='active'))
			OR (cardinality(EXCLUDED.allowed_groups)=8 AND cardinality(EXCLUDED.retained_allocations)=0
				AND EXISTS(SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=$1 AND f.node_id=$2
					AND f.node_uid=$3 AND f.state='revoked')))
        RETURNING revision`, n.ClusterID, n.NodeID, n.NodeUID, n.NodeBootID, maximum, n.Revision, groups, retained, n.CompatibilityCapacity).Scan(&revision)
	if err != nil {
		return 0, err
	}
	return revision, tx.Commit(ctx)
}

// RuntimeCarrierBusyAllocations includes retained cleanup custody, not just
// running sandboxes. A dead heartbeat never permits removing their placement.
func (s *PGSandboxStore) RuntimeCarrierBusyAllocations(ctx context.Context, n RuntimeCarrierNode) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT allocation_id FROM manager.runtime_slots s
        WHERE cluster_id=$1 AND node_id=$2 AND (state IN ('claiming','starting','active','quiescing','orphaned')
            OR EXISTS(SELECT 1 FROM manager.runtime_resource_leases l WHERE l.slot_id=s.slot_id AND l.lease_state='active'))
        ORDER BY allocation_id LIMIT 577`, n.ClusterID, n.NodeID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	if len(result) > 576 {
		return nil, fmt.Errorf("carrier busy inventory exceeds bound")
	}
	return result, rows.Err()
}

// RuntimeCarrierReadyAllocations confirms ctld registration independently of
// Nomad task status. Busy carriers remain usable inventory while their heartbeat
// is live; terminal or retired slots cannot complete a replenishment intent.
func (s *PGSandboxStore) RuntimeCarrierReadyAllocations(ctx context.Context, n RuntimeCarrierNode) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT allocation_id FROM manager.runtime_slots
		WHERE cluster_id=$1 AND node_id=$2 AND node_uid=$3 AND node_boot_id=$4
		AND NOT carrier_retired AND heartbeat_expires_at>NOW()
		AND state IN ('fastpath_ready','claiming','starting','active') LIMIT 577`,
		n.ClusterID, n.NodeID, n.NodeUID, n.NodeBootID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	if len(result) > 576 {
		return nil, ErrRuntimeSlotInvalid
	}
	return result, rows.Err()
}

// AdmitRuntimeCarrierReadyAllocations publishes individually proven carriers
// without completing a resize. The caller proves Nomad placement and stopped
// removals; this CAS rechecks exact live ctld identity and preserves every other
// fence. Claim still acquires its normal physical resource and writer leases.
func (s *PGSandboxStore) AdmitRuntimeCarrierReadyAllocations(ctx context.Context, n RuntimeCarrierNode, allocations []string) error {
	if len(allocations) == 0 {
		return nil
	}
	if len(allocations) > 576 || n.ClusterID == "" || n.NodeID == "" || n.NodeUID == "" || n.NodeBootID == "" || n.Revision <= 0 || n.Retiring || n.StaleIdentity {
		return ErrRuntimeSlotInvalid
	}
	seen := map[string]bool{}
	for _, id := range allocations {
		if id == "" || len(id) > 256 || seen[id] {
			return ErrRuntimeSlotInvalid
		}
		seen[id] = true
	}
	tag, err := s.pool.Exec(ctx, `UPDATE manager.runtime_carrier_resizes r
        SET retained_allocations=ARRAY(SELECT DISTINCT id FROM unnest(r.retained_allocations || $6::text[]) id ORDER BY id)
        WHERE r.cluster_id=$1 AND r.node_id=$2 AND r.node_uid=$3 AND r.node_boot_id=$4 AND r.revision=$5 AND r.pending
        AND (SELECT COUNT(DISTINCT id) FROM unnest(r.retained_allocations || $6::text[]) id)<=8192
        AND EXISTS(SELECT 1 FROM manager.runtime_node_capacities c WHERE c.cluster_id=$1 AND c.node_id=$2
            AND c.node_uid=$3 AND c.node_boot_id=$4 AND c.heartbeat_expires_at>NOW())
        AND NOT EXISTS(SELECT 1 FROM manager.runtime_node_fences f WHERE f.cluster_id=$1 AND f.node_id=$2
            AND f.node_uid=$3 AND f.state IN ('warming','draining','revoked'))
        AND (SELECT COUNT(*) FROM manager.runtime_slots s WHERE s.cluster_id=$1 AND s.node_id=$2
            AND s.node_uid=$3 AND s.node_boot_id=$4 AND s.allocation_id=ANY($6::text[])
            AND NOT s.carrier_retired AND s.heartbeat_expires_at>NOW()
            AND s.state IN ('fastpath_ready','claiming','starting','active'))=cardinality($6::text[])`,
		n.ClusterID, n.NodeID, n.NodeUID, n.NodeBootID, n.Revision, allocations)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return ErrRuntimeSlotConflict
	}
	return nil
}

func (s *PGSandboxStore) CompleteRuntimeCarrierResize(ctx context.Context, n RuntimeCarrierNode, removed []string) error {
	if len(removed) > 8192 {
		return ErrRuntimeSlotInvalid
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var retained []string
	if err := tx.QueryRow(ctx, `SELECT retained_allocations FROM manager.runtime_carrier_resizes
		WHERE cluster_id=$1 AND node_id=$2 AND revision=$3 AND pending FOR UPDATE`, n.ClusterID, n.NodeID, n.Revision).Scan(&retained); err != nil {
		return err
	}
	kept := map[string]bool{}
	for _, id := range retained {
		kept[id] = true
	}
	for _, id := range removed {
		if kept[id] {
			return ErrRuntimeSlotConflict
		}
	}
	// Nomad has stopped the removed carriers. Fence their stale regional ready
	// rows before reopening admission; the existing terminal reconciler still
	// owns physical cleanup and any resource-release proof.
	if _, err := tx.Exec(ctx, `UPDATE manager.runtime_slots SET carrier_retired=true,revision=revision+1,
		heartbeat_expires_at=LEAST(heartbeat_expires_at,NOW()),updated_at=NOW()
		WHERE cluster_id=$1 AND node_id=$2 AND allocation_id=ANY($3::text[])
		AND state IN ('registered','fastpath_ready')`, n.ClusterID, n.NodeID, removed); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE manager.runtime_carrier_resizes SET pending=false,completed_at=NOW(),updated_at=NOW()
        WHERE cluster_id=$1 AND node_id=$2 AND revision=$3 AND pending AND allowed_groups IS NOT NULL`,
		n.ClusterID, n.NodeID, n.Revision)
	if err == nil && tag.RowsAffected() != 1 {
		return ErrRuntimeSlotConflict
	}
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ListRuntimeCarrierDemand projects still-unfulfilled pressure from the existing
// regional demand ledger. One operation is counted once, including HTTP retries.
func (s *PGSandboxStore) ListRuntimeCarrierDemand(ctx context.Context, cluster string) ([]RuntimeNodePoolDemandShape, error) {
	rows, err := s.pool.Query(ctx, `SELECT compatibility_digest,cpu_millicores,memory_bytes,LEAST(SUM(slots),4096)::integer
 FROM manager.runtime_node_pool_demands d WHERE cluster_id=$1 AND expires_at>NOW()
 AND NOT EXISTS(SELECT 1 FROM manager.runtime_slots s WHERE s.cluster_id=d.cluster_id AND s.claim_operation_id=d.operation_id)
 GROUP BY compatibility_digest,cpu_millicores,memory_bytes
 ORDER BY cpu_millicores DESC,memory_bytes DESC,compatibility_digest LIMIT 1024`, cluster)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []RuntimeNodePoolDemandShape
	for rows.Next() {
		var d RuntimeNodePoolDemandShape
		if err := rows.Scan(&d.CompatibilityDigest, &d.CPUMillicores, &d.MemoryBytes, &d.Slots); err != nil {
			return nil, err
		}
		result = append(result, d)
	}
	return result, rows.Err()
}
