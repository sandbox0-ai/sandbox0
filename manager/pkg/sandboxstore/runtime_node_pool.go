package sandboxstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const (
	RuntimeNodePoolKindFixed   = "fixed"
	RuntimeNodePoolKindElastic = "elastic"

	RuntimeNodeInstanceEnrolling = "enrolling"
	RuntimeNodeInstanceActive    = "active"
	RuntimeNodeInstanceDraining  = "draining"
	RuntimeNodeInstanceRevoked   = "revoked"
)

var ErrRuntimeNodeNotFound = errors.New("runtime node instance not found")

// RuntimeNodePoolState is the durable desired-capacity and stabilization state
// for one provider-backed worker pool. Cloud inventory remains authoritative
// for actual instance count.
type RuntimeNodePoolState struct {
	PoolID              string
	ClusterID           string
	DesiredNodes        int
	LowPressureSince    time.Time
	LastScaleOutAt      time.Time
	LastScaleInAt       time.Time
	Revision            int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
	AuthorityObservedAt time.Time
}

type RuntimeNodePoolDemandRequest struct {
	PoolID        string
	OperationID   string
	ClusterID     string
	CPUMillicores int64
	MemoryBytes   int64
	Slots         int
	TTL           time.Duration
}

// RuntimeNodePoolNodeUsage joins provider membership to the regional capacity
// ledger. Missing or expired capacity is represented as zero rather than as a
// healthy empty node.
type RuntimeNodePoolNodeUsage struct {
	PoolID             string
	ProviderInstanceID string
	PoolKind           string
	ClusterID          string
	NodeName           string
	NodeID             string
	NodeUID            string
	PrivateIP          string
	AllocationCIDR     string
	State              string
	CPUMillicores      int64
	MemoryBytes        int64
	UsedCPUMillicores  int64
	UsedMemoryBytes    int64
	ActiveLeases       int
	ReadySlots         int
	NonterminalSlots   int
	CapacityLive       bool
	ProviderReady      bool
	AdmittedAt         time.Time
	DrainStartedAt     time.Time
}

type RuntimeNodePoolSnapshot struct {
	State                   RuntimeNodePoolState
	Nodes                   []RuntimeNodePoolNodeUsage
	LiveCPUMillicores       int64
	LiveMemoryBytes         int64
	UsedCPUMillicores       int64
	UsedMemoryBytes         int64
	ActiveLeases            int
	ReadySlots              int
	DemandCPUMillicores     int64
	DemandMemoryBytes       int64
	DemandSlots             int
	ClusterUsedCPU          int64
	ClusterUsedMemory       int64
	ClusterActiveLeases     int
	ClusterReadySlots       int
	ClusterFixedUsableSlots int
	AuthorityObservedAt     time.Time
}

type RuntimeNodeDrainStatus struct {
	Instance RuntimeNodePoolNodeUsage
}

type RuntimeNodeCertificateIdentity struct {
	CommonName string
	ClusterID  string
	NodeID     string
	NodeUID    string
	AgentUID   string
}

type RuntimeNodeEndpointIdentity struct {
	ClusterID string
	NodeID    string
	NodeUID   string
	PrivateIP string
}

type ReserveRuntimeNodeRequest struct {
	PoolID             string
	ProviderInstanceID string
	PoolKind           string
	ClusterID          string
	NodeName           string
	NodeUID            string
	PrivateIP          string
	AllocationSupernet string
	AllocationPrefix   int
}

type ActivateRuntimeNodeRequest struct {
	PoolID              string
	ProviderInstanceID  string
	NomadNodeID         string
	AuthorityCommonName string
	AgentUID            string
}

type RuntimeNodeLifecycleAction struct {
	Token               string
	PoolID              string
	LifecycleHookID     string
	ProviderInstanceIDs []string
	Transition          string
	State               string
	FirstObservedAt     time.Time
	CompletedAt         time.Time
	UpdatedAt           time.Time
}

type ObserveRuntimeNodeLifecycleActionRequest struct {
	Token               string
	PoolID              string
	LifecycleHookID     string
	ProviderInstanceIDs []string
	Transition          string
}

func (s RuntimeNodeDrainStatus) SafeToStop() bool {
	return s.Instance.State == RuntimeNodeInstanceDraining &&
		s.Instance.ActiveLeases == 0 && s.Instance.NonterminalSlots == 0
}

func (s *PGSandboxStore) EnsureRuntimeNodePoolState(
	ctx context.Context,
	poolID, clusterID string,
) (*RuntimeNodePoolState, error) {
	poolID, clusterID, err := normalizeRuntimeNodePoolIdentity(poolID, clusterID)
	if err != nil {
		return nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.runtime_node_pool_states (pool_id, cluster_id)
		VALUES ($1, $2)
		ON CONFLICT (pool_id) DO UPDATE
		SET updated_at = NOW()
		WHERE manager.runtime_node_pool_states.cluster_id = EXCLUDED.cluster_id
	`, poolID, clusterID)
	if err != nil {
		return nil, fmt.Errorf("ensure runtime node pool state: %w", err)
	}
	state, err := s.GetRuntimeNodePoolState(ctx, poolID)
	if err != nil {
		return nil, err
	}
	if state.ClusterID != clusterID {
		return nil, fmt.Errorf("runtime node pool %q belongs to cluster %q, not %q", poolID, state.ClusterID, clusterID)
	}
	return state, nil
}

func (s *PGSandboxStore) GetRuntimeNodePoolState(
	ctx context.Context,
	poolID string,
) (*RuntimeNodePoolState, error) {
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return nil, fmt.Errorf("runtime node pool ID is required")
	}
	state, err := scanRuntimeNodePoolState(s.pool.QueryRow(ctx, `
		SELECT pool_id, cluster_id, desired_nodes, low_pressure_since,
			last_scale_out_at, last_scale_in_at, revision, created_at, updated_at, NOW()
		FROM manager.runtime_node_pool_states
		WHERE pool_id = $1
	`, poolID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("runtime node pool %q not found", poolID)
	}
	return state, err
}

func (s *PGSandboxStore) AcquireRuntimeNodePoolControllerLease(
	ctx context.Context,
	poolID, ownerID string,
	ttl time.Duration,
) (bool, error) {
	poolID = strings.TrimSpace(poolID)
	ownerID = strings.TrimSpace(ownerID)
	if poolID == "" || ownerID == "" || len(ownerID) > 256 {
		return false, fmt.Errorf("runtime node pool lease requires canonical pool and owner IDs")
	}
	if ttl < time.Second || ttl > 5*time.Minute {
		return false, fmt.Errorf("runtime node pool controller lease TTL must be between one second and five minutes")
	}
	var acquired bool
	err := s.pool.QueryRow(ctx, `
		INSERT INTO manager.runtime_node_pool_controller_leases (
			pool_id, owner_id, lease_expires_at
		) VALUES ($1, $2, NOW() + ($3::double precision * INTERVAL '1 millisecond'))
		ON CONFLICT (pool_id) DO UPDATE
		SET owner_id = EXCLUDED.owner_id,
			lease_expires_at = EXCLUDED.lease_expires_at,
			revision = manager.runtime_node_pool_controller_leases.revision + 1,
			updated_at = NOW()
		WHERE manager.runtime_node_pool_controller_leases.owner_id = EXCLUDED.owner_id
			OR manager.runtime_node_pool_controller_leases.lease_expires_at <= NOW()
		RETURNING TRUE
	`, poolID, ownerID, ttl.Milliseconds()).Scan(&acquired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("acquire runtime node pool controller lease: %w", err)
	}
	return acquired, nil
}

func (s *PGSandboxStore) RecordRuntimeNodePoolDemand(
	ctx context.Context,
	request *RuntimeNodePoolDemandRequest,
) error {
	normalized, err := normalizeRuntimeNodePoolDemand(request)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.runtime_node_pool_demands (
			pool_id, operation_id, cluster_id, cpu_millicores,
			memory_bytes, slots, expires_at
		) VALUES ($1, $2, $3, $4, $5, $6,
			NOW() + ($7::double precision * INTERVAL '1 millisecond'))
		ON CONFLICT (pool_id, operation_id) DO UPDATE
		SET cpu_millicores = EXCLUDED.cpu_millicores,
			memory_bytes = EXCLUDED.memory_bytes,
			slots = EXCLUDED.slots,
			expires_at = EXCLUDED.expires_at,
			updated_at = NOW()
		WHERE manager.runtime_node_pool_demands.cluster_id = EXCLUDED.cluster_id
	`, normalized.PoolID, normalized.OperationID, normalized.ClusterID,
		normalized.CPUMillicores, normalized.MemoryBytes, normalized.Slots,
		normalized.TTL.Milliseconds())
	if err != nil {
		return fmt.Errorf("record runtime node pool demand: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) GetRuntimeNodePoolSnapshot(
	ctx context.Context,
	poolID string,
) (*RuntimeNodePoolSnapshot, error) {
	state, err := s.GetRuntimeNodePoolState(ctx, poolID)
	if err != nil {
		return nil, err
	}
	rows, err := s.pool.Query(ctx, `
		WITH live_capacity AS (
			SELECT DISTINCT ON (node_uid)
				cluster_id, node_id, node_uid, node_boot_id,
				cpu_millicores, memory_bytes
			FROM manager.runtime_node_capacities
			WHERE cluster_id = $2 AND heartbeat_expires_at > NOW()
			ORDER BY node_uid, updated_at DESC
		), lease_usage AS (
			SELECT cluster_id, node_id, node_uid, node_boot_id,
				COALESCE(SUM(cpu_millicores), 0)::bigint AS used_cpu,
				COALESCE(SUM(memory_bytes), 0)::bigint AS used_memory,
				COUNT(*)::integer AS active_leases
			FROM manager.runtime_resource_leases
			WHERE cluster_id = $2 AND lease_state = 'active'
			GROUP BY cluster_id, node_id, node_uid, node_boot_id
		), slot_usage AS (
			SELECT cluster_id, node_id, node_uid,
				COUNT(*) FILTER (
					WHERE state = 'fastpath_ready' AND heartbeat_expires_at > NOW()
						AND EXISTS (
							SELECT 1 FROM live_capacity AS current_capacity
							WHERE current_capacity.cluster_id = runtime_slots.cluster_id
								AND current_capacity.node_id = runtime_slots.node_id
								AND current_capacity.node_uid = runtime_slots.node_uid
								AND current_capacity.node_boot_id = runtime_slots.node_boot_id
						)
				)::integer AS ready_slots,
				COUNT(*) FILTER (WHERE state <> 'terminal')::integer AS nonterminal_slots
			FROM manager.runtime_slots
			WHERE cluster_id = $2
			GROUP BY cluster_id, node_id, node_uid
		)
		SELECT instance.pool_id, instance.provider_instance_id, instance.pool_kind,
			instance.cluster_id, instance.node_name, COALESCE(instance.nomad_node_id, ''),
			instance.node_uid, host(instance.private_ip), instance.allocation_cidr::text,
			instance.state,
			COALESCE(capacity.cpu_millicores, 0), COALESCE(capacity.memory_bytes, 0),
			COALESCE(leases.used_cpu, 0), COALESCE(leases.used_memory, 0),
			COALESCE(leases.active_leases, 0), COALESCE(slots.ready_slots, 0),
			COALESCE(slots.nonterminal_slots, 0), capacity.node_uid IS NOT NULL,
			instance.provider_ready_at IS NOT NULL,
			instance.admitted_at, instance.drain_started_at
		FROM manager.runtime_node_instances AS instance
		LEFT JOIN live_capacity AS capacity
			ON capacity.cluster_id = instance.cluster_id
			AND capacity.node_id = instance.nomad_node_id
			AND capacity.node_uid = instance.node_uid
		LEFT JOIN lease_usage AS leases
			ON leases.cluster_id = capacity.cluster_id
			AND leases.node_id = capacity.node_id
			AND leases.node_uid = capacity.node_uid
			AND leases.node_boot_id = capacity.node_boot_id
		LEFT JOIN slot_usage AS slots
			ON slots.cluster_id = instance.cluster_id
			AND slots.node_id = instance.nomad_node_id
			AND slots.node_uid = instance.node_uid
		WHERE instance.pool_id = $1 AND instance.state <> 'revoked'
		ORDER BY instance.provider_instance_id
	`, poolID, state.ClusterID)
	if err != nil {
		return nil, fmt.Errorf("query runtime node pool nodes: %w", err)
	}
	defer rows.Close()
	snapshot := &RuntimeNodePoolSnapshot{State: *state, AuthorityObservedAt: state.AuthorityObservedAt}
	for rows.Next() {
		node, err := scanRuntimeNodePoolNodeUsage(rows)
		if err != nil {
			return nil, err
		}
		snapshot.Nodes = append(snapshot.Nodes, node)
		snapshot.LiveCPUMillicores += node.CPUMillicores
		snapshot.LiveMemoryBytes += node.MemoryBytes
		snapshot.UsedCPUMillicores += node.UsedCPUMillicores
		snapshot.UsedMemoryBytes += node.UsedMemoryBytes
		snapshot.ActiveLeases += node.ActiveLeases
		snapshot.ReadySlots += node.ReadySlots
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scan runtime node pool nodes: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(SUM(cpu_millicores), 0)::bigint,
			COALESCE(SUM(memory_bytes), 0)::bigint,
			COALESCE(SUM(slots), 0)::integer,
			NOW()
		FROM manager.runtime_node_pool_demands
		WHERE pool_id = $1 AND cluster_id = $2 AND expires_at > NOW()
	`, poolID, state.ClusterID).Scan(
		&snapshot.DemandCPUMillicores, &snapshot.DemandMemoryBytes,
		&snapshot.DemandSlots, &snapshot.AuthorityObservedAt,
	); err != nil {
		return nil, fmt.Errorf("query runtime node pool demand: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `
		WITH live_capacity AS (
			SELECT DISTINCT ON (node_uid)
				cluster_id, node_id, node_uid, node_boot_id
			FROM manager.runtime_node_capacities
			WHERE cluster_id = $1 AND heartbeat_expires_at > NOW()
			ORDER BY node_uid, updated_at DESC
		)
		SELECT
			COALESCE(SUM(lease.cpu_millicores), 0)::bigint,
			COALESCE(SUM(lease.memory_bytes), 0)::bigint,
			COUNT(lease.lease_id)::integer,
			(
				SELECT COUNT(*)::integer
				FROM manager.runtime_slots AS slot
				JOIN live_capacity AS capacity
					ON capacity.cluster_id = slot.cluster_id
					AND capacity.node_id = slot.node_id
					AND capacity.node_uid = slot.node_uid
					AND capacity.node_boot_id = slot.node_boot_id
				WHERE slot.cluster_id = $1
					AND slot.state = 'fastpath_ready'
					AND slot.heartbeat_expires_at > NOW()
			),
			(
				SELECT COUNT(*)::integer
				FROM manager.runtime_slots AS slot
				JOIN live_capacity AS capacity
					ON capacity.cluster_id = slot.cluster_id
					AND capacity.node_id = slot.node_id
					AND capacity.node_uid = slot.node_uid
					AND capacity.node_boot_id = slot.node_boot_id
				WHERE slot.cluster_id = $1
					AND (
						(slot.state = 'fastpath_ready' AND slot.heartbeat_expires_at > NOW())
						OR EXISTS (
							SELECT 1 FROM manager.runtime_resource_leases AS active_lease
							WHERE active_lease.lease_id = slot.resource_lease_id
								AND active_lease.lease_state = 'active'
						)
					)
					AND NOT EXISTS (
						SELECT 1 FROM manager.runtime_node_instances AS elastic_node
						WHERE elastic_node.cluster_id = slot.cluster_id
							AND elastic_node.nomad_node_id = slot.node_id
							AND elastic_node.node_uid = slot.node_uid
							AND elastic_node.pool_kind = 'elastic'
							AND elastic_node.state <> 'revoked'
					)
			)
		FROM live_capacity AS capacity
		LEFT JOIN manager.runtime_resource_leases AS lease
			ON lease.cluster_id = capacity.cluster_id
			AND lease.node_id = capacity.node_id
			AND lease.node_uid = capacity.node_uid
			AND lease.node_boot_id = capacity.node_boot_id
			AND lease.lease_state = 'active'
	`, state.ClusterID).Scan(
		&snapshot.ClusterUsedCPU, &snapshot.ClusterUsedMemory,
		&snapshot.ClusterActiveLeases, &snapshot.ClusterReadySlots,
		&snapshot.ClusterFixedUsableSlots,
	); err != nil {
		return nil, fmt.Errorf("query runtime node pool cluster usage: %w", err)
	}
	return snapshot, nil
}

func (s *PGSandboxStore) UpdateRuntimeNodePoolScaleState(
	ctx context.Context,
	poolID string,
	desiredNodes int,
	lowPressureSince time.Time,
	scaleDirection string,
) (*RuntimeNodePoolState, error) {
	poolID = strings.TrimSpace(poolID)
	if poolID == "" || desiredNodes < 0 || desiredNodes > 299 {
		return nil, fmt.Errorf("invalid runtime node pool scale state")
	}
	if scaleDirection != "" && scaleDirection != "out" && scaleDirection != "in" {
		return nil, fmt.Errorf("runtime node pool scale direction must be empty, out, or in")
	}
	var lowPressure any
	if !lowPressureSince.IsZero() {
		lowPressure = lowPressureSince
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_pool_states
		SET desired_nodes = $2,
			low_pressure_since = $3,
			last_scale_out_at = CASE WHEN $4 = 'out' THEN NOW() ELSE last_scale_out_at END,
			last_scale_in_at = CASE WHEN $4 = 'in' THEN NOW() ELSE last_scale_in_at END,
			revision = revision + 1,
			updated_at = NOW()
		WHERE pool_id = $1
	`, poolID, desiredNodes, lowPressure, scaleDirection)
	if err != nil {
		return nil, fmt.Errorf("update runtime node pool scale state: %w", err)
	}
	return s.GetRuntimeNodePoolState(ctx, poolID)
}

// ReserveRuntimeNode atomically assigns the first free per-node allocation
// subnet. Repeated enrollment for the same provider instance must present the
// same immutable identity.
func (s *PGSandboxStore) ReserveRuntimeNode(
	ctx context.Context,
	request *ReserveRuntimeNodeRequest,
) (*RuntimeNodePoolNodeUsage, error) {
	normalized, supernet, err := normalizeReserveRuntimeNodeRequest(request)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, fmt.Errorf("begin runtime node reservation: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, normalized.PoolID); err != nil {
		return nil, fmt.Errorf("lock runtime node address pool: %w", err)
	}
	var poolCluster string
	if err := tx.QueryRow(ctx, `
		SELECT cluster_id FROM manager.runtime_node_pool_states WHERE pool_id = $1
	`, normalized.PoolID).Scan(&poolCluster); err != nil {
		return nil, fmt.Errorf("load runtime node pool for reservation: %w", err)
	}
	if poolCluster != normalized.ClusterID {
		return nil, fmt.Errorf("runtime node pool cluster does not match enrollment")
	}
	var abandoned bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.runtime_node_lifecycle_actions AS action
			JOIN manager.runtime_node_lifecycle_action_instances AS child
				ON child.lifecycle_action_token = action.lifecycle_action_token
			WHERE action.pool_id = $1
				AND action.transition = 'scale_out'
				AND action.state IN ('draining', 'abandoned')
				AND child.provider_instance_id = $2
		)
	`, normalized.PoolID, normalized.ProviderInstanceID).Scan(&abandoned); err != nil {
		return nil, fmt.Errorf("check abandoned runtime node enrollment: %w", err)
	}
	if abandoned {
		return nil, fmt.Errorf("provider instance enrollment was abandoned by its scale-out lifecycle action")
	}

	var existing RuntimeNodePoolNodeUsage
	var admittedAt, drainStartedAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT pool_id, provider_instance_id, pool_kind, cluster_id, node_name,
			COALESCE(nomad_node_id, ''), node_uid, host(private_ip), allocation_cidr::text,
			state, 0::bigint, 0::bigint, 0::bigint, 0::bigint,
			0::integer, 0::integer, 0::integer, FALSE,
			provider_ready_at IS NOT NULL, admitted_at, drain_started_at
		FROM manager.runtime_node_instances
		WHERE pool_id = $1 AND provider_instance_id = $2
	`, normalized.PoolID, normalized.ProviderInstanceID).Scan(
		&existing.PoolID, &existing.ProviderInstanceID, &existing.PoolKind,
		&existing.ClusterID, &existing.NodeName, &existing.NodeID, &existing.NodeUID,
		&existing.PrivateIP, &existing.AllocationCIDR, &existing.State,
		&existing.CPUMillicores, &existing.MemoryBytes, &existing.UsedCPUMillicores,
		&existing.UsedMemoryBytes, &existing.ActiveLeases, &existing.ReadySlots,
		&existing.NonterminalSlots, &existing.CapacityLive, &existing.ProviderReady,
		&admittedAt, &drainStartedAt,
	)
	if err == nil {
		if admittedAt != nil {
			existing.AdmittedAt = *admittedAt
		}
		if drainStartedAt != nil {
			existing.DrainStartedAt = *drainStartedAt
		}
		if existing.State == RuntimeNodeInstanceRevoked || existing.PoolKind != normalized.PoolKind ||
			existing.ClusterID != normalized.ClusterID || existing.NodeName != normalized.NodeName ||
			existing.NodeUID != normalized.NodeUID || existing.PrivateIP != normalized.PrivateIP {
			return nil, fmt.Errorf("provider instance enrollment conflicts with durable runtime node identity")
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return &existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("load existing runtime node reservation: %w", err)
	}
	rows, err := tx.Query(ctx, `
		SELECT allocation_cidr::text
		FROM manager.runtime_node_instances
		WHERE pool_id = $1 AND state <> 'revoked'
	`, normalized.PoolID)
	if err != nil {
		return nil, fmt.Errorf("list allocated runtime node subnets: %w", err)
	}
	allocated := make(map[netip.Prefix]struct{})
	for rows.Next() {
		var value string
		if err := rows.Scan(&value); err != nil {
			rows.Close()
			return nil, err
		}
		prefix, err := netip.ParsePrefix(value)
		if err != nil {
			rows.Close()
			return nil, err
		}
		allocated[prefix] = struct{}{}
	}
	rows.Close()
	cidr, ok := firstFreeRuntimeNodePrefix(supernet, normalized.AllocationPrefix, allocated)
	if !ok {
		return nil, fmt.Errorf("runtime node allocation supernet is exhausted")
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO manager.runtime_node_instances (
			pool_id, provider, provider_instance_id, pool_kind, cluster_id,
			node_name, node_uid, private_ip, allocation_cidr, state
		) VALUES ($1, 'aliyun', $2, $3, $4, $5, $6, $7::inet, $8::cidr, 'enrolling')
	`, normalized.PoolID, normalized.ProviderInstanceID, normalized.PoolKind,
		normalized.ClusterID, normalized.NodeName, normalized.NodeUID,
		normalized.PrivateIP, cidr.String())
	if err != nil {
		return nil, fmt.Errorf("reserve runtime node: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit runtime node reservation: %w", err)
	}
	return &RuntimeNodePoolNodeUsage{
		PoolID: normalized.PoolID, ProviderInstanceID: normalized.ProviderInstanceID,
		PoolKind: normalized.PoolKind, ClusterID: normalized.ClusterID,
		NodeName: normalized.NodeName, NodeUID: normalized.NodeUID,
		PrivateIP: normalized.PrivateIP, AllocationCIDR: cidr.String(),
		State: RuntimeNodeInstanceEnrolling,
	}, nil
}

// PutRuntimeNodeEnrollmentChallenge stores only a digest, so database access
// cannot be used to replay an enrollment audience.
func (s *PGSandboxStore) PutRuntimeNodeEnrollmentChallenge(
	ctx context.Context,
	poolID, providerInstanceID, remoteIP, challenge string,
	ttl time.Duration,
) error {
	if strings.TrimSpace(challenge) == "" || ttl < time.Second || ttl > 5*time.Minute {
		return fmt.Errorf("invalid runtime node enrollment challenge")
	}
	if err := validateRuntimeNodeAddress(remoteIP, "10.0.0.0/32"); err != nil {
		return err
	}
	digest := sha256.Sum256([]byte(challenge))
	_, err := s.pool.Exec(ctx, `
		WITH expired AS (
			DELETE FROM manager.runtime_node_enrollment_challenges
			WHERE expires_at < NOW() - INTERVAL '1 hour'
			RETURNING challenge_digest
		)
		INSERT INTO manager.runtime_node_enrollment_challenges (
			challenge_digest, pool_id, provider_instance_id, remote_ip, expires_at
		) VALUES ($1, $2, $3, $4::inet,
			NOW() + ($5::double precision * INTERVAL '1 millisecond'))
	`, digest[:], strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID),
		remoteIP, ttl.Milliseconds())
	if err != nil {
		return fmt.Errorf("put runtime node enrollment challenge: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) ConsumeRuntimeNodeEnrollmentChallenge(
	ctx context.Context,
	poolID, providerInstanceID, remoteIP, challenge string,
) error {
	digest := sha256.Sum256([]byte(challenge))
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_enrollment_challenges
		SET consumed_at = NOW()
		WHERE challenge_digest = $1 AND pool_id = $2 AND provider_instance_id = $3
			AND remote_ip = $4::inet AND consumed_at IS NULL AND expires_at > NOW()
	`, digest[:], strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID), remoteIP)
	if err != nil {
		return fmt.Errorf("consume runtime node enrollment challenge: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node enrollment challenge is invalid, expired, or replayed")
	}
	return nil
}

func (s *PGSandboxStore) ActivateRuntimeNode(
	ctx context.Context,
	request *ActivateRuntimeNodeRequest,
) error {
	if request == nil {
		return fmt.Errorf("runtime node activation is required")
	}
	poolID := strings.TrimSpace(request.PoolID)
	instanceID := strings.TrimSpace(request.ProviderInstanceID)
	nodeID := strings.TrimSpace(request.NomadNodeID)
	commonName := strings.TrimSpace(request.AuthorityCommonName)
	agentUID := strings.TrimSpace(request.AgentUID)
	if poolID == "" || instanceID == "" || nodeID == "" || commonName == "" || agentUID == "" {
		return fmt.Errorf("runtime node activation identities are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var state, currentNodeID, currentCommonName, currentAgentUID, clusterID, nodeUID string
	if err := tx.QueryRow(ctx, `
		SELECT state, COALESCE(nomad_node_id, ''), COALESCE(authority_common_name, ''),
			COALESCE(agent_uid, ''), cluster_id, node_uid
		FROM manager.runtime_node_instances
		WHERE pool_id = $1 AND provider_instance_id = $2
		FOR UPDATE
	`, poolID, instanceID).Scan(&state, &currentNodeID, &currentCommonName,
		&currentAgentUID, &clusterID, &nodeUID); err != nil {
		return fmt.Errorf("lock runtime node activation: %w", err)
	}
	if state == RuntimeNodeInstanceActive {
		if currentNodeID != nodeID || currentCommonName != commonName || currentAgentUID != agentUID {
			return fmt.Errorf("runtime node activation conflicts with its durable identity or lifecycle state")
		}
		return tx.Commit(ctx)
	}
	if state != RuntimeNodeInstanceEnrolling {
		return fmt.Errorf("runtime node activation conflicts with its durable identity or lifecycle state")
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.runtime_node_instances
		SET state = 'active', nomad_node_id = $3, authority_common_name = $4,
			agent_uid = $5, admitted_at = NOW(), updated_at = NOW()
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state = 'enrolling'
	`, poolID, instanceID, nodeID, commonName, agentUID)
	if err != nil {
		return fmt.Errorf("activate runtime node: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node activation lost its enrollment state")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO manager.runtime_node_fences (
			cluster_id, node_id, node_uid, state, reason
		) VALUES ($1, $2, $3, 'warming', 'awaiting provider scale-out admission')
		ON CONFLICT (cluster_id, node_id, node_uid) DO NOTHING
	`, clusterID, nodeID, nodeUID); err != nil {
		return fmt.Errorf("fence warming runtime node: %w", err)
	}
	return tx.Commit(ctx)
}

// MarkRuntimeNodeProviderReady removes only the warming claim fence after the
// provider has continued scale-out. Capacity and all eight warm slots are
// rechecked in the same transaction so a stale lifecycle observation cannot
// expose an incomplete node to the claim hot path.
func (s *PGSandboxStore) MarkRuntimeNodeProviderReady(
	ctx context.Context,
	poolID, providerInstanceID string,
	warmSlots int,
) error {
	if strings.TrimSpace(poolID) == "" || strings.TrimSpace(providerInstanceID) == "" || warmSlots != 8 {
		return fmt.Errorf("runtime node provider admission identity is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var alreadyReady bool
	if err := tx.QueryRow(ctx, `
		SELECT provider_ready_at IS NOT NULL
		FROM manager.runtime_node_instances
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state = 'active'
		FOR UPDATE
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID)).Scan(&alreadyReady); err != nil {
		return fmt.Errorf("lock runtime node provider admission: %w", err)
	}
	if alreadyReady {
		var stillReady bool
		if err := tx.QueryRow(ctx, `
			SELECT
				NOT EXISTS (
					SELECT 1 FROM manager.runtime_node_fences AS fence
					JOIN manager.runtime_node_instances AS instance
						ON instance.cluster_id = fence.cluster_id
						AND instance.nomad_node_id = fence.node_id
						AND instance.node_uid = fence.node_uid
					WHERE instance.pool_id = $1 AND instance.provider_instance_id = $2
				)
				AND EXISTS (
					SELECT 1
					FROM manager.runtime_node_instances AS instance
					JOIN manager.runtime_node_capacities AS capacity
						ON capacity.cluster_id = instance.cluster_id
						AND capacity.node_id = instance.nomad_node_id
						AND capacity.node_uid = instance.node_uid
					WHERE instance.pool_id = $1 AND instance.provider_instance_id = $2
						AND capacity.heartbeat_expires_at > NOW()
				)
				AND (
					SELECT COUNT(*) FROM manager.runtime_slots AS slot
					JOIN manager.runtime_node_capacities AS capacity
						ON capacity.cluster_id = slot.cluster_id
						AND capacity.node_id = slot.node_id
						AND capacity.node_uid = slot.node_uid
						AND capacity.node_boot_id = slot.node_boot_id
					JOIN manager.runtime_node_instances AS instance
						ON instance.cluster_id = slot.cluster_id
						AND instance.nomad_node_id = slot.node_id
						AND instance.node_uid = slot.node_uid
					WHERE instance.pool_id = $1 AND instance.provider_instance_id = $2
						AND slot.state = 'fastpath_ready'
						AND slot.heartbeat_expires_at > NOW()
						AND capacity.heartbeat_expires_at > NOW()
				) >= $3
		`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID), warmSlots).Scan(&stillReady); err != nil {
			return fmt.Errorf("recheck runtime node provider admission: %w", err)
		}
		if !stillReady {
			return fmt.Errorf("runtime node provider lifecycle admission is no longer ready")
		}
		return tx.Commit(ctx)
	}
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.runtime_node_fences AS fence
		USING manager.runtime_node_instances AS instance
		WHERE instance.pool_id = $1
			AND instance.provider_instance_id = $2
			AND instance.state = 'active'
			AND fence.cluster_id = instance.cluster_id
			AND fence.node_id = instance.nomad_node_id
			AND fence.node_uid = instance.node_uid
			AND fence.state = 'warming'
			AND EXISTS (
				SELECT 1 FROM manager.runtime_node_capacities AS capacity
				WHERE capacity.cluster_id = instance.cluster_id
					AND capacity.node_id = instance.nomad_node_id
					AND capacity.node_uid = instance.node_uid
					AND capacity.heartbeat_expires_at > NOW()
			)
			AND (
				SELECT COUNT(*) FROM manager.runtime_slots AS slot
				JOIN manager.runtime_node_capacities AS capacity
					ON capacity.cluster_id = slot.cluster_id
					AND capacity.node_id = slot.node_id
					AND capacity.node_uid = slot.node_uid
					AND capacity.node_boot_id = slot.node_boot_id
				WHERE slot.cluster_id = instance.cluster_id
					AND slot.node_id = instance.nomad_node_id
					AND slot.node_uid = instance.node_uid
					AND slot.state = 'fastpath_ready'
					AND slot.heartbeat_expires_at > NOW()
					AND capacity.heartbeat_expires_at > NOW()
			) >= $3
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID), warmSlots)
	if err != nil {
		return fmt.Errorf("admit runtime node provider lifecycle: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node provider lifecycle admission is not ready")
	}
	tag, err = tx.Exec(ctx, `
		UPDATE manager.runtime_node_instances
		SET provider_ready_at = COALESCE(provider_ready_at, NOW()), updated_at = NOW()
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state = 'active'
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID))
	if err != nil {
		return fmt.Errorf("persist runtime node provider admission: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node provider lifecycle identity changed")
	}
	return tx.Commit(ctx)
}

// CompleteReadyRuntimeNodeScaleOutActions closes the narrow recovery window
// where ESS accepted CONTINUE and every child was durably provider-ready, but
// manager lost the response before updating the lifecycle audit row.
func (s *PGSandboxStore) CompleteReadyRuntimeNodeScaleOutActions(
	ctx context.Context,
	poolID string,
) error {
	poolID = strings.TrimSpace(poolID)
	if poolID == "" {
		return fmt.Errorf("runtime node pool ID is required")
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions AS action
		SET state = 'completed', completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE action.pool_id = $1
			AND action.transition = 'scale_out'
			AND action.state = 'pending'
			AND EXISTS (
				SELECT 1 FROM manager.runtime_node_lifecycle_action_instances AS child
				WHERE child.lifecycle_action_token = action.lifecycle_action_token
			)
			AND NOT EXISTS (
				SELECT 1
				FROM manager.runtime_node_lifecycle_action_instances AS child
				WHERE child.lifecycle_action_token = action.lifecycle_action_token
					AND NOT EXISTS (
						SELECT 1 FROM manager.runtime_node_instances AS instance
						WHERE instance.pool_id = action.pool_id
							AND instance.provider_instance_id = child.provider_instance_id
							AND instance.state = 'active'
							AND instance.provider_ready_at IS NOT NULL
					)
			)
	`, poolID)
	if err != nil {
		return fmt.Errorf("complete recovered runtime node scale-out actions: %w", err)
	}
	return nil
}

// AbandonRuntimeNodeEnrollment releases a reservation that never obtained a
// Nomad node identity. Such a row cannot own capacity, slots, or leases.
func (s *PGSandboxStore) AbandonRuntimeNodeEnrollment(
	ctx context.Context,
	poolID, providerInstanceID string,
) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	tag, err := tx.Exec(ctx, `
		DELETE FROM manager.runtime_node_instances
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state = 'enrolling'
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID))
	if err != nil {
		return fmt.Errorf("abandon runtime node enrollment: %w", err)
	}
	if tag.RowsAffected() > 1 {
		return fmt.Errorf("abandoned multiple runtime node enrollment rows")
	}
	if _, err := tx.Exec(ctx, `
		DELETE FROM manager.runtime_node_enrollment_challenges
		WHERE pool_id = $1 AND provider_instance_id = $2
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *PGSandboxStore) GetRuntimeNodeCertificateIdentity(
	ctx context.Context,
	commonName string,
) (*RuntimeNodeCertificateIdentity, error) {
	identity := &RuntimeNodeCertificateIdentity{}
	err := s.pool.QueryRow(ctx, `
		SELECT authority_common_name, cluster_id, nomad_node_id, node_uid, agent_uid
		FROM manager.runtime_node_instances
		WHERE authority_common_name = $1 AND state IN ('active', 'draining')
	`, strings.TrimSpace(commonName)).Scan(
		&identity.CommonName, &identity.ClusterID, &identity.NodeID,
		&identity.NodeUID, &identity.AgentUID,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("runtime node certificate identity not found")
	}
	if err != nil {
		return nil, err
	}
	return identity, nil
}

func (s *PGSandboxStore) GetRuntimeNodeEndpointIdentity(
	ctx context.Context,
	clusterID, nodeID string,
) (*RuntimeNodeEndpointIdentity, error) {
	identity := &RuntimeNodeEndpointIdentity{}
	err := s.pool.QueryRow(ctx, `
		SELECT cluster_id, nomad_node_id, node_uid, host(private_ip)
		FROM manager.runtime_node_instances
		WHERE cluster_id = $1 AND nomad_node_id = $2 AND state IN ('active', 'draining')
	`, strings.TrimSpace(clusterID), strings.TrimSpace(nodeID)).Scan(
		&identity.ClusterID, &identity.NodeID, &identity.NodeUID, &identity.PrivateIP,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("runtime node endpoint identity not found")
	}
	if err != nil {
		return nil, err
	}
	return identity, nil
}

func (s *PGSandboxStore) BeginRuntimeNodeDrain(
	ctx context.Context,
	poolID, providerInstanceID, reason string,
) error {
	reason = strings.TrimSpace(reason)
	if reason == "" || len(reason) > 1024 {
		return fmt.Errorf("runtime node drain reason is required and bounded")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var clusterID, nodeID, nodeUID string
	err = tx.QueryRow(ctx, `
		UPDATE manager.runtime_node_instances
		SET state = 'draining', drain_started_at = COALESCE(drain_started_at, NOW()), updated_at = NOW()
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state IN ('active', 'draining')
		RETURNING cluster_id, nomad_node_id, node_uid
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID)).Scan(&clusterID, &nodeID, &nodeUID)
	if err != nil {
		return fmt.Errorf("begin runtime node drain: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO manager.runtime_node_fences (cluster_id, node_id, node_uid, state, reason)
		VALUES ($1, $2, $3, 'draining', $4)
		ON CONFLICT (cluster_id, node_id, node_uid) DO UPDATE
		SET state = 'draining', reason = EXCLUDED.reason, updated_at = NOW()
		WHERE manager.runtime_node_fences.state IN ('warming', 'draining')
	`, clusterID, nodeID, nodeUID, reason)
	if err != nil {
		return fmt.Errorf("fence draining runtime node: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PGSandboxStore) GetRuntimeNodeDrainStatus(
	ctx context.Context,
	poolID, providerInstanceID string,
) (*RuntimeNodeDrainStatus, error) {
	snapshot, err := s.GetRuntimeNodePoolSnapshot(ctx, strings.TrimSpace(poolID))
	if err != nil {
		return nil, err
	}
	providerInstanceID = strings.TrimSpace(providerInstanceID)
	for _, node := range snapshot.Nodes {
		if node.ProviderInstanceID == providerInstanceID {
			return &RuntimeNodeDrainStatus{Instance: node}, nil
		}
	}
	return nil, ErrRuntimeNodeNotFound
}

// RuntimeNodeAdmissionReady proves that the exact activated node is sending a
// live capacity heartbeat. A Nomad registration alone is not sufficient to
// admit work because ctld owns the cgroup and runtime cleanup boundary.
func (s *PGSandboxStore) RuntimeNodeAdmissionReady(
	ctx context.Context,
	poolID, providerInstanceID, nomadNodeID string,
) (bool, error) {
	var ready bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1
			FROM manager.runtime_node_instances AS instance
			JOIN manager.runtime_node_capacities AS capacity
				ON capacity.cluster_id = instance.cluster_id
				AND capacity.node_id = instance.nomad_node_id
				AND capacity.node_uid = instance.node_uid
			WHERE instance.pool_id = $1
				AND instance.provider_instance_id = $2
				AND instance.nomad_node_id = $3
				AND instance.state = 'active'
				AND capacity.heartbeat_expires_at > NOW()
		)
	`, strings.TrimSpace(poolID), strings.TrimSpace(providerInstanceID),
		strings.TrimSpace(nomadNodeID)).Scan(&ready)
	if err != nil {
		return false, fmt.Errorf("check runtime node admission readiness: %w", err)
	}
	return ready, nil
}

// RevokeRuntimeNode atomically rechecks the usage ledger before releasing the
// node's allocation subnet. The durable revoked fence remains after the cloud
// instance and its local cache have disappeared.
func (s *PGSandboxStore) RevokeRuntimeNode(
	ctx context.Context,
	poolID, providerInstanceID, reason string,
) error {
	poolID = strings.TrimSpace(poolID)
	providerInstanceID = strings.TrimSpace(providerInstanceID)
	reason = strings.TrimSpace(reason)
	if poolID == "" || providerInstanceID == "" || reason == "" || len(reason) > 1024 {
		return fmt.Errorf("runtime node revocation identity and reason are required")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	var clusterID, nodeID, nodeUID string
	err = tx.QueryRow(ctx, `
		SELECT cluster_id, nomad_node_id, node_uid
		FROM manager.runtime_node_instances
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state = 'draining'
		FOR UPDATE
	`, poolID, providerInstanceID).Scan(&clusterID, &nodeID, &nodeUID)
	if err != nil {
		return fmt.Errorf("lock draining runtime node: %w", err)
	}
	var activeLeases, nonterminalSlots int
	err = tx.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*)::integer
			 FROM manager.runtime_resource_leases
			 WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3
				AND lease_state = 'active'),
			(SELECT COUNT(*)::integer
			 FROM manager.runtime_slots
			 WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3
				AND state <> 'terminal')
	`, clusterID, nodeID, nodeUID).Scan(&activeLeases, &nonterminalSlots)
	if err != nil {
		return fmt.Errorf("recheck draining runtime node usage: %w", err)
	}
	if activeLeases != 0 || nonterminalSlots != 0 {
		return fmt.Errorf("runtime node still has %d active leases and %d nonterminal slots",
			activeLeases, nonterminalSlots)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.runtime_node_instances
		SET state = 'revoked', revoked_at = NOW(), updated_at = NOW()
		WHERE pool_id = $1 AND provider_instance_id = $2 AND state = 'draining'
	`, poolID, providerInstanceID); err != nil {
		return fmt.Errorf("revoke runtime node: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE manager.runtime_node_fences
		SET state = 'revoked', reason = $4, updated_at = NOW()
		WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3 AND state = 'draining'
	`, clusterID, nodeID, nodeUID, reason); err != nil {
		return fmt.Errorf("revoke runtime node fence: %w", err)
	}
	return tx.Commit(ctx)
}

func (s *PGSandboxStore) ObserveRuntimeNodeLifecycleAction(
	ctx context.Context,
	request *ObserveRuntimeNodeLifecycleActionRequest,
) (*RuntimeNodeLifecycleAction, error) {
	if request == nil {
		return nil, fmt.Errorf("runtime node lifecycle action is required")
	}
	normalized := *request
	normalized.Token = strings.TrimSpace(normalized.Token)
	normalized.PoolID = strings.TrimSpace(normalized.PoolID)
	normalized.LifecycleHookID = strings.TrimSpace(normalized.LifecycleHookID)
	normalized.Transition = strings.TrimSpace(normalized.Transition)
	seenInstances := make(map[string]struct{}, len(normalized.ProviderInstanceIDs))
	cleanInstances := make([]string, 0, len(normalized.ProviderInstanceIDs))
	for _, instanceID := range normalized.ProviderInstanceIDs {
		instanceID = strings.TrimSpace(instanceID)
		if instanceID == "" || len(instanceID) > 256 {
			return nil, fmt.Errorf("runtime node lifecycle instance identity is invalid")
		}
		if _, exists := seenInstances[instanceID]; !exists {
			seenInstances[instanceID] = struct{}{}
			cleanInstances = append(cleanInstances, instanceID)
		}
	}
	slices.Sort(cleanInstances)
	normalized.ProviderInstanceIDs = cleanInstances
	if normalized.Token == "" || len(normalized.Token) > 512 || normalized.PoolID == "" ||
		normalized.LifecycleHookID == "" || len(normalized.LifecycleHookID) > 256 ||
		len(normalized.ProviderInstanceIDs) == 0 || len(normalized.ProviderInstanceIDs) > 299 ||
		(normalized.Transition != "scale_out" && normalized.Transition != "scale_in") {
		return nil, fmt.Errorf("runtime node lifecycle action identity is invalid")
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	row := tx.QueryRow(ctx, `
		INSERT INTO manager.runtime_node_lifecycle_actions (
			lifecycle_action_token, pool_id, lifecycle_hook_id, transition, state
		) VALUES ($1, $2, $3, $4, 'pending')
		ON CONFLICT (lifecycle_action_token) DO UPDATE
		SET updated_at = NOW()
		WHERE manager.runtime_node_lifecycle_actions.pool_id = EXCLUDED.pool_id
			AND manager.runtime_node_lifecycle_actions.lifecycle_hook_id = EXCLUDED.lifecycle_hook_id
			AND manager.runtime_node_lifecycle_actions.transition = EXCLUDED.transition
		RETURNING lifecycle_action_token, pool_id, lifecycle_hook_id,
			transition, state, first_observed_at,
			completed_at, updated_at
	`, normalized.Token, normalized.PoolID, normalized.LifecycleHookID, normalized.Transition)
	action, err := scanRuntimeNodeLifecycleAction(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("runtime node lifecycle token conflicts with durable identity")
	}
	if err != nil {
		return nil, fmt.Errorf("observe runtime node lifecycle action: %w", err)
	}
	for _, instanceID := range normalized.ProviderInstanceIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO manager.runtime_node_lifecycle_action_instances (
				lifecycle_action_token, provider_instance_id
			) VALUES ($1, $2)
			ON CONFLICT DO NOTHING
		`, normalized.Token, instanceID); err != nil {
			return nil, fmt.Errorf("observe runtime node lifecycle instance: %w", err)
		}
	}
	rows, err := tx.Query(ctx, `
		SELECT provider_instance_id
		FROM manager.runtime_node_lifecycle_action_instances
		WHERE lifecycle_action_token = $1
		ORDER BY provider_instance_id
	`, normalized.Token)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var instanceID string
		if err := rows.Scan(&instanceID); err != nil {
			rows.Close()
			return nil, err
		}
		action.ProviderInstanceIDs = append(action.ProviderInstanceIDs, instanceID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !slices.Equal(action.ProviderInstanceIDs, normalized.ProviderInstanceIDs) {
		return nil, fmt.Errorf("runtime node lifecycle token conflicts with durable instance set")
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return action, nil
}

func (s *PGSandboxStore) CompleteRuntimeNodeLifecycleAction(
	ctx context.Context,
	token, state string,
) error {
	token = strings.TrimSpace(token)
	state = strings.TrimSpace(state)
	if token == "" || (state != "completed" && state != "abandoned") {
		return fmt.Errorf("runtime node lifecycle completion is invalid")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions
		SET state = $2, completed_at = COALESCE(completed_at, NOW()), updated_at = NOW()
		WHERE lifecycle_action_token = $1
			AND (state IN ('pending', 'draining') OR state = $2)
	`, token, state)
	if err != nil {
		return fmt.Errorf("complete runtime node lifecycle action: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node lifecycle action is not completable")
	}
	return nil
}

// BeginRuntimeNodeLifecycleActionCleanup durably blocks late enrollment
// retries before any reservation or route is removed. Repeated calls, including
// after final abandonment, are idempotent so provider completion can retry.
func (s *PGSandboxStore) BeginRuntimeNodeLifecycleActionCleanup(
	ctx context.Context,
	token string,
) error {
	token = strings.TrimSpace(token)
	if token == "" {
		return fmt.Errorf("runtime node lifecycle cleanup token is required")
	}
	tag, err := s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_lifecycle_actions
		SET state = CASE WHEN state = 'pending' THEN 'draining' ELSE state END,
			updated_at = NOW()
		WHERE lifecycle_action_token = $1
			AND transition = 'scale_out'
			AND state IN ('pending', 'draining', 'abandoned')
	`, token)
	if err != nil {
		return fmt.Errorf("begin runtime node lifecycle cleanup: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("runtime node lifecycle action is not cleanable")
	}
	return nil
}

func scanRuntimeNodeLifecycleAction(row pgx.Row) (*RuntimeNodeLifecycleAction, error) {
	action := &RuntimeNodeLifecycleAction{}
	var completedAt *time.Time
	err := row.Scan(&action.Token, &action.PoolID, &action.LifecycleHookID,
		&action.Transition, &action.State,
		&action.FirstObservedAt, &completedAt, &action.UpdatedAt)
	if completedAt != nil {
		action.CompletedAt = *completedAt
	}
	return action, err
}

func firstFreeRuntimeNodePrefix(
	supernet netip.Prefix,
	prefixBits int,
	allocated map[netip.Prefix]struct{},
) (netip.Prefix, bool) {
	if !supernet.Addr().Is4() || prefixBits < supernet.Bits() || prefixBits > 30 {
		return netip.Prefix{}, false
	}
	step := uint32(1) << uint32(32-prefixBits)
	start := supernet.Masked().Addr().As4()
	base := uint32(start[0])<<24 | uint32(start[1])<<16 | uint32(start[2])<<8 | uint32(start[3])
	count := uint64(1) << uint64(prefixBits-supernet.Bits())
	for index := uint64(0); index < count; index++ {
		value := base + uint32(index)*step
		addr := netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
		candidate := netip.PrefixFrom(addr, prefixBits)
		if _, exists := allocated[candidate]; !exists {
			return candidate, true
		}
	}
	return netip.Prefix{}, false
}

func normalizeReserveRuntimeNodeRequest(
	request *ReserveRuntimeNodeRequest,
) (*ReserveRuntimeNodeRequest, netip.Prefix, error) {
	if request == nil {
		return nil, netip.Prefix{}, fmt.Errorf("runtime node reservation is required")
	}
	normalized := *request
	var err error
	normalized.PoolID, normalized.ClusterID, err = normalizeRuntimeNodePoolIdentity(
		normalized.PoolID, normalized.ClusterID,
	)
	if err != nil {
		return nil, netip.Prefix{}, err
	}
	normalized.ProviderInstanceID = strings.TrimSpace(normalized.ProviderInstanceID)
	normalized.PoolKind = strings.TrimSpace(normalized.PoolKind)
	normalized.NodeName = strings.TrimSpace(normalized.NodeName)
	normalized.NodeUID = strings.TrimSpace(normalized.NodeUID)
	normalized.PrivateIP = strings.TrimSpace(normalized.PrivateIP)
	normalized.AllocationSupernet = strings.TrimSpace(normalized.AllocationSupernet)
	if normalized.ProviderInstanceID == "" || len(normalized.ProviderInstanceID) > 256 ||
		(normalized.PoolKind != RuntimeNodePoolKindFixed && normalized.PoolKind != RuntimeNodePoolKindElastic) ||
		normalized.NodeName == "" || len(normalized.NodeName) > 128 ||
		normalized.NodeUID == "" || len(normalized.NodeUID) > 512 {
		return nil, netip.Prefix{}, fmt.Errorf("runtime node reservation identities are invalid")
	}
	if err := validateRuntimeNodeAddress(normalized.PrivateIP, normalized.AllocationSupernet); err != nil {
		return nil, netip.Prefix{}, err
	}
	supernet, _ := netip.ParsePrefix(normalized.AllocationSupernet)
	if normalized.AllocationPrefix < supernet.Bits() || normalized.AllocationPrefix > 30 {
		return nil, netip.Prefix{}, fmt.Errorf("runtime node allocation prefix is outside its supernet")
	}
	return &normalized, supernet, nil
}

func normalizeRuntimeNodePoolIdentity(poolID, clusterID string) (string, string, error) {
	poolID = strings.TrimSpace(poolID)
	clusterID = strings.TrimSpace(clusterID)
	if poolID == "" || len(poolID) > 128 || clusterID == "" || len(clusterID) > 512 {
		return "", "", fmt.Errorf("runtime node pool and cluster IDs are required and bounded")
	}
	return poolID, clusterID, nil
}

func normalizeRuntimeNodePoolDemand(request *RuntimeNodePoolDemandRequest) (*RuntimeNodePoolDemandRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("runtime node pool demand is required")
	}
	normalized := *request
	var err error
	normalized.PoolID, normalized.ClusterID, err = normalizeRuntimeNodePoolIdentity(normalized.PoolID, normalized.ClusterID)
	if err != nil {
		return nil, err
	}
	normalized.OperationID = strings.TrimSpace(normalized.OperationID)
	if normalized.OperationID == "" || len(normalized.OperationID) > 512 ||
		normalized.CPUMillicores <= 0 || normalized.MemoryBytes <= 0 {
		return nil, fmt.Errorf("runtime node pool demand identity and resources are required")
	}
	if normalized.Slots == 0 {
		normalized.Slots = 1
	}
	if normalized.Slots < 1 || normalized.Slots > 1024 {
		return nil, fmt.Errorf("runtime node pool demand slots must be between 1 and 1024")
	}
	if normalized.TTL < time.Second || normalized.TTL > 30*time.Minute {
		return nil, fmt.Errorf("runtime node pool demand TTL must be between one second and 30 minutes")
	}
	normalized.TTL = time.Duration(normalized.TTL.Milliseconds()) * time.Millisecond
	return &normalized, nil
}

func scanRuntimeNodePoolState(row runtimeSlotScanner) (*RuntimeNodePoolState, error) {
	var state RuntimeNodePoolState
	var lowPressureSince, lastScaleOutAt, lastScaleInAt *time.Time
	if err := row.Scan(
		&state.PoolID, &state.ClusterID, &state.DesiredNodes, &lowPressureSince,
		&lastScaleOutAt, &lastScaleInAt, &state.Revision, &state.CreatedAt,
		&state.UpdatedAt, &state.AuthorityObservedAt,
	); err != nil {
		return nil, err
	}
	if lowPressureSince != nil {
		state.LowPressureSince = *lowPressureSince
	}
	if lastScaleOutAt != nil {
		state.LastScaleOutAt = *lastScaleOutAt
	}
	if lastScaleInAt != nil {
		state.LastScaleInAt = *lastScaleInAt
	}
	return &state, nil
}

func scanRuntimeNodePoolNodeUsage(row runtimeSlotScanner) (RuntimeNodePoolNodeUsage, error) {
	var node RuntimeNodePoolNodeUsage
	var admittedAt, drainStartedAt *time.Time
	if err := row.Scan(
		&node.PoolID, &node.ProviderInstanceID, &node.PoolKind, &node.ClusterID,
		&node.NodeName, &node.NodeID, &node.NodeUID, &node.PrivateIP,
		&node.AllocationCIDR, &node.State, &node.CPUMillicores, &node.MemoryBytes,
		&node.UsedCPUMillicores, &node.UsedMemoryBytes, &node.ActiveLeases,
		&node.ReadySlots, &node.NonterminalSlots, &node.CapacityLive, &node.ProviderReady,
		&admittedAt, &drainStartedAt,
	); err != nil {
		return RuntimeNodePoolNodeUsage{}, err
	}
	if admittedAt != nil {
		node.AdmittedAt = *admittedAt
	}
	if drainStartedAt != nil {
		node.DrainStartedAt = *drainStartedAt
	}
	return node, nil
}

func validateRuntimeNodeAddress(privateIP, allocationCIDR string) error {
	ip, err := netip.ParseAddr(privateIP)
	if err != nil || !ip.Is4() || !ip.IsPrivate() || ip.String() != privateIP {
		return fmt.Errorf("runtime node private IP must be canonical private IPv4")
	}
	prefix, err := netip.ParsePrefix(allocationCIDR)
	if err != nil || !prefix.Addr().Is4() || !prefix.Addr().IsPrivate() || prefix.Masked() != prefix || prefix.String() != allocationCIDR {
		return fmt.Errorf("runtime node allocation CIDR must be canonical private IPv4")
	}
	return nil
}
