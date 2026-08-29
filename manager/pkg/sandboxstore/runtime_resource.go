package sandboxstore

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	DefaultRuntimeNodeCapacityTTL = 90 * time.Second
	MaxRuntimeNodeCapacityTTL     = 10 * time.Minute
	RuntimeResourceLeaseActive    = "active"
	RuntimeResourceLeaseReleased  = "released"
)

// RuntimeNodeCapacity is one ctld-reported dedicated node incarnation. The
// CPU set is a confinement boundary; CPU millicores may be lower to reserve
// host overhead but may never exceed physical set cardinality.
type RuntimeNodeCapacity struct {
	ClusterID           string
	NodeID              string
	NodeUID             string
	NodeBootID          string
	CPUMillicores       int64
	MemoryBytes         int64
	CPUSetCPUs          string
	CPUSetMems          string
	HeartbeatExpiresAt  time.Time
	Revision            int64
	CreatedAt           time.Time
	UpdatedAt           time.Time
	AuthorityObservedAt time.Time
}

type RegisterRuntimeNodeCapacityRequest struct {
	ClusterID     string
	NodeID        string
	NodeUID       string
	NodeBootID    string
	CPUMillicores int64
	MemoryBytes   int64
	CPUSetCPUs    string
	CPUSetMems    string
	TTL           time.Duration
}

// ExpireRuntimeNodeCapacityRequest identifies one exact node boot whose
// authenticated runtime channel is not eligible for new claims.
type ExpireRuntimeNodeCapacityRequest struct {
	ClusterID  string
	NodeID     string
	NodeUID    string
	NodeBootID string
}

// RegisterRuntimeNodeCapacity creates or refreshes one exact capacity shape.
// A reconnect cannot resize a live node incarnation in place.
func (s *PGSandboxStore) RegisterRuntimeNodeCapacity(
	ctx context.Context,
	request *RegisterRuntimeNodeCapacityRequest,
) (*RuntimeNodeCapacity, error) {
	normalized, err := normalizeRuntimeNodeCapacityRequest(request)
	if err != nil {
		return nil, err
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO manager.runtime_node_capacities (
			cluster_id, node_id, node_uid, node_boot_id,
			cpu_millicores, memory_bytes, cpuset_cpus, cpuset_mems,
			heartbeat_expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8,
			NOW() + ($9::double precision * INTERVAL '1 millisecond'))
		ON CONFLICT (cluster_id, node_id, node_uid, node_boot_id) DO UPDATE
		SET heartbeat_expires_at = EXCLUDED.heartbeat_expires_at,
			revision = manager.runtime_node_capacities.revision + 1,
			updated_at = NOW()
		WHERE manager.runtime_node_capacities.cpu_millicores = EXCLUDED.cpu_millicores
			AND manager.runtime_node_capacities.memory_bytes = EXCLUDED.memory_bytes
			AND manager.runtime_node_capacities.cpuset_cpus = EXCLUDED.cpuset_cpus
			AND manager.runtime_node_capacities.cpuset_mems = EXCLUDED.cpuset_mems
	`, normalized.ClusterID, normalized.NodeID, normalized.NodeUID, normalized.NodeBootID,
		normalized.CPUMillicores, normalized.MemoryBytes, normalized.CPUSetCPUs, normalized.CPUSetMems,
		normalized.TTL.Milliseconds())
	if err != nil {
		return nil, mapRuntimeSlotConflict("register runtime node capacity", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: runtime node capacity changed within one boot", ErrRuntimeSlotConflict)
	}
	return s.GetRuntimeNodeCapacity(ctx, normalized.ClusterID, normalized.NodeID, normalized.NodeUID, normalized.NodeBootID)
}

// ExpireRuntimeNodeCapacity immediately removes one exact node boot from new
// claim admission while preserving its row for active resource-lease and
// terminal-cleanup foreign keys. Missing rows are already safely expired.
func (s *PGSandboxStore) ExpireRuntimeNodeCapacity(
	ctx context.Context,
	request *ExpireRuntimeNodeCapacityRequest,
) error {
	normalized, err := normalizeExpireRuntimeNodeCapacityRequest(request)
	if err != nil {
		return err
	}
	_, err = s.pool.Exec(ctx, `
		UPDATE manager.runtime_node_capacities
		SET heartbeat_expires_at = LEAST(heartbeat_expires_at, NOW()),
			revision = revision + 1,
			updated_at = NOW()
		WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3 AND node_boot_id = $4
	`, normalized.ClusterID, normalized.NodeID, normalized.NodeUID, normalized.NodeBootID)
	if err != nil {
		return fmt.Errorf("expire runtime node capacity: %w", err)
	}
	return nil
}

func (s *PGSandboxStore) GetRuntimeNodeCapacity(
	ctx context.Context,
	clusterID, nodeID, nodeUID, nodeBootID string,
) (*RuntimeNodeCapacity, error) {
	capacity, err := scanRuntimeNodeCapacity(s.pool.QueryRow(ctx, runtimeNodeCapacitySelectSQL()+`
		WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3 AND node_boot_id = $4
	`, clusterID, nodeID, nodeUID, nodeBootID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: runtime node capacity", ErrRuntimeSlotNotFound)
	}
	return capacity, err
}

func normalizeRuntimeNodeCapacityRequest(
	request *RegisterRuntimeNodeCapacityRequest,
) (*RegisterRuntimeNodeCapacityRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("runtime node capacity request is required")
	}
	normalized := *request
	identity, err := normalizeRuntimeNodeCapacityIdentity(
		normalized.ClusterID, normalized.NodeID, normalized.NodeUID, normalized.NodeBootID,
	)
	if err != nil {
		return nil, err
	}
	normalized.ClusterID, normalized.NodeID = identity[0], identity[1]
	normalized.NodeUID, normalized.NodeBootID = identity[2], identity[3]
	physicalCPUs, err := protocol.ValidateCPUSet(normalized.CPUSetCPUs)
	if err != nil {
		return nil, fmt.Errorf("cpuset_cpus: %w", err)
	}
	if _, err := protocol.ValidateCPUSet(normalized.CPUSetMems); err != nil {
		return nil, fmt.Errorf("cpuset_mems: %w", err)
	}
	if normalized.CPUMillicores < protocol.MinRuntimeCPUMillicores ||
		normalized.CPUMillicores > int64(physicalCPUs)*1_000 ||
		normalized.CPUMillicores > protocol.MaxRuntimeCPUMillicores {
		return nil, fmt.Errorf("cpu_millicores must fit the dedicated CPU set")
	}
	if normalized.MemoryBytes < 1 || normalized.MemoryBytes > protocol.MaxRuntimeMemoryBytes {
		return nil, fmt.Errorf("memory_bytes is outside the supported capacity range")
	}
	if normalized.TTL == 0 {
		normalized.TTL = DefaultRuntimeNodeCapacityTTL
	}
	if normalized.TTL < time.Second || normalized.TTL > MaxRuntimeNodeCapacityTTL {
		return nil, fmt.Errorf("capacity TTL must be between one second and %s", MaxRuntimeNodeCapacityTTL)
	}
	normalized.TTL = time.Duration(normalized.TTL.Milliseconds()) * time.Millisecond
	return &normalized, nil
}

func normalizeExpireRuntimeNodeCapacityRequest(
	request *ExpireRuntimeNodeCapacityRequest,
) (*ExpireRuntimeNodeCapacityRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("runtime node capacity expiry request is required")
	}
	normalized := *request
	identity, err := normalizeRuntimeNodeCapacityIdentity(
		normalized.ClusterID, normalized.NodeID, normalized.NodeUID, normalized.NodeBootID,
	)
	if err != nil {
		return nil, err
	}
	normalized.ClusterID, normalized.NodeID = identity[0], identity[1]
	normalized.NodeUID, normalized.NodeBootID = identity[2], identity[3]
	return &normalized, nil
}

func normalizeRuntimeNodeCapacityIdentity(
	clusterID, nodeID, nodeUID, nodeBootID string,
) ([4]string, error) {
	identity := [4]string{
		strings.TrimSpace(clusterID), strings.TrimSpace(nodeID),
		strings.TrimSpace(nodeUID), strings.TrimSpace(nodeBootID),
	}
	names := [4]string{"cluster_id", "node_id", "node_uid", "node_boot_id"}
	for index, value := range identity {
		if value == "" || len(value) > 512 {
			return [4]string{}, fmt.Errorf("%s is required and must not exceed 512 bytes", names[index])
		}
	}
	return identity, nil
}

func runtimeNodeCapacitySelectSQL() string {
	return `
		SELECT cluster_id, node_id, node_uid, node_boot_id,
			cpu_millicores, memory_bytes, cpuset_cpus, cpuset_mems,
			heartbeat_expires_at, revision, created_at, updated_at, NOW()
		FROM manager.runtime_node_capacities `
}

func scanRuntimeNodeCapacity(row runtimeSlotScanner) (*RuntimeNodeCapacity, error) {
	var capacity RuntimeNodeCapacity
	if err := row.Scan(
		&capacity.ClusterID, &capacity.NodeID, &capacity.NodeUID, &capacity.NodeBootID,
		&capacity.CPUMillicores, &capacity.MemoryBytes, &capacity.CPUSetCPUs, &capacity.CPUSetMems,
		&capacity.HeartbeatExpiresAt, &capacity.Revision, &capacity.CreatedAt, &capacity.UpdatedAt,
		&capacity.AuthorityObservedAt,
	); err != nil {
		return nil, err
	}
	return &capacity, nil
}
