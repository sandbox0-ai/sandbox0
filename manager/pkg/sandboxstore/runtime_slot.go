package sandboxstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/opencontainers/go-digest"
	protocol "github.com/sandbox0-ai/sandbox0/pkg/runtimeslot"
)

const (
	RuntimeSlotStateRegistered    = string(protocol.StateRegistered)
	RuntimeSlotStateFastpathReady = string(protocol.StateFastpathReady)
	RuntimeSlotStateClaiming      = string(protocol.StateClaiming)
	RuntimeSlotStateStarting      = string(protocol.StateStarting)
	RuntimeSlotStateActive        = string(protocol.StateActive)
	RuntimeSlotStateQuiescing     = string(protocol.StateQuiescing)
	RuntimeSlotStateOrphaned      = string(protocol.StateOrphaned)
	RuntimeSlotStateTerminal      = string(protocol.StateTerminal)

	DefaultRuntimeSlotHeartbeatTTL   = 30 * time.Second
	DefaultRuntimeSlotClaimTTL       = 15 * time.Second
	MaxRuntimeSlotReconcileLimit     = 1_000
	maxRuntimeSlotCapacityCandidates = 1_024
)

var (
	ErrRuntimeSlotNotFound    = errors.New("runtime slot not found")
	ErrRuntimeSlotConflict    = errors.New("runtime slot conflict")
	ErrRuntimeSlotInvalid     = errors.New("runtime slot state is invalid")
	ErrRuntimeSlotNotDue      = errors.New("runtime slot is not due for reconciliation")
	ErrRuntimeSlotUnavailable = errors.New("no fast-path runtime slot is available")
)

// RuntimeSlot is the region-authoritative state of one generic warm runtime
// allocation. Nomad's allocation catalog remains a physical placement view.
type RuntimeSlot struct {
	ID                             string
	ClusterID                      string
	AllocationID                   string
	AllocationNamespace            string
	NodeID                         string
	NodeUID                        string
	NodeBootID                     string
	NetNSIdentity                  string
	ControlEndpoint                string
	CompatibilityDigest            string
	State                          string
	Revision                       int64
	RuntimeReadyDigest             []byte
	NetworkReadyDigest             []byte
	StorageReadyDigest             []byte
	HeartbeatExpiresAt             time.Time
	FastpathReadyAt                time.Time
	ClaimOperationID               string
	ClaimID                        string
	ClaimClusterFilter             string
	ClaimTTL                       time.Duration
	ClaimRuntimeAssignmentRevision string
	ClaimNetworkPolicyDigest       string
	SandboxID                      string
	FilesystemID                   string
	SourceGenerationID             string
	WriterGrantID                  string
	ClaimLeaseExpiresAt            time.Time
	ClaimedAt                      time.Time
	LaunchAttempt                  string
	RunscContainerID               string
	RootFSBindingDigest            []byte
	ClaimNetworkDigest             []byte
	StartingAt                     time.Time
	ProcdInstanceID                string
	ProcdAddress                   string
	CommandReadyDigest             []byte
	CommandReadyAt                 time.Time
	QuiescingAt                    time.Time
	OrphanObservationDigest        []byte
	TerminalReason                 string
	TerminalProofDigest            []byte
	TerminalAt                     time.Time
	CreatedAt                      time.Time
	UpdatedAt                      time.Time
	AuthorityObservedAt            time.Time
	ResourceLease                  protocol.RuntimeResourceLease
	ResourceLeaseDigest            []byte
	ResourceLeaseState             string
	ResourceLeaseReleasedAt        time.Time
}

type RegisterRuntimeSlotRequest struct {
	SlotID              string
	ClusterID           string
	AllocationID        string
	AllocationNamespace string
	NodeID              string
	NodeUID             string
	NodeBootID          string
	NetNSIdentity       string
	ControlEndpoint     string
	CompatibilityDigest string
	HeartbeatTTL        time.Duration
}

type ReportRuntimeSlotReadyRequest struct {
	SlotID             string
	AllocationID       string
	NodeUID            string
	NodeBootID         string
	RuntimeReadyDigest []byte
	NetworkReadyDigest []byte
	StorageReadyDigest []byte
	HeartbeatTTL       time.Duration
}

type HeartbeatRuntimeSlotRequest struct {
	SlotID       string
	AllocationID string
	NodeUID      string
	NodeBootID   string
	TTL          time.Duration
}

type AcquireRuntimeSlotRequest struct {
	OperationID               string
	ClaimID                   string
	SandboxID                 string
	FilesystemID              string
	SourceGenerationID        string
	CompatibilityDigest       string
	ClusterID                 string
	RuntimeAssignmentRevision string
	NetworkPolicyDigest       string
	ClaimTTL                  time.Duration
	Resources                 protocol.RuntimeResourceRequest
}

type BindRuntimeSlotWriterGrantRequest struct {
	SlotID      string
	OperationID string
	ClaimID     string
	GrantID     string
}

type StartRuntimeSlotRequest struct {
	SlotID              string
	AllocationID        string
	NodeUID             string
	NodeBootID          string
	OperationID         string
	ClaimID             string
	LaunchAttempt       string
	RunscContainerID    string
	RootFSBindingDigest []byte
	ClaimNetworkDigest  []byte
	ResourceLeaseID     string
	ResourceLeaseDigest []byte
}

type MarkRuntimeSlotCommandReadyRequest struct {
	SlotID             string
	AllocationID       string
	NodeUID            string
	NodeBootID         string
	OperationID        string
	ClaimID            string
	ProcdInstanceID    string
	ProcdAddress       string
	CommandReadyDigest []byte
}

type BeginRuntimeSlotQuiesceRequest struct {
	SlotID      string
	OperationID string
	ClaimID     string
}

type FenceRuntimeSlotForReconcileRequest struct {
	SlotID           string
	ExpectedRevision int64
}

type MarkRuntimeSlotAllocationMissingRequest struct {
	SlotID            string
	AllocationID      string
	NodeUID           string
	NodeBootID        string
	ObservationDigest []byte
}

type FinalizeRuntimeSlotRequest struct {
	SlotID               string
	OperationID          string
	ClaimID              string
	Reason               string
	ProofDigest          []byte
	ResourceLeaseID      string
	ResourceLeaseDigest  []byte
	ResourceCgroupAbsent bool
}

// RegisterRuntimeSlot creates one immutable allocation incarnation. Slot IDs
// are never reused after a claim or terminal transition.
func (s *PGSandboxStore) RegisterRuntimeSlot(ctx context.Context, request *RegisterRuntimeSlotRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeRegisterRuntimeSlotRequest(request)
	if err != nil {
		return nil, err
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO manager.runtime_slots (
			slot_id, cluster_id, allocation_id, allocation_namespace,
			node_id, node_uid, node_boot_id, netns_identity, control_endpoint,
			compatibility_digest, state, heartbeat_expires_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
			NOW() + ($12 * INTERVAL '1 millisecond'))
		ON CONFLICT (slot_id) DO NOTHING
	`, normalized.SlotID, normalized.ClusterID, normalized.AllocationID, normalized.AllocationNamespace,
		normalized.NodeID, normalized.NodeUID, normalized.NodeBootID, normalized.NetNSIdentity,
		normalized.ControlEndpoint, normalized.CompatibilityDigest, RuntimeSlotStateRegistered,
		normalized.HeartbeatTTL.Milliseconds())
	if err != nil {
		return nil, mapRuntimeSlotConflict("register runtime slot", err)
	}
	stored, err := s.GetRuntimeSlot(ctx, normalized.SlotID)
	if err != nil {
		return nil, err
	}
	if !runtimeSlotRegistrationMatches(stored, normalized) {
		return nil, fmt.Errorf("%w: slot ID is already bound to another allocation incarnation", ErrRuntimeSlotConflict)
	}
	return stored, nil
}

// GetRuntimeSlot returns a slot without changing its heartbeat or claim lease.
func (s *PGSandboxStore) GetRuntimeSlot(ctx context.Context, slotID string) (*RuntimeSlot, error) {
	slotID = strings.TrimSpace(slotID)
	if slotID == "" {
		return nil, fmt.Errorf("slot_id is required")
	}
	slot, err := scanRuntimeSlot(s.pool.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id = $1`, slotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRuntimeSlotNotFound, slotID)
	}
	return slot, err
}

// GetRuntimeSlotBySandboxID returns the one non-terminal physical runtime
// incarnation currently bound to a logical sandbox.
func (s *PGSandboxStore) GetRuntimeSlotBySandboxID(ctx context.Context, sandboxID string) (*RuntimeSlot, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" {
		return nil, fmt.Errorf("sandbox_id is required")
	}
	slot, err := scanRuntimeSlot(s.pool.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE sandbox_id = $1 AND state <> $2
	`, sandboxID, RuntimeSlotStateTerminal))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: sandbox %s", ErrRuntimeSlotNotFound, sandboxID)
	}
	return slot, err
}

// ReportRuntimeSlotReady makes a slot selectable only after runtime, baseline
// network, and storage-capacity proofs are all durable.
func (s *PGSandboxStore) ReportRuntimeSlotReady(ctx context.Context, request *ReportRuntimeSlotReadyRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeReportRuntimeSlotReadyRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotCallerMatches(slot, normalized.AllocationID, normalized.NodeUID, normalized.NodeBootID) {
			return nil, fmt.Errorf("%w: readiness caller does not match slot incarnation", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateFastpathReady {
			if !bytes.Equal(slot.RuntimeReadyDigest, normalized.RuntimeReadyDigest) ||
				!bytes.Equal(slot.NetworkReadyDigest, normalized.NetworkReadyDigest) ||
				!bytes.Equal(slot.StorageReadyDigest, normalized.StorageReadyDigest) {
				return nil, fmt.Errorf("%w: ready proofs changed", ErrRuntimeSlotConflict)
			}
			_, err := tx.Exec(ctx, `
				UPDATE manager.runtime_slots
				SET heartbeat_expires_at = NOW() + ($2 * INTERVAL '1 millisecond'), updated_at = NOW()
				WHERE slot_id = $1
			`, slot.ID, normalized.HeartbeatTTL.Milliseconds())
			return nil, err
		}
		if slot.State != RuntimeSlotStateRegistered {
			return nil, fmt.Errorf("%w: cannot mark slot %s ready from %s", ErrRuntimeSlotInvalid, slot.ID, slot.State)
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1,
				runtime_ready_digest = $3, network_ready_digest = $4, storage_ready_digest = $5,
				heartbeat_expires_at = NOW() + ($6 * INTERVAL '1 millisecond'),
				fastpath_ready_at = NOW(), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateFastpathReady, normalized.RuntimeReadyDigest,
			normalized.NetworkReadyDigest, normalized.StorageReadyDigest, normalized.HeartbeatTTL.Milliseconds())
		return nil, err
	})
}

// HeartbeatRuntimeSlot extends liveness for the exact allocation and node-boot
// incarnation without changing readiness or claim state.
func (s *PGSandboxStore) HeartbeatRuntimeSlot(ctx context.Context, request *HeartbeatRuntimeSlotRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeHeartbeatRuntimeSlotRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotCallerMatches(slot, normalized.AllocationID, normalized.NodeUID, normalized.NodeBootID) {
			return nil, fmt.Errorf("%w: heartbeat caller does not match slot incarnation", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateQuiescing || slot.State == RuntimeSlotStateOrphaned ||
			slot.State == RuntimeSlotStateTerminal {
			return nil, fmt.Errorf("%w: slot in %s cannot heartbeat", ErrRuntimeSlotInvalid, slot.State)
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET heartbeat_expires_at = NOW() + ($2 * INTERVAL '1 millisecond'), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, normalized.TTL.Milliseconds())
		return nil, err
	})
}

// FenceRuntimeSlotForReconcile atomically rechecks regional expiry before a
// plugin-independent controller takes ownership of a claimed runtime. The
// expired heartbeat is retained so a reconciler crash remains immediately
// discoverable, and later node heartbeats cannot revive the fenced slot. A
// consumed writer is locked and must also be past its renewal grace so a
// queued renewal cannot revive it after this transaction commits.
func (s *PGSandboxStore) FenceRuntimeSlotForReconcile(
	ctx context.Context,
	request *FenceRuntimeSlotForReconcileRequest,
) (*RuntimeSlot, error) {
	normalized, err := normalizeFenceRuntimeSlotForReconcileRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if slot.State == RuntimeSlotStateQuiescing || slot.State == RuntimeSlotStateOrphaned {
			return nil, nil
		}
		if slot.ClaimID == "" ||
			(slot.State != RuntimeSlotStateClaiming && slot.State != RuntimeSlotStateStarting && slot.State != RuntimeSlotStateActive) {
			return nil, fmt.Errorf("%w: only a live claimed slot can be reconcile-fenced", ErrRuntimeSlotInvalid)
		}
		if slot.Revision != normalized.ExpectedRevision {
			return nil, fmt.Errorf("%w: runtime slot revision changed", ErrRuntimeSlotConflict)
		}
		due := !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt)
		if slot.State == RuntimeSlotStateClaiming {
			due = due || !slot.ClaimLeaseExpiresAt.After(slot.AuthorityObservedAt)
		}
		if !due {
			return nil, ErrRuntimeSlotNotDue
		}
		if slot.WriterGrantID != "" {
			var grantState string
			var leaseExpiresAt *time.Time
			var authorityObservedAt time.Time
			err := tx.QueryRow(ctx, `
				SELECT state, lease_expires_at, NOW()
				FROM manager.rootfs_writer_grants
				WHERE grant_id = $1 AND slot_id = $2
				FOR UPDATE
			`, slot.WriterGrantID, slot.ID).Scan(&grantState, &leaseExpiresAt, &authorityObservedAt)
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: runtime slot writer grant binding changed", ErrRuntimeSlotConflict)
			}
			if err != nil {
				return nil, err
			}
			switch grantState {
			case RootFSWriterGrantStateIssued, RootFSWriterGrantStateRetiring,
				RootFSWriterGrantStateRetired, RootFSWriterGrantStateCanceled:
			case RootFSWriterGrantStateConsumed:
				if leaseExpiresAt == nil || leaseExpiresAt.Add(RootFSWriterCrashAbandonGrace).After(authorityObservedAt) {
					return nil, ErrRuntimeSlotNotDue
				}
			default:
				return nil, fmt.Errorf("%w: runtime slot writer grant is in invalid state %s", ErrRuntimeSlotInvalid, grantState)
			}
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1,
				heartbeat_expires_at = LEAST(heartbeat_expires_at, NOW()),
				quiescing_at = NOW(), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateQuiescing)
		return nil, err
	})
}

// AcquireRuntimeSlot reserves the oldest live compatible slot with
// FOR UPDATE SKIP LOCKED. An operation ID retry returns its original slot.
func (s *PGSandboxStore) AcquireRuntimeSlot(ctx context.Context, request *AcquireRuntimeSlotRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeAcquireRuntimeSlotRequest(request)
	if err != nil {
		return nil, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// A unique index detects conflicting operation IDs, but it cannot make two
	// simultaneous identical retries choose the same SKIP LOCKED row. Serialize
	// only that operation key before consulting its durable binding.
	if err := lockRuntimeSlotClaimOperation(ctx, tx, normalized.OperationID); err != nil {
		return nil, err
	}
	allowNewSlot, err := lockRuntimeSlotClaimAdmission(ctx, tx, normalized)
	if err != nil {
		return nil, err
	}

	existing, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE claim_operation_id = $1
		FOR UPDATE OF runtime_slots
	`, normalized.OperationID))
	if err == nil {
		if !runtimeSlotClaimMatches(existing, normalized) {
			return nil, fmt.Errorf("%w: claim operation is already bound to different inputs", ErrRuntimeSlotConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, err
		}
		return existing, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if !allowNewSlot {
		return nil, fmt.Errorf("%w: ready sandbox claim has no runtime slot", ErrRuntimeSlotConflict)
	}
	var sourceMarker int
	if err := tx.QueryRow(ctx, `
		SELECT 1
		FROM manager.sandboxes AS sandbox
		JOIN manager.sandbox_rootfs_bindings AS binding
			ON binding.sandbox_id = sandbox.sandbox_id
		JOIN manager.rootfs_filesystems AS filesystem
			ON filesystem.filesystem_id = binding.filesystem_id
		JOIN manager.rootfs_generations AS generation
			ON generation.generation_id = filesystem.head_generation_id
			AND generation.filesystem_id = filesystem.filesystem_id
		WHERE sandbox.sandbox_id = $1
			AND sandbox.deleted_at IS NULL
			AND sandbox.desired_state IN ($4, $5)
			AND filesystem.filesystem_id = $2
			AND generation.generation_id = $3
		FOR SHARE OF sandbox, binding, filesystem, generation
	`, normalized.SandboxID, normalized.FilesystemID, normalized.SourceGenerationID,
		SandboxDesiredStateActive, SandboxDesiredStatePaused).Scan(&sourceMarker); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("%w: claim source is not the sandbox's current RootFS generation", ErrRuntimeSlotConflict)
		}
		return nil, err
	}

	slot, resourceLease, resourceLeaseDigest, err := selectRuntimeSlotResourceLease(ctx, tx, normalized)
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO manager.runtime_resource_leases (
			lease_id, slot_id, operation_id, claim_id,
			cluster_id, node_id, node_uid, node_boot_id,
			cpu_millicores, cpu_period_micros, cpu_quota_micros,
			cpu_shares, cpu_weight, cpuset_cpus, cpuset_mems,
			memory_bytes, pids_limit, cgroup_name, lease_digest, lease_state
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8,
			$9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
		)
	`, resourceLease.LeaseID, resourceLease.SlotID, resourceLease.OperationID, resourceLease.ClaimID,
		resourceLease.ClusterID, resourceLease.NodeID, resourceLease.NodeUID, resourceLease.NodeBootID,
		resourceLease.CPUMillicores, int64(resourceLease.CPUPeriodMicros), resourceLease.CPUQuotaMicros,
		int64(resourceLease.CPUShares), int64(resourceLease.CPUWeight), resourceLease.CPUSetCPUs,
		resourceLease.CPUSetMems, resourceLease.MemoryBytes, resourceLease.PIDsLimit,
		resourceLease.CgroupName, resourceLeaseDigest, RuntimeResourceLeaseActive)
	if err != nil {
		return nil, mapRuntimeSlotConflict("insert runtime resource lease", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE manager.runtime_slots
		SET state = $2, revision = revision + 1,
			claim_operation_id = $3, claim_id = $4, sandbox_id = $5,
			filesystem_id = $6, source_generation_id = $7,
			claim_cluster_filter = $8, claim_ttl_milliseconds = $9::bigint,
			claim_runtime_assignment_revision = $10, claim_network_policy_digest = $11,
			claim_lease_expires_at = NOW() + ($9::double precision * INTERVAL '1 millisecond'),
			claimed_at = NOW(), updated_at = NOW(), resource_lease_id = $12
		WHERE slot_id = $1 AND state = $13 AND resource_lease_id IS NULL
	`, slot.ID, RuntimeSlotStateClaiming, normalized.OperationID, normalized.ClaimID,
		normalized.SandboxID, normalized.FilesystemID, normalized.SourceGenerationID,
		normalized.ClusterID, normalized.ClaimTTL.Milliseconds(), normalized.RuntimeAssignmentRevision,
		normalized.NetworkPolicyDigest, resourceLease.LeaseID, RuntimeSlotStateFastpathReady)
	if err != nil {
		return nil, mapRuntimeSlotConflict("acquire runtime slot", err)
	}
	if tag.RowsAffected() != 1 {
		return nil, fmt.Errorf("%w: selected runtime slot changed before resource lease attachment", ErrRuntimeSlotConflict)
	}
	result, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id = $1`, slot.ID))
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, mapRuntimeSlotConflict("commit runtime slot claim", err)
	}
	return result, nil
}

func selectRuntimeSlotResourceLease(
	ctx context.Context,
	tx pgx.Tx,
	request *AcquireRuntimeSlotRequest,
) (*RuntimeSlot, protocol.RuntimeResourceLease, []byte, error) {
	excludedSlots := make([]string, 0, 8)
	for attempts := 0; attempts < maxRuntimeSlotCapacityCandidates; attempts++ {
		slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
			WHERE state = $1
				AND heartbeat_expires_at > NOW()
				AND compatibility_digest = $2
				AND ($3 = '' OR cluster_id = $3)
				AND slot_id <> ALL($6::text[])
				AND EXISTS (
					SELECT 1
					FROM manager.runtime_node_capacities AS capacity
					WHERE capacity.cluster_id = runtime_slots.cluster_id
						AND capacity.node_id = runtime_slots.node_id
						AND capacity.node_uid = runtime_slots.node_uid
						AND capacity.node_boot_id = runtime_slots.node_boot_id
						AND capacity.heartbeat_expires_at > NOW()
						AND capacity.cpu_millicores >= $4 + COALESCE((
							SELECT SUM(lease.cpu_millicores)
							FROM manager.runtime_resource_leases AS lease
							WHERE lease.cluster_id = capacity.cluster_id
								AND lease.node_id = capacity.node_id
								AND lease.node_uid = capacity.node_uid
								AND lease.node_boot_id = capacity.node_boot_id
								AND lease.lease_state = 'active'
						), 0)
						AND capacity.memory_bytes >= $5 + COALESCE((
							SELECT SUM(lease.memory_bytes)
							FROM manager.runtime_resource_leases AS lease
							WHERE lease.cluster_id = capacity.cluster_id
								AND lease.node_id = capacity.node_id
								AND lease.node_uid = capacity.node_uid
								AND lease.node_boot_id = capacity.node_boot_id
								AND lease.lease_state = 'active'
						), 0)
				)
			ORDER BY fastpath_ready_at, slot_id
			FOR UPDATE OF runtime_slots SKIP LOCKED
			LIMIT 1
		`, RuntimeSlotStateFastpathReady, request.CompatibilityDigest, request.ClusterID,
			request.Resources.CPUMillicores, request.Resources.MemoryBytes, excludedSlots))
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, protocol.RuntimeResourceLease{}, nil, ErrRuntimeSlotUnavailable
		}
		if err != nil {
			return nil, protocol.RuntimeResourceLease{}, nil, err
		}
		excludedSlots = append(excludedSlots, slot.ID)

		var capacityCPU, capacityMemory int64
		var cpusetCPUs, cpusetMems string
		err = tx.QueryRow(ctx, `
			SELECT cpu_millicores, memory_bytes, cpuset_cpus, cpuset_mems
			FROM manager.runtime_node_capacities
			WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3 AND node_boot_id = $4
				AND heartbeat_expires_at > NOW()
			FOR UPDATE
		`, slot.ClusterID, slot.NodeID, slot.NodeUID, slot.NodeBootID).Scan(
			&capacityCPU, &capacityMemory, &cpusetCPUs, &cpusetMems,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, protocol.RuntimeResourceLease{}, nil, err
		}
		var usedCPU, usedMemory int64
		if err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(cpu_millicores), 0), COALESCE(SUM(memory_bytes), 0)
			FROM manager.runtime_resource_leases
			WHERE cluster_id = $1 AND node_id = $2 AND node_uid = $3 AND node_boot_id = $4
				AND lease_state = 'active'
		`, slot.ClusterID, slot.NodeID, slot.NodeUID, slot.NodeBootID).Scan(&usedCPU, &usedMemory); err != nil {
			return nil, protocol.RuntimeResourceLease{}, nil, err
		}
		if capacityCPU-usedCPU < request.Resources.CPUMillicores ||
			capacityMemory-usedMemory < request.Resources.MemoryBytes {
			continue
		}
		lease, err := protocol.NewRuntimeResourceLease(
			request.OperationID, request.ClaimID, slot.ID, slot.ClusterID,
			slot.NodeID, slot.NodeUID, slot.NodeBootID, request.Resources, cpusetCPUs, cpusetMems,
		)
		if err != nil {
			return nil, protocol.RuntimeResourceLease{}, nil, fmt.Errorf("build runtime resource lease: %w", err)
		}
		digestValue, err := lease.Digest()
		if err != nil {
			return nil, protocol.RuntimeResourceLease{}, nil, err
		}
		digestBytes, err := hex.DecodeString(strings.TrimPrefix(digestValue, "sha256:"))
		if err != nil || len(digestBytes) != 32 {
			return nil, protocol.RuntimeResourceLease{}, nil, fmt.Errorf("decode runtime resource lease digest")
		}
		return slot, lease, digestBytes, nil
	}
	return nil, protocol.RuntimeResourceLease{}, nil, ErrRuntimeSlotUnavailable
}

func lockRuntimeSlotClaimOperation(ctx context.Context, tx pgx.Tx, operationID string) error {
	if _, err := tx.Exec(ctx, `
		SELECT pg_advisory_xact_lock(hashtextextended($1, 0))
	`, operationID); err != nil {
		return fmt.Errorf("lock runtime slot claim operation: %w", err)
	}
	return nil
}

// lockRuntimeSlotClaimAdmission uses the same sandbox-then-claim lock order as
// completion and cleanup. A ready Nomad claim may only recover its existing
// slot; a cleanup-fenced or expired claim cannot acquire a late allocation.
func lockRuntimeSlotClaimAdmission(
	ctx context.Context,
	tx pgx.Tx,
	request *AcquireRuntimeSlotRequest,
) (bool, error) {
	var desiredState, runtimeID, runtimeNamespace string
	var runtimeGeneration int64
	var live bool
	if err := tx.QueryRow(ctx, `
		SELECT desired_state, deleted_at IS NULL,
			runtime_generation, runtime_id, runtime_namespace
		FROM manager.sandboxes
		WHERE sandbox_id = $1
		FOR SHARE
	`, request.SandboxID).Scan(
		&desiredState, &live, &runtimeGeneration, &runtimeID, &runtimeNamespace,
	); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("%w: sandbox record is missing", ErrRuntimeSlotConflict)
		}
		return false, fmt.Errorf("lock runtime slot claim sandbox: %w", err)
	}
	if !live {
		return false, fmt.Errorf("%w: sandbox does not accept a runtime claim", ErrRuntimeSlotConflict)
	}
	var operationID, phase string
	var leaseLive bool
	if err := tx.QueryRow(ctx, `
		SELECT operation_id, phase, COALESCE(lease_expires_at > NOW(), FALSE)
		FROM manager.sandbox_runtime_claims
		WHERE sandbox_id = $1
		FOR SHARE
	`, request.SandboxID).Scan(&operationID, &phase, &leaseLive); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, fmt.Errorf("%w: Nomad sandbox claim is missing", ErrRuntimeSlotConflict)
		}
		return false, fmt.Errorf("lock Nomad sandbox claim admission: %w", err)
	}
	if operationID == request.OperationID {
		if desiredState != SandboxDesiredStateActive {
			return false, fmt.Errorf("%w: initial Nomad claim requires an active sandbox", ErrRuntimeSlotConflict)
		}
		switch phase {
		case SandboxRuntimeClaimPhaseClaiming:
			if !leaseLive {
				return false, fmt.Errorf("%w: Nomad sandbox claim lease expired", ErrRuntimeSlotConflict)
			}
			return true, nil
		case SandboxRuntimeClaimPhaseReady:
			return false, nil
		case SandboxRuntimeClaimPhaseCleanupPending, SandboxRuntimeClaimPhaseCleaned:
			return false, fmt.Errorf("%w: Nomad sandbox claim cleanup is pending", ErrRuntimeSlotConflict)
		default:
			return false, fmt.Errorf("%w: Nomad sandbox claim phase is %s", ErrRuntimeSlotConflict, phase)
		}
	}

	lifecycle, err := scanLifecycleTxn(tx.QueryRow(ctx, lifecycleTxnSelectSQL()+`
		WHERE txn_id = $1 AND sandbox_id = $2
		FOR SHARE
	`, request.OperationID, request.SandboxID))
	if lifecycle == nil || errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("%w: Nomad sandbox operation identity changed", ErrRuntimeSlotConflict)
	}
	if err != nil {
		return false, fmt.Errorf("lock Nomad resume slot admission: %w", err)
	}
	if lifecycle.Kind != SandboxLifecycleKindResume || lifecycle.Source != SandboxLifecycleSourceManual ||
		lifecycle.Cancelable || !lifecycle.CancelRequestedAt.IsZero() ||
		lifecycle.ExpectedGenerationID != request.SourceGenerationID ||
		lifecycle.ToGeneration != lifecycle.FromGeneration+1 {
		return false, fmt.Errorf("%w: Nomad resume lifecycle identity changed", ErrRuntimeSlotConflict)
	}
	switch lifecycle.Phase {
	case SandboxLifecyclePhasePreparing, SandboxLifecyclePhaseBarriered,
		SandboxLifecyclePhasePublishing, SandboxLifecyclePhaseCommitting:
		if desiredState != SandboxDesiredStatePaused || runtimeGeneration != lifecycle.FromGeneration ||
			runtimeID != "" || runtimeNamespace != "" {
			return false, fmt.Errorf("%w: paused Nomad resume admission changed", ErrRuntimeSlotConflict)
		}
		return true, nil
	case SandboxLifecyclePhaseCommitted:
		if desiredState != SandboxDesiredStateActive || runtimeGeneration != lifecycle.ToGeneration ||
			runtimeID != lifecycle.ToRuntimeID || runtimeNamespace != lifecycle.ToRuntimeNamespace ||
			runtimeID == "" || runtimeNamespace == "" {
			return false, fmt.Errorf("%w: committed Nomad resume binding changed", ErrRuntimeSlotConflict)
		}
		return false, nil
	default:
		return false, fmt.Errorf("%w: Nomad resume lifecycle is %s", ErrRuntimeSlotConflict, lifecycle.Phase)
	}
}

// BindRuntimeSlotWriterGrant joins the slot reservation to the independently
// fenced RootFS grant. Every identity is checked before the binding is stored.
func (s *PGSandboxStore) BindRuntimeSlotWriterGrant(ctx context.Context, request *BindRuntimeSlotWriterGrantRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeBindRuntimeSlotWriterGrantRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotClaimIdentityMatches(slot, normalized.OperationID, normalized.ClaimID) {
			return nil, fmt.Errorf("%w: writer grant caller does not match slot claim", ErrRuntimeSlotConflict)
		}
		if slot.WriterGrantID != "" {
			if slot.WriterGrantID == normalized.GrantID {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: slot is already bound to another writer grant", ErrRuntimeSlotConflict)
		}
		if slot.State != RuntimeSlotStateClaiming {
			return nil, fmt.Errorf("%w: writer grant can only bind a claiming slot", ErrRuntimeSlotInvalid)
		}
		var claimID, grantSlotID, sandboxID, filesystemID, nodeUID, nodeBootID, state string
		if err := tx.QueryRow(ctx, `
			SELECT claim_id, slot_id, sandbox_id, filesystem_id, node_uid, node_boot_id, state
			FROM manager.rootfs_writer_grants
			WHERE grant_id = $1
			FOR UPDATE
		`, normalized.GrantID).Scan(
			&claimID, &grantSlotID, &sandboxID, &filesystemID, &nodeUID, &nodeBootID, &state,
		); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, fmt.Errorf("%w: writer grant is absent", ErrRuntimeSlotConflict)
			}
			return nil, err
		}
		if claimID != slot.ClaimID || grantSlotID != slot.ID || sandboxID != slot.SandboxID ||
			filesystemID != slot.FilesystemID || nodeUID != slot.NodeUID || nodeBootID != slot.NodeBootID ||
			(state != RootFSWriterGrantStateIssued && state != RootFSWriterGrantStateConsumed) {
			return nil, fmt.Errorf("%w: writer grant identity does not match slot claim", ErrRuntimeSlotConflict)
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET writer_grant_id = $2, revision = revision + 1, updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, normalized.GrantID)
		return nil, err
	})
}

// StartRuntimeSlot records the exact post-bind, post-policy launch attempt. It
// requires a consumed writer grant, so an issued token alone cannot start D.
func (s *PGSandboxStore) StartRuntimeSlot(ctx context.Context, request *StartRuntimeSlotRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeStartRuntimeSlotRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotCallerMatches(slot, normalized.AllocationID, normalized.NodeUID, normalized.NodeBootID) {
			return nil, fmt.Errorf("%w: start caller does not match slot incarnation", ErrRuntimeSlotConflict)
		}
		if !runtimeSlotClaimIdentityMatches(slot, normalized.OperationID, normalized.ClaimID) {
			return nil, fmt.Errorf("%w: start caller does not match slot claim", ErrRuntimeSlotConflict)
		}
		if slot.ResourceLease.LeaseID != normalized.ResourceLeaseID ||
			!bytes.Equal(slot.ResourceLeaseDigest, normalized.ResourceLeaseDigest) ||
			slot.ResourceLeaseState != RuntimeResourceLeaseActive || !slot.ResourceLeaseReleasedAt.IsZero() {
			return nil, fmt.Errorf("%w: start resource lease does not match slot claim", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateStarting || slot.State == RuntimeSlotStateActive {
			if slot.LaunchAttempt == normalized.LaunchAttempt &&
				slot.RunscContainerID == normalized.RunscContainerID &&
				bytes.Equal(slot.RootFSBindingDigest, normalized.RootFSBindingDigest) &&
				bytes.Equal(slot.ClaimNetworkDigest, normalized.ClaimNetworkDigest) {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: launch attempt changed", ErrRuntimeSlotConflict)
		}
		if slot.State != RuntimeSlotStateClaiming || slot.WriterGrantID == "" {
			return nil, fmt.Errorf("%w: slot is not a grant-bound claim", ErrRuntimeSlotInvalid)
		}
		if !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
			return nil, fmt.Errorf("%w: slot heartbeat expired before start", ErrRuntimeSlotInvalid)
		}
		var grantState string
		if err := tx.QueryRow(ctx, `
			SELECT state FROM manager.rootfs_writer_grants
			WHERE grant_id = $1
			FOR UPDATE
		`, slot.WriterGrantID).Scan(&grantState); err != nil {
			return nil, err
		}
		if grantState != RootFSWriterGrantStateConsumed {
			return nil, fmt.Errorf("%w: writer grant must be consumed before start", ErrRuntimeSlotInvalid)
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1,
				launch_attempt = $3, runsc_container_id = $4,
				rootfs_binding_digest = $5, claim_network_digest = $6,
				starting_at = NOW(), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateStarting, normalized.LaunchAttempt, normalized.RunscContainerID,
			normalized.RootFSBindingDigest, normalized.ClaimNetworkDigest)
		return nil, err
	})
}

// MarkRuntimeSlotCommandReady is the slot-registry endpoint of the complete
// claim timer: runsc has started and the exact procd instance accepts commands.
func (s *PGSandboxStore) MarkRuntimeSlotCommandReady(ctx context.Context, request *MarkRuntimeSlotCommandReadyRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeMarkRuntimeSlotCommandReadyRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotCallerMatches(slot, normalized.AllocationID, normalized.NodeUID, normalized.NodeBootID) {
			return nil, fmt.Errorf("%w: command-ready caller does not match slot incarnation", ErrRuntimeSlotConflict)
		}
		if !runtimeSlotClaimIdentityMatches(slot, normalized.OperationID, normalized.ClaimID) {
			return nil, fmt.Errorf("%w: command-ready caller does not match slot claim", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateActive {
			if slot.ProcdInstanceID == normalized.ProcdInstanceID &&
				slot.ProcdAddress == normalized.ProcdAddress &&
				bytes.Equal(slot.CommandReadyDigest, normalized.CommandReadyDigest) {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: command-ready proof changed", ErrRuntimeSlotConflict)
		}
		if slot.State != RuntimeSlotStateStarting {
			return nil, fmt.Errorf("%w: slot is not starting", ErrRuntimeSlotInvalid)
		}
		if !slot.HeartbeatExpiresAt.After(slot.AuthorityObservedAt) {
			return nil, fmt.Errorf("%w: slot heartbeat expired before command readiness", ErrRuntimeSlotInvalid)
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1,
				procd_instance_id = $3, procd_address = $4, command_ready_digest = $5,
				command_ready_at = NOW(), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateActive, normalized.ProcdInstanceID, normalized.ProcdAddress,
			normalized.CommandReadyDigest)
		return nil, err
	})
}

// BeginRuntimeSlotQuiesce durably removes a claimed slot from the active path
// before runsc, mounts, or the writer grant are retired.
func (s *PGSandboxStore) BeginRuntimeSlotQuiesce(ctx context.Context, request *BeginRuntimeSlotQuiesceRequest) (*RuntimeSlot, error) {
	if request == nil {
		return nil, fmt.Errorf("begin runtime slot quiesce request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotClaimIDs(&normalized.SlotID, &normalized.OperationID, &normalized.ClaimID); err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotClaimIdentityMatches(slot, normalized.OperationID, normalized.ClaimID) {
			return nil, fmt.Errorf("%w: quiesce caller does not match slot claim", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateQuiescing {
			return nil, nil
		}
		if slot.State == RuntimeSlotStateOrphaned {
			return nil, nil
		}
		if slot.State != RuntimeSlotStateClaiming && slot.State != RuntimeSlotStateStarting && slot.State != RuntimeSlotStateActive {
			return nil, fmt.Errorf("%w: cannot quiesce slot from %s", ErrRuntimeSlotInvalid, slot.State)
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1, quiescing_at = NOW(), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateQuiescing)
		return nil, err
	})
}

// MarkRuntimeSlotAllocationMissing preserves a claimed slot as an orphan even
// after Nomad purges its allocation record. Unclaimed slots become terminal;
// claimed slots remain reconcilable until the RootFS grant is terminal.
func (s *PGSandboxStore) MarkRuntimeSlotAllocationMissing(
	ctx context.Context,
	request *MarkRuntimeSlotAllocationMissingRequest,
) (*RuntimeSlot, error) {
	normalized, err := normalizeMarkRuntimeSlotAllocationMissingRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotCallerMatches(slot, normalized.AllocationID, normalized.NodeUID, normalized.NodeBootID) {
			return nil, fmt.Errorf("%w: missing-allocation observation does not match slot", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateTerminal {
			if slot.TerminalReason == "allocation_missing" &&
				bytes.Equal(slot.TerminalProofDigest, normalized.ObservationDigest) {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: terminal observation changed", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateOrphaned {
			if bytes.Equal(slot.OrphanObservationDigest, normalized.ObservationDigest) {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: orphan observation changed", ErrRuntimeSlotConflict)
		}
		if slot.ClaimID == "" {
			_, err := tx.Exec(ctx, `
				UPDATE manager.runtime_slots
				SET state = $2, revision = revision + 1,
					terminal_reason = 'allocation_missing', terminal_proof_digest = $3,
					terminal_at = NOW(), updated_at = NOW()
				WHERE slot_id = $1
			`, slot.ID, RuntimeSlotStateTerminal, normalized.ObservationDigest)
			return nil, err
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1,
				orphan_observation_digest = $3, updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateOrphaned, normalized.ObservationDigest)
		return nil, err
	})
}

// FinalizeRuntimeSlot forgets no claim binding. A grant-bound slot can become
// terminal only after the regional writer grant is retired or canceled.
func (s *PGSandboxStore) FinalizeRuntimeSlot(ctx context.Context, request *FinalizeRuntimeSlotRequest) (*RuntimeSlot, error) {
	normalized, err := normalizeFinalizeRuntimeSlotRequest(request)
	if err != nil {
		return nil, err
	}
	return s.withLockedRuntimeSlot(ctx, normalized.SlotID, func(tx pgx.Tx, slot *RuntimeSlot) (*RuntimeSlot, error) {
		if !runtimeSlotClaimIdentityMatches(slot, normalized.OperationID, normalized.ClaimID) {
			return nil, fmt.Errorf("%w: terminal caller does not match slot claim", ErrRuntimeSlotConflict)
		}
		if slot.ResourceLease.IsZero() {
			if normalized.ResourceLeaseID != "" || len(normalized.ResourceLeaseDigest) != 0 || normalized.ResourceCgroupAbsent {
				return nil, fmt.Errorf("%w: legacy slot received resource cleanup facts", ErrRuntimeSlotConflict)
			}
		} else if normalized.ResourceLeaseID != slot.ResourceLease.LeaseID ||
			!bytes.Equal(normalized.ResourceLeaseDigest, slot.ResourceLeaseDigest) || !normalized.ResourceCgroupAbsent {
			return nil, fmt.Errorf("%w: resource cleanup proof does not match slot lease", ErrRuntimeSlotConflict)
		}
		if slot.State == RuntimeSlotStateTerminal {
			if slot.TerminalReason == normalized.Reason &&
				bytes.Equal(slot.TerminalProofDigest, normalized.ProofDigest) {
				return nil, nil
			}
			return nil, fmt.Errorf("%w: terminal proof changed", ErrRuntimeSlotConflict)
		}
		if slot.WriterGrantID == "" {
			if normalized.Reason != "prelaunch_abort" {
				return nil, fmt.Errorf("%w: grantless claim requires prelaunch_abort", ErrRuntimeSlotInvalid)
			}
		} else {
			var grantState string
			if err := tx.QueryRow(ctx, `
				SELECT state FROM manager.rootfs_writer_grants
				WHERE grant_id = $1
				FOR UPDATE
			`, slot.WriterGrantID).Scan(&grantState); err != nil {
				return nil, err
			}
			if grantState != RootFSWriterGrantStateRetired && grantState != RootFSWriterGrantStateCanceled {
				return nil, fmt.Errorf("%w: writer grant remains %s", ErrRuntimeSlotInvalid, grantState)
			}
		}
		if slot.ResourceLease.LeaseID != "" {
			if slot.ResourceLeaseState != RuntimeResourceLeaseActive || !slot.ResourceLeaseReleasedAt.IsZero() {
				return nil, fmt.Errorf("%w: resource lease is not active before terminal cleanup", ErrRuntimeSlotInvalid)
			}
			tag, err := tx.Exec(ctx, `
				UPDATE manager.runtime_resource_leases
				SET lease_state = $2, released_at = NOW(), updated_at = NOW()
				WHERE lease_id = $1 AND slot_id = $3 AND operation_id = $4 AND claim_id = $5
					AND lease_state = $6 AND released_at IS NULL
			`, slot.ResourceLease.LeaseID, RuntimeResourceLeaseReleased, slot.ID,
				normalized.OperationID, normalized.ClaimID, RuntimeResourceLeaseActive)
			if err != nil {
				return nil, err
			}
			if tag.RowsAffected() != 1 {
				return nil, fmt.Errorf("%w: resource lease changed before terminal cleanup", ErrRuntimeSlotConflict)
			}
		}
		_, err := tx.Exec(ctx, `
			UPDATE manager.runtime_slots
			SET state = $2, revision = revision + 1,
				terminal_reason = $3, terminal_proof_digest = $4,
				terminal_at = NOW(), updated_at = NOW()
			WHERE slot_id = $1
		`, slot.ID, RuntimeSlotStateTerminal, normalized.Reason, normalized.ProofDigest)
		return nil, err
	})
}

// ListRuntimeSlotsForReconcile prioritizes orphaned and explicitly quiescing
// physical incarnations before lease-expired candidates without deleting their
// durable claim and writer identities.
func (s *PGSandboxStore) ListRuntimeSlotsForReconcile(ctx context.Context, limit int) ([]RuntimeSlot, error) {
	if limit <= 0 || limit > MaxRuntimeSlotReconcileLimit {
		return nil, fmt.Errorf("runtime slot reconcile limit must be between 1 and %d", MaxRuntimeSlotReconcileLimit)
	}
	rows, err := s.pool.Query(ctx, runtimeSlotSelectSQL()+`
		WHERE state IN ($1, $2)
			OR (state <> $3 AND heartbeat_expires_at <= NOW())
			OR (state = $4 AND claim_lease_expires_at <= NOW())
		ORDER BY
			CASE WHEN state = $1 THEN 0 WHEN state = $2 THEN 1 ELSE 2 END,
			heartbeat_expires_at, slot_id
		LIMIT $5
	`, RuntimeSlotStateOrphaned, RuntimeSlotStateQuiescing, RuntimeSlotStateTerminal,
		RuntimeSlotStateClaiming, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RuntimeSlot, 0, limit)
	for rows.Next() {
		slot, err := scanRuntimeSlot(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *slot)
	}
	return result, rows.Err()
}

func (s *PGSandboxStore) withLockedRuntimeSlot(
	ctx context.Context,
	slotID string,
	fn func(pgx.Tx, *RuntimeSlot) (*RuntimeSlot, error),
) (*RuntimeSlot, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	slot, err := scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+`
		WHERE slot_id = $1
		FOR UPDATE OF runtime_slots
	`, slotID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %s", ErrRuntimeSlotNotFound, slotID)
	}
	if err != nil {
		return nil, err
	}
	replacement, err := fn(tx, slot)
	if err != nil {
		return nil, mapRuntimeSlotConflict("update runtime slot", err)
	}
	if replacement == nil {
		replacement, err = scanRuntimeSlot(tx.QueryRow(ctx, runtimeSlotSelectSQL()+` WHERE slot_id = $1`, slotID))
		if err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, mapRuntimeSlotConflict("commit runtime slot", err)
	}
	return replacement, nil
}

func normalizeRegisterRuntimeSlotRequest(request *RegisterRuntimeSlotRequest) (*RegisterRuntimeSlotRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("register runtime slot request is required")
	}
	normalized := *request
	fields := map[string]*string{
		"slot_id": &normalized.SlotID, "cluster_id": &normalized.ClusterID,
		"allocation_id": &normalized.AllocationID, "allocation_namespace": &normalized.AllocationNamespace,
		"node_id": &normalized.NodeID, "node_uid": &normalized.NodeUID,
		"node_boot_id": &normalized.NodeBootID, "netns_identity": &normalized.NetNSIdentity,
	}
	for name, value := range fields {
		*value = strings.TrimSpace(*value)
		if *value == "" || len(*value) > 512 {
			return nil, fmt.Errorf("%s is required and must not exceed 512 bytes", name)
		}
	}
	normalized.ControlEndpoint = strings.TrimSpace(normalized.ControlEndpoint)
	if err := validateRuntimeSlotControlEndpoint(normalized.ControlEndpoint); err != nil {
		return nil, err
	}
	compatibility, err := normalizeRuntimeSlotDigest("compatibility_digest", normalized.CompatibilityDigest)
	if err != nil {
		return nil, err
	}
	normalized.CompatibilityDigest = compatibility
	normalized.HeartbeatTTL, err = normalizeRuntimeSlotTTL(normalized.HeartbeatTTL, DefaultRuntimeSlotHeartbeatTTL, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("heartbeat_ttl: %w", err)
	}
	return &normalized, nil
}

func normalizeReportRuntimeSlotReadyRequest(request *ReportRuntimeSlotReadyRequest) (*ReportRuntimeSlotReadyRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("report runtime slot ready request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotCaller(&normalized.SlotID, &normalized.AllocationID, &normalized.NodeUID, &normalized.NodeBootID); err != nil {
		return nil, err
	}
	var err error
	normalized.RuntimeReadyDigest, err = normalizeRuntimeSlotProof("runtime_ready_digest", normalized.RuntimeReadyDigest)
	if err != nil {
		return nil, err
	}
	normalized.NetworkReadyDigest, err = normalizeRuntimeSlotProof("network_ready_digest", normalized.NetworkReadyDigest)
	if err != nil {
		return nil, err
	}
	normalized.StorageReadyDigest, err = normalizeRuntimeSlotProof("storage_ready_digest", normalized.StorageReadyDigest)
	if err != nil {
		return nil, err
	}
	normalized.HeartbeatTTL, err = normalizeRuntimeSlotTTL(normalized.HeartbeatTTL, DefaultRuntimeSlotHeartbeatTTL, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("heartbeat_ttl: %w", err)
	}
	return &normalized, nil
}

func normalizeHeartbeatRuntimeSlotRequest(request *HeartbeatRuntimeSlotRequest) (*HeartbeatRuntimeSlotRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("runtime slot heartbeat request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotCaller(&normalized.SlotID, &normalized.AllocationID, &normalized.NodeUID, &normalized.NodeBootID); err != nil {
		return nil, err
	}
	var err error
	normalized.TTL, err = normalizeRuntimeSlotTTL(normalized.TTL, DefaultRuntimeSlotHeartbeatTTL, 5*time.Minute)
	if err != nil {
		return nil, fmt.Errorf("heartbeat_ttl: %w", err)
	}
	return &normalized, nil
}

func normalizeAcquireRuntimeSlotRequest(request *AcquireRuntimeSlotRequest) (*AcquireRuntimeSlotRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("acquire runtime slot request is required")
	}
	normalized := *request
	fields := map[string]*string{
		"operation_id": &normalized.OperationID, "claim_id": &normalized.ClaimID,
		"sandbox_id": &normalized.SandboxID, "filesystem_id": &normalized.FilesystemID,
		"source_generation_id": &normalized.SourceGenerationID,
	}
	for name, value := range fields {
		*value = strings.TrimSpace(*value)
		if *value == "" || len(*value) > 512 {
			return nil, fmt.Errorf("%s is required and must not exceed 512 bytes", name)
		}
	}
	normalized.ClusterID = strings.TrimSpace(normalized.ClusterID)
	if len(normalized.ClusterID) > 512 {
		return nil, fmt.Errorf("cluster_id must not exceed 512 bytes")
	}
	compatibility, err := normalizeRuntimeSlotDigest("compatibility_digest", normalized.CompatibilityDigest)
	if err != nil {
		return nil, err
	}
	normalized.CompatibilityDigest = compatibility
	normalized.RuntimeAssignmentRevision, err = normalizeRuntimeSlotRevision(
		"runtime_assignment_revision", normalized.RuntimeAssignmentRevision,
	)
	if err != nil {
		return nil, err
	}
	normalized.NetworkPolicyDigest, err = normalizeRuntimeSlotDigest("network_policy_digest", normalized.NetworkPolicyDigest)
	if err != nil {
		return nil, err
	}
	normalized.ClaimTTL, err = normalizeRuntimeSlotTTL(normalized.ClaimTTL, DefaultRuntimeSlotClaimTTL, time.Minute)
	if err != nil {
		return nil, fmt.Errorf("claim_ttl: %w", err)
	}
	// PostgreSQL persists and applies claim TTLs at millisecond precision.
	normalized.ClaimTTL = time.Duration(normalized.ClaimTTL.Milliseconds()) * time.Millisecond
	if err := normalized.Resources.Validate(); err != nil {
		return nil, fmt.Errorf("resources: %w", err)
	}
	return &normalized, nil
}

func normalizeBindRuntimeSlotWriterGrantRequest(request *BindRuntimeSlotWriterGrantRequest) (*BindRuntimeSlotWriterGrantRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("bind runtime slot writer grant request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotClaimIDs(&normalized.SlotID, &normalized.OperationID, &normalized.ClaimID); err != nil {
		return nil, err
	}
	normalized.GrantID = strings.TrimSpace(normalized.GrantID)
	if normalized.GrantID == "" || len(normalized.GrantID) > 512 {
		return nil, fmt.Errorf("grant_id is required and must not exceed 512 bytes")
	}
	return &normalized, nil
}

func normalizeFenceRuntimeSlotForReconcileRequest(request *FenceRuntimeSlotForReconcileRequest) (*FenceRuntimeSlotForReconcileRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("runtime slot reconcile fence request is required")
	}
	normalized := *request
	normalized.SlotID = strings.TrimSpace(normalized.SlotID)
	if normalized.SlotID == "" || len(normalized.SlotID) > 512 {
		return nil, fmt.Errorf("slot_id is required and must not exceed 512 bytes")
	}
	if normalized.ExpectedRevision <= 0 {
		return nil, fmt.Errorf("expected_revision must be positive")
	}
	return &normalized, nil
}

func normalizeStartRuntimeSlotRequest(request *StartRuntimeSlotRequest) (*StartRuntimeSlotRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("start runtime slot request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotNodeClaim(
		&normalized.SlotID, &normalized.AllocationID, &normalized.NodeUID, &normalized.NodeBootID,
		&normalized.OperationID, &normalized.ClaimID,
	); err != nil {
		return nil, err
	}
	normalized.LaunchAttempt = strings.TrimSpace(normalized.LaunchAttempt)
	normalized.RunscContainerID = strings.TrimSpace(normalized.RunscContainerID)
	if normalized.LaunchAttempt == "" || normalized.RunscContainerID == "" ||
		len(normalized.LaunchAttempt) > 512 || len(normalized.RunscContainerID) > 512 {
		return nil, fmt.Errorf("launch_attempt and runsc_container_id are required and must not exceed 512 bytes")
	}
	var err error
	normalized.RootFSBindingDigest, err = normalizeRuntimeSlotProof("rootfs_binding_digest", normalized.RootFSBindingDigest)
	if err != nil {
		return nil, err
	}
	normalized.ClaimNetworkDigest, err = normalizeRuntimeSlotProof("claim_network_digest", normalized.ClaimNetworkDigest)
	if err != nil {
		return nil, err
	}
	normalized.ResourceLeaseID = strings.TrimSpace(normalized.ResourceLeaseID)
	if normalized.ResourceLeaseID == "" || len(normalized.ResourceLeaseID) > 512 {
		return nil, fmt.Errorf("resource_lease_id is required and must not exceed 512 bytes")
	}
	normalized.ResourceLeaseDigest, err = normalizeRuntimeSlotProof("resource_lease_digest", normalized.ResourceLeaseDigest)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

func normalizeMarkRuntimeSlotCommandReadyRequest(request *MarkRuntimeSlotCommandReadyRequest) (*MarkRuntimeSlotCommandReadyRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("mark runtime slot command ready request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotNodeClaim(
		&normalized.SlotID, &normalized.AllocationID, &normalized.NodeUID, &normalized.NodeBootID,
		&normalized.OperationID, &normalized.ClaimID,
	); err != nil {
		return nil, err
	}
	normalized.ProcdInstanceID = strings.TrimSpace(normalized.ProcdInstanceID)
	if normalized.ProcdInstanceID == "" || len(normalized.ProcdInstanceID) > 512 {
		return nil, fmt.Errorf("procd_instance_id is required and must not exceed 512 bytes")
	}
	if err := protocol.ValidateNomadProcdAddress(normalized.ProcdAddress); err != nil {
		return nil, fmt.Errorf("procd_address: %w", err)
	}
	var err error
	normalized.CommandReadyDigest, err = normalizeRuntimeSlotProof("command_ready_digest", normalized.CommandReadyDigest)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

func normalizeMarkRuntimeSlotAllocationMissingRequest(request *MarkRuntimeSlotAllocationMissingRequest) (*MarkRuntimeSlotAllocationMissingRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("mark runtime slot allocation missing request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotCaller(&normalized.SlotID, &normalized.AllocationID, &normalized.NodeUID, &normalized.NodeBootID); err != nil {
		return nil, err
	}
	var err error
	normalized.ObservationDigest, err = normalizeRuntimeSlotProof("observation_digest", normalized.ObservationDigest)
	if err != nil {
		return nil, err
	}
	return &normalized, nil
}

func normalizeFinalizeRuntimeSlotRequest(request *FinalizeRuntimeSlotRequest) (*FinalizeRuntimeSlotRequest, error) {
	if request == nil {
		return nil, fmt.Errorf("finalize runtime slot request is required")
	}
	normalized := *request
	if err := normalizeRuntimeSlotClaimIDs(&normalized.SlotID, &normalized.OperationID, &normalized.ClaimID); err != nil {
		return nil, err
	}
	normalized.Reason = strings.TrimSpace(normalized.Reason)
	if normalized.Reason == "" || len(normalized.Reason) > 512 {
		return nil, fmt.Errorf("terminal reason is required and must not exceed 512 bytes")
	}
	var err error
	normalized.ProofDigest, err = normalizeRuntimeSlotProof("proof_digest", normalized.ProofDigest)
	if err != nil {
		return nil, err
	}
	normalized.ResourceLeaseID = strings.TrimSpace(normalized.ResourceLeaseID)
	if normalized.ResourceLeaseID == "" {
		if len(normalized.ResourceLeaseDigest) != 0 || normalized.ResourceCgroupAbsent {
			return nil, fmt.Errorf("resource cleanup facts require resource_lease_id")
		}
	} else {
		if len(normalized.ResourceLeaseID) > 512 || !normalized.ResourceCgroupAbsent {
			return nil, fmt.Errorf("resource lease ID and cgroup absence are required")
		}
		normalized.ResourceLeaseDigest, err = normalizeRuntimeSlotProof(
			"resource_lease_digest", normalized.ResourceLeaseDigest,
		)
		if err != nil {
			return nil, err
		}
	}
	return &normalized, nil
}

func normalizeRuntimeSlotCaller(slotID, allocationID, nodeUID, nodeBootID *string) error {
	fields := map[string]*string{
		"slot_id": slotID, "allocation_id": allocationID,
		"node_uid": nodeUID, "node_boot_id": nodeBootID,
	}
	for name, value := range fields {
		*value = strings.TrimSpace(*value)
		if *value == "" || len(*value) > 512 {
			return fmt.Errorf("%s is required and must not exceed 512 bytes", name)
		}
	}
	return nil
}

func normalizeRuntimeSlotClaimIDs(slotID, operationID, claimID *string) error {
	fields := map[string]*string{
		"slot_id": slotID, "operation_id": operationID, "claim_id": claimID,
	}
	for name, value := range fields {
		*value = strings.TrimSpace(*value)
		if *value == "" || len(*value) > 512 {
			return fmt.Errorf("%s is required and must not exceed 512 bytes", name)
		}
	}
	return nil
}

func normalizeRuntimeSlotNodeClaim(slotID, allocationID, nodeUID, nodeBootID, operationID, claimID *string) error {
	if err := normalizeRuntimeSlotCaller(slotID, allocationID, nodeUID, nodeBootID); err != nil {
		return err
	}
	fields := map[string]*string{"operation_id": operationID, "claim_id": claimID}
	for name, value := range fields {
		*value = strings.TrimSpace(*value)
		if *value == "" || len(*value) > 512 {
			return fmt.Errorf("%s is required and must not exceed 512 bytes", name)
		}
	}
	return nil
}

func normalizeRuntimeSlotProof(name string, value []byte) ([]byte, error) {
	if len(value) != 32 {
		return nil, fmt.Errorf("%s must be a 32-byte SHA-256 digest", name)
	}
	return append([]byte(nil), value...), nil
}

func normalizeRuntimeSlotDigest(name, value string) (string, error) {
	value = strings.TrimSpace(value)
	parsed, err := digest.Parse(value)
	if err != nil || parsed.Algorithm() != digest.SHA256 || parsed.String() != value {
		return "", fmt.Errorf("%s must be a canonical sha256 digest", name)
	}
	return value, nil
}

func normalizeRuntimeSlotRevision(name, value string) (string, error) {
	trimmed := strings.TrimSpace(value)
	decoded, err := hex.DecodeString(trimmed)
	if err != nil || len(decoded) != 32 || hex.EncodeToString(decoded) != trimmed {
		return "", fmt.Errorf("%s must be a canonical 32-byte hexadecimal digest", name)
	}
	return trimmed, nil
}

func normalizeRuntimeSlotTTL(value, defaultValue, maximum time.Duration) (time.Duration, error) {
	if value == 0 {
		value = defaultValue
	}
	if value < time.Second || value > maximum {
		return 0, fmt.Errorf("must be between 1s and %s", maximum)
	}
	return value, nil
}

func validateRuntimeSlotControlEndpoint(value string) error {
	if value == "" || len(value) > 2_048 {
		return fmt.Errorf("control_endpoint is required and must not exceed 2048 bytes")
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
		return fmt.Errorf("control_endpoint is invalid")
	}
	switch parsed.Scheme {
	case "http", "https":
		if parsed.Host == "" {
			return fmt.Errorf("control_endpoint HTTP origin requires a host")
		}
	case "unix":
		if parsed.Host != "" || parsed.Path == "" || !strings.HasPrefix(parsed.Path, "/") {
			return fmt.Errorf("control_endpoint unix origin requires an absolute path")
		}
	default:
		return fmt.Errorf("control_endpoint scheme must be http, https, or unix")
	}
	return nil
}

func runtimeSlotRegistrationMatches(slot *RuntimeSlot, request *RegisterRuntimeSlotRequest) bool {
	return slot != nil &&
		slot.ID == request.SlotID &&
		slot.ClusterID == request.ClusterID &&
		slot.AllocationID == request.AllocationID &&
		slot.AllocationNamespace == request.AllocationNamespace &&
		slot.NodeID == request.NodeID &&
		slot.NodeUID == request.NodeUID &&
		slot.NodeBootID == request.NodeBootID &&
		slot.NetNSIdentity == request.NetNSIdentity &&
		slot.ControlEndpoint == request.ControlEndpoint &&
		slot.CompatibilityDigest == request.CompatibilityDigest
}

func runtimeSlotCallerMatches(slot *RuntimeSlot, allocationID, nodeUID, nodeBootID string) bool {
	return slot != nil && slot.AllocationID == allocationID &&
		slot.NodeUID == nodeUID && slot.NodeBootID == nodeBootID
}

func runtimeSlotClaimMatches(slot *RuntimeSlot, request *AcquireRuntimeSlotRequest) bool {
	return slot != nil &&
		slot.ClaimOperationID == request.OperationID &&
		slot.ClaimID == request.ClaimID &&
		slot.SandboxID == request.SandboxID &&
		slot.FilesystemID == request.FilesystemID &&
		slot.SourceGenerationID == request.SourceGenerationID &&
		slot.CompatibilityDigest == request.CompatibilityDigest &&
		slot.ClaimClusterFilter == request.ClusterID &&
		slot.ClaimRuntimeAssignmentRevision == request.RuntimeAssignmentRevision &&
		slot.ClaimNetworkPolicyDigest == request.NetworkPolicyDigest &&
		slot.ClaimTTL == request.ClaimTTL &&
		slot.ResourceLease.CPUMillicores == request.Resources.CPUMillicores &&
		slot.ResourceLease.MemoryBytes == request.Resources.MemoryBytes &&
		slot.ResourceLease.PIDsLimit == request.Resources.PIDsLimit &&
		slot.ResourceLease.OperationID == request.OperationID &&
		slot.ResourceLease.ClaimID == request.ClaimID
}

func runtimeSlotClaimIdentityMatches(slot *RuntimeSlot, operationID, claimID string) bool {
	return slot != nil && slot.ClaimOperationID == operationID && slot.ClaimID == claimID
}

func mapRuntimeSlotConflict(operation string, err error) error {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23503", "23505", "23514", "40001":
			return fmt.Errorf("%s: %w: %s", operation, ErrRuntimeSlotConflict, pgErr.ConstraintName)
		}
	}
	return err
}

func runtimeSlotSelectSQL() string {
	return `
		SELECT
			slot_id, cluster_id, allocation_id, allocation_namespace,
			node_id, node_uid, node_boot_id, netns_identity, control_endpoint,
			compatibility_digest, state, revision,
			runtime_ready_digest, network_ready_digest, storage_ready_digest,
			heartbeat_expires_at, fastpath_ready_at,
			claim_operation_id, claim_id, claim_cluster_filter, claim_ttl_milliseconds,
			claim_runtime_assignment_revision, claim_network_policy_digest,
			COALESCE(sandbox_id, ''), COALESCE(filesystem_id, ''),
			COALESCE(source_generation_id, ''), COALESCE(writer_grant_id, ''),
			claim_lease_expires_at, claimed_at,
			launch_attempt, runsc_container_id,
			rootfs_binding_digest, claim_network_digest, starting_at,
			procd_instance_id, procd_address, command_ready_digest, command_ready_at,
			quiescing_at, orphan_observation_digest,
			terminal_reason, terminal_proof_digest, terminal_at,
			created_at, updated_at,
			resource_lease.resource_lease_id, resource_lease.resource_operation_id,
			resource_lease.resource_claim_id, resource_lease.resource_slot_id,
			resource_lease.resource_cluster_id, resource_lease.resource_node_id,
			resource_lease.resource_node_uid, resource_lease.resource_node_boot_id,
			resource_lease.resource_cpu_millicores, resource_lease.resource_cpu_period_micros,
			resource_lease.resource_cpu_quota_micros, resource_lease.resource_cpu_shares,
			resource_lease.resource_cpu_weight, resource_lease.resource_cpuset_cpus,
			resource_lease.resource_cpuset_mems, resource_lease.resource_memory_bytes,
			resource_lease.resource_pids_limit, resource_lease.resource_cgroup_name,
			resource_lease.resource_lease_digest, resource_lease.resource_lease_state,
			resource_lease.resource_released_at,
			NOW()
		FROM manager.runtime_slots AS runtime_slots
		LEFT JOIN LATERAL (
			SELECT lease_id AS resource_lease_id,
				operation_id AS resource_operation_id,
				claim_id AS resource_claim_id, slot_id AS resource_slot_id,
				cluster_id AS resource_cluster_id, node_id AS resource_node_id,
				node_uid AS resource_node_uid, node_boot_id AS resource_node_boot_id,
				cpu_millicores AS resource_cpu_millicores,
				cpu_period_micros AS resource_cpu_period_micros,
				cpu_quota_micros AS resource_cpu_quota_micros,
				cpu_shares AS resource_cpu_shares, cpu_weight AS resource_cpu_weight,
				cpuset_cpus AS resource_cpuset_cpus, cpuset_mems AS resource_cpuset_mems,
				memory_bytes AS resource_memory_bytes, pids_limit AS resource_pids_limit,
				cgroup_name AS resource_cgroup_name, lease_digest AS resource_lease_digest,
				lease_state AS resource_lease_state, released_at AS resource_released_at
			FROM manager.runtime_resource_leases
			WHERE lease_id = runtime_slots.resource_lease_id
		) AS resource_lease ON TRUE`
}

type runtimeSlotScanner interface {
	Scan(...any) error
}

func scanRuntimeSlot(row runtimeSlotScanner) (*RuntimeSlot, error) {
	var slot RuntimeSlot
	var claimTTLMilliseconds int64
	var fastpathReadyAt, claimLeaseExpiresAt, claimedAt, startingAt pgtype.Timestamptz
	var commandReadyAt, quiescingAt, terminalAt pgtype.Timestamptz
	var resourceLeaseID, resourceOperationID, resourceClaimID, resourceSlotID pgtype.Text
	var resourceClusterID, resourceNodeID, resourceNodeUID, resourceNodeBootID pgtype.Text
	var resourceCPU, resourceCPUPeriod, resourceCPUQuota, resourceCPUShares, resourceCPUWeight pgtype.Int8
	var resourceCPUSetCPUs, resourceCPUSetMems pgtype.Text
	var resourceMemory, resourcePIDs pgtype.Int8
	var resourceCgroupName, resourceLeaseState pgtype.Text
	var resourceLeaseDigest []byte
	var resourceReleasedAt pgtype.Timestamptz
	if err := row.Scan(
		&slot.ID, &slot.ClusterID, &slot.AllocationID, &slot.AllocationNamespace,
		&slot.NodeID, &slot.NodeUID, &slot.NodeBootID, &slot.NetNSIdentity, &slot.ControlEndpoint,
		&slot.CompatibilityDigest, &slot.State, &slot.Revision,
		&slot.RuntimeReadyDigest, &slot.NetworkReadyDigest, &slot.StorageReadyDigest,
		&slot.HeartbeatExpiresAt, &fastpathReadyAt,
		&slot.ClaimOperationID, &slot.ClaimID, &slot.ClaimClusterFilter, &claimTTLMilliseconds,
		&slot.ClaimRuntimeAssignmentRevision, &slot.ClaimNetworkPolicyDigest,
		&slot.SandboxID, &slot.FilesystemID, &slot.SourceGenerationID, &slot.WriterGrantID,
		&claimLeaseExpiresAt, &claimedAt,
		&slot.LaunchAttempt, &slot.RunscContainerID,
		&slot.RootFSBindingDigest, &slot.ClaimNetworkDigest, &startingAt,
		&slot.ProcdInstanceID, &slot.ProcdAddress, &slot.CommandReadyDigest, &commandReadyAt,
		&quiescingAt, &slot.OrphanObservationDigest,
		&slot.TerminalReason, &slot.TerminalProofDigest, &terminalAt,
		&slot.CreatedAt, &slot.UpdatedAt,
		&resourceLeaseID, &resourceOperationID, &resourceClaimID, &resourceSlotID,
		&resourceClusterID, &resourceNodeID, &resourceNodeUID, &resourceNodeBootID,
		&resourceCPU, &resourceCPUPeriod, &resourceCPUQuota, &resourceCPUShares,
		&resourceCPUWeight, &resourceCPUSetCPUs, &resourceCPUSetMems,
		&resourceMemory, &resourcePIDs, &resourceCgroupName,
		&resourceLeaseDigest, &resourceLeaseState, &resourceReleasedAt,
		&slot.AuthorityObservedAt,
	); err != nil {
		return nil, err
	}
	slot.ClaimTTL = time.Duration(claimTTLMilliseconds) * time.Millisecond
	if fastpathReadyAt.Valid {
		slot.FastpathReadyAt = fastpathReadyAt.Time
	}
	if claimLeaseExpiresAt.Valid {
		slot.ClaimLeaseExpiresAt = claimLeaseExpiresAt.Time
	}
	if claimedAt.Valid {
		slot.ClaimedAt = claimedAt.Time
	}
	if startingAt.Valid {
		slot.StartingAt = startingAt.Time
	}
	if commandReadyAt.Valid {
		slot.CommandReadyAt = commandReadyAt.Time
	}
	if quiescingAt.Valid {
		slot.QuiescingAt = quiescingAt.Time
	}
	if terminalAt.Valid {
		slot.TerminalAt = terminalAt.Time
	}
	if resourceLeaseID.Valid {
		fieldsValid := resourceOperationID.Valid && resourceClaimID.Valid && resourceSlotID.Valid &&
			resourceClusterID.Valid && resourceNodeID.Valid && resourceNodeUID.Valid && resourceNodeBootID.Valid &&
			resourceCPU.Valid && resourceCPUPeriod.Valid && resourceCPUQuota.Valid && resourceCPUShares.Valid &&
			resourceCPUWeight.Valid && resourceCPUSetCPUs.Valid && resourceCPUSetMems.Valid &&
			resourceMemory.Valid && resourcePIDs.Valid && resourceCgroupName.Valid && resourceLeaseState.Valid &&
			len(resourceLeaseDigest) == 32
		if !fieldsValid || resourceCPUPeriod.Int64 < 0 || resourceCPUShares.Int64 < 0 || resourceCPUWeight.Int64 < 0 {
			return nil, fmt.Errorf("runtime slot has an incomplete resource lease")
		}
		slot.ResourceLease = protocol.RuntimeResourceLease{
			Version: protocol.RuntimeResourceLeaseVersion,
			LeaseID: resourceLeaseID.String, OperationID: resourceOperationID.String,
			ClaimID: resourceClaimID.String, SlotID: resourceSlotID.String,
			ClusterID: resourceClusterID.String, NodeID: resourceNodeID.String,
			NodeUID: resourceNodeUID.String, NodeBootID: resourceNodeBootID.String,
			CPUMillicores: resourceCPU.Int64, CPUPeriodMicros: uint64(resourceCPUPeriod.Int64),
			CPUQuotaMicros: resourceCPUQuota.Int64, CPUShares: uint64(resourceCPUShares.Int64),
			CPUWeight: uint64(resourceCPUWeight.Int64), CPUSetCPUs: resourceCPUSetCPUs.String,
			CPUSetMems: resourceCPUSetMems.String, MemoryBytes: resourceMemory.Int64,
			PIDsLimit: resourcePIDs.Int64, CgroupName: resourceCgroupName.String,
		}
		resourceDigest, err := slot.ResourceLease.Digest()
		if err != nil {
			return nil, fmt.Errorf("runtime slot resource lease: %w", err)
		}
		expectedDigest, err := hex.DecodeString(strings.TrimPrefix(resourceDigest, "sha256:"))
		if err != nil || !bytes.Equal(expectedDigest, resourceLeaseDigest) {
			return nil, fmt.Errorf("runtime slot resource lease digest changed")
		}
		slot.ResourceLeaseDigest = append([]byte(nil), resourceLeaseDigest...)
		slot.ResourceLeaseState = resourceLeaseState.String
		if resourceReleasedAt.Valid {
			slot.ResourceLeaseReleasedAt = resourceReleasedAt.Time
		}
	}
	slot.RuntimeReadyDigest = append([]byte(nil), slot.RuntimeReadyDigest...)
	slot.NetworkReadyDigest = append([]byte(nil), slot.NetworkReadyDigest...)
	slot.StorageReadyDigest = append([]byte(nil), slot.StorageReadyDigest...)
	slot.RootFSBindingDigest = append([]byte(nil), slot.RootFSBindingDigest...)
	slot.ClaimNetworkDigest = append([]byte(nil), slot.ClaimNetworkDigest...)
	slot.CommandReadyDigest = append([]byte(nil), slot.CommandReadyDigest...)
	slot.OrphanObservationDigest = append([]byte(nil), slot.OrphanObservationDigest...)
	slot.TerminalProofDigest = append([]byte(nil), slot.TerminalProofDigest...)
	return &slot, nil
}
