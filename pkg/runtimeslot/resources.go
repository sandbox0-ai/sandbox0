package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

const (
	RuntimeResourceRequestVersion = 1
	RuntimeResourceLeaseVersion   = 1
	RuntimeCPUPeriodMicros        = 100_000
	RuntimeResourceCgroupRoot     = "/sys/fs/cgroup/sandbox0"
	// Linux's CFS bandwidth controller requires a quota of at least one
	// millisecond. With the fixed 100-millisecond period, 10m is the smallest
	// resource request that can be enforced without silently rounding up.
	MinRuntimeCPUMillicores = 10
	DefaultRuntimePIDsLimit = 4_096
	MaxRuntimeCPUMillicores = 1_000_000
	MaxRuntimeMemoryBytes   = 1 << 40
)

// RuntimeResourceRequest is the claim-time resource shape. It is deliberately
// separate from RuntimeCompatibility because a resource-neutral warm slot can
// serve any request that fits the selected node's remaining capacity.
type RuntimeResourceRequest struct {
	Version       int   `json:"version"`
	CPUMillicores int64 `json:"cpu_millicores"`
	MemoryBytes   int64 `json:"memory_bytes"`
	PIDsLimit     int64 `json:"pids_limit"`
}

// Validate rejects unbounded or non-versioned claim resources before a
// PostgreSQL capacity transaction starts.
func (r RuntimeResourceRequest) Validate() error {
	if r.Version != RuntimeResourceRequestVersion {
		return fmt.Errorf("runtime resource request version must be %d", RuntimeResourceRequestVersion)
	}
	if r.CPUMillicores < MinRuntimeCPUMillicores || r.CPUMillicores > MaxRuntimeCPUMillicores {
		return fmt.Errorf("runtime CPU must be between %d and %d millicores",
			MinRuntimeCPUMillicores, MaxRuntimeCPUMillicores)
	}
	if r.MemoryBytes < 1 || r.MemoryBytes > MaxRuntimeMemoryBytes {
		return fmt.Errorf("runtime memory must be between 1 and %d bytes", int64(MaxRuntimeMemoryBytes))
	}
	if r.PIDsLimit < 1 || r.PIDsLimit > 1_048_576 {
		return fmt.Errorf("runtime PIDs limit must be between 1 and 1048576")
	}
	return nil
}

// Digest returns the canonical retry binding for a resource request.
func (r RuntimeResourceRequest) Digest() (string, error) {
	if err := r.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return "", fmt.Errorf("encode runtime resource request: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

// RuntimeResourceLease is the immutable PostgreSQL result of atomically
// assigning node capacity and a resource-neutral warm slot to one claim.
// Ctld and the task driver must consume this exact object; neither may derive
// sandbox limits from the carrier's Nomad allocation.
type RuntimeResourceLease struct {
	Version         int    `json:"version"`
	LeaseID         string `json:"lease_id"`
	OperationID     string `json:"operation_id"`
	ClaimID         string `json:"claim_id"`
	SlotID          string `json:"slot_id"`
	ClusterID       string `json:"cluster_id"`
	NodeID          string `json:"node_id"`
	NodeUID         string `json:"node_uid"`
	NodeBootID      string `json:"node_boot_id"`
	CPUMillicores   int64  `json:"cpu_millicores"`
	CPUPeriodMicros uint64 `json:"cpu_period_micros"`
	CPUQuotaMicros  int64  `json:"cpu_quota_micros"`
	CPUShares       uint64 `json:"cpu_shares"`
	CPUWeight       uint64 `json:"cpu_weight"`
	CPUSetCPUs      string `json:"cpuset_cpus"`
	CPUSetMems      string `json:"cpuset_mems"`
	MemoryBytes     int64  `json:"memory_bytes"`
	PIDsLimit       int64  `json:"pids_limit"`
	CgroupName      string `json:"cgroup_name"`
}

// IsZero reports whether no resource lease is attached. A zero lease is
// accepted only on the cleanup path for rows created before resource leasing
// became mandatory.
func (l RuntimeResourceLease) IsZero() bool {
	return l == (RuntimeResourceLease{})
}

// NewRuntimeResourceLease builds the only accepted claim resource shape.
func NewRuntimeResourceLease(
	operationID, claimID, slotID, clusterID, nodeID, nodeUID, nodeBootID string,
	request RuntimeResourceRequest,
	cpusetCPUs, cpusetMems string,
) (RuntimeResourceLease, error) {
	derived, err := deriveRuntimeResourceLease(operationID, request, cpusetCPUs, cpusetMems)
	if err != nil {
		return RuntimeResourceLease{}, err
	}
	derived.Version = RuntimeResourceLeaseVersion
	derived.OperationID = operationID
	derived.ClaimID = claimID
	derived.SlotID = slotID
	derived.ClusterID = clusterID
	derived.NodeID = nodeID
	derived.NodeUID = nodeUID
	derived.NodeBootID = nodeBootID
	derived.CPUMillicores = request.CPUMillicores
	derived.CPUSetCPUs = cpusetCPUs
	derived.CPUSetMems = cpusetMems
	derived.MemoryBytes = request.MemoryBytes
	derived.PIDsLimit = request.PIDsLimit
	if err := derived.Validate(); err != nil {
		return RuntimeResourceLease{}, err
	}
	return derived, nil
}

// Validate proves that every derived cgroup value and physical identity is
// canonical. This prevents a stale or forged retry from changing resources.
func (l RuntimeResourceLease) Validate() error {
	if l.Version != RuntimeResourceLeaseVersion {
		return fmt.Errorf("runtime resource lease version must be %d", RuntimeResourceLeaseVersion)
	}
	for name, value := range map[string]string{
		"lease_id": l.LeaseID, "operation_id": l.OperationID, "claim_id": l.ClaimID,
		"slot_id": l.SlotID, "cluster_id": l.ClusterID, "node_id": l.NodeID,
		"node_uid": l.NodeUID, "node_boot_id": l.NodeBootID,
	} {
		if err := validateRequiredID(name, value); err != nil {
			return err
		}
	}
	request := RuntimeResourceRequest{
		Version: RuntimeResourceRequestVersion, CPUMillicores: l.CPUMillicores,
		MemoryBytes: l.MemoryBytes, PIDsLimit: l.PIDsLimit,
	}
	expected, err := deriveRuntimeResourceLease(l.OperationID, request, l.CPUSetCPUs, l.CPUSetMems)
	if err != nil {
		return err
	}
	if l.LeaseID != expected.LeaseID || l.CgroupName != expected.CgroupName ||
		l.CPUPeriodMicros != expected.CPUPeriodMicros || l.CPUQuotaMicros != expected.CPUQuotaMicros ||
		l.CPUShares != expected.CPUShares || l.CPUWeight != expected.CPUWeight {
		return fmt.Errorf("runtime resource lease derived values are not canonical")
	}
	return nil
}

func deriveRuntimeResourceLease(
	operationID string,
	request RuntimeResourceRequest,
	cpusetCPUs, cpusetMems string,
) (RuntimeResourceLease, error) {
	if err := request.Validate(); err != nil {
		return RuntimeResourceLease{}, err
	}
	if err := validateRequiredID("operation_id", operationID); err != nil {
		return RuntimeResourceLease{}, err
	}
	if _, err := ValidateCPUSet(cpusetCPUs); err != nil {
		return RuntimeResourceLease{}, fmt.Errorf("cpuset_cpus: %w", err)
	}
	if _, err := ValidateCPUSet(cpusetMems); err != nil {
		return RuntimeResourceLease{}, fmt.Errorf("cpuset_mems: %w", err)
	}
	hash := sha256.Sum256([]byte("sandbox0-runtime-resource-lease-v1\x00" + operationID))
	suffix := hex.EncodeToString(hash[:])
	shares := cpuSharesForMillicores(request.CPUMillicores)
	return RuntimeResourceLease{
		LeaseID: "resource-lease-" + suffix, CgroupName: "s0-" + suffix,
		CPUPeriodMicros: RuntimeCPUPeriodMicros,
		CPUQuotaMicros:  request.CPUMillicores * (RuntimeCPUPeriodMicros / 1_000),
		CPUShares:       shares, CPUWeight: cpuWeightForShares(shares),
	}, nil
}

// Digest returns the canonical lease proof stored by PostgreSQL and checked
// independently by manager, ctld, and the task driver.
func (l RuntimeResourceLease) Digest() (string, error) {
	if err := l.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(l)
	if err != nil {
		return "", fmt.Errorf("encode runtime resource lease: %w", err)
	}
	sum := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func cpuSharesForMillicores(millicores int64) uint64 {
	shares := millicores * 1_024 / 1_000
	if shares < 2 {
		return 2
	}
	if shares > 262_144 {
		return 262_144
	}
	return uint64(shares)
}

func cpuWeightForShares(shares uint64) uint64 {
	if shares <= 2 {
		return 1
	}
	if shares >= 262_144 {
		return 10_000
	}
	return 1 + ((shares-2)*9_999)/262_142
}

// ValidateCPUSet accepts only the Linux canonical ascending list/range form
// and returns its cardinality.
func ValidateCPUSet(value string) (int, error) {
	_, count, err := parseCPUSet(value)
	return count, err
}

// CPUSetContains reports whether every CPU or NUMA node in child is present
// in parent. Both inputs must use the canonical Linux list/range form.
func CPUSetContains(parent, child string) (bool, error) {
	parentRanges, _, err := parseCPUSet(parent)
	if err != nil {
		return false, fmt.Errorf("parent CPU set: %w", err)
	}
	childRanges, _, err := parseCPUSet(child)
	if err != nil {
		return false, fmt.Errorf("child CPU set: %w", err)
	}
	parentIndex := 0
	for _, childRange := range childRanges {
		for parentIndex < len(parentRanges) && parentRanges[parentIndex].end < childRange.start {
			parentIndex++
		}
		if parentIndex == len(parentRanges) || parentRanges[parentIndex].start > childRange.start ||
			parentRanges[parentIndex].end < childRange.end {
			return false, nil
		}
	}
	return true, nil
}

type cpuSetRange struct {
	start int
	end   int
}

func parseCPUSet(value string) ([]cpuSetRange, int, error) {
	if value == "" || value != strings.TrimSpace(value) || len(value) > 4_096 {
		return nil, 0, fmt.Errorf("CPU set must be non-empty, canonical, and at most 4096 bytes")
	}
	ranges := make([]cpuSetRange, 0, strings.Count(value, ",")+1)
	count := 0
	previous := -1
	for _, part := range strings.Split(value, ",") {
		bounds := strings.Split(part, "-")
		if len(bounds) < 1 || len(bounds) > 2 || bounds[0] == "" {
			return nil, 0, fmt.Errorf("CPU set is invalid")
		}
		start, err := parseCanonicalCPUSetNumber(bounds[0])
		if err != nil {
			return nil, 0, err
		}
		end := start
		if len(bounds) == 2 {
			if bounds[1] == "" {
				return nil, 0, fmt.Errorf("CPU set is invalid")
			}
			end, err = parseCanonicalCPUSetNumber(bounds[1])
			if err != nil || end <= start {
				return nil, 0, fmt.Errorf("CPU set range is invalid")
			}
		}
		if start <= previous {
			return nil, 0, fmt.Errorf("CPU set must be sorted and non-overlapping")
		}
		count += end - start + 1
		if count > 1_048_576 {
			return nil, 0, fmt.Errorf("CPU set is too large")
		}
		ranges = append(ranges, cpuSetRange{start: start, end: end})
		previous = end
	}
	return ranges, count, nil
}

func parseCanonicalCPUSetNumber(value string) (int, error) {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return 0, fmt.Errorf("CPU set number is not canonical")
	}
	parsed, err := strconv.ParseInt(value, 10, 31)
	if err != nil || parsed < 0 {
		return 0, fmt.Errorf("CPU set number is invalid")
	}
	return int(parsed), nil
}
