package quota

import (
	"errors"
	"fmt"
)

const BytesPerGB int64 = 1_000_000_000

type Dimension string

const (
	DimensionActiveSandboxes Dimension = "active_sandboxes"
	DimensionCPU             Dimension = "cpu_millicpu"
	DimensionMemory          Dimension = "memory_mib"
	DimensionVolumeStorageGB Dimension = "volume_storage_gb"
	DimensionSnapshotGB      Dimension = "snapshot_storage_gb"
	DimensionAPIRequests     Dimension = "api_requests"
	DimensionNetworkEgress   Dimension = "network_egress_bytes"
	DimensionNetworkIngress  Dimension = "network_ingress_bytes"
)

var dimensions = []Dimension{
	DimensionActiveSandboxes,
	DimensionCPU,
	DimensionMemory,
	DimensionVolumeStorageGB,
	DimensionSnapshotGB,
	DimensionAPIRequests,
	DimensionNetworkEgress,
	DimensionNetworkIngress,
}

// Dimensions returns every supported quota dimension in API display order.
func Dimensions() []Dimension {
	return append([]Dimension(nil), dimensions...)
}

func KnownDimension(d Dimension) bool {
	switch d {
	case DimensionActiveSandboxes,
		DimensionCPU,
		DimensionMemory,
		DimensionVolumeStorageGB,
		DimensionSnapshotGB,
		DimensionAPIRequests,
		DimensionNetworkEgress,
		DimensionNetworkIngress:
		return true
	default:
		return false
	}
}

type Kind string

const (
	KindCapacity Kind = "capacity"
	KindRate     Kind = "rate"
)

// KindForDimension returns the admission model used by a quota dimension.
func KindForDimension(d Dimension) Kind {
	switch d {
	case DimensionActiveSandboxes,
		DimensionCPU,
		DimensionMemory,
		DimensionVolumeStorageGB,
		DimensionSnapshotGB:
		return KindCapacity
	case DimensionAPIRequests,
		DimensionNetworkEgress,
		DimensionNetworkIngress:
		return KindRate
	default:
		return ""
	}
}

type Source string

const (
	SourceTeamOverride  Source = "team_override"
	SourceRegionDefault Source = "region_default"
	SourceUnlimited     Source = "unlimited"
)

// Policy is the resolved quota policy for a team and dimension.
type Policy struct {
	TeamID     string    `json:"team_id"`
	Dimension  Dimension `json:"dimension"`
	Kind       Kind      `json:"kind"`
	LimitValue int64     `json:"limit_value"`
	IntervalMS int64     `json:"interval_ms,omitempty"`
	BurstValue int64     `json:"burst_value,omitempty"`
	Source     Source    `json:"source"`
}

type Limit struct {
	TeamID     string    `json:"team_id"`
	Dimension  Dimension `json:"dimension"`
	LimitValue int64     `json:"limit_value"`
}

// DefaultLimit bootstraps a region-wide policy when the database has no policy
// for the dimension. Team-specific database policies override these defaults.
type DefaultLimit struct {
	Dimension  Dimension
	LimitValue int64
	IntervalMS int64
	BurstValue int64
}

// Status describes the current quota view for a team and dimension.
type Status struct {
	TeamID     string    `json:"team_id"`
	Dimension  Dimension `json:"dimension"`
	Kind       Kind      `json:"kind"`
	LimitValue *int64    `json:"limit_value"`
	IntervalMS *int64    `json:"interval_ms"`
	BurstValue *int64    `json:"burst_value"`
	Current    *int64    `json:"current"`
	Remaining  *int64    `json:"remaining"`
	Unlimited  bool      `json:"unlimited"`
	Unit       string    `json:"unit"`
	Source     Source    `json:"source"`
}

// NewStatus builds a user-facing quota status from a resolved policy and
// capacity usage. Rate quotas intentionally omit current and remaining because
// Redis token state is admission state, not cumulative usage.
func NewStatus(teamID string, dimension Dimension, policy *Policy, current int64) Status {
	kind := KindForDimension(dimension)
	status := Status{
		TeamID:    teamID,
		Dimension: dimension,
		Kind:      kind,
		Unlimited: policy == nil,
		Unit:      UnitForDimension(dimension),
		Source:    SourceUnlimited,
	}
	if kind == KindCapacity {
		status.Current = int64Pointer(current)
	}
	if policy == nil {
		return status
	}
	status.LimitValue = int64Pointer(policy.LimitValue)
	status.Source = policy.Source
	if kind == KindRate {
		status.IntervalMS = int64Pointer(policy.IntervalMS)
		status.BurstValue = int64Pointer(policy.BurstValue)
		return status
	}
	remaining := max(policy.LimitValue-current, 0)
	status.Remaining = int64Pointer(remaining)
	return status
}

// UnitForDimension returns the unit used by limit and usage values for a quota dimension.
func UnitForDimension(d Dimension) string {
	switch d {
	case DimensionActiveSandboxes:
		return "count"
	case DimensionCPU:
		return "millicpu"
	case DimensionMemory:
		return "MiB"
	case DimensionVolumeStorageGB, DimensionSnapshotGB:
		return "GB"
	case DimensionAPIRequests:
		return "requests"
	case DimensionNetworkEgress, DimensionNetworkIngress:
		return "bytes"
	default:
		return ""
	}
}

// ValidatePolicyValues checks the fields stored for a quota policy.
func ValidatePolicyValues(dimension Dimension, limitValue, intervalMS, burstValue int64) error {
	if !KnownDimension(dimension) {
		return fmt.Errorf("unknown quota dimension %q", dimension)
	}
	if limitValue < 0 {
		return fmt.Errorf("limit_value must be non-negative")
	}
	switch KindForDimension(dimension) {
	case KindCapacity:
		if intervalMS != 0 {
			return fmt.Errorf("interval_ms is only valid for rate quotas")
		}
		if burstValue != 0 {
			return fmt.Errorf("burst_value is only valid for rate quotas")
		}
	case KindRate:
		if intervalMS <= 0 {
			return fmt.Errorf("interval_ms must be positive for rate quotas")
		}
		if burstValue < 0 {
			return fmt.Errorf("burst_value must be non-negative")
		}
		if limitValue == 0 && burstValue != 0 {
			return fmt.Errorf("burst_value must be zero when limit_value is zero")
		}
		if limitValue > 0 && burstValue == 0 {
			return fmt.Errorf("burst_value must be positive when limit_value is positive")
		}
	}
	return nil
}

func int64Pointer(value int64) *int64 {
	return &value
}

type Decision struct {
	Allowed    bool      `json:"allowed"`
	TeamID     string    `json:"team_id"`
	Dimension  Dimension `json:"dimension"`
	LimitValue int64     `json:"limit_value"`
	Current    int64     `json:"current"`
	Requested  int64     `json:"requested"`
}

func (d Decision) Err() error {
	if d.Allowed {
		return nil
	}
	return &ExceededError{Decision: d}
}

type ExceededError struct {
	Decision Decision
}

func (e *ExceededError) Error() string {
	if e == nil {
		return "quota exceeded"
	}
	d := e.Decision
	return fmt.Sprintf("quota exceeded for %s: current %d + requested %d exceeds limit %d", d.Dimension, d.Current, d.Requested, d.LimitValue)
}

func IsExceeded(err error) bool {
	var exceeded *ExceededError
	return errors.As(err, &exceeded)
}

func BytesToGBRoundUp(value int64) int64 {
	if value <= 0 {
		return 0
	}
	return (value + BytesPerGB - 1) / BytesPerGB
}

func Check(teamID string, dimension Dimension, current, requested int64, limit *Limit) Decision {
	decision := Decision{
		Allowed:   true,
		TeamID:    teamID,
		Dimension: dimension,
		Current:   current,
		Requested: requested,
	}
	if limit == nil {
		return decision
	}
	decision.LimitValue = limit.LimitValue
	if requested < 0 {
		requested = 0
		decision.Requested = 0
	}
	decision.Allowed = current+requested <= limit.LimitValue
	return decision
}
