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
	DimensionFilesystemGB    Dimension = "filesystem_storage_gb"
	DimensionFSSnapshotGB    Dimension = "filesystem_snapshot_storage_gb"
	DimensionEgress          Dimension = "egress"
	DimensionIngress         Dimension = "ingress"
)

func KnownDimension(d Dimension) bool {
	switch d {
	case DimensionActiveSandboxes,
		DimensionCPU,
		DimensionMemory,
		DimensionVolumeStorageGB,
		DimensionSnapshotGB,
		DimensionFilesystemGB,
		DimensionFSSnapshotGB,
		DimensionEgress,
		DimensionIngress:
		return true
	default:
		return false
	}
}

type Limit struct {
	TeamID     string    `json:"team_id"`
	Dimension  Dimension `json:"dimension"`
	LimitValue int64     `json:"limit_value"`
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
