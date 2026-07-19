package teamquota

import (
	"context"
	"time"
)

// RecoveryStore exposes the durable quota saga inventory to reconcilers.
// It is intentionally separate from CapacityStore so ordinary admission
// consumers do not need recovery access.
type RecoveryStore interface {
	ListRecoveryTransfers(
		ctx context.Context,
		clusterID string,
		staleAfter time.Duration,
		limit int,
	) ([]RecoveryTransfer, error)
	ListRecoveryAllocations(ctx context.Context, filter RecoveryAllocationFilter) ([]RecoveryAllocation, error)
	GetRecoveryAllocation(ctx context.Context, owner Owner) (*RecoveryAllocation, error)
}

// RecoveryAllocationFilter selects one stable keyset page of allocations.
// ClusterID is required because allocation recovery is owned by a data-plane
// cluster. TeamID and OwnerKind are optional.
type RecoveryAllocationFilter struct {
	ClusterID         string
	TeamID            string
	OwnerKind         string
	AfterAllocationID string
	// OnlyDue excludes steady allocations and operations whose PostgreSQL
	// recovery deadline has not elapsed.
	OnlyDue bool
	Limit   int
}

// RecoveryAllocation contains enough durable state to resolve an interrupted
// reservation or release against the actual runtime and resource catalog.
type RecoveryAllocation struct {
	AllocationID       string     `json:"allocation_id"`
	Revision           int64      `json:"revision"`
	Owner              Owner      `json:"owner"`
	State              string     `json:"state"`
	Operation          *Operation `json:"operation,omitempty"`
	OperationBaseState string     `json:"operation_base_state,omitempty"`
	Runtime            RuntimeRef `json:"runtime,omitempty"`
	Committed          Values     `json:"committed"`
	Pending            Values     `json:"pending,omitempty"`
	ReconcileAfter     *time.Time `json:"reconcile_after,omitempty"`
	// ReconcileDue is calculated by PostgreSQL so recovery decisions use the
	// same clock that created ReconcileAfter.
	ReconcileDue bool      `json:"reconcile_due"`
	UpdatedAt    time.Time `json:"updated_at"`
}

// RecoveryTransfer is a prepared ownership transfer that still needs an
// external-state decision before it can be committed or aborted.
type RecoveryTransfer struct {
	Operation            Operation  `json:"operation"`
	Source               Owner      `json:"source"`
	Destination          Owner      `json:"destination"`
	Runtime              RuntimeRef `json:"runtime"`
	SourceDecrease       Values     `json:"source_decrease"`
	DestinationCommitted Values     `json:"destination_committed"`
	DestinationTarget    Values     `json:"destination_target"`
	Reserved             Values     `json:"reserved"`
	CreatedAt            time.Time  `json:"created_at"`
}
