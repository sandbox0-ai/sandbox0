package outbox

import (
	"context"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/metering"
)

type OperationType string

const (
	OperationEvent              OperationType = "event"
	OperationWindow             OperationType = "window"
	OperationWatermark          OperationType = "watermark"
	OperationSandboxState       OperationType = "sandbox_state"
	OperationStorageState       OperationType = "storage_state"
	OperationStorageStateDelete OperationType = "storage_state_delete"
)

type WatermarkOperation struct {
	Producer       string    `json:"producer"`
	RegionID       string    `json:"region_id,omitempty"`
	CompleteBefore time.Time `json:"complete_before"`
}

type StorageStateDeleteOperation struct {
	State     *metering.StorageProjectionState `json:"state"`
	DeletedAt time.Time                        `json:"deleted_at"`
}

// Operation is one idempotent ClickHouse mutation captured in PostgreSQL.
type Operation struct {
	Sequence       int64
	BatchID        int64
	Type           OperationType
	DedupeKey      string
	Payload        []byte
	Attempts       int
	CreatedAt      time.Time
	ClaimExpiresAt *time.Time
}

// Batch is the oldest pending PostgreSQL transaction worth of operations.
type Batch struct {
	ID         int64
	Operations []*Operation
}

// Sink applies durable outbox operations to the long-term metering store.
type Sink interface {
	AppendEvent(context.Context, *metering.Event) error
	AppendWindow(context.Context, *metering.Window) error
	UpsertProducerWatermark(context.Context, string, string, time.Time) error
	UpsertSandboxProjectionState(context.Context, *metering.SandboxProjectionState) error
	UpsertStorageProjectionState(context.Context, *metering.StorageProjectionState) error
	DeleteStorageProjectionState(context.Context, *metering.StorageProjectionState, time.Time) error
}

// Stats summarizes outstanding delivery work.
type Stats struct {
	Pending       int64
	OldestPending *time.Time
}
