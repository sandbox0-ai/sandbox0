package metering

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	meteringpkg "github.com/sandbox0-ai/sandbox0/pkg/metering"
)

const sandboxLifecycleProducer = "manager.sandbox_lifecycle"

// repository is the transactional persistence boundary shared by lifecycle
// projectors and the PostgreSQL implementation.
type repository interface {
	InTx(ctx context.Context, fn func(tx pgx.Tx) error) error
	GetSandboxProjectionStateTx(ctx context.Context, tx pgx.Tx, sandboxID string) (*meteringpkg.SandboxProjectionState, error)
	AppendEventTx(ctx context.Context, tx pgx.Tx, event *meteringpkg.Event) error
	AppendWindowTx(ctx context.Context, tx pgx.Tx, window *meteringpkg.Window) error
	UpsertProducerWatermarkTx(ctx context.Context, tx pgx.Tx, producer string, regionID string, completeBefore time.Time) error
	UpsertSandboxProjectionStateTx(ctx context.Context, tx pgx.Tx, state *meteringpkg.SandboxProjectionState) error
}

func mustJSON(value any) json.RawMessage {
	if value == nil {
		return json.RawMessage(`{}`)
	}
	payload, err := json.Marshal(value)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return payload
}

func runtimeWindowData(state *meteringpkg.SandboxProjectionState, durationMS int64) json.RawMessage {
	return mustJSON(map[string]any{
		"product":               meteringpkg.ProductSandbox,
		"resource_millicpu":     state.ResourceMillicpu,
		"resource_memory_mib":   state.ResourceMemoryMiB,
		"duration_milliseconds": durationMS,
	})
}

func claimedEventID(sandboxID string, claimedAt time.Time) string {
	return fmt.Sprintf("sandbox/%s/claimed/%d", sandboxID, claimedAt.UTC().UnixNano())
}

func sandboxWindowID(sandboxID, windowType string, start, end time.Time) string {
	return fmt.Sprintf("sandbox/%s/windows/%s/%d/%d", sandboxID, windowType, start.UTC().UnixNano(), end.UTC().UnixNano())
}
