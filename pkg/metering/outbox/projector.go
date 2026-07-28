package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	obsmetrics "github.com/sandbox0-ai/sandbox0/pkg/observability/metrics"
	"go.uber.org/zap"
)

const (
	defaultPollInterval       = 2 * time.Second
	defaultClaimLease         = 30 * time.Second
	defaultCleanupAge         = 24 * time.Hour
	defaultDeliveryOperations = 500
	defaultDeliveryBatches    = 500
)

type projectionStore interface {
	ClaimNextBatch(context.Context, string, time.Duration) (*Batch, error)
	MarkDelivered(context.Context, int64, string) error
	MarkFailed(context.Context, int64, string, string, time.Time) error
	DeleteDeliveredBefore(context.Context, time.Time, int) (int64, error)
	Stats(context.Context) (*Stats, error)
}

type coalescingProjectionStore interface {
	ClaimNextBatches(context.Context, string, time.Duration, int, int) ([]*Batch, error)
	MarkDeliveredBatches(context.Context, []int64, string) error
	MarkFailedBatches(context.Context, []int64, string, string, time.Time) error
}

// ProjectorConfig controls PostgreSQL outbox delivery to ClickHouse.
type ProjectorConfig struct {
	WorkerID           string
	PollInterval       time.Duration
	ClaimLease         time.Duration
	MaxDeliveryOps     int
	MaxDeliveryBatches int
	CleanupAge         time.Duration
	CleanupBatch       int
}

// Projector retries exact, idempotent operations until ClickHouse accepts the
// complete PostgreSQL transaction batch.
type Projector struct {
	store       projectionStore
	sink        Sink
	config      ProjectorConfig
	logger      *zap.Logger
	metrics     *obsmetrics.ManagerMetrics
	now         func() time.Time
	lastStatsAt time.Time
}

func (p *Projector) SetMetrics(metrics *obsmetrics.ManagerMetrics) {
	if p != nil {
		p.metrics = metrics
	}
}

func NewProjector(store projectionStore, sink Sink, config ProjectorConfig, logger *zap.Logger) *Projector {
	if config.WorkerID == "" {
		config.WorkerID = "metering-projector/" + uuid.NewString()
	}
	if config.PollInterval <= 0 {
		config.PollInterval = defaultPollInterval
	}
	if config.ClaimLease <= 0 {
		config.ClaimLease = defaultClaimLease
	}
	if config.MaxDeliveryOps <= 0 {
		config.MaxDeliveryOps = defaultDeliveryOperations
	}
	if config.MaxDeliveryBatches <= 0 {
		config.MaxDeliveryBatches = defaultDeliveryBatches
	}
	if config.CleanupAge <= 0 {
		config.CleanupAge = defaultCleanupAge
	}
	if config.CleanupBatch <= 0 {
		config.CleanupBatch = 1000
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Projector{
		store:  store,
		sink:   sink,
		config: config,
		logger: logger,
		now: func() time.Time {
			return time.Now().UTC()
		},
	}
}

// Run drains ready work immediately and polls while the queue is empty or a
// failed head batch is waiting for its retry time.
func (p *Projector) Run(ctx context.Context) error {
	if p == nil || p.store == nil {
		return fmt.Errorf("metering outbox store is not configured")
	}
	if p.sink == nil {
		return fmt.Errorf("metering projection sink is not configured")
	}
	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	cleanupTicker := time.NewTicker(time.Hour)
	defer cleanupTicker.Stop()

	for {
		processed, err := p.ProjectOnce(ctx)
		p.observeStats(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Warn("Failed to project metering outbox batch", zap.Error(err))
		}
		if processed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-cleanupTicker.C:
			p.cleanup(ctx)
		case <-ticker.C:
		}
	}
}

// ProjectOnce applies the oldest pending transaction batch. A partially
// applied batch is released for an exact-payload retry.
func (p *Projector) ProjectOnce(ctx context.Context) (bool, error) {
	if p == nil || p.store == nil {
		return false, fmt.Errorf("metering outbox store is not configured")
	}
	if p.sink == nil {
		return false, fmt.Errorf("metering projection sink is not configured")
	}
	batches, err := p.claimBatches(ctx)
	if err != nil || len(batches) == 0 {
		return false, err
	}
	operations := orderedOperations(batches)
	applyErr := p.applyOperations(ctx, operations)
	if applyErr != nil {
		for _, operation := range operations {
			p.recordOperation(operation.Type, "error")
		}
		for range batches {
			p.recordBatch("error")
		}
		retryAt := p.timestamp().Add(retryDelay(maxAttempts(operations)))
		markErr := p.markFailed(ctx, batches, applyErr.Error(), retryAt)
		if markErr != nil {
			return true, errors.Join(applyErr, markErr)
		}
		return true, fmt.Errorf("apply metering outbox batches %v: %w", batchIDs(batches), applyErr)
	}
	for _, operation := range operations {
		p.recordOperation(operation.Type, "success")
	}
	if err := p.markDelivered(ctx, batches); err != nil {
		for range batches {
			p.recordBatch("error")
		}
		return true, err
	}
	for range batches {
		p.recordBatch("success")
	}
	return true, nil
}

func (p *Projector) claimBatches(ctx context.Context) ([]*Batch, error) {
	batchSink, supportsBatchSink := p.sink.(BatchSink)
	store, supportsCoalescing := p.store.(coalescingProjectionStore)
	if supportsBatchSink && batchSink != nil && supportsCoalescing {
		return store.ClaimNextBatches(
			ctx,
			p.config.WorkerID,
			p.config.ClaimLease,
			p.config.MaxDeliveryBatches,
			p.config.MaxDeliveryOps,
		)
	}
	batch, err := p.store.ClaimNextBatch(ctx, p.config.WorkerID, p.config.ClaimLease)
	if err != nil || batch == nil {
		return nil, err
	}
	return []*Batch{batch}, nil
}

func (p *Projector) applyOperations(ctx context.Context, operations []*Operation) error {
	if sink, ok := p.sink.(BatchSink); ok && sink != nil {
		batch, err := decodeProjectionBatch(operations)
		if err != nil {
			return err
		}
		return sink.ApplyProjectionBatch(ctx, batch)
	}
	for _, operation := range operations {
		if err := p.apply(ctx, operation); err != nil {
			return fmt.Errorf("operation %d (%s): %w", operation.Sequence, operation.Type, err)
		}
	}
	return nil
}

func (p *Projector) markDelivered(ctx context.Context, batches []*Batch) error {
	if store, ok := p.store.(coalescingProjectionStore); ok && len(batches) > 1 {
		return store.MarkDeliveredBatches(ctx, batchIDs(batches), p.config.WorkerID)
	}
	return p.store.MarkDelivered(ctx, batches[0].ID, p.config.WorkerID)
}

func (p *Projector) markFailed(ctx context.Context, batches []*Batch, message string, retryAt time.Time) error {
	if store, ok := p.store.(coalescingProjectionStore); ok && len(batches) > 1 {
		return store.MarkFailedBatches(ctx, batchIDs(batches), p.config.WorkerID, message, retryAt)
	}
	return p.store.MarkFailed(ctx, batches[0].ID, p.config.WorkerID, message, retryAt)
}

func orderedOperations(batches []*Batch) []*Operation {
	operations := make([]*Operation, 0)
	for _, batch := range batches {
		if batch == nil {
			continue
		}
		operations = append(operations, batch.Operations...)
	}
	sort.Slice(operations, func(i, j int) bool {
		return operations[i].Sequence < operations[j].Sequence
	})
	return operations
}

func batchIDs(batches []*Batch) []int64 {
	ids := make([]int64, 0, len(batches))
	for _, batch := range batches {
		if batch != nil {
			ids = append(ids, batch.ID)
		}
	}
	return ids
}

func maxAttempts(operations []*Operation) int {
	attempts := 1
	for _, operation := range operations {
		if operation != nil && operation.Attempts > attempts {
			attempts = operation.Attempts
		}
	}
	return attempts
}

func decodeProjectionBatch(operations []*Operation) (*metering.ProjectionBatch, error) {
	batch := &metering.ProjectionBatch{}
	for _, operation := range operations {
		if operation == nil {
			return nil, fmt.Errorf("metering outbox operation is nil")
		}
		switch operation.Type {
		case OperationEvent:
			value := &metering.Event{}
			if err := json.Unmarshal(operation.Payload, value); err != nil {
				return nil, fmt.Errorf("decode event operation %d: %w", operation.Sequence, err)
			}
			value.Sequence = operation.Sequence
			batch.Events = append(batch.Events, value)
		case OperationWindow:
			value := &metering.Window{}
			if err := json.Unmarshal(operation.Payload, value); err != nil {
				return nil, fmt.Errorf("decode window operation %d: %w", operation.Sequence, err)
			}
			value.Sequence = operation.Sequence
			batch.Windows = append(batch.Windows, value)
		case OperationWatermark:
			value := &WatermarkOperation{}
			if err := json.Unmarshal(operation.Payload, value); err != nil {
				return nil, fmt.Errorf("decode watermark operation %d: %w", operation.Sequence, err)
			}
			batch.Watermarks = append(batch.Watermarks, &metering.ProducerWatermark{
				Producer:       value.Producer,
				RegionID:       value.RegionID,
				CompleteBefore: value.CompleteBefore,
			})
		case OperationSandboxState:
			value := &metering.SandboxProjectionState{}
			if err := json.Unmarshal(operation.Payload, value); err != nil {
				return nil, fmt.Errorf("decode sandbox state operation %d: %w", operation.Sequence, err)
			}
			batch.SandboxStates = append(batch.SandboxStates, value)
		case OperationStorageState:
			value := &metering.StorageProjectionState{}
			if err := json.Unmarshal(operation.Payload, value); err != nil {
				return nil, fmt.Errorf("decode storage state operation %d: %w", operation.Sequence, err)
			}
			batch.StorageMutations = append(batch.StorageMutations, &metering.StorageProjectionMutation{
				State: value,
			})
		case OperationStorageStateDelete:
			value := &metering.StorageProjectionStateTombstone{}
			if err := json.Unmarshal(operation.Payload, value); err != nil {
				return nil, fmt.Errorf("decode storage state delete operation %d: %w", operation.Sequence, err)
			}
			if value.State == nil {
				return nil, fmt.Errorf("storage state delete operation %d is missing state", operation.Sequence)
			}
			batch.StorageMutations = append(batch.StorageMutations, &metering.StorageProjectionMutation{
				State:     value.State,
				Deleted:   true,
				DeletedAt: value.DeletedAt,
			})
		default:
			return nil, fmt.Errorf("unsupported metering outbox operation type %q", operation.Type)
		}
	}
	return batch, nil
}

func (p *Projector) apply(ctx context.Context, operation *Operation) error {
	if operation == nil {
		return fmt.Errorf("metering outbox operation is nil")
	}
	switch operation.Type {
	case OperationEvent:
		value := &metering.Event{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode event operation: %w", err)
		}
		value.Sequence = operation.Sequence
		return p.sink.AppendEvent(ctx, value)
	case OperationWindow:
		value := &metering.Window{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode window operation: %w", err)
		}
		value.Sequence = operation.Sequence
		return p.sink.AppendWindow(ctx, value)
	case OperationWatermark:
		value := &WatermarkOperation{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode watermark operation: %w", err)
		}
		return p.sink.UpsertProducerWatermark(ctx, value.Producer, value.RegionID, value.CompleteBefore)
	case OperationSandboxState:
		value := &metering.SandboxProjectionState{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode sandbox state operation: %w", err)
		}
		return p.sink.UpsertSandboxProjectionState(ctx, value)
	case OperationStorageState:
		value := &metering.StorageProjectionState{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode storage state operation: %w", err)
		}
		return p.sink.UpsertStorageProjectionState(ctx, value)
	case OperationStorageStateDelete:
		value := &StorageStateDeleteOperation{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode storage state delete operation: %w", err)
		}
		if value.State == nil {
			return fmt.Errorf("storage state delete operation is missing state")
		}
		return p.sink.DeleteStorageProjectionState(ctx, value.State, value.DeletedAt)
	default:
		return fmt.Errorf("unsupported metering outbox operation type %q", operation.Type)
	}
}

func (p *Projector) cleanup(ctx context.Context) {
	cutoff := p.timestamp().Add(-p.config.CleanupAge)
	var total int64
	for {
		deleted, err := p.store.DeleteDeliveredBefore(ctx, cutoff, p.config.CleanupBatch)
		if err != nil {
			p.logger.Warn("Failed to clean delivered metering outbox operations", zap.Error(err))
			return
		}
		total += deleted
		if deleted < int64(p.config.CleanupBatch) {
			break
		}
	}
	if total > 0 {
		p.logger.Debug("Cleaned delivered metering outbox operations", zap.Int64("operations", total))
	}
}

func (p *Projector) observeStats(ctx context.Context) {
	if p == nil || p.metrics == nil {
		return
	}
	now := p.timestamp()
	if !p.lastStatsAt.IsZero() && now.Sub(p.lastStatsAt) < 10*time.Second {
		return
	}
	p.lastStatsAt = now
	stats, err := p.store.Stats(ctx)
	if err != nil {
		p.logger.Warn("Failed to observe metering outbox backlog", zap.Error(err))
		return
	}
	p.metrics.MeteringOutboxPendingOperations.Set(float64(stats.Pending))
	age := 0.0
	if stats.OldestPending != nil {
		age = max(0, now.Sub(stats.OldestPending.UTC()).Seconds())
	}
	p.metrics.MeteringOutboxOldestPendingAge.Set(age)
}

func (p *Projector) recordBatch(result string) {
	if p != nil && p.metrics != nil {
		p.metrics.MeteringOutboxBatchesTotal.WithLabelValues(result).Inc()
	}
}

func (p *Projector) recordOperation(operationType OperationType, result string) {
	if p != nil && p.metrics != nil {
		p.metrics.MeteringOutboxOperationsTotal.WithLabelValues(string(operationType), result).Inc()
	}
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	power := math.Min(float64(attempt-1), 6)
	delay := time.Second * time.Duration(math.Pow(2, power))
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}

func (p *Projector) timestamp() time.Time {
	if p == nil || p.now == nil {
		return time.Now().UTC()
	}
	return p.now().UTC()
}
