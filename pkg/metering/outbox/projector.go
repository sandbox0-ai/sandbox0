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
	defaultPollInterval = 2 * time.Second
	defaultClaimLease   = 30 * time.Second
	defaultCleanupAge   = 24 * time.Hour
)

type projectionStore interface {
	ClaimNextBatch(context.Context, string, time.Duration) (*Batch, error)
	MarkDelivered(context.Context, int64, string) error
	MarkFailed(context.Context, int64, string, string, time.Time) error
	DeleteDeliveredBefore(context.Context, time.Time, int) (int64, error)
	Stats(context.Context) (*Stats, error)
}

// ProjectorConfig controls PostgreSQL outbox delivery to ClickHouse.
type ProjectorConfig struct {
	WorkerID     string
	PollInterval time.Duration
	ClaimLease   time.Duration
	CleanupAge   time.Duration
	CleanupBatch int
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
	batch, err := p.store.ClaimNextBatch(ctx, p.config.WorkerID, p.config.ClaimLease)
	if err != nil || batch == nil {
		return false, err
	}
	sort.Slice(batch.Operations, func(i, j int) bool {
		return batch.Operations[i].Sequence < batch.Operations[j].Sequence
	})
	for _, operation := range batch.Operations {
		if err := p.apply(ctx, operation); err != nil {
			p.recordOperation(operation.Type, "error")
			p.recordBatch("error")
			retryAt := p.timestamp().Add(retryDelay(operation.Attempts))
			markErr := p.store.MarkFailed(ctx, batch.ID, p.config.WorkerID, err.Error(), retryAt)
			if markErr != nil {
				return true, errors.Join(err, markErr)
			}
			return true, fmt.Errorf("apply metering outbox batch %d operation %d (%s): %w", batch.ID, operation.Sequence, operation.Type, err)
		}
		p.recordOperation(operation.Type, "success")
	}
	if err := p.store.MarkDelivered(ctx, batch.ID, p.config.WorkerID); err != nil {
		p.recordBatch("error")
		return true, err
	}
	p.recordBatch("success")
	return true, nil
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
		return p.sink.AppendEvent(ctx, value)
	case OperationWindow:
		value := &metering.Window{}
		if err := json.Unmarshal(operation.Payload, value); err != nil {
			return fmt.Errorf("decode window operation: %w", err)
		}
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
