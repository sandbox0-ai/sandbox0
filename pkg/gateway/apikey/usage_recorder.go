package apikey

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	DefaultUsageRecorderFlushInterval = time.Second
	DefaultUsageRecorderFlushTimeout  = 2 * time.Second
	DefaultUsageRecorderCloseTimeout  = 4 * time.Second
	DefaultUsageRecorderQueueSize     = 16_384
	DefaultUsageRecorderMaxPending    = 10_000
)

var (
	apiKeyUsageRecorderEvents = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sandbox0_gateway_api_key_usage_recorder_events_total",
			Help: "API key usage recorder events by outcome.",
		},
		[]string{"outcome"},
	)
	apiKeyUsageRecorderBatches = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "sandbox0_gateway_api_key_usage_recorder_batches_total",
			Help: "API key usage recorder batches by outcome.",
		},
		[]string{"outcome"},
	)
)

type UsageRecorderConfig struct {
	FlushInterval time.Duration
	FlushTimeout  time.Duration
	CloseTimeout  time.Duration
	QueueSize     int
	MaxPending    int
}

func DefaultUsageRecorderConfig() UsageRecorderConfig {
	return UsageRecorderConfig{
		FlushInterval: DefaultUsageRecorderFlushInterval,
		FlushTimeout:  DefaultUsageRecorderFlushTimeout,
		CloseTimeout:  DefaultUsageRecorderCloseTimeout,
		QueueSize:     DefaultUsageRecorderQueueSize,
		MaxPending:    DefaultUsageRecorderMaxPending,
	}
}

func normalizeUsageRecorderConfig(config UsageRecorderConfig) UsageRecorderConfig {
	defaults := DefaultUsageRecorderConfig()
	if config.FlushInterval <= 0 {
		config.FlushInterval = defaults.FlushInterval
	}
	if config.FlushTimeout <= 0 {
		config.FlushTimeout = defaults.FlushTimeout
	}
	if config.CloseTimeout <= 0 {
		config.CloseTimeout = defaults.CloseTimeout
	}
	if config.QueueSize <= 0 {
		config.QueueSize = defaults.QueueSize
	}
	if config.MaxPending <= 0 {
		config.MaxPending = defaults.MaxPending
	}
	return config
}

type APIKeyUsage struct {
	KeyID    string
	Count    int64
	LastUsed time.Time
}

type UsageBatchWriter interface {
	WriteAPIKeyUsageBatch(ctx context.Context, batch []APIKeyUsage) error
}

type usageEvent struct {
	keyID string
	at    time.Time
}

type usageAggregate struct {
	count    int64
	lastUsed time.Time
}

type usageRecorder struct {
	config UsageRecorderConfig
	writer UsageBatchWriter

	events chan usageEvent
	stop   chan struct{}
	done   chan error

	acceptMu  sync.RWMutex
	closed    bool
	closeOnce sync.Once
	closeErr  error
}

func newUsageRecorder(config UsageRecorderConfig, writer UsageBatchWriter) *usageRecorder {
	if writer == nil {
		return nil
	}
	config = normalizeUsageRecorderConfig(config)
	recorder := &usageRecorder{
		config: config,
		writer: writer,
		events: make(chan usageEvent, config.QueueSize),
		stop:   make(chan struct{}),
		done:   make(chan error, 1),
	}
	go recorder.run()
	return recorder
}

func (r *usageRecorder) enqueue(keyID string, at time.Time) bool {
	if r == nil || keyID == "" {
		return false
	}
	r.acceptMu.RLock()
	defer r.acceptMu.RUnlock()
	if r.closed {
		apiKeyUsageRecorderEvents.WithLabelValues("dropped_closed").Inc()
		return false
	}
	select {
	case r.events <- usageEvent{keyID: keyID, at: at}:
		apiKeyUsageRecorderEvents.WithLabelValues("enqueued").Inc()
		return true
	default:
		apiKeyUsageRecorderEvents.WithLabelValues("dropped_queue_full").Inc()
		return false
	}
}

func (r *usageRecorder) Close() error {
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		r.acceptMu.Lock()
		r.closed = true
		close(r.stop)
		r.acceptMu.Unlock()

		timer := time.NewTimer(r.config.CloseTimeout)
		defer timer.Stop()
		select {
		case r.closeErr = <-r.done:
		case <-timer.C:
			r.closeErr = fmt.Errorf("close API key usage recorder: timed out after %s", r.config.CloseTimeout)
		}
	})
	return r.closeErr
}

func (r *usageRecorder) run() {
	ticker := time.NewTicker(r.config.FlushInterval)
	defer ticker.Stop()
	pending := make(map[string]usageAggregate, r.config.MaxPending)

	for {
		select {
		case event := <-r.events:
			r.addPending(pending, event)
		case <-ticker.C:
			r.flushWithTimeout(pending, r.config.FlushTimeout)
		case <-r.stop:
			r.drain(pending)
			ctx, cancel := context.WithTimeout(context.Background(), r.config.FlushTimeout)
			err := r.flush(ctx, pending)
			cancel()
			r.done <- err
			return
		}
	}
}

func (r *usageRecorder) drain(pending map[string]usageAggregate) {
	for {
		select {
		case event := <-r.events:
			r.addPending(pending, event)
		default:
			return
		}
	}
}

func (r *usageRecorder) addPending(
	pending map[string]usageAggregate,
	event usageEvent,
) {
	aggregate, exists := pending[event.keyID]
	if !exists && len(pending) >= r.config.MaxPending {
		apiKeyUsageRecorderEvents.WithLabelValues("dropped_capacity").Inc()
		return
	}
	aggregate.count++
	if aggregate.lastUsed.Before(event.at) {
		aggregate.lastUsed = event.at
	}
	pending[event.keyID] = aggregate
}

func (r *usageRecorder) flushWithTimeout(
	pending map[string]usageAggregate,
	timeout time.Duration,
) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = r.flush(ctx, pending)
}

func (r *usageRecorder) flush(
	ctx context.Context,
	pending map[string]usageAggregate,
) error {
	if len(pending) == 0 {
		return nil
	}
	ids := make([]string, 0, len(pending))
	for keyID := range pending {
		ids = append(ids, keyID)
	}
	sort.Strings(ids)
	batch := make([]APIKeyUsage, 0, len(ids))
	for _, keyID := range ids {
		aggregate := pending[keyID]
		batch = append(batch, APIKeyUsage{
			KeyID:    keyID,
			Count:    aggregate.count,
			LastUsed: aggregate.lastUsed,
		})
	}
	if err := r.writer.WriteAPIKeyUsageBatch(ctx, batch); err != nil {
		apiKeyUsageRecorderBatches.WithLabelValues("error").Inc()
		return fmt.Errorf("write API key usage batch: %w", err)
	}
	for _, keyID := range ids {
		delete(pending, keyID)
	}
	apiKeyUsageRecorderBatches.WithLabelValues("success").Inc()
	return nil
}
