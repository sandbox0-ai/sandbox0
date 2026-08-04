package http

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	obsmetrics "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/metrics"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

const (
	auditReplayInterval        = time.Second
	auditReplayBatchSize       = 500
	auditCanonicalBatchWindow  = 2 * time.Millisecond
	auditCanonicalWriterSlots  = 4
	auditCanonicalRequestQueue = 2048
)

var (
	errAuditDeliveryPending = errors.New("canonical audit event is pending")
	errAuditUnrecorded      = errors.New("audit event is unrecorded")
	errAuditSpoolWrite      = errors.New("audit spool write failed")
)

type auditEventInserter interface {
	InsertEvents(context.Context, []sandboxobservability.Event) error
}

type auditCanonicalCall struct {
	event sandboxobservability.Event
	done  chan struct{}
	once  sync.Once
	err   error
}

// auditDelivery is an fsync-backed delivery buffer. ClickHouse remains
// the sole canonical store; files are removed only after ClickHouse ACKs the
// exact signed event.
type auditDelivery struct {
	dir             string
	writer          auditEventInserter
	logger          *zap.Logger
	verificationKey ed25519.PublicKey
	metrics         *obsmetrics.ClusterGatewayMetrics
	mu              sync.Mutex
	once            sync.Once
	started         atomic.Bool
	wake            chan struct{}
	canonicalQueue  chan *auditCanonicalCall
	canonicalSlot   chan struct{}
	canonicalGate   sync.RWMutex
	canonicalMu     sync.Mutex
	canonicalCalls  map[string]*auditCanonicalCall
	foregroundCalls atomic.Int64
	pendingCalls    atomic.Int64
}

func newAuditDelivery(
	dir string,
	writer auditEventInserter,
	logger *zap.Logger,
	verificationKey ed25519.PublicKey,
	metrics ...*obsmetrics.ClusterGatewayMetrics,
) (*auditDelivery, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, fmt.Errorf("audit spool directory is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("audit writer is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create audit spool: %w", err)
	}
	var deliveryMetrics *obsmetrics.ClusterGatewayMetrics
	if len(metrics) > 0 {
		deliveryMetrics = metrics[0]
	}
	delivery := &auditDelivery{
		dir:             dir,
		writer:          writer,
		logger:          logger,
		verificationKey: verificationKey,
		metrics:         deliveryMetrics,
		wake:            make(chan struct{}, 1),
		canonicalQueue:  make(chan *auditCanonicalCall, auditCanonicalRequestQueue),
		canonicalSlot:   make(chan struct{}, auditCanonicalWriterSlots),
		canonicalCalls:  make(map[string]*auditCanonicalCall),
	}
	if _, err := delivery.loadLocked(); err != nil {
		return nil, err
	}
	return delivery, nil
}

func (d *auditDelivery) Start(ctx context.Context) {
	if d == nil {
		return
	}
	d.once.Do(func() {
		d.started.Store(true)
		go d.runCanonicalBatches(ctx)
		go d.runReplay(ctx)
	})
}

// EnqueueDurable returns as soon as the event has been fsynced to the local
// spool. If the spool cannot be written, it falls back to a synchronous
// canonical insert so an event is never accepted without durable custody.
func (d *auditDelivery) EnqueueDurable(ctx context.Context, event sandboxobservability.Event) error {
	spooled, err := d.spoolOrCanonical(ctx, event, "durable")
	if err != nil {
		return err
	}
	if spooled {
		d.signalReplay()
	}
	return nil
}

// PersistCanonical fsyncs the event locally and waits for canonical storage to
// acknowledge it. A canonical failure leaves the event in the spool for replay
// and is returned to the caller as pending.
func (d *auditDelivery) PersistCanonical(ctx context.Context, event sandboxobservability.Event) error {
	if d == nil {
		return fmt.Errorf("%w: audit delivery is not configured", errAuditUnrecorded)
	}
	d.foregroundCalls.Add(1)
	defer d.foregroundCalls.Add(-1)

	spooled, err := d.spoolOrCanonical(ctx, event, "canonical")
	if err != nil || !spooled {
		return err
	}

	d.mu.Lock()
	pending, pendingErr := d.pendingLocked(event.EventID)
	d.mu.Unlock()
	if pendingErr != nil {
		d.signalReplay()
		return fmt.Errorf("%w: inspect durably buffered event: %v", errAuditDeliveryPending, pendingErr)
	}
	if !pending {
		return nil
	}

	call, leader := d.joinCanonicalCall(event)
	if leader {
		if d.started.Load() {
			d.observeQueueDelta(1)
			select {
			case d.canonicalQueue <- call:
			case <-ctx.Done():
				d.observeQueueDelta(-1)
				d.completeCanonicalCall(call, d.pendingCanonicalError("canonical delivery did not enter the batch queue", ctx.Err()))
			}
		} else {
			d.dispatchCanonicalBatch(ctx, []*auditCanonicalCall{call}, "foreground")
		}
	}

	waitStarted := time.Now()
	select {
	case <-call.done:
		d.observeStage("canonical", "batch_wait", waitStarted, call.err)
		if call.err != nil {
			d.signalReplay()
		}
		return call.err
	case <-ctx.Done():
		err := d.pendingCanonicalError("canonical delivery acknowledgement was interrupted", ctx.Err())
		d.observeStage("canonical", "batch_wait", waitStarted, err)
		d.signalReplay()
		return err
	}
}

// spoolOrCanonical returns true when the event is in the local spool. A false,
// nil result means the spool write failed but the canonical fallback succeeded.
func (d *auditDelivery) spoolOrCanonical(
	ctx context.Context,
	event sandboxobservability.Event,
	mode string,
) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("%w: audit delivery is not configured", errAuditUnrecorded)
	}
	spoolStarted := time.Now()
	d.mu.Lock()
	spoolErr := d.putLocked(event)
	d.mu.Unlock()
	d.observeStage(mode, "spool_write", spoolStarted, spoolErr)
	if spoolErr == nil {
		return true, nil
	}
	if !errors.Is(spoolErr, errAuditSpoolWrite) {
		d.logger.Error("Sandbox audit event was rejected before delivery",
			zap.String("event_id", event.EventID),
			zap.Error(spoolErr),
		)
		return false, fmt.Errorf("%w: durable spool rejected the event: %v", errAuditUnrecorded, spoolErr)
	}

	d.logger.Error("Failed to persist sandbox audit event to the durable spool; attempting canonical fallback",
		zap.String("event_id", event.EventID),
		zap.Error(spoolErr),
	)
	insertStarted := time.Now()
	canonicalErr := d.writer.InsertEvents(ctx, []sandboxobservability.Event{event})
	d.observeStage("fallback", "clickhouse_insert", insertStarted, canonicalErr)
	d.observeBatchSize("fallback", 1, canonicalErr)
	if canonicalErr != nil {
		d.logger.Error("Sandbox audit event is unrecorded after spool and canonical delivery both failed",
			zap.String("event_id", event.EventID),
			zap.Error(spoolErr),
			zap.NamedError("canonical_error", canonicalErr),
		)
		return false, fmt.Errorf("%w: durable spool failed: %v; canonical insert failed: %v", errAuditUnrecorded, spoolErr, canonicalErr)
	}
	d.logger.Warn("Sandbox audit event reached the canonical store through the synchronous spool fallback",
		zap.String("event_id", event.EventID),
		zap.Error(spoolErr),
	)
	return false, nil
}

func (d *auditDelivery) joinCanonicalCall(event sandboxobservability.Event) (*auditCanonicalCall, bool) {
	d.canonicalMu.Lock()
	defer d.canonicalMu.Unlock()
	if call := d.canonicalCalls[event.EventID]; call != nil {
		return call, false
	}
	call := &auditCanonicalCall{
		event: event,
		done:  make(chan struct{}),
	}
	d.canonicalCalls[event.EventID] = call
	d.pendingCalls.Add(1)
	return call, true
}

func (d *auditDelivery) completeCanonicalCall(call *auditCanonicalCall, err error) {
	if d == nil || call == nil {
		return
	}
	call.once.Do(func() {
		d.canonicalMu.Lock()
		call.err = err
		if d.canonicalCalls[call.event.EventID] == call {
			delete(d.canonicalCalls, call.event.EventID)
		}
		d.pendingCalls.Add(-1)
		close(call.done)
		d.canonicalMu.Unlock()
	})
}

func (d *auditDelivery) pendingCanonicalError(stage string, err error) error {
	return fmt.Errorf("%w: event is durably buffered but %s: %v", errAuditDeliveryPending, stage, err)
}

func (d *auditDelivery) runCanonicalBatches(ctx context.Context) {
	for {
		var first *auditCanonicalCall
		select {
		case <-ctx.Done():
			d.failQueuedCanonicalCalls(ctx.Err())
			return
		case first = <-d.canonicalQueue:
			d.observeQueueDelta(-1)
		}

		batch := []*auditCanonicalCall{first}
		for len(batch) < auditReplayBatchSize {
			select {
			case call := <-d.canonicalQueue:
				d.observeQueueDelta(-1)
				batch = append(batch, call)
			default:
				goto drained
			}
		}

	drained:
		if len(batch) < auditReplayBatchSize && d.foregroundCalls.Load() > 1 {
			timer := time.NewTimer(auditCanonicalBatchWindow)
			collecting := true
			for collecting && len(batch) < auditReplayBatchSize {
				select {
				case call := <-d.canonicalQueue:
					d.observeQueueDelta(-1)
					batch = append(batch, call)
				case <-timer.C:
					collecting = false
				case <-ctx.Done():
					if !timer.Stop() {
						select {
						case <-timer.C:
						default:
						}
					}
					d.failCanonicalBatch(batch, ctx.Err())
					d.failQueuedCanonicalCalls(ctx.Err())
					return
				}
			}
			if collecting && !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}
		d.dispatchCanonicalBatch(ctx, batch, "foreground")
	}
}

func (d *auditDelivery) failQueuedCanonicalCalls(cause error) {
	for {
		select {
		case call := <-d.canonicalQueue:
			d.observeQueueDelta(-1)
			d.completeCanonicalCall(call, d.pendingCanonicalError("canonical delivery stopped", cause))
		default:
			return
		}
	}
}

func (d *auditDelivery) failCanonicalBatch(batch []*auditCanonicalCall, cause error) {
	for _, call := range batch {
		d.completeCanonicalCall(call, d.pendingCanonicalError("canonical delivery stopped", cause))
	}
}

func (d *auditDelivery) dispatchCanonicalBatch(
	ctx context.Context,
	batch []*auditCanonicalCall,
	source string,
) {
	if len(batch) == 0 {
		return
	}
	slotStarted := time.Now()
	d.canonicalGate.RLock()
	if err := d.acquireCanonicalSlot(ctx); err != nil {
		d.canonicalGate.RUnlock()
		d.observeStage(source, "slot_wait", slotStarted, err)
		for _, call := range batch {
			d.completeCanonicalCall(call, d.pendingCanonicalError("canonical delivery did not start", err))
		}
		return
	}
	d.observeStage(source, "slot_wait", slotStarted, nil)

	go func() {
		results := func() []error {
			defer d.canonicalGate.RUnlock()
			defer d.releaseCanonicalSlot()
			d.observeInFlightDelta(1)
			defer d.observeInFlightDelta(-1)

			deliveryCtx, cancel := context.WithTimeout(ctx, auditCanonicalDeliveryTimeout)
			defer cancel()
			return d.deliverCanonicalBatch(deliveryCtx, batch, source)
		}()

		// Publish completion only after the batch has released every shared
		// resource so callers observe a quiescent canonical delivery state.
		for index, call := range batch {
			d.completeCanonicalCall(call, results[index])
		}
	}()
}

func (d *auditDelivery) deliverCanonicalBatch(
	ctx context.Context,
	batch []*auditCanonicalCall,
	source string,
) []error {
	results := make([]error, len(batch))
	pendingIndexes := make([]int, 0, len(batch))
	events := make([]sandboxobservability.Event, 0, len(batch))
	d.mu.Lock()
	for index, call := range batch {
		isPending, err := d.pendingLocked(call.event.EventID)
		if err != nil {
			d.mu.Unlock()
			pendingErr := d.pendingCanonicalError("durable spool inspection failed", err)
			for index := range results {
				results[index] = pendingErr
			}
			return results
		}
		if !isPending {
			continue
		}
		pendingIndexes = append(pendingIndexes, index)
		events = append(events, call.event)
	}
	d.mu.Unlock()
	if len(events) == 0 {
		return results
	}

	insertStarted := time.Now()
	insertErr := d.writer.InsertEvents(ctx, events)
	d.observeStage(source, "clickhouse_insert", insertStarted, insertErr)
	d.observeBatchSize(source, len(events), insertErr)
	if insertErr != nil {
		pendingErr := d.pendingCanonicalError("canonical storage did not acknowledge the batch", insertErr)
		for _, index := range pendingIndexes {
			results[index] = pendingErr
		}
		d.logger.Warn("Sandbox audit batch buffered for retry",
			zap.Int("batch_size", len(events)),
			zap.Error(insertErr),
		)
		d.signalReplay()
		return results
	}

	cleanupStarted := time.Now()
	d.mu.Lock()
	cleanupErr := d.removeBatchLocked(events)
	d.mu.Unlock()
	d.observeStage(source, "spool_cleanup", cleanupStarted, cleanupErr)
	if cleanupErr != nil {
		d.logger.Error("Canonical sandbox audit batch was acknowledged but spool cleanup failed",
			zap.Int("batch_size", len(events)),
			zap.Error(cleanupErr),
		)
	}
	return results
}

func (d *auditDelivery) signalReplay() {
	if d == nil {
		return
	}
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (d *auditDelivery) acquireCanonicalSlot(ctx context.Context) error {
	if d == nil || d.canonicalSlot == nil {
		return fmt.Errorf("audit canonical delivery is not configured")
	}
	select {
	case d.canonicalSlot <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *auditDelivery) releaseCanonicalSlot() {
	if d == nil || d.canonicalSlot == nil {
		return
	}
	<-d.canonicalSlot
}

func (d *auditDelivery) runReplay(ctx context.Context) {
	ticker := time.NewTicker(auditReplayInterval)
	defer ticker.Stop()
	for {
		replayCtx, cancel := context.WithTimeout(ctx, auditCanonicalDeliveryTimeout)
		err := d.replay(replayCtx)
		cancel()
		if err != nil && ctx.Err() == nil {
			d.logger.Error("Failed to replay sandbox audit buffer", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-d.wake:
		}
	}
}

func (d *auditDelivery) replay(ctx context.Context) error {
	if d.foregroundCalls.Load() > 0 || d.pendingCalls.Load() > 0 {
		return nil
	}
	if !d.canonicalGate.TryLock() {
		return nil
	}
	defer d.canonicalGate.Unlock()
	if d.foregroundCalls.Load() > 0 || d.pendingCalls.Load() > 0 {
		return nil
	}

	slotStarted := time.Now()
	if err := d.acquireCanonicalSlot(ctx); err != nil {
		d.observeStage("replay", "slot_wait", slotStarted, err)
		return err
	}
	d.observeStage("replay", "slot_wait", slotStarted, nil)
	defer d.releaseCanonicalSlot()
	d.observeInFlightDelta(1)
	defer d.observeInFlightDelta(-1)

	d.mu.Lock()
	events, err := d.loadBatchLocked(auditReplayBatchSize)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	insertStarted := time.Now()
	insertErr := d.writer.InsertEvents(ctx, events)
	d.observeStage("replay", "clickhouse_insert", insertStarted, insertErr)
	d.observeBatchSize("replay", len(events), insertErr)
	if insertErr != nil {
		return insertErr
	}
	cleanupStarted := time.Now()
	d.mu.Lock()
	cleanupErr := d.removeBatchLocked(events)
	if cleanupErr != nil {
		d.mu.Unlock()
		d.observeStage("replay", "spool_cleanup", cleanupStarted, cleanupErr)
		return cleanupErr
	}
	d.mu.Unlock()
	d.observeStage("replay", "spool_cleanup", cleanupStarted, nil)
	if len(events) == auditReplayBatchSize {
		// Yield the canonical slot after each batch so a strict mutation cannot
		// be starved behind an arbitrarily large recovery backlog.
		d.signalReplay()
	}
	return nil
}

func (d *auditDelivery) pendingLocked(eventID string) (bool, error) {
	if _, err := uuid.Parse(eventID); err != nil {
		return false, fmt.Errorf("audit event_id is invalid")
	}
	_, err := os.Stat(d.path(eventID))
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}

func (d *auditDelivery) putLocked(event sandboxobservability.Event) error {
	if err := sandboxobservability.ValidateSignedEvent(event); err != nil {
		return fmt.Errorf("audit event is invalid: %w", err)
	}
	if len(d.verificationKey) == ed25519.PublicKeySize {
		if err := sandboxobservability.VerifyEventIntegrity(event, d.verificationKey); err != nil {
			return fmt.Errorf("audit event integrity is invalid: %w", err)
		}
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal audit event: %w", err)
	}
	path := d.path(event.EventID)
	if existing, readErr := os.ReadFile(path); readErr == nil {
		if string(existing) != string(payload) {
			return fmt.Errorf("audit event_id collision")
		}
		return nil
	} else if !os.IsNotExist(readErr) {
		return auditSpoolWriteError("read existing record", readErr)
	}
	tmp, err := os.CreateTemp(d.dir, ".audit-*.tmp")
	if err != nil {
		return auditSpoolWriteError("create temp file", err)
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return auditSpoolWriteError("chmod temp file", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		return auditSpoolWriteError("write temp file", err)
	}
	if err := tmp.Sync(); err != nil {
		return auditSpoolWriteError("fsync temp file", err)
	}
	if err := tmp.Close(); err != nil {
		return auditSpoolWriteError("close temp file", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return auditSpoolWriteError("commit record", err)
	}
	if err := syncAuditDirectory(d.dir); err != nil {
		return auditSpoolWriteError("fsync directory", err)
	}
	committed = true
	return nil
}

func auditSpoolWriteError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", errAuditSpoolWrite, operation, err)
}

func (d *auditDelivery) loadLocked() ([]sandboxobservability.Event, error) {
	return d.loadBatchLocked(0)
}

func (d *auditDelivery) loadBatchLocked(limit int) ([]sandboxobservability.Event, error) {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, fmt.Errorf("read audit spool: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	events := make([]sandboxobservability.Event, 0, len(names))
	for _, name := range names {
		if limit > 0 && len(events) >= limit {
			break
		}
		payload, err := os.ReadFile(filepath.Join(d.dir, name))
		if err != nil {
			return nil, err
		}
		var event sandboxobservability.Event
		if err := json.Unmarshal(payload, &event); err != nil || strings.TrimSpace(event.EventID) == "" {
			return nil, fmt.Errorf("corrupt audit spool record %s", name)
		}
		if err := sandboxobservability.ValidateSignedEvent(event); err != nil {
			return nil, fmt.Errorf("invalid audit spool event %s: %w", name, err)
		}
		if name != event.EventID+".json" {
			return nil, fmt.Errorf("corrupt audit spool identity %s", name)
		}
		if len(d.verificationKey) == ed25519.PublicKeySize {
			if err := sandboxobservability.VerifyEventIntegrity(event, d.verificationKey); err != nil {
				return nil, fmt.Errorf("invalid audit spool integrity %s: %w", name, err)
			}
		}
		events = append(events, event)
	}
	return events, nil
}

func (d *auditDelivery) removeBatchLocked(events []sandboxobservability.Event) error {
	for _, event := range events {
		if _, err := uuid.Parse(event.EventID); err != nil {
			return fmt.Errorf("audit event_id is invalid")
		}
		if err := os.Remove(d.path(event.EventID)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncAuditDirectory(d.dir)
}

func (d *auditDelivery) observeStage(mode, stage string, started time.Time, err error) {
	if d == nil || d.metrics == nil || d.metrics.AuditDeliveryStageDuration == nil {
		return
	}
	d.metrics.AuditDeliveryStageDuration.
		WithLabelValues(mode, stage, auditDeliveryMetricResult(err)).
		Observe(time.Since(started).Seconds())
}

func (d *auditDelivery) observeBatchSize(source string, size int, err error) {
	if d == nil || d.metrics == nil || d.metrics.AuditCanonicalBatchSize == nil {
		return
	}
	d.metrics.AuditCanonicalBatchSize.
		WithLabelValues(source, auditDeliveryMetricResult(err)).
		Observe(float64(size))
}

func (d *auditDelivery) observeQueueDelta(delta float64) {
	if d == nil || d.metrics == nil || d.metrics.AuditCanonicalQueueDepth == nil {
		return
	}
	d.metrics.AuditCanonicalQueueDepth.Add(delta)
}

func (d *auditDelivery) observeInFlightDelta(delta float64) {
	if d == nil || d.metrics == nil || d.metrics.AuditCanonicalInFlight == nil {
		return
	}
	d.metrics.AuditCanonicalInFlight.Add(delta)
}

func auditDeliveryMetricResult(err error) string {
	switch {
	case err == nil:
		return "success"
	case errors.Is(err, errAuditDeliveryPending):
		return "pending"
	default:
		return "error"
	}
}

func (d *auditDelivery) path(eventID string) string {
	return filepath.Join(d.dir, eventID+".json")
}

func syncAuditDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
