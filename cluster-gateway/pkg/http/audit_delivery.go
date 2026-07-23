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
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

const (
	auditReplayInterval   = time.Second
	auditReplayBatchSize  = 500
	auditSpoolWriteShards = 64
	auditSpoolQuietPeriod = 2 * time.Millisecond
	auditSpoolDrainLimit  = 40 * time.Millisecond
)

var (
	errAuditDeliveryPending = errors.New("canonical audit event is pending")
	errAuditUnrecorded      = errors.New("audit event is unrecorded")
	errAuditSpoolWrite      = errors.New("audit spool write failed")
)

type auditEventInserter interface {
	InsertEvents(context.Context, []sandboxobservability.Event) error
}

// auditDelivery is an fsync-backed delivery buffer. ClickHouse remains
// the sole canonical store; files are removed only after ClickHouse ACKs the
// exact signed event.
type auditDelivery struct {
	dir             string
	writer          auditEventInserter
	logger          *zap.Logger
	verificationKey ed25519.PublicKey
	mu              sync.RWMutex
	spoolWriteLocks [auditSpoolWriteShards]sync.Mutex
	spoolWrites     atomic.Int64
	canonicalCalls  atomic.Int64
	canonicalTurns  atomic.Int64
	canonicalWakeMu sync.Mutex
	canonicalWake   chan struct{}
	once            sync.Once
	wake            chan struct{}
	canonicalSlot   chan struct{}
}

func newAuditDelivery(dir string, writer auditEventInserter, logger *zap.Logger, verificationKey ed25519.PublicKey) (*auditDelivery, error) {
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
	delivery := &auditDelivery{
		dir:             dir,
		writer:          writer,
		logger:          logger,
		verificationKey: verificationKey,
		wake:            make(chan struct{}, 1),
		canonicalSlot:   make(chan struct{}, 1),
		canonicalWake:   make(chan struct{}),
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
		go d.run(ctx)
	})
}

// EnqueueDurable returns as soon as the event has been fsynced to the local
// spool. If the spool cannot be written, it falls back to a synchronous
// canonical insert so an event is never accepted without durable custody.
func (d *auditDelivery) EnqueueDurable(ctx context.Context, event sandboxobservability.Event) error {
	spooled, err := d.spoolOrCanonical(ctx, event)
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
	d.canonicalCalls.Add(1)
	defer d.canonicalCalls.Add(-1)

	spooled, err := d.spoolOrCanonical(ctx, event)
	if err != nil || !spooled {
		return err
	}
	acquired, err := d.acquireCanonicalSlotOrAck(ctx, event.EventID)
	if err != nil {
		d.signalReplay()
		return fmt.Errorf("%w: event is durably buffered but canonical delivery did not start: %v", errAuditDeliveryPending, err)
	}
	if !acquired {
		return nil
	}
	defer d.releaseCanonicalSlot()

	if err := d.waitForConcurrentSpoolWrites(ctx); err != nil {
		d.signalReplay()
		return fmt.Errorf("%w: event is durably buffered but canonical batching was interrupted: %v", errAuditDeliveryPending, err)
	}

	// Replay may have acknowledged this exact signed event between the local
	// fsync and acquisition of the canonical delivery slot.
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
	d.mu.Lock()
	events, batchErr := d.loadBatchContainingLocked(auditReplayBatchSize, event.EventID)
	d.mu.Unlock()
	if batchErr != nil {
		d.signalReplay()
		return fmt.Errorf("%w: load durably buffered canonical batch: %v", errAuditDeliveryPending, batchErr)
	}
	if err := d.writer.InsertEvents(ctx, events); err != nil {
		d.logger.Warn("Sandbox audit event buffered for retry", zap.String("event_id", event.EventID), zap.Error(err))
		d.signalReplay()
		return fmt.Errorf("%w: event is durably buffered but not yet acknowledged: %v", errAuditDeliveryPending, err)
	}
	d.mu.Lock()
	removeErr := d.removeBatchLocked(events)
	d.mu.Unlock()
	if removeErr != nil {
		// The canonical insert already succeeded. Leaving the record in place can
		// cause a duplicate retry, but the stable event ID makes that safe and is
		// preferable to reporting a false delivery failure.
		d.logger.Error("Canonical sandbox audit batch was acknowledged but spool cleanup failed",
			zap.String("event_id", event.EventID),
			zap.Int("batch_size", len(events)),
			zap.Error(removeErr),
		)
	}
	return nil
}

// spoolOrCanonical returns true when the event is in the local spool. A false,
// nil result means the spool write failed but the canonical fallback succeeded.
func (d *auditDelivery) spoolOrCanonical(ctx context.Context, event sandboxobservability.Event) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("%w: audit delivery is not configured", errAuditUnrecorded)
	}
	spoolErr := d.put(event)
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
	if canonicalErr := d.writer.InsertEvents(ctx, []sandboxobservability.Event{event}); canonicalErr != nil {
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

func (d *auditDelivery) put(event sandboxobservability.Event) error {
	d.spoolWrites.Add(1)
	defer d.spoolWrites.Add(-1)

	lock := &d.spoolWriteLocks[auditSpoolWriteShard(event.EventID)]
	lock.Lock()
	defer lock.Unlock()

	// Replay and cleanup need a stable directory view, while unrelated event
	// writes may fsync concurrently so burst traffic can share the filesystem's
	// group commit.
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.putLocked(event)
}

func auditSpoolWriteShard(eventID string) int {
	var hash uint32 = 2166136261
	for i := range len(eventID) {
		hash ^= uint32(eventID[i])
		hash *= 16777619
	}
	return int(hash % auditSpoolWriteShards)
}

func (d *auditDelivery) waitForConcurrentSpoolWrites(ctx context.Context) error {
	if d == nil || d.canonicalCalls.Load() <= 1 {
		return nil
	}
	deadline := time.NewTimer(auditSpoolDrainLimit)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	var quietSince time.Time
	for {
		if d.spoolWrites.Load() == 0 {
			if quietSince.IsZero() {
				quietSince = time.Now()
			} else if time.Since(quietSince) >= auditSpoolQuietPeriod {
				return nil
			}
		} else {
			quietSince = time.Time{}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return nil
		case <-ticker.C:
		}
	}
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
		d.canonicalTurns.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *auditDelivery) acquireCanonicalSlotOrAck(ctx context.Context, eventID string) (bool, error) {
	if d == nil || d.canonicalSlot == nil {
		return false, fmt.Errorf("audit canonical delivery is not configured")
	}
	for {
		d.canonicalWakeMu.Lock()
		wake := d.canonicalWake
		d.canonicalWakeMu.Unlock()
		if wake == nil {
			return false, fmt.Errorf("audit canonical delivery is not configured")
		}

		d.mu.RLock()
		pending, err := d.pendingLocked(eventID)
		d.mu.RUnlock()
		if err != nil {
			return false, err
		}
		if !pending {
			return false, nil
		}

		select {
		case <-wake:
			continue
		default:
		}
		select {
		case d.canonicalSlot <- struct{}{}:
			select {
			case <-wake:
				<-d.canonicalSlot
				continue
			default:
			}
			d.canonicalTurns.Add(1)
			return true, nil
		case <-wake:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

func (d *auditDelivery) releaseCanonicalSlot() {
	if d == nil || d.canonicalSlot == nil {
		return
	}
	d.canonicalWakeMu.Lock()
	wake := d.canonicalWake
	if wake == nil {
		d.canonicalWakeMu.Unlock()
		<-d.canonicalSlot
		return
	}
	d.canonicalWake = make(chan struct{})
	close(wake)
	d.canonicalWakeMu.Unlock()
	<-d.canonicalSlot
}

func (d *auditDelivery) run(ctx context.Context) {
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
	if err := d.acquireCanonicalSlot(ctx); err != nil {
		return err
	}
	defer d.releaseCanonicalSlot()

	d.mu.Lock()
	events, err := d.loadBatchLocked(auditReplayBatchSize)
	d.mu.Unlock()
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}
	if err := d.writer.InsertEvents(ctx, events); err != nil {
		return err
	}
	d.mu.Lock()
	if err := d.removeBatchLocked(events); err != nil {
		d.mu.Unlock()
		return err
	}
	d.mu.Unlock()
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
	return d.loadBatchContainingLocked(limit, "")
}

func (d *auditDelivery) loadBatchContainingLocked(limit int, eventID string) ([]sandboxobservability.Event, error) {
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
	if eventID != "" {
		targetName := eventID + ".json"
		targetIndex := sort.SearchStrings(names, targetName)
		if targetIndex >= len(names) || names[targetIndex] != targetName {
			return nil, fmt.Errorf("canonical audit event %s is missing from the spool", eventID)
		}
		copy(names[1:targetIndex+1], names[:targetIndex])
		names[0] = targetName
	}
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
