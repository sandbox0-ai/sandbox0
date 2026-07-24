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
	auditReplayInterval    = time.Second
	auditReplayBatchSize   = 500
	auditReplayQuietPeriod = 25 * time.Millisecond
	auditCanonicalSlots    = 4
	auditSpoolWriteShards  = 64
	auditSpoolQuietPeriod  = 2 * time.Millisecond
	auditSpoolDrainLimit   = 40 * time.Millisecond
	auditDirSyncQuietTime  = time.Millisecond
	auditDirSyncWaitLimit  = 10 * time.Millisecond
)

var (
	errAuditDeliveryPending = errors.New("canonical audit event is pending")
	errAuditUnrecorded      = errors.New("audit event is unrecorded")
	errAuditSpoolWrite      = errors.New("audit spool write failed")
	errAuditBatchReserved   = errors.New("audit event is in an active canonical batch")
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
	dirSync         func(string) error
	dirSyncMu       sync.Mutex
	dirSyncWake     chan struct{}
	dirSyncRunning  bool
	dirSyncSequence uint64
	dirSyncDurable  uint64
	dirSyncFailed   uint64
	dirSyncErr      error
	canonicalCalls  atomic.Int64
	canonicalTurns  atomic.Int64
	canonicalWakeMu sync.Mutex
	canonicalWake   chan struct{}
	canonicalActive map[string]struct{}
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
		dirSync:         syncAuditDirectory,
		dirSyncWake:     make(chan struct{}),
		wake:            make(chan struct{}, 1),
		canonicalSlot:   make(chan struct{}, auditCanonicalSlots),
		canonicalWake:   make(chan struct{}),
		canonicalActive: make(map[string]struct{}),
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
	for {
		acquired, err := d.acquireCanonicalSlotOrAck(ctx, event.EventID)
		if err != nil {
			d.signalReplay()
			return fmt.Errorf("%w: event is durably buffered but canonical delivery did not start: %v", errAuditDeliveryPending, err)
		}
		if !acquired {
			return nil
		}

		if err := d.waitForConcurrentSpoolWrites(ctx); err != nil {
			d.releaseCanonicalSlot()
			d.signalReplay()
			return fmt.Errorf("%w: event is durably buffered but canonical batching was interrupted: %v", errAuditDeliveryPending, err)
		}

		// Replay or another bounded writer may have acknowledged or reserved the
		// target while this caller waited for a canonical slot.
		d.mu.Lock()
		events, acknowledged, batchErr := d.reserveCanonicalBatchLocked(auditReplayBatchSize, event.EventID)
		d.mu.Unlock()
		switch {
		case errors.Is(batchErr, errAuditBatchReserved):
			d.releaseCanonicalSlot()
			continue
		case batchErr != nil:
			d.releaseCanonicalSlot()
			d.signalReplay()
			return fmt.Errorf("%w: load durably buffered canonical batch: %v", errAuditDeliveryPending, batchErr)
		case acknowledged:
			d.releaseCanonicalSlot()
			return nil
		}

		d.canonicalTurns.Add(1)
		if err := d.writer.InsertEvents(ctx, events); err != nil {
			d.finishCanonicalBatch(events, false)
			d.logger.Warn("Sandbox audit event buffered for retry", zap.String("event_id", event.EventID), zap.Error(err))
			d.signalReplay()
			return fmt.Errorf("%w: event is durably buffered but not yet acknowledged: %v", errAuditDeliveryPending, err)
		}
		removeErr := d.finishCanonicalBatch(events, true)
		if removeErr != nil {
			// The canonical insert already succeeded. Leaving the record in place
			// can cause a duplicate retry, but the stable event ID makes that safe
			// and is preferable to reporting a false delivery failure.
			d.logger.Error("Canonical sandbox audit batch was acknowledged but spool cleanup failed",
				zap.String("event_id", event.EventID),
				zap.Int("batch_size", len(events)),
				zap.Error(removeErr),
			)
		}
		return nil
	}
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
		_, active := d.canonicalActive[eventID]
		d.mu.RUnlock()
		if err != nil {
			return false, err
		}
		if !pending {
			return false, nil
		}
		if active {
			select {
			case <-wake:
				continue
			case <-ctx.Done():
				return false, ctx.Err()
			}
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

// finishCanonicalBatch releases each event reservation after the ClickHouse
// outcome is known, then wakes callers waiting on the bounded writer pool.
func (d *auditDelivery) finishCanonicalBatch(events []sandboxobservability.Event, acknowledged bool) error {
	d.mu.Lock()
	var removeErr error
	if acknowledged {
		removeErr = d.removeBatchLocked(events)
	}
	for _, event := range events {
		delete(d.canonicalActive, event.EventID)
	}
	d.mu.Unlock()
	d.releaseCanonicalSlot()
	return removeErr
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
		if !d.waitForReplayQuiet(ctx) {
			return
		}
	}
}

// waitForReplayQuiet batches fsync-backed events without delaying their
// responses. Synchronous canonical callers bypass this background worker.
func (d *auditDelivery) waitForReplayQuiet(ctx context.Context) bool {
	timer := time.NewTimer(auditReplayQuietPeriod)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-d.wake:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(auditReplayQuietPeriod)
		case <-timer.C:
			return true
		}
	}
}

func (d *auditDelivery) replay(ctx context.Context) error {
	if d.canonicalCalls.Load() > 0 {
		return nil
	}
	if err := d.acquireCanonicalSlot(ctx); err != nil {
		return err
	}
	if d.canonicalCalls.Load() > 0 {
		d.releaseCanonicalSlot()
		return nil
	}

	d.mu.Lock()
	events, _, err := d.reserveCanonicalBatchLocked(auditReplayBatchSize, "")
	d.mu.Unlock()
	if err != nil {
		d.releaseCanonicalSlot()
		return err
	}
	if len(events) == 0 {
		d.releaseCanonicalSlot()
		return nil
	}
	d.canonicalTurns.Add(1)
	if err := d.writer.InsertEvents(ctx, events); err != nil {
		d.finishCanonicalBatch(events, false)
		return err
	}
	if err := d.finishCanonicalBatch(events, true); err != nil {
		return err
	}
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
	if err := d.waitForDirectorySync(); err != nil {
		return auditSpoolWriteError("fsync directory", err)
	}
	committed = true
	return nil
}

// waitForDirectorySync groups concurrent record renames behind the same
// directory durability barrier. Each caller still waits for a successful
// fsync whose snapshot includes its own rename.
func (d *auditDelivery) waitForDirectorySync() error {
	d.dirSyncMu.Lock()
	d.dirSyncSequence++
	target := d.dirSyncSequence
	leader := !d.dirSyncRunning
	if leader {
		d.dirSyncRunning = true
	}
	d.dirSyncMu.Unlock()

	if leader {
		d.runDirectorySync()
	}
	for {
		d.dirSyncMu.Lock()
		switch {
		case d.dirSyncDurable >= target:
			d.dirSyncMu.Unlock()
			return nil
		case d.dirSyncFailed >= target:
			err := d.dirSyncErr
			d.dirSyncMu.Unlock()
			return err
		}
		wake := d.dirSyncWake
		d.dirSyncMu.Unlock()
		<-wake
	}
}

func (d *auditDelivery) runDirectorySync() {
	for {
		target := d.waitForDirectorySyncBatch()
		err := d.dirSync(d.dir)

		d.dirSyncMu.Lock()
		if err != nil {
			// A failed barrier cannot prove durability for any rename that
			// arrived while it was running. Wake every current waiter so the
			// canonical fallback can take custody.
			d.dirSyncFailed = d.dirSyncSequence
			d.dirSyncErr = err
			d.dirSyncRunning = false
			d.notifyDirectorySyncWaitersLocked()
			d.dirSyncMu.Unlock()
			return
		}
		if target > d.dirSyncDurable {
			d.dirSyncDurable = target
		}
		d.notifyDirectorySyncWaitersLocked()
		if d.dirSyncSequence == target {
			d.dirSyncRunning = false
			d.dirSyncMu.Unlock()
			return
		}
		d.dirSyncMu.Unlock()
	}
}

func (d *auditDelivery) waitForDirectorySyncBatch() uint64 {
	deadline := time.Now().Add(auditDirSyncWaitLimit)
	for {
		d.dirSyncMu.Lock()
		target := d.dirSyncSequence
		d.dirSyncMu.Unlock()

		time.Sleep(auditDirSyncQuietTime)

		d.dirSyncMu.Lock()
		stable := d.dirSyncSequence == target
		target = d.dirSyncSequence
		d.dirSyncMu.Unlock()
		if stable || !time.Now().Before(deadline) {
			return target
		}
	}
}

func (d *auditDelivery) notifyDirectorySyncWaitersLocked() {
	close(d.dirSyncWake)
	d.dirSyncWake = make(chan struct{})
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
	return d.loadAvailableBatchContainingLocked(limit, eventID, nil)
}

// reserveCanonicalBatchLocked assigns unclaimed spool records to one writer.
// The caller must hold d.mu until every returned event is marked active.
func (d *auditDelivery) reserveCanonicalBatchLocked(limit int, eventID string) ([]sandboxobservability.Event, bool, error) {
	if d.canonicalActive == nil {
		d.canonicalActive = make(map[string]struct{})
	}
	if eventID != "" {
		pending, err := d.pendingLocked(eventID)
		if err != nil {
			return nil, false, err
		}
		if !pending {
			return nil, true, nil
		}
		if _, active := d.canonicalActive[eventID]; active {
			return nil, false, errAuditBatchReserved
		}
	}
	events, err := d.loadAvailableBatchContainingLocked(limit, eventID, d.canonicalActive)
	if err != nil {
		return nil, false, err
	}
	for _, event := range events {
		d.canonicalActive[event.EventID] = struct{}{}
	}
	return events, false, nil
}

func (d *auditDelivery) loadAvailableBatchContainingLocked(limit int, eventID string, active map[string]struct{}) ([]sandboxobservability.Event, error) {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, fmt.Errorf("read audit spool: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			entryEventID := strings.TrimSuffix(entry.Name(), ".json")
			if _, reserved := active[entryEventID]; reserved {
				continue
			}
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
