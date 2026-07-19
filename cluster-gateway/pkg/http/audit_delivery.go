package http

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

const (
	auditReplayInterval  = time.Second
	auditReplayBatchSize = 500
)

var (
	errAuditDeliveryPending = errors.New("canonical audit event is pending")
	errAuditUnrecorded      = errors.New("audit event is unrecorded")
	errAuditSpoolWrite      = errors.New("audit spool write failed")
	errAuditSpoolCapacity   = errors.New("audit spool capacity exceeded")
	errAuditSpoolCorrupt    = errors.New("audit spool is corrupt")
)

type auditEventInserter interface {
	InsertEvents(context.Context, []sandboxobservability.Event) error
}

type auditDeliveryLimits struct {
	maxBytes       int64
	maxEntries     int64
	maxTeamBytes   int64
	maxTeamEntries int64
	minFreeBytes   int64
	maxRecordBytes int64
}

type auditDeliveryRecord struct {
	teamID string
	bytes  int64
	digest [sha256.Size]byte
}

type auditDeliveryTeamUsage struct {
	bytes   int64
	entries int64
}

type auditDeliveryUsage struct {
	bytes   int64
	entries int64
	teams   map[string]auditDeliveryTeamUsage
}

// auditDelivery is an fsync-backed delivery buffer. ClickHouse remains
// the sole canonical store; files are removed only after ClickHouse ACKs the
// exact signed event.
type auditDelivery struct {
	dir             string
	writer          auditEventInserter
	logger          *zap.Logger
	verificationKey ed25519.PublicKey
	limits          auditDeliveryLimits
	freeBytes       func(string) (int64, error)
	records         map[string]auditDeliveryRecord
	usage           auditDeliveryUsage
	mu              sync.Mutex
	once            sync.Once
	wake            chan struct{}
	canonicalSlot   chan struct{}
}

func newAuditDelivery(dir string, writer auditEventInserter, logger *zap.Logger, verificationKey ed25519.PublicKey) (*auditDelivery, error) {
	return newAuditDeliveryWithLimits(
		dir,
		writer,
		logger,
		verificationKey,
		defaultAuditDeliveryLimits(),
	)
}

func newAuditDeliveryWithLimits(
	dir string,
	writer auditEventInserter,
	logger *zap.Logger,
	verificationKey ed25519.PublicKey,
	limits auditDeliveryLimits,
) (*auditDelivery, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, fmt.Errorf("audit spool directory is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("audit writer is required")
	}
	if err := validateAuditDeliveryLimits(limits); err != nil {
		return nil, err
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
		limits:          limits,
		freeBytes:       auditDeliveryFilesystemFreeBytes,
		records:         make(map[string]auditDeliveryRecord),
		usage: auditDeliveryUsage{
			teams: make(map[string]auditDeliveryTeamUsage),
		},
		wake:          make(chan struct{}, 1),
		canonicalSlot: make(chan struct{}, 1),
	}
	if err := delivery.rebuildUsageLocked(); err != nil {
		return nil, err
	}
	return delivery, nil
}

func defaultAuditDeliveryLimits() auditDeliveryLimits {
	return auditDeliveryLimitsFromConfig(config.AuditSpoolLimitsConfig{})
}

func auditDeliveryLimitsFromConfig(configured config.AuditSpoolLimitsConfig) auditDeliveryLimits {
	if configured.MaxBytes == 0 {
		configured.MaxBytes = config.DefaultAuditSpoolMaxBytes
	}
	if configured.MaxEntries == 0 {
		configured.MaxEntries = config.DefaultAuditSpoolMaxEntries
	}
	if configured.MaxTeamBytes == 0 {
		configured.MaxTeamBytes = config.DefaultAuditSpoolMaxTeamBytes
	}
	if configured.MaxTeamEntries == 0 {
		configured.MaxTeamEntries = config.DefaultAuditSpoolMaxTeamEntries
	}
	if configured.MinFreeBytes == 0 {
		configured.MinFreeBytes = config.DefaultAuditSpoolMinFreeBytes
	}
	if configured.MaxRecordBytes == 0 {
		configured.MaxRecordBytes = config.DefaultAuditSpoolMaxRecordBytes
	}
	return auditDeliveryLimits{
		maxBytes:       configured.MaxBytes,
		maxEntries:     configured.MaxEntries,
		maxTeamBytes:   configured.MaxTeamBytes,
		maxTeamEntries: configured.MaxTeamEntries,
		minFreeBytes:   configured.MinFreeBytes,
		maxRecordBytes: configured.MaxRecordBytes,
	}
}

func validateAuditDeliveryLimits(limits auditDeliveryLimits) error {
	if limits.maxBytes <= 0 {
		return fmt.Errorf("audit spool max bytes must be positive")
	}
	if limits.maxEntries <= 0 {
		return fmt.Errorf("audit spool max entries must be positive")
	}
	if limits.maxTeamBytes <= 0 || limits.maxTeamBytes > limits.maxBytes {
		return fmt.Errorf("audit spool max team bytes must be positive and not exceed max bytes")
	}
	if limits.maxTeamEntries <= 0 || limits.maxTeamEntries > limits.maxEntries {
		return fmt.Errorf("audit spool max team entries must be positive and not exceed max entries")
	}
	if limits.minFreeBytes < 0 {
		return fmt.Errorf("audit spool minimum free bytes must be non-negative")
	}
	if limits.maxRecordBytes <= 0 || limits.maxRecordBytes > limits.maxTeamBytes {
		return fmt.Errorf("audit spool max record bytes must be positive and not exceed max team bytes")
	}
	return nil
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
	spooled, err := d.spoolOrCanonical(ctx, event)
	if err != nil || !spooled {
		return err
	}
	if err := d.acquireCanonicalSlot(ctx); err != nil {
		d.signalReplay()
		return fmt.Errorf("%w: event is durably buffered but canonical delivery did not start: %v", errAuditDeliveryPending, err)
	}
	defer d.releaseCanonicalSlot()

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
	if err := d.writer.InsertEvents(ctx, []sandboxobservability.Event{event}); err != nil {
		d.logger.Warn("Sandbox audit event buffered for retry", zap.String("event_id", event.EventID), zap.Error(err))
		d.signalReplay()
		return fmt.Errorf("%w: event is durably buffered but not yet acknowledged: %v", errAuditDeliveryPending, err)
	}
	d.mu.Lock()
	removeErr := d.removeLocked(event.EventID)
	d.mu.Unlock()
	if removeErr != nil {
		// The canonical insert already succeeded. Leaving the record in place can
		// cause a duplicate retry, but the stable event ID makes that safe and is
		// preferable to reporting a false delivery failure.
		d.logger.Error("Canonical sandbox audit event was acknowledged but spool cleanup failed",
			zap.String("event_id", event.EventID),
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
	d.mu.Lock()
	spoolErr := d.putLocked(event)
	d.mu.Unlock()
	if spoolErr == nil {
		return true, nil
	}
	if !errors.Is(spoolErr, errAuditSpoolWrite) {
		d.logger.Error("Sandbox audit event was rejected before delivery",
			zap.String("event_id", event.EventID),
			zap.Error(spoolErr),
		)
		return false, fmt.Errorf("%w: durable spool rejected the event: %w", errAuditUnrecorded, spoolErr)
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
	parsed, err := uuid.Parse(eventID)
	if err != nil || parsed.String() != eventID {
		return false, fmt.Errorf("audit event_id is invalid")
	}
	record, tracked := d.records[eventID]
	payload, err := readAuditDeliveryPayload(d.path(eventID), d.limits.maxRecordBytes)
	if !tracked {
		if os.IsNotExist(err) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return false, auditSpoolCorruptError("record %s exists on disk but is not tracked", eventID)
	}
	if os.IsNotExist(err) {
		return false, auditSpoolCorruptError("tracked record %s is missing from disk", eventID)
	}
	if err != nil {
		return false, err
	}
	event, decoded, err := d.decodeRecord(eventID+".json", payload)
	if err != nil {
		return false, err
	}
	if event.EventID != eventID || decoded != record {
		return false, auditSpoolCorruptError("record %s differs from startup usage state", eventID)
	}
	return true, nil
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
	recordBytes := int64(len(payload))
	if recordBytes > d.limits.maxRecordBytes {
		return auditSpoolCapacityError(
			"record bytes %d exceed per-record maximum %d",
			recordBytes,
			d.limits.maxRecordBytes,
		)
	}
	record := auditDeliveryRecord{
		teamID: event.TeamID,
		bytes:  recordBytes,
		digest: sha256.Sum256(payload),
	}
	path := d.path(event.EventID)
	existing, readErr := readAuditDeliveryPayload(path, d.limits.maxRecordBytes)
	if readErr == nil {
		if string(existing) != string(payload) {
			return fmt.Errorf("audit event_id collision")
		}
		tracked, ok := d.records[event.EventID]
		if !ok || tracked != record {
			return auditSpoolCorruptError(
				"record %s is not represented by startup usage state",
				event.EventID,
			)
		}
		return nil
	}
	if !os.IsNotExist(readErr) {
		if errors.Is(readErr, errAuditSpoolCapacity) ||
			errors.Is(readErr, errAuditSpoolCorrupt) {
			return readErr
		}
		return auditSpoolWriteError("read existing record", readErr)
	}
	if _, tracked := d.records[event.EventID]; tracked {
		return auditSpoolCorruptError("tracked record %s is missing from disk", event.EventID)
	}
	if err := d.checkCapacityLocked(event.TeamID, recordBytes); err != nil {
		return err
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
	committed = true
	d.addRecordLocked(event.EventID, record)
	if err := syncAuditDirectory(d.dir); err != nil {
		return auditSpoolWriteError("fsync directory", err)
	}
	return nil
}

func auditSpoolWriteError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", errAuditSpoolWrite, operation, err)
}

func auditSpoolCapacityError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errAuditSpoolCapacity, fmt.Sprintf(format, args...))
}

func auditSpoolCorruptError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", errAuditSpoolCorrupt, fmt.Sprintf(format, args...))
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
		if !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return nil, auditSpoolCorruptError("inspect record %s: %v", entry.Name(), err)
		}
		if !info.Mode().IsRegular() {
			return nil, auditSpoolCorruptError("record path %s is not a regular file", entry.Name())
		}
		names = append(names, entry.Name())
	}
	if len(names) != len(d.records) {
		return nil, auditSpoolCorruptError(
			"record count on disk is %d but startup usage state tracks %d",
			len(names),
			len(d.records),
		)
	}
	sort.Strings(names)
	capacity := len(names)
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	events := make([]sandboxobservability.Event, 0, capacity)
	for _, name := range names {
		if limit > 0 && len(events) >= limit {
			break
		}
		payload, err := readAuditDeliveryPayload(
			filepath.Join(d.dir, name),
			d.limits.maxRecordBytes,
		)
		if err != nil {
			return nil, auditSpoolCorruptError("read record %s: %v", name, err)
		}
		event, record, err := d.decodeRecord(name, payload)
		if err != nil {
			return nil, err
		}
		tracked, ok := d.records[event.EventID]
		if !ok || tracked != record {
			return nil, auditSpoolCorruptError("record %s differs from startup usage state", name)
		}
		events = append(events, event)
	}
	return events, nil
}

func (d *auditDelivery) removeLocked(eventID string) error {
	return d.removeIDsLocked([]string{eventID})
}

func (d *auditDelivery) removeBatchLocked(events []sandboxobservability.Event) error {
	eventIDs := make([]string, 0, len(events))
	for _, event := range events {
		eventIDs = append(eventIDs, event.EventID)
	}
	return d.removeIDsLocked(eventIDs)
}

func (d *auditDelivery) removeIDsLocked(eventIDs []string) error {
	type removal struct {
		eventID string
		record  auditDeliveryRecord
	}
	removals := make([]removal, 0, len(eventIDs))
	seen := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		parsed, err := uuid.Parse(eventID)
		if err != nil || parsed.String() != eventID {
			return fmt.Errorf("audit event_id is invalid")
		}
		if _, duplicate := seen[eventID]; duplicate {
			continue
		}
		seen[eventID] = struct{}{}
		record, tracked := d.records[eventID]
		payload, err := readAuditDeliveryPayload(d.path(eventID), d.limits.maxRecordBytes)
		if !tracked {
			if os.IsNotExist(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("inspect audit spool record: %w", err)
			}
			return auditSpoolCorruptError("record %s exists on disk but is not tracked", eventID)
		}
		if os.IsNotExist(err) {
			return auditSpoolCorruptError("tracked record %s is missing from disk", eventID)
		}
		if err != nil {
			return fmt.Errorf("read audit spool record before remove: %w", err)
		}
		if record.bytes != int64(len(payload)) || record.digest != sha256.Sum256(payload) {
			return auditSpoolCorruptError("record %s changed before removal", eventID)
		}
		removals = append(removals, removal{eventID: eventID, record: record})
	}
	if len(removals) == 0 {
		return nil
	}
	for _, removal := range removals {
		if err := os.Remove(d.path(removal.eventID)); err != nil {
			return errors.Join(
				fmt.Errorf("remove audit spool record: %w", err),
				syncAuditDirectory(d.dir),
			)
		}
		d.removeRecordLocked(removal.eventID, removal.record)
	}
	return syncAuditDirectory(d.dir)
}

func (d *auditDelivery) rebuildUsageLocked() error {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return fmt.Errorf("read audit spool: %w", err)
	}
	removedTemporary := false
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() &&
			strings.HasPrefix(name, ".audit-") &&
			strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(d.dir, name)); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove incomplete audit spool record %s: %w", name, err)
			}
			removedTemporary = true
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return auditSpoolCorruptError("inspect record %s: %v", name, err)
		}
		if !info.Mode().IsRegular() {
			return auditSpoolCorruptError("record path %s is not a regular file", name)
		}
		payload, err := readAuditDeliveryPayload(
			filepath.Join(d.dir, name),
			d.limits.maxRecordBytes,
		)
		if err != nil {
			if errors.Is(err, errAuditSpoolCapacity) ||
				errors.Is(err, errAuditSpoolCorrupt) {
				return err
			}
			return auditSpoolCorruptError("read record %s: %v", name, err)
		}
		event, record, err := d.decodeRecord(name, payload)
		if err != nil {
			return err
		}
		if _, duplicate := d.records[event.EventID]; duplicate {
			return auditSpoolCorruptError("duplicate record for event_id %s", event.EventID)
		}
		d.addRecordLocked(event.EventID, record)
		if err := d.checkRebuiltCapacityLocked(event.TeamID); err != nil {
			return err
		}
	}
	if removedTemporary {
		if err := syncAuditDirectory(d.dir); err != nil {
			return auditSpoolWriteError("fsync directory after incomplete-record cleanup", err)
		}
	}
	return nil
}

func (d *auditDelivery) decodeRecord(
	name string,
	payload []byte,
) (sandboxobservability.Event, auditDeliveryRecord, error) {
	var event sandboxobservability.Event
	if err := json.Unmarshal(payload, &event); err != nil {
		return sandboxobservability.Event{}, auditDeliveryRecord{}, auditSpoolCorruptError(
			"decode record %s: %v",
			name,
			err,
		)
	}
	if err := sandboxobservability.ValidateSignedEvent(event); err != nil {
		return sandboxobservability.Event{}, auditDeliveryRecord{}, auditSpoolCorruptError(
			"validate record %s: %v",
			name,
			err,
		)
	}
	if name != event.EventID+".json" {
		return sandboxobservability.Event{}, auditDeliveryRecord{}, auditSpoolCorruptError(
			"record identity %s does not match event_id %s",
			name,
			event.EventID,
		)
	}
	if len(d.verificationKey) == ed25519.PublicKeySize {
		if err := sandboxobservability.VerifyEventIntegrity(event, d.verificationKey); err != nil {
			return sandboxobservability.Event{}, auditDeliveryRecord{}, auditSpoolCorruptError(
				"verify record %s integrity: %v",
				name,
				err,
			)
		}
	}
	return event, auditDeliveryRecord{
		teamID: event.TeamID,
		bytes:  int64(len(payload)),
		digest: sha256.Sum256(payload),
	}, nil
}

func readAuditDeliveryPayload(path string, maxRecordBytes int64) ([]byte, error) {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !pathInfo.Mode().IsRegular() {
		return nil, auditSpoolCorruptError(
			"record path %s is not a regular file",
			filepath.Base(path),
		)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat audit spool record: %w", err)
	}
	if !info.Mode().IsRegular() || !os.SameFile(pathInfo, info) {
		return nil, auditSpoolCorruptError(
			"record %s changed while it was opened",
			filepath.Base(path),
		)
	}
	if info.Size() < 0 || info.Size() > maxRecordBytes {
		return nil, auditSpoolCapacityError(
			"record %s has %d bytes, exceeding per-record maximum %d",
			filepath.Base(path),
			info.Size(),
			maxRecordBytes,
		)
	}
	readLimit := maxRecordBytes
	const maxInt64 = int64(^uint64(0) >> 1)
	if readLimit < maxInt64 {
		readLimit++
	}
	payload, err := io.ReadAll(io.LimitReader(file, readLimit))
	if err != nil {
		return nil, fmt.Errorf("read audit spool record: %w", err)
	}
	if int64(len(payload)) > maxRecordBytes {
		return nil, auditSpoolCapacityError(
			"record %s grew beyond per-record maximum %d",
			filepath.Base(path),
			maxRecordBytes,
		)
	}
	if int64(len(payload)) != info.Size() {
		return nil, auditSpoolCorruptError(
			"record %s changed while it was read",
			filepath.Base(path),
		)
	}
	return payload, nil
}

func (d *auditDelivery) checkCapacityLocked(teamID string, recordBytes int64) error {
	if d.usage.entries >= d.limits.maxEntries {
		return auditSpoolCapacityError(
			"global entries %d reached maximum %d",
			d.usage.entries,
			d.limits.maxEntries,
		)
	}
	if recordBytes > d.limits.maxBytes-d.usage.bytes {
		return auditSpoolCapacityError(
			"global bytes %d plus record bytes %d exceed maximum %d",
			d.usage.bytes,
			recordBytes,
			d.limits.maxBytes,
		)
	}
	team := d.usage.teams[teamID]
	if team.entries >= d.limits.maxTeamEntries {
		return auditSpoolCapacityError(
			"team %s entries %d reached maximum %d",
			teamID,
			team.entries,
			d.limits.maxTeamEntries,
		)
	}
	if recordBytes > d.limits.maxTeamBytes-team.bytes {
		return auditSpoolCapacityError(
			"team %s bytes %d plus record bytes %d exceed maximum %d",
			teamID,
			team.bytes,
			recordBytes,
			d.limits.maxTeamBytes,
		)
	}
	if d.limits.minFreeBytes > 0 {
		freeBytes, err := d.freeBytes(d.dir)
		if err != nil {
			return auditSpoolCapacityError("check filesystem free bytes: %v", err)
		}
		if freeBytes < recordBytes ||
			freeBytes-recordBytes < d.limits.minFreeBytes {
			return auditSpoolCapacityError(
				"projected filesystem free bytes %d are below minimum %d",
				freeBytes-recordBytes,
				d.limits.minFreeBytes,
			)
		}
	}
	return nil
}

func (d *auditDelivery) checkRebuiltCapacityLocked(teamID string) error {
	if d.usage.entries > d.limits.maxEntries {
		return auditSpoolCapacityError(
			"startup global entries %d exceed maximum %d",
			d.usage.entries,
			d.limits.maxEntries,
		)
	}
	if d.usage.bytes > d.limits.maxBytes {
		return auditSpoolCapacityError(
			"startup global bytes %d exceed maximum %d",
			d.usage.bytes,
			d.limits.maxBytes,
		)
	}
	team := d.usage.teams[teamID]
	if team.entries > d.limits.maxTeamEntries {
		return auditSpoolCapacityError(
			"startup team %s entries %d exceed maximum %d",
			teamID,
			team.entries,
			d.limits.maxTeamEntries,
		)
	}
	if team.bytes > d.limits.maxTeamBytes {
		return auditSpoolCapacityError(
			"startup team %s bytes %d exceed maximum %d",
			teamID,
			team.bytes,
			d.limits.maxTeamBytes,
		)
	}
	return nil
}

func (d *auditDelivery) addRecordLocked(eventID string, record auditDeliveryRecord) {
	d.records[eventID] = record
	d.usage.bytes += record.bytes
	d.usage.entries++
	team := d.usage.teams[record.teamID]
	team.bytes += record.bytes
	team.entries++
	d.usage.teams[record.teamID] = team
}

func (d *auditDelivery) removeRecordLocked(eventID string, record auditDeliveryRecord) {
	delete(d.records, eventID)
	d.usage.bytes -= record.bytes
	d.usage.entries--
	team := d.usage.teams[record.teamID]
	team.bytes -= record.bytes
	team.entries--
	if team.bytes == 0 && team.entries == 0 {
		delete(d.usage.teams, record.teamID)
		return
	}
	d.usage.teams[record.teamID] = team
}

func (d *auditDelivery) path(eventID string) string {
	return filepath.Join(d.dir, eventID+".json")
}

func auditDeliveryFilesystemFreeBytes(path string) (int64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}
	blockSize := int64(stat.Bsize)
	if blockSize <= 0 {
		return 0, fmt.Errorf("filesystem block size is invalid")
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if uint64(stat.Bavail) > uint64(maxInt64)/uint64(blockSize) {
		return 0, fmt.Errorf("filesystem free byte count overflows int64")
	}
	return int64(stat.Bavail) * blockSize, nil
}

func syncAuditDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
