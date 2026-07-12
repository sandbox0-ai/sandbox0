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
	"time"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
	"go.uber.org/zap"
)

const auditResultReplayInterval = time.Second

var (
	errAuditResultPending    = errors.New("canonical audit result is pending")
	errAuditResultUnrecorded = errors.New("audit result is unrecorded")
	errAuditResultSpoolWrite = errors.New("audit result spool write failed")
)

// auditResultDelivery is an fsync-backed delivery buffer. ClickHouse remains
// the sole canonical store; files are removed only after ClickHouse ACKs the
// exact signed event.
type auditResultDelivery struct {
	dir             string
	writer          sandboxobservability.Writer
	logger          *zap.Logger
	verificationKey ed25519.PublicKey
	mu              sync.Mutex
	once            sync.Once
}

func newAuditResultDelivery(dir string, writer sandboxobservability.Writer, logger *zap.Logger, verificationKeys ...ed25519.PublicKey) (*auditResultDelivery, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, fmt.Errorf("audit result spool directory is required")
	}
	if writer == nil {
		return nil, fmt.Errorf("audit writer is required")
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create audit result spool: %w", err)
	}
	delivery := &auditResultDelivery{dir: dir, writer: writer, logger: logger}
	if len(verificationKeys) > 0 {
		delivery.verificationKey = verificationKeys[0]
	}
	if _, err := delivery.loadLocked(); err != nil {
		return nil, err
	}
	return delivery, nil
}

func (d *auditResultDelivery) Start(ctx context.Context) {
	if d == nil {
		return
	}
	d.once.Do(func() {
		go d.run(ctx)
	})
}

func (d *auditResultDelivery) Persist(ctx context.Context, event sandboxobservability.Event) error {
	if d == nil {
		return fmt.Errorf("%w: audit result delivery is not configured", errAuditResultUnrecorded)
	}
	d.mu.Lock()
	spoolErr := d.putLocked(event)
	d.mu.Unlock()
	if spoolErr != nil {
		if !errors.Is(spoolErr, errAuditResultSpoolWrite) {
			d.logger.Error("Sandbox audit result was rejected before delivery",
				zap.String("event_id", event.EventID),
				zap.Error(spoolErr),
			)
			return fmt.Errorf("%w: durable spool rejected the result: %v", errAuditResultUnrecorded, spoolErr)
		}

		d.logger.Error("Failed to persist sandbox audit result to the durable spool; attempting canonical fallback",
			zap.String("event_id", event.EventID),
			zap.Error(spoolErr),
		)
		if canonicalErr := d.writer.InsertEvents(ctx, []sandboxobservability.Event{event}); canonicalErr != nil {
			d.logger.Error("Sandbox audit result is unrecorded after spool and canonical delivery both failed",
				zap.String("event_id", event.EventID),
				zap.Error(spoolErr),
				zap.NamedError("canonical_error", canonicalErr),
			)
			return fmt.Errorf("%w: durable spool failed: %v; canonical insert failed: %v", errAuditResultUnrecorded, spoolErr, canonicalErr)
		}
		d.logger.Warn("Sandbox audit result reached the canonical store through the synchronous spool fallback",
			zap.String("event_id", event.EventID),
			zap.Error(spoolErr),
		)
		return nil
	}
	if err := d.writer.InsertEvents(ctx, []sandboxobservability.Event{event}); err != nil {
		d.logger.Warn("Sandbox audit result buffered for retry", zap.String("event_id", event.EventID), zap.Error(err))
		return fmt.Errorf("%w: result is durably buffered but not yet acknowledged: %v", errAuditResultPending, err)
	}
	d.mu.Lock()
	removeErr := d.removeLocked(event.EventID)
	d.mu.Unlock()
	if removeErr != nil {
		// The canonical insert already succeeded. Leaving the record in place can
		// cause a duplicate retry, but the stable event ID makes that safe and is
		// preferable to withholding a successful response with a false pending
		// status.
		d.logger.Error("Canonical sandbox audit result was acknowledged but spool cleanup failed",
			zap.String("event_id", event.EventID),
			zap.Error(removeErr),
		)
	}
	return nil
}

func (d *auditResultDelivery) run(ctx context.Context) {
	ticker := time.NewTicker(auditResultReplayInterval)
	defer ticker.Stop()
	for {
		if err := d.replay(ctx); err != nil && ctx.Err() == nil {
			d.logger.Error("Failed to replay sandbox audit result buffer", zap.Error(err))
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (d *auditResultDelivery) replay(ctx context.Context) error {
	d.mu.Lock()
	events, err := d.loadLocked()
	d.mu.Unlock()
	if err != nil {
		return err
	}
	for _, event := range events {
		if err := d.writer.InsertEvents(ctx, []sandboxobservability.Event{event}); err != nil {
			return err
		}
		d.mu.Lock()
		if err := d.removeLocked(event.EventID); err != nil {
			d.mu.Unlock()
			return err
		}
		d.mu.Unlock()
	}
	return nil
}

func (d *auditResultDelivery) putLocked(event sandboxobservability.Event) error {
	if strings.TrimSpace(event.EventID) == "" {
		return fmt.Errorf("audit result event_id is required")
	}
	if _, err := uuid.Parse(event.EventID); err != nil {
		return fmt.Errorf("audit result event_id is invalid")
	}
	if len(d.verificationKey) == ed25519.PublicKeySize {
		if err := sandboxobservability.VerifyEventIntegrity(event, d.verificationKey); err != nil {
			return fmt.Errorf("audit result integrity is invalid: %w", err)
		}
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal audit result: %w", err)
	}
	path := d.path(event.EventID)
	if existing, readErr := os.ReadFile(path); readErr == nil {
		if string(existing) != string(payload) {
			return fmt.Errorf("audit result event_id collision")
		}
		return nil
	} else if !os.IsNotExist(readErr) {
		return auditResultSpoolWriteError("read existing record", readErr)
	}
	tmp, err := os.CreateTemp(d.dir, ".audit-result-*.tmp")
	if err != nil {
		return auditResultSpoolWriteError("create temp file", err)
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
		return auditResultSpoolWriteError("chmod temp file", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		return auditResultSpoolWriteError("write temp file", err)
	}
	if err := tmp.Sync(); err != nil {
		return auditResultSpoolWriteError("fsync temp file", err)
	}
	if err := tmp.Close(); err != nil {
		return auditResultSpoolWriteError("close temp file", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return auditResultSpoolWriteError("commit record", err)
	}
	if err := syncAuditResultDirectory(d.dir); err != nil {
		return auditResultSpoolWriteError("fsync directory", err)
	}
	committed = true
	return nil
}

func auditResultSpoolWriteError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", errAuditResultSpoolWrite, operation, err)
}

func (d *auditResultDelivery) loadLocked() ([]sandboxobservability.Event, error) {
	entries, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, fmt.Errorf("read audit result spool: %w", err)
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
		payload, err := os.ReadFile(filepath.Join(d.dir, name))
		if err != nil {
			return nil, err
		}
		var event sandboxobservability.Event
		if err := json.Unmarshal(payload, &event); err != nil || strings.TrimSpace(event.EventID) == "" {
			return nil, fmt.Errorf("corrupt audit result spool record %s", name)
		}
		if _, err := uuid.Parse(event.EventID); err != nil || name != event.EventID+".json" {
			return nil, fmt.Errorf("corrupt audit result spool identity %s", name)
		}
		if len(d.verificationKey) == ed25519.PublicKeySize {
			if err := sandboxobservability.VerifyEventIntegrity(event, d.verificationKey); err != nil {
				return nil, fmt.Errorf("invalid audit result spool integrity %s: %w", name, err)
			}
		}
		events = append(events, event)
	}
	return events, nil
}

func (d *auditResultDelivery) removeLocked(eventID string) error {
	if _, err := uuid.Parse(eventID); err != nil {
		return fmt.Errorf("audit result event_id is invalid")
	}
	if err := os.Remove(d.path(eventID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncAuditResultDirectory(d.dir)
}

func (d *auditResultDelivery) path(eventID string) string {
	return filepath.Join(d.dir, eventID+".json")
}

func syncAuditResultDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
