package proxy

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxobservability"
)

var errAuditSpoolWrite = errors.New("audit spool write failed")

type auditSpool struct {
	dir string
	// putMu makes the read-before-rename collision check atomic for concurrent
	// writes of the same event ID without serializing Put behind Load.
	putMu sync.Mutex
	// recordsMu lets atomic Put and read-only Load run together while excluding
	// Remove, so replay cannot classify a concurrently acknowledged record as
	// corrupt without adding a full spool scan to admission latency.
	recordsMu sync.RWMutex
}

func newAuditSpool(dir string) (*auditSpool, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create audit spool: %w", err)
	}
	spool := &auditSpool{dir: dir}
	if err := spool.validateRecords(); err != nil {
		return nil, err
	}
	return spool, nil
}

func (s *auditSpool) Put(event auditEvent) error {
	if s == nil {
		return nil
	}
	s.putMu.Lock()
	defer s.putMu.Unlock()
	s.recordsMu.RLock()
	defer s.recordsMu.RUnlock()
	if err := validateSpoolAuditEvent(event); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal audit spool record: %w", err)
	}
	path := s.path(event.EventID)
	if existing, err := os.ReadFile(path); err == nil {
		if string(existing) != string(payload) {
			return fmt.Errorf("audit spool event_id collision")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return auditSpoolWriteError("read existing record", err)
	}
	tmp, err := os.CreateTemp(s.dir, ".audit-*.tmp")
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
	if err := syncDirectory(s.dir); err != nil {
		return auditSpoolWriteError("fsync directory", err)
	}
	committed = true
	return nil
}

func auditSpoolWriteError(operation string, err error) error {
	return fmt.Errorf("%w: %s: %v", errAuditSpoolWrite, operation, err)
}

func (s *auditSpool) Load(limit int) ([]auditEvent, error) {
	if s == nil || limit <= 0 {
		return nil, nil
	}
	s.recordsMu.RLock()
	defer s.recordsMu.RUnlock()
	entries, err := os.ReadDir(s.dir)
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
	events := make([]auditEvent, 0, min(limit, len(names)))
	for _, name := range names {
		if len(events) >= limit {
			break
		}
		payload, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, fmt.Errorf("read audit spool record: %w", err)
		}
		var event auditEvent
		if err := json.Unmarshal(payload, &event); err != nil || event.EventID == "" {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, fmt.Errorf("corrupt audit spool record %s", name)
		}
		event = normalizeLoadedAuditEvent(event)
		if validateSpoolAuditEvent(event) != nil || name != event.EventID+".json" {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, fmt.Errorf("corrupt audit spool identity %s", name)
		}
		events = append(events, event)
	}
	return events, nil
}

func (s *auditSpool) Remove(eventIDs ...string) error {
	if s == nil {
		return nil
	}
	s.recordsMu.Lock()
	defer s.recordsMu.Unlock()
	for _, eventID := range eventIDs {
		if _, err := uuid.Parse(eventID); err != nil {
			return fmt.Errorf("audit event_id is invalid")
		}
		if err := os.Remove(s.path(eventID)); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove audit spool record: %w", err)
		}
	}
	return syncDirectory(s.dir)
}

func (s *auditSpool) path(eventID string) string {
	return filepath.Join(s.dir, eventID+".json")
}

func (s *auditSpool) validateRecords() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read audit spool: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		payload, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return err
		}
		var event auditEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return fmt.Errorf("corrupt audit spool record %s", entry.Name())
		}
		event = normalizeLoadedAuditEvent(event)
		if validateSpoolAuditEvent(event) != nil || entry.Name() != event.EventID+".json" {
			return fmt.Errorf("corrupt audit spool identity %s", entry.Name())
		}
	}
	return nil
}

func normalizeLoadedAuditEvent(event auditEvent) auditEvent {
	if event.SchemaVersion == 0 {
		event.SchemaVersion = sandboxobservability.LegacyEventSchemaVersion
	}
	return event
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open audit spool directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync audit spool directory: %w", err)
	}
	return nil
}
