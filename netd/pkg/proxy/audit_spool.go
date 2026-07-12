package proxy

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
)

type auditSpool struct {
	dir      string
	mu       sync.Mutex
	sequence uint64
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
	if payload, err := os.ReadFile(filepath.Join(dir, ".sequence")); err == nil {
		sequence, parseErr := strconv.ParseUint(strings.TrimSpace(string(payload)), 10, 64)
		if parseErr != nil {
			return nil, fmt.Errorf("parse audit spool sequence: %w", parseErr)
		}
		spool.sequence = sequence
	} else if !os.IsNotExist(err) {
		return nil, fmt.Errorf("read audit spool sequence: %w", err)
	}
	if err := spool.validateRecords(); err != nil {
		return nil, err
	}
	return spool, nil
}

func (s *auditSpool) NextSequence() (uint64, error) {
	if s == nil {
		return 0, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sequence == ^uint64(0) {
		return 0, fmt.Errorf("audit producer sequence exhausted")
	}
	next := s.sequence + 1
	tmp, err := os.CreateTemp(s.dir, ".sequence-*.tmp")
	if err != nil {
		return 0, fmt.Errorf("create audit sequence temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("chmod audit sequence temp file: %w", err)
	}
	if _, err := tmp.WriteString(strconv.FormatUint(next, 10)); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("write audit sequence: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return 0, fmt.Errorf("sync audit sequence: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return 0, fmt.Errorf("close audit sequence: %w", err)
	}
	if err := os.Rename(tmpPath, filepath.Join(s.dir, ".sequence")); err != nil {
		return 0, fmt.Errorf("commit audit sequence: %w", err)
	}
	if err := syncDirectory(s.dir); err != nil {
		return 0, err
	}
	s.sequence = next
	return next, nil
}

func (s *auditSpool) Put(event auditEvent) error {
	if s == nil {
		return nil
	}
	if strings.TrimSpace(event.EventID) == "" {
		return fmt.Errorf("audit event_id is required")
	}
	if _, err := uuid.Parse(event.EventID); err != nil {
		return fmt.Errorf("audit event_id is invalid")
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
		return fmt.Errorf("read audit spool record: %w", err)
	}
	tmp, err := os.CreateTemp(s.dir, ".audit-*.tmp")
	if err != nil {
		return fmt.Errorf("create audit spool temp file: %w", err)
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
		return fmt.Errorf("chmod audit spool temp file: %w", err)
	}
	if _, err := tmp.Write(payload); err != nil {
		return fmt.Errorf("write audit spool record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync audit spool record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close audit spool record: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("commit audit spool record: %w", err)
	}
	if err := syncDirectory(s.dir); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *auditSpool) Load(limit int) ([]auditEvent, error) {
	if s == nil || limit <= 0 {
		return nil, nil
	}
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
			return nil, fmt.Errorf("read audit spool record: %w", err)
		}
		var event auditEvent
		if err := json.Unmarshal(payload, &event); err != nil || event.EventID == "" {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, fmt.Errorf("corrupt audit spool record %s", name)
		}
		if _, err := uuid.Parse(event.EventID); err != nil || name != event.EventID+".json" {
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
		if _, err := uuid.Parse(event.EventID); err != nil || entry.Name() != event.EventID+".json" {
			return fmt.Errorf("corrupt audit spool identity %s", entry.Name())
		}
	}
	return nil
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
