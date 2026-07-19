package proxy

import (
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

	"github.com/google/uuid"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
)

var (
	errAuditSpoolWrite    = errors.New("audit spool write failed")
	errAuditSpoolCapacity = errors.New("audit spool capacity exceeded")
	errAuditSpoolCorrupt  = errors.New("audit spool is corrupt")
)

type auditSpoolLimits struct {
	MaxBytes       int64
	MaxEntries     int64
	MaxTeamBytes   int64
	MaxTeamEntries int64
	MinFreeBytes   int64
	MaxRecordBytes int64
}

type auditSpoolRecord struct {
	teamID string
	bytes  int64
	digest [sha256.Size]byte
}

type auditSpoolTeamUsage struct {
	bytes   int64
	entries int64
}

type auditSpoolUsage struct {
	bytes   int64
	entries int64
	teams   map[string]auditSpoolTeamUsage
}

type auditSpool struct {
	dir       string
	limits    auditSpoolLimits
	freeBytes func(string) (int64, error)

	mu      sync.RWMutex
	records map[string]auditSpoolRecord
	usage   auditSpoolUsage
}

func newAuditSpool(dir string) (*auditSpool, error) {
	return newAuditSpoolWithLimits(dir, defaultAuditSpoolLimits())
}

func newAuditSpoolWithLimits(
	dir string,
	limits auditSpoolLimits,
) (*auditSpool, error) {
	dir = strings.TrimSpace(dir)
	if dir == "" {
		return nil, nil
	}
	if err := validateAuditSpoolLimits(limits); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create audit spool: %w", err)
	}
	spool := &auditSpool{
		dir:       dir,
		limits:    limits,
		freeBytes: auditSpoolFilesystemFreeBytes,
		records:   make(map[string]auditSpoolRecord),
		usage: auditSpoolUsage{
			teams: make(map[string]auditSpoolTeamUsage),
		},
	}
	if err := spool.rebuildUsage(); err != nil {
		return nil, err
	}
	return spool, nil
}

func defaultAuditSpoolLimits() auditSpoolLimits {
	return auditSpoolLimits{
		MaxBytes:       config.DefaultAuditSpoolMaxBytes,
		MaxEntries:     config.DefaultAuditSpoolMaxEntries,
		MaxTeamBytes:   config.DefaultAuditSpoolMaxTeamBytes,
		MaxTeamEntries: config.DefaultAuditSpoolMaxTeamEntries,
		MinFreeBytes:   config.DefaultAuditSpoolMinFreeBytes,
		MaxRecordBytes: config.DefaultAuditSpoolMaxRecordBytes,
	}
}

func validateAuditSpoolLimits(limits auditSpoolLimits) error {
	if limits.MaxBytes <= 0 {
		return fmt.Errorf("audit spool max bytes must be positive")
	}
	if limits.MaxEntries <= 0 {
		return fmt.Errorf("audit spool max entries must be positive")
	}
	if limits.MaxTeamBytes <= 0 ||
		limits.MaxTeamBytes > limits.MaxBytes {
		return fmt.Errorf(
			"audit spool max team bytes must be positive and not exceed max bytes",
		)
	}
	if limits.MaxTeamEntries <= 0 ||
		limits.MaxTeamEntries > limits.MaxEntries {
		return fmt.Errorf(
			"audit spool max team entries must be positive and not exceed max entries",
		)
	}
	if limits.MinFreeBytes < 0 {
		return fmt.Errorf("audit spool minimum free bytes must be non-negative")
	}
	if limits.MaxRecordBytes <= 0 ||
		limits.MaxRecordBytes > limits.MaxTeamBytes {
		return fmt.Errorf(
			"audit spool max record bytes must be positive and not exceed max team bytes",
		)
	}
	return nil
}

func (s *auditSpool) Put(event auditEvent) error {
	if s == nil {
		return nil
	}
	if err := validateSpoolAuditEvent(event); err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal audit spool record: %w", err)
	}
	recordBytes := int64(len(payload))
	if recordBytes > s.limits.MaxRecordBytes {
		return auditSpoolCapacityError(
			"record bytes %d exceed per-record maximum %d",
			recordBytes,
			s.limits.MaxRecordBytes,
		)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.path(event.EventID)
	if existing, err := readAuditSpoolPayload(
		path,
		s.limits.MaxRecordBytes,
	); err == nil {
		if string(existing) != string(payload) {
			return fmt.Errorf("audit spool event_id collision")
		}
		record, ok := s.records[event.EventID]
		if !ok ||
			record.teamID != event.TeamID ||
			record.bytes != recordBytes ||
			record.digest != sha256.Sum256(payload) {
			return auditSpoolCorruptError(
				"record %s is not represented by startup usage state",
				event.EventID,
			)
		}
		return nil
	} else if !os.IsNotExist(err) {
		if errors.Is(err, errAuditSpoolCapacity) ||
			errors.Is(err, errAuditSpoolCorrupt) {
			return err
		}
		return auditSpoolWriteError("read existing record", err)
	}
	if _, ok := s.records[event.EventID]; ok {
		return auditSpoolCorruptError(
			"tracked record %s is missing from disk",
			event.EventID,
		)
	}
	if err := s.checkCapacityLocked(event.TeamID, recordBytes); err != nil {
		return err
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
	committed = true
	s.addRecordLocked(event.EventID, auditSpoolRecord{
		teamID: event.TeamID,
		bytes:  recordBytes,
		digest: sha256.Sum256(payload),
	})
	if err := syncDirectory(s.dir); err != nil {
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

func (s *auditSpool) Load(limit int) ([]auditEvent, error) {
	if s == nil || limit <= 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read audit spool: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, auditSpoolCorruptError(
				"unexpected path %s in audit spool",
				entry.Name(),
			)
		}
		info, err := entry.Info()
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, auditSpoolCorruptError(
				"inspect record %s: %v",
				entry.Name(),
				err,
			)
		}
		if !info.Mode().IsRegular() {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, auditSpoolCorruptError(
				"record path %s is not a regular file",
				entry.Name(),
			)
		}
		names = append(names, entry.Name())
	}
	if len(names) != len(s.records) {
		proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
		return nil, auditSpoolCorruptError(
			"record count on disk is %d but startup usage state tracks %d",
			len(names),
			len(s.records),
		)
	}
	sort.Strings(names)
	events := make([]auditEvent, 0, min(limit, len(names)))
	for _, name := range names {
		if len(events) >= limit {
			break
		}
		payload, err := readAuditSpoolPayload(
			filepath.Join(s.dir, name),
			s.limits.MaxRecordBytes,
		)
		if err != nil {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, auditSpoolCorruptError(
				"read record %s: %v",
				name,
				err,
			)
		}
		event, record, err := decodeAuditSpoolRecord(name, payload)
		if err != nil {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, err
		}
		tracked, ok := s.records[event.EventID]
		if !ok || tracked != record {
			proxyMetrics.RecordAuditIngestEvents("corrupt", 1)
			return nil, auditSpoolCorruptError(
				"record %s differs from startup usage state",
				name,
			)
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
		parsed, err := uuid.Parse(eventID)
		if err != nil || parsed.String() != eventID {
			return fmt.Errorf("audit event_id is invalid")
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	type removal struct {
		eventID string
		record  auditSpoolRecord
	}
	removals := make([]removal, 0, len(eventIDs))
	seen := make(map[string]struct{}, len(eventIDs))
	for _, eventID := range eventIDs {
		if _, ok := seen[eventID]; ok {
			continue
		}
		seen[eventID] = struct{}{}
		path := s.path(eventID)
		record, tracked := s.records[eventID]
		payload, err := readAuditSpoolPayload(
			path,
			s.limits.MaxRecordBytes,
		)
		if !tracked {
			if err == nil {
				return auditSpoolCorruptError(
					"record %s exists on disk but is not tracked",
					eventID,
				)
			}
			if os.IsNotExist(err) {
				continue
			}
			return fmt.Errorf("inspect audit spool record: %w", err)
		}
		if err != nil {
			if os.IsNotExist(err) {
				return auditSpoolCorruptError(
					"tracked record %s is missing from disk",
					eventID,
				)
			}
			return fmt.Errorf("read audit spool record before remove: %w", err)
		}
		if record.bytes != int64(len(payload)) ||
			record.digest != sha256.Sum256(payload) {
			return auditSpoolCorruptError(
				"record %s changed before removal",
				eventID,
			)
		}
		removals = append(removals, removal{
			eventID: eventID,
			record:  record,
		})
	}
	if len(removals) == 0 {
		return nil
	}
	for _, removal := range removals {
		if err := os.Remove(s.path(removal.eventID)); err != nil {
			return errors.Join(
				fmt.Errorf("remove audit spool record: %w", err),
				syncDirectory(s.dir),
			)
		}
		s.removeRecordLocked(removal.eventID, removal.record)
	}
	return syncDirectory(s.dir)
}

func (s *auditSpool) path(eventID string) string {
	return filepath.Join(s.dir, eventID+".json")
}

func (s *auditSpool) rebuildUsage() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("read audit spool: %w", err)
	}
	removedTemporary := false
	for _, entry := range entries {
		name := entry.Name()
		if !entry.IsDir() &&
			strings.HasPrefix(name, ".audit-") &&
			strings.HasSuffix(name, ".tmp") {
			if err := os.Remove(filepath.Join(s.dir, name)); err != nil &&
				!os.IsNotExist(err) {
				return fmt.Errorf(
					"remove incomplete audit spool record %s: %w",
					name,
					err,
				)
			}
			removedTemporary = true
			continue
		}
		if !strings.HasSuffix(name, ".json") {
			return auditSpoolCorruptError(
				"unexpected path %s in audit spool",
				name,
			)
		}
		if entry.IsDir() {
			return auditSpoolCorruptError(
				"record path %s is a directory",
				name,
			)
		}
		info, err := entry.Info()
		if err != nil {
			return auditSpoolCorruptError(
				"inspect record %s: %v",
				name,
				err,
			)
		}
		if !info.Mode().IsRegular() {
			return auditSpoolCorruptError(
				"record path %s is not a regular file",
				name,
			)
		}
		payload, err := readAuditSpoolPayload(
			filepath.Join(s.dir, name),
			s.limits.MaxRecordBytes,
		)
		if err != nil {
			if errors.Is(err, errAuditSpoolCapacity) ||
				errors.Is(err, errAuditSpoolCorrupt) {
				return err
			}
			return auditSpoolCorruptError(
				"read record %s: %v",
				name,
				err,
			)
		}
		event, record, err := decodeAuditSpoolRecord(name, payload)
		if err != nil {
			return err
		}
		if record.bytes > s.limits.MaxRecordBytes {
			return auditSpoolCapacityError(
				"startup record %s has %d bytes, exceeding per-record maximum %d",
				name,
				record.bytes,
				s.limits.MaxRecordBytes,
			)
		}
		s.addRecordLocked(event.EventID, record)
	}
	if removedTemporary {
		if err := syncDirectory(s.dir); err != nil {
			return auditSpoolWriteError(
				"fsync directory after incomplete-record cleanup",
				err,
			)
		}
	}
	return nil
}

func decodeAuditSpoolRecord(
	name string,
	payload []byte,
) (auditEvent, auditSpoolRecord, error) {
	var event auditEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return auditEvent{}, auditSpoolRecord{}, auditSpoolCorruptError(
			"decode record %s: %v",
			name,
			err,
		)
	}
	if err := validateSpoolAuditEvent(event); err != nil {
		return auditEvent{}, auditSpoolRecord{}, auditSpoolCorruptError(
			"validate record %s: %v",
			name,
			err,
		)
	}
	if name != event.EventID+".json" {
		return auditEvent{}, auditSpoolRecord{}, auditSpoolCorruptError(
			"record identity %s does not match event_id %s",
			name,
			event.EventID,
		)
	}
	return event, auditSpoolRecord{
		teamID: event.TeamID,
		bytes:  int64(len(payload)),
		digest: sha256.Sum256(payload),
	}, nil
}

func readAuditSpoolPayload(
	path string,
	maxRecordBytes int64,
) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat audit spool record: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, auditSpoolCorruptError(
			"record path %s is not a regular file",
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

func (s *auditSpool) checkCapacityLocked(
	teamID string,
	recordBytes int64,
) error {
	if s.usage.entries >= s.limits.MaxEntries {
		return auditSpoolCapacityError(
			"global entries %d reached maximum %d",
			s.usage.entries,
			s.limits.MaxEntries,
		)
	}
	if recordBytes > s.limits.MaxBytes-s.usage.bytes {
		return auditSpoolCapacityError(
			"global bytes %d plus record bytes %d exceed maximum %d",
			s.usage.bytes,
			recordBytes,
			s.limits.MaxBytes,
		)
	}
	team := s.usage.teams[teamID]
	if team.entries >= s.limits.MaxTeamEntries {
		return auditSpoolCapacityError(
			"team %s entries %d reached maximum %d",
			teamID,
			team.entries,
			s.limits.MaxTeamEntries,
		)
	}
	if recordBytes > s.limits.MaxTeamBytes-team.bytes {
		return auditSpoolCapacityError(
			"team %s bytes %d plus record bytes %d exceed maximum %d",
			teamID,
			team.bytes,
			recordBytes,
			s.limits.MaxTeamBytes,
		)
	}
	if s.limits.MinFreeBytes > 0 {
		freeBytes, err := s.freeBytes(s.dir)
		if err != nil {
			return auditSpoolCapacityError(
				"check filesystem free bytes: %v",
				err,
			)
		}
		if freeBytes < recordBytes ||
			freeBytes-recordBytes < s.limits.MinFreeBytes {
			return auditSpoolCapacityError(
				"projected filesystem free bytes %d are below minimum %d",
				freeBytes-recordBytes,
				s.limits.MinFreeBytes,
			)
		}
	}
	return nil
}

func (s *auditSpool) addRecordLocked(
	eventID string,
	record auditSpoolRecord,
) {
	s.records[eventID] = record
	s.usage.bytes += record.bytes
	s.usage.entries++
	team := s.usage.teams[record.teamID]
	team.bytes += record.bytes
	team.entries++
	s.usage.teams[record.teamID] = team
}

func (s *auditSpool) removeRecordLocked(
	eventID string,
	record auditSpoolRecord,
) {
	delete(s.records, eventID)
	s.usage.bytes -= record.bytes
	s.usage.entries--
	team := s.usage.teams[record.teamID]
	team.bytes -= record.bytes
	team.entries--
	if team.bytes == 0 && team.entries == 0 {
		delete(s.usage.teams, record.teamID)
		return
	}
	s.usage.teams[record.teamID] = team
}

func auditSpoolFilesystemFreeBytes(path string) (int64, error) {
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
