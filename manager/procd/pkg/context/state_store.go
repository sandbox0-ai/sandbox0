package context

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/process/repl"
	"github.com/sandbox0-ai/sandbox0/pkg/procdstate"
)

type persistedContext struct {
	Version       int                   `json:"version"`
	ID            string                `json:"id"`
	Config        process.ProcessConfig `json:"config"`
	REPLConfig    *repl.REPLConfig      `json:"repl_config,omitempty"`
	CleanupPolicy CleanupPolicy         `json:"cleanup_policy"`
	DesiredState  process.ProcessState  `json:"desired_state"`
	CreatedAt     time.Time             `json:"created_at"`
	UpdatedAt     time.Time             `json:"updated_at"`
}

type contextStateStore interface {
	Save(persistedContext) error
	Delete(string) error
	Load() ([]persistedContext, error)
	Clear() error
}

// FileStateStore persists context definitions in the sandbox webhook-state volume.
type FileStateStore struct {
	dir         string
	requestPath string
}

func NewFileStateStore(dir string) (*FileStateStore, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	if dir == "." || !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("context state directory must be absolute")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create context state directory: %w", err)
	}
	return &FileStateStore{
		dir:         dir,
		requestPath: filepath.Join(filepath.Dir(dir), procdstate.RecoveryRequestFilename),
	}, nil
}

func (s *FileStateStore) Save(record persistedContext) error {
	if s == nil {
		return nil
	}
	path, err := s.contextPath(record.ID)
	if err != nil {
		return err
	}
	record.Version = 1
	record.UpdatedAt = time.Now().UTC()
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal context state: %w", err)
	}
	tmp, err := os.CreateTemp(s.dir, ".context-*.tmp")
	if err != nil {
		return fmt.Errorf("create context state temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return fmt.Errorf("chmod context state temp file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write context state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync context state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close context state: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("publish context state: %w", err)
	}
	return syncDirectory(s.dir)
}

func (s *FileStateStore) Delete(id string) error {
	if s == nil {
		return nil
	}
	path, err := s.contextPath(id)
	if err != nil {
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("delete context state: %w", err)
	}
	return syncDirectory(s.dir)
}

func (s *FileStateStore) Load() ([]persistedContext, error) {
	if s == nil {
		return nil, nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("list context states: %w", err)
	}
	records := make([]persistedContext, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.dir, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read context state %s: %w", entry.Name(), err)
		}
		var record persistedContext
		if err := json.Unmarshal(data, &record); err != nil {
			return nil, fmt.Errorf("decode context state %s: %w", entry.Name(), err)
		}
		if record.Version != 1 || record.ID == "" {
			return nil, fmt.Errorf("unsupported context state %s", entry.Name())
		}
		recordPath, err := s.contextPath(record.ID)
		if err != nil || filepath.Base(recordPath) != entry.Name() {
			return nil, fmt.Errorf("context state %s has invalid id %q", entry.Name(), record.ID)
		}
		records = append(records, record)
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	return records, nil
}

func (s *FileStateStore) Clear() error {
	if s == nil {
		return nil
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("list context states: %w", err)
	}
	var errs []error
	for _, entry := range entries {
		isRecord := strings.HasSuffix(entry.Name(), ".json")
		isTemporary := strings.HasPrefix(entry.Name(), ".context-") && strings.HasSuffix(entry.Name(), ".tmp")
		if entry.IsDir() || (!isRecord && !isTemporary) {
			continue
		}
		if err := os.Remove(filepath.Join(s.dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("delete context state %s: %w", entry.Name(), err))
		}
	}
	if len(errs) == 0 {
		return syncDirectory(s.dir)
	}
	return errors.Join(errs...)
}

// ConsumeRecoveryRequest removes and reports the one-shot ctld recovery marker.
func (s *FileStateStore) ConsumeRecoveryRequest() (bool, error) {
	if s == nil {
		return false, nil
	}
	if _, err := os.Stat(s.requestPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, fmt.Errorf("inspect context recovery request: %w", err)
	}
	if err := os.Remove(s.requestPath); err != nil {
		return false, fmt.Errorf("consume context recovery request: %w", err)
	}
	return true, syncDirectory(filepath.Dir(s.requestPath))
}

func (s *FileStateStore) RecoveryRequested() (bool, error) {
	if s == nil {
		return false, nil
	}
	_, err := os.Stat(s.requestPath)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("inspect context recovery request: %w", err)
}

func (s *FileStateStore) contextPath(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" || filepath.Base(id) != id || strings.ContainsAny(id, `/\\`) {
		return "", fmt.Errorf("invalid context id %q", id)
	}
	return filepath.Join(s.dir, id+".json"), nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
