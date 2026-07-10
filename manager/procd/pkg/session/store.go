package session

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	sessionStateFile  = "state.json"
	sessionEventsFile = "events.jsonl"
	sandboxIDFile     = "sandbox-id"
)

type persistedSession struct {
	Session
	InputReceipts []InputReceipt `json:"input_receipts,omitempty"`
	CreationKey   string         `json:"creation_key,omitempty"`
}

// FileStore persists session control state under a procd-owned state directory.
type FileStore struct {
	root string
}

func NewFileStore(root string) (*FileStore, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "." || !filepath.IsAbs(root) {
		return nil, errors.New("session state directory must be absolute")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create session state directory: %w", err)
	}
	return &FileStore{root: root}, nil
}

func (s *FileStore) Load() ([]Session, error) {
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return nil, fmt.Errorf("read session state directory: %w", err)
	}
	sessions := make([]Session, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "ses-") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.root, entry.Name(), sessionStateFile))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("read session %s: %w", entry.Name(), err)
		}
		var stored persistedSession
		if err := json.Unmarshal(data, &stored); err != nil {
			return nil, fmt.Errorf("decode session %s: %w", entry.Name(), err)
		}
		if stored.ID == "" || stored.ID != entry.Name() {
			return nil, fmt.Errorf("session state directory %s has mismatched id %q", entry.Name(), stored.ID)
		}
		stored.Spec = normalizeSpec(stored.Spec)
		stored.Session.InputReceipts = append([]InputReceipt(nil), stored.InputReceipts...)
		stored.Session.CreationKey = stored.CreationKey
		sessions = append(sessions, stored.Session)
	}
	return sessions, nil
}

// BindSandbox binds persisted sessions to one sandbox identity. Legacy stores
// without an identity are adopted in place. A copied store is cleared before
// it is rebound so a fork cannot execute the source sandbox's sessions.
func (s *FileStore) BindSandbox(sandboxID string) (bool, error) {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || strings.ContainsAny(sandboxID, "\r\n") {
		return false, errors.New("sandbox id is required")
	}
	path := filepath.Join(s.root, sandboxIDFile)
	data, err := os.ReadFile(path)
	switch {
	case err == nil:
		if strings.TrimSpace(string(data)) == sandboxID {
			return true, nil
		}
		return false, nil
	case errors.Is(err, fs.ErrNotExist):
		if err := writeFileAtomic(path, []byte(sandboxID+"\n"), 0o600); err != nil {
			return false, fmt.Errorf("persist sandbox identity: %w", err)
		}
		return true, nil
	default:
		return false, fmt.Errorf("read sandbox identity: %w", err)
	}
}

// ResetForSandbox removes copied session state before changing its owner.
func (s *FileStore) ResetForSandbox(sandboxID string) error {
	sandboxID = strings.TrimSpace(sandboxID)
	if sandboxID == "" || strings.ContainsAny(sandboxID, "\r\n") {
		return errors.New("sandbox id is required")
	}
	entries, err := os.ReadDir(s.root)
	if err != nil {
		return fmt.Errorf("read session state directory: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || !strings.HasPrefix(entry.Name(), "ses-") {
			continue
		}
		if err := os.RemoveAll(filepath.Join(s.root, entry.Name())); err != nil {
			return fmt.Errorf("remove copied session %s: %w", entry.Name(), err)
		}
	}
	if err := writeFileAtomic(filepath.Join(s.root, sandboxIDFile), []byte(sandboxID+"\n"), 0o600); err != nil {
		return fmt.Errorf("persist sandbox identity: %w", err)
	}
	return nil
}

func (s *FileStore) Save(value Session) error {
	dir, err := s.sessionDir(value.ID)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create session directory: %w", err)
	}
	stored := persistedSession{
		Session:       cloneSession(value),
		InputReceipts: append([]InputReceipt(nil), value.InputReceipts...),
		CreationKey:   value.CreationKey,
	}
	data, err := json.MarshalIndent(stored, "", "  ")
	if err != nil {
		return fmt.Errorf("encode session state: %w", err)
	}
	data = append(data, '\n')
	if err := writeFileAtomic(filepath.Join(dir, sessionStateFile), data, 0o600); err != nil {
		return fmt.Errorf("persist session state: %w", err)
	}
	return nil
}

func (s *FileStore) Delete(id string) error {
	dir, err := s.sessionDir(id)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("delete session state: %w", err)
	}
	return nil
}

func (s *FileStore) JournalPath(id string) (string, error) {
	dir, err := s.sessionDir(id)
	if err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", fmt.Errorf("create session directory: %w", err)
	}
	return filepath.Join(dir, sessionEventsFile), nil
}

func (s *FileStore) sessionDir(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" || filepath.Base(id) != id || !strings.HasPrefix(id, "ses-") {
		return "", errors.New("invalid session id")
	}
	return filepath.Join(s.root, id), nil
}

func writeFileAtomic(path string, data []byte, mode fs.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer os.Remove(tmpPath)
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
