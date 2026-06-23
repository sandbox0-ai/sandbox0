// Package file provides file system operations for Procd.
package file

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

// FileType represents the type of a file.
type FileType string

const (
	FileTypeFile    FileType = "file"
	FileTypeDir     FileType = "dir"
	FileTypeSymlink FileType = "symlink"
)

// FileInfo represents information about a file.
type FileInfo struct {
	Name       string    `json:"name"`
	Path       string    `json:"path"`
	Type       FileType  `json:"type"`
	Size       int64     `json:"size"`
	Mode       string    `json:"mode"`
	ModTime    time.Time `json:"mod_time"`
	IsLink     bool      `json:"is_link"`
	LinkTarget string    `json:"link_target,omitempty"`
}

// MaxFileSize is the maximum file size allowed for write operations.
const MaxFileSize = 100 * 1024 * 1024 // 100MB

// Manager handles file system operations.
type Manager struct {
	rootPath        string
	rootFSMu        sync.RWMutex
	rootFS          string
	watcherMgr      *WatcherManager
	allowExecutable bool
}

// NewManager creates a new file manager.
func NewManager(rootPath string) (*Manager, error) {
	// Ensure root path exists
	if err := os.MkdirAll(rootPath, 0755); err != nil {
		return nil, fmt.Errorf("create root path: %w", err)
	}

	watcherMgr, err := NewWatcherManager()
	if err != nil {
		return nil, fmt.Errorf("create watcher manager: %w", err)
	}

	return &Manager{
		rootPath:        rootPath,
		watcherMgr:      watcherMgr,
		allowExecutable: true,
	}, nil
}

// SetRootFS sets the mounted root filesystem used for user-visible file paths.
func (m *Manager) SetRootFS(rootFS string) {
	value := filepath.Clean(strings.TrimSpace(rootFS))
	if value == "." {
		value = ""
	}

	m.rootFSMu.Lock()
	defer m.rootFSMu.Unlock()
	m.rootFS = value
}

// RootFS returns the mounted root filesystem used for user-visible file paths.
func (m *Manager) RootFS() string {
	m.rootFSMu.RLock()
	defer m.rootFSMu.RUnlock()
	return m.rootFS
}

// sanitizePath cleans the path, resolves relative paths against rootPath, and
// maps the resulting sandbox-visible path into the mounted rootfs when present.
func (m *Manager) sanitizePath(path string) string {
	return m.resolvePath(path)
}

func (m *Manager) virtualPath(path string) string {
	cleanPath := filepath.Clean(path)
	if filepath.IsAbs(cleanPath) {
		return cleanPath
	}
	return filepath.Clean(filepath.Join(m.rootPath, cleanPath))
}

func (m *Manager) resolvePath(path string) string {
	virtualPath := m.virtualPath(path)
	rootFS := m.RootFS()
	if rootFS == "" {
		return virtualPath
	}
	return joinRootFS(rootFS, virtualPath)
}

func joinRootFS(rootFS, virtualPath string) string {
	rootFS = filepath.Clean(rootFS)
	virtualPath = filepath.Clean(virtualPath)
	if virtualPath == "." || virtualPath == string(filepath.Separator) {
		return rootFS
	}
	return filepath.Join(rootFS, strings.TrimPrefix(virtualPath, string(filepath.Separator)))
}

func (m *Manager) displayPath(path string) string {
	rootFS := m.RootFS()
	if rootFS == "" {
		return path
	}

	cleanPath := filepath.Clean(path)
	cleanRootFS := filepath.Clean(rootFS)
	if cleanPath == cleanRootFS {
		return string(filepath.Separator)
	}
	prefix := cleanRootFS + string(filepath.Separator)
	if strings.HasPrefix(cleanPath, prefix) {
		return string(filepath.Separator) + strings.TrimPrefix(cleanPath, prefix)
	}
	return path
}

func (m *Manager) displayEvent(event WatchEvent) WatchEvent {
	event.Path = m.displayPath(event.Path)
	if event.OldPath != "" {
		event.OldPath = m.displayPath(event.OldPath)
	}
	return event
}

func pathIsDir(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return info.IsDir(), nil
}

func isPathNotDir(err error, path string) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOTDIR) {
		return true
	}
	if os.IsExist(err) {
		isDir, statErr := pathIsDir(path)
		if statErr == nil {
			return !isDir
		}
		return true
	}
	return false
}

// ReadFile reads a file.
func (m *Manager) ReadFile(path string) ([]byte, error) {
	cleanPath := m.sanitizePath(path)

	data, err := os.ReadFile(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		if os.IsPermission(err) {
			return nil, ErrPermissionDenied
		}
		if errors.Is(err, syscall.EISDIR) {
			return nil, ErrPathNotFile
		}
		return nil, err
	}

	return data, nil
}

// WriteFile writes data to a file.
func (m *Manager) WriteFile(path string, data []byte, perm os.FileMode) error {
	if len(data) > MaxFileSize {
		return ErrFileTooLarge
	}

	if !m.allowExecutable && perm&0111 != 0 {
		return ErrPermissionDenied
	}

	cleanPath := m.sanitizePath(path)
	if info, err := os.Stat(cleanPath); err == nil && info.IsDir() {
		return ErrPathNotFile
	} else if err != nil && !os.IsNotExist(err) {
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		if errors.Is(err, syscall.ENOTDIR) {
			return ErrPathNotDir
		}
		return err
	}

	// Ensure parent directory exists
	dir := filepath.Dir(cleanPath)
	if info, err := os.Stat(dir); err == nil {
		// Path exists, verify it's a directory
		if !info.IsDir() {
			return ErrPathNotDir
		}
		// Already exists as directory, continue
	} else if os.IsNotExist(err) {
		// Path doesn't exist, create it
		if err := os.MkdirAll(dir, 0755); err != nil {
			if os.IsPermission(err) {
				return ErrPermissionDenied
			}
			if isPathNotDir(err, dir) {
				return ErrPathNotDir
			}
			return err
		}
	} else {
		// Other stat error (permission, etc)
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		return err
	}

	// Atomic write using temp file
	tmpPath := cleanPath + ".tmp"
	if err := os.WriteFile(tmpPath, data, perm); err != nil {
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		return err
	}

	if err := os.Rename(tmpPath, cleanPath); err != nil {
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		return err
	}
	return nil
}

// Stat returns file information.
func (m *Manager) Stat(path string) (*FileInfo, error) {
	cleanPath := m.sanitizePath(path)

	info, err := os.Lstat(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		if os.IsPermission(err) {
			return nil, ErrPermissionDenied
		}
		return nil, err
	}

	fileInfo := &FileInfo{
		Name:    info.Name(),
		Path:    path,
		Size:    info.Size(),
		Mode:    fmt.Sprintf("%04o", info.Mode().Perm()),
		ModTime: info.ModTime(),
	}

	switch {
	case info.Mode()&os.ModeSymlink != 0:
		fileInfo.Type = FileTypeSymlink
		fileInfo.IsLink = true
		if target, err := os.Readlink(cleanPath); err == nil {
			fileInfo.LinkTarget = target
		}
	case info.IsDir():
		fileInfo.Type = FileTypeDir
	default:
		fileInfo.Type = FileTypeFile
	}

	return fileInfo, nil
}

// ListDir lists the contents of a directory.
func (m *Manager) ListDir(path string) ([]*FileInfo, error) {
	cleanPath := m.sanitizePath(path)

	entries, err := os.ReadDir(cleanPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrDirNotFound
		}
		if os.IsPermission(err) {
			return nil, ErrPermissionDenied
		}
		if errors.Is(err, syscall.ENOTDIR) {
			return nil, ErrPathNotDir
		}
		return nil, err
	}

	result := make([]*FileInfo, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}

		fileInfo := &FileInfo{
			Name:    entry.Name(),
			Path:    filepath.Join(path, entry.Name()),
			Size:    info.Size(),
			Mode:    fmt.Sprintf("%04o", info.Mode().Perm()),
			ModTime: info.ModTime(),
		}

		if entry.IsDir() {
			fileInfo.Type = FileTypeDir
		} else if info.Mode()&os.ModeSymlink != 0 {
			fileInfo.Type = FileTypeSymlink
			fileInfo.IsLink = true
		} else {
			fileInfo.Type = FileTypeFile
		}

		result = append(result, fileInfo)
	}

	return result, nil
}

// MakeDir creates a directory.
func (m *Manager) MakeDir(path string, perm os.FileMode, recursive bool) error {
	cleanPath := m.sanitizePath(path)

	if recursive {
		if err := os.MkdirAll(cleanPath, perm); err != nil {
			if os.IsPermission(err) {
				return ErrPermissionDenied
			}
			if isPathNotDir(err, cleanPath) {
				return ErrPathNotDir
			}
			return err
		}
		return nil
	}

	if err := os.Mkdir(cleanPath, perm); err != nil {
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		if os.IsExist(err) {
			isDir, statErr := pathIsDir(cleanPath)
			if statErr == nil && isDir {
				return ErrPathAlreadyExists
			}
			return ErrPathNotDir
		}
		if errors.Is(err, syscall.ENOTDIR) {
			return ErrPathNotDir
		}
		return err
	}
	return nil
}

// Move moves/renames a file or directory.
func (m *Manager) Move(src, dst string) error {
	cleanSrc := m.sanitizePath(src)
	cleanDst := m.sanitizePath(dst)

	// Ensure destination directory exists
	dstDir := filepath.Dir(cleanDst)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		if isPathNotDir(err, dstDir) {
			return ErrPathNotDir
		}
		return err
	}

	if err := os.Rename(cleanSrc, cleanDst); err != nil {
		if os.IsNotExist(err) {
			return ErrFileNotFound
		}
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		return err
	}
	return nil
}

// Remove removes a file or directory.
func (m *Manager) Remove(path string) error {
	cleanPath := m.sanitizePath(path)

	if err := os.RemoveAll(cleanPath); err != nil {
		if os.IsPermission(err) {
			return ErrPermissionDenied
		}
		return err
	}
	return nil
}

// WatchDir starts watching a directory for changes.
func (m *Manager) WatchDir(path string, recursive bool) (*Watcher, error) {
	cleanPath := m.sanitizePath(path)

	return m.watcherMgr.WatchDir(cleanPath, recursive)
}

// SubscribeWatch starts watching a directory and forwards events to handler.
func (m *Manager) SubscribeWatch(path string, recursive bool, handler func(WatchEvent)) (*Watcher, func() error, error) {
	if handler == nil {
		return nil, nil, fmt.Errorf("watch handler is required")
	}

	watcher, err := m.WatchDir(path, recursive)
	if err != nil {
		return nil, nil, err
	}

	go func(w *Watcher) {
		for event := range w.EventChan {
			handler(m.displayEvent(event))
		}
	}(watcher)

	unsubscribe := func() error {
		return m.UnwatchDir(watcher.ID)
	}

	return watcher, unsubscribe, nil
}

// UnwatchDir stops watching a directory.
func (m *Manager) UnwatchDir(watchID string) error {
	return m.watcherMgr.UnwatchDir(watchID)
}

// Emit broadcasts an external event to watchers.
func (m *Manager) Emit(event WatchEvent) {
	event.Path = m.resolvePath(event.Path)
	if event.OldPath != "" {
		event.OldPath = m.resolvePath(event.OldPath)
	}
	m.watcherMgr.Emit(event)
}

// GetRootPath returns the root path.
func (m *Manager) GetRootPath() string {
	return m.rootPath
}

// Close closes the file manager.
func (m *Manager) Close() error {
	return m.watcherMgr.Close()
}
