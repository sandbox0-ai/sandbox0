package server

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
)

const testProcdMaxFileSize = 100 * 1024 * 1024

var (
	errTestProcdFileNotFound      = errors.New("file not found")
	errTestProcdDirNotFound       = errors.New("directory not found")
	errTestProcdFileTooLarge      = errors.New("file too large")
	errTestProcdPermissionDenied  = errors.New("permission denied")
	errTestProcdPathAlreadyExists = errors.New("path already exists")
	errTestProcdPathNotDir        = errors.New("path is not a directory")
	errTestProcdPathNotFile       = errors.New("path is not a file")
)

type testProcdFileManager struct {
	root string
}

func newTestProcdFileManager(root string) (*testProcdFileManager, error) {
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	return &testProcdFileManager{root: root}, nil
}

func (m *testProcdFileManager) ReadFile(filePath string) ([]byte, error) {
	hostPath := m.hostPath(filePath)
	data, err := os.ReadFile(hostPath)
	if err != nil {
		return nil, mapTestProcdFileError(err, errTestProcdFileNotFound)
	}
	return data, nil
}

func (m *testProcdFileManager) WriteFile(filePath string, data []byte, mode os.FileMode) error {
	if len(data) > testProcdMaxFileSize {
		return errTestProcdFileTooLarge
	}
	if err := os.WriteFile(m.hostPath(filePath), data, mode); err != nil {
		return mapTestProcdFileError(err, errTestProcdFileNotFound)
	}
	return nil
}

func (m *testProcdFileManager) MakeDir(dirPath string, mode os.FileMode, recursive bool) error {
	hostPath := m.hostPath(dirPath)
	var err error
	if recursive {
		err = os.MkdirAll(hostPath, mode)
	} else {
		err = os.Mkdir(hostPath, mode)
	}
	if err != nil {
		return mapTestProcdFileError(err, errTestProcdDirNotFound)
	}
	return nil
}

func (m *testProcdFileManager) Remove(filePath string) error {
	if err := os.Remove(m.hostPath(filePath)); err != nil {
		return mapTestProcdFileError(err, errTestProcdFileNotFound)
	}
	return nil
}

func (m *testProcdFileManager) Move(source, destination string) error {
	if err := os.Rename(m.hostPath(source), m.hostPath(destination)); err != nil {
		return mapTestProcdFileError(err, errTestProcdFileNotFound)
	}
	return nil
}

func (m *testProcdFileManager) Stat(filePath string) (procdStatResponse, error) {
	logicalPath := normalizeTestProcdPath(filePath)
	return m.statLogical(logicalPath)
}

func (m *testProcdFileManager) ListDir(dirPath string) ([]procdStatResponse, error) {
	logicalPath := normalizeTestProcdPath(dirPath)
	entries, err := os.ReadDir(m.hostPath(logicalPath))
	if err != nil {
		return nil, mapTestProcdFileError(err, errTestProcdDirNotFound)
	}
	out := make([]procdStatResponse, 0, len(entries))
	for _, entry := range entries {
		childPath := entry.Name()
		if logicalPath != "." {
			childPath = path.Join(logicalPath, entry.Name())
		}
		stat, err := m.statLogical(childPath)
		if err != nil {
			return nil, err
		}
		out = append(out, stat)
	}
	return out, nil
}

func (m *testProcdFileManager) statLogical(logicalPath string) (procdStatResponse, error) {
	hostPath := m.hostPath(logicalPath)
	info, err := os.Lstat(hostPath)
	if err != nil {
		return procdStatResponse{}, mapTestProcdFileError(err, errTestProcdFileNotFound)
	}
	fileType := procdFileTypeFile
	isLink := false
	linkTarget := ""
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		fileType = "symlink"
		isLink = true
		linkTarget, _ = os.Readlink(hostPath)
	case info.IsDir():
		fileType = procdFileTypeDir
	}
	return procdStatResponse{
		Name:       path.Base(logicalPath),
		Path:       logicalPath,
		Type:       fileType,
		Size:       info.Size(),
		Mode:       modeString(info.Mode()),
		ModTime:    info.ModTime(),
		IsLink:     isLink,
		LinkTarget: linkTarget,
	}, nil
}

func (m *testProcdFileManager) hostPath(filePath string) string {
	logicalPath := normalizeTestProcdPath(filePath)
	if logicalPath == "." {
		return m.root
	}
	return filepath.Join(m.root, filepath.FromSlash(logicalPath))
}

func normalizeTestProcdPath(filePath string) string {
	trimmed := strings.TrimSpace(filePath)
	trimmed = strings.TrimPrefix(trimmed, "/")
	if trimmed == "" {
		return "."
	}
	cleaned := path.Clean(trimmed)
	if cleaned == "/" || cleaned == "." {
		return "."
	}
	return strings.TrimPrefix(cleaned, "../")
}

func mapTestProcdFileError(err error, notFound error) error {
	switch {
	case errors.Is(err, os.ErrPermission):
		return errTestProcdPermissionDenied
	case errors.Is(err, os.ErrExist):
		return errTestProcdPathAlreadyExists
	case errors.Is(err, os.ErrNotExist):
		return notFound
	case strings.Contains(err.Error(), "is a directory"):
		return errTestProcdPathNotFile
	case strings.Contains(err.Error(), "not a directory"):
		return errTestProcdPathNotDir
	default:
		return err
	}
}

func modeString(mode os.FileMode) string {
	return fmt.Sprintf("%04o", mode.Perm())
}
