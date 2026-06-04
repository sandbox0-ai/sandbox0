package handlers

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const rootfsBootstrapMarker = ".sandbox0/rootfs-bootstrap.json"

type rootfsBootstrapMarkerData struct {
	SourceRoot     string    `json:"source_root"`
	Status         string    `json:"status"`
	BootstrappedAt time.Time `json:"bootstrapped_at"`
}

func (h *InitializeHandler) bootstrapRootfs(rootPath string) error {
	sourceRoot := h.rootfsBootstrapSource
	if strings.TrimSpace(sourceRoot) == "" {
		sourceRoot = "/"
	}
	return bootstrapRootfsFrom(sourceRoot, rootPath)
}

func bootstrapRootfsFrom(sourceRoot, targetRoot string) error {
	sourceRoot = filepath.Clean(strings.TrimSpace(sourceRoot))
	targetRoot = filepath.Clean(strings.TrimSpace(targetRoot))
	if sourceRoot == "" || sourceRoot == "." {
		return fmt.Errorf("rootfs bootstrap source is required")
	}
	if targetRoot == "" || targetRoot == "." || targetRoot == string(filepath.Separator) {
		return fmt.Errorf("rootfs bootstrap target is invalid")
	}
	sourceAbs, err := filepath.Abs(sourceRoot)
	if err != nil {
		return fmt.Errorf("resolve rootfs bootstrap source: %w", err)
	}
	targetAbs, err := filepath.Abs(targetRoot)
	if err != nil {
		return fmt.Errorf("resolve rootfs bootstrap target: %w", err)
	}
	if sourceAbs == targetAbs {
		return fmt.Errorf("rootfs bootstrap source and target must differ")
	}
	if err := os.MkdirAll(targetAbs, 0o755); err != nil {
		return fmt.Errorf("create rootfs target: %w", err)
	}
	if rootfsBootstrapMarkerExists(targetAbs) {
		return nil
	}

	if err := filepath.WalkDir(sourceAbs, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if os.IsPermission(walkErr) {
				if entry != nil && entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			return walkErr
		}
		if path == sourceAbs {
			return nil
		}

		rel, err := filepath.Rel(sourceAbs, path)
		if err != nil {
			return err
		}
		if shouldSkipRootfsBootstrapPath(path, rel, targetAbs) {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		info, err := entry.Info()
		if err != nil {
			if os.IsPermission(err) {
				if entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			return err
		}
		dst := filepath.Join(targetAbs, rel)
		mode := info.Mode()
		switch {
		case mode&os.ModeSymlink != 0:
			return copyRootfsSymlink(path, dst)
		case mode.IsDir():
			return copyRootfsDirectory(dst, mode)
		case mode.IsRegular():
			return copyRootfsRegularFile(path, dst, info)
		default:
			return nil
		}
	}); err != nil {
		return fmt.Errorf("copy rootfs bootstrap content: %w", err)
	}
	if err := ensureRootfsRuntimeDirs(targetAbs); err != nil {
		return err
	}
	return writeRootfsBootstrapMarker(targetAbs, sourceAbs, "bootstrapped")
}

func rootfsBootstrapMarkerExists(targetRoot string) bool {
	_, err := os.Stat(filepath.Join(targetRoot, rootfsBootstrapMarker))
	return err == nil
}

func shouldSkipRootfsBootstrapPath(path, rel, targetRoot string) bool {
	path = filepath.Clean(path)
	targetRoot = filepath.Clean(targetRoot)
	if path == targetRoot || strings.HasPrefix(path, targetRoot+string(filepath.Separator)) {
		return true
	}

	rel = "/" + filepath.ToSlash(filepath.Clean(rel))
	for _, skipped := range []string{
		"/dev",
		"/proc",
		"/run",
		"/sys",
		"/tmp",
		"/var/lib/sandbox0",
	} {
		if rel == skipped || strings.HasPrefix(rel, skipped+"/") {
			return true
		}
	}
	return false
}

func copyRootfsDirectory(dst string, mode os.FileMode) error {
	if err := os.MkdirAll(dst, mode.Perm()); err != nil {
		return err
	}
	return os.Chmod(dst, mode.Perm())
}

func copyRootfsSymlink(src, dst string) error {
	target, err := os.Readlink(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if _, err := os.Lstat(dst); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Symlink(target, dst)
}

func copyRootfsRegularFile(src, dst string, info os.FileInfo) error {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	if _, err := os.Lstat(dst); err == nil {
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		if os.IsPermission(err) {
			return nil
		}
		return err
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, info.Mode().Perm())
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Chmod(dst, info.Mode().Perm()); err != nil {
		return err
	}
	return os.Chtimes(dst, info.ModTime(), info.ModTime())
}

func ensureRootfsRuntimeDirs(targetRoot string) error {
	for _, dir := range []struct {
		path string
		mode os.FileMode
	}{
		{path: "dev", mode: 0o755},
		{path: "proc", mode: 0o555},
		{path: "run", mode: 0o755},
		{path: "sys", mode: 0o555},
		{path: "tmp", mode: os.ModeSticky | 0o777},
	} {
		fullPath := filepath.Join(targetRoot, dir.path)
		if err := os.MkdirAll(fullPath, dir.mode); err != nil {
			return err
		}
		if err := os.Chmod(fullPath, dir.mode); err != nil {
			return err
		}
	}
	return nil
}

func writeRootfsBootstrapMarker(targetRoot, sourceRoot, status string) error {
	dir := filepath.Join(targetRoot, ".sandbox0")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	payload, err := json.Marshal(rootfsBootstrapMarkerData{
		SourceRoot:     sourceRoot,
		Status:         status,
		BootstrappedAt: time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(targetRoot, rootfsBootstrapMarker), append(payload, '\n'), 0o644)
}
