package portal

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"go.uber.org/zap"
	"golang.org/x/sys/unix"
)

const defaultKubeletPodsRoot = "/var/lib/kubelet/pods"
const kubeletCSIVolumeDir = "kubernetes.io~csi"
const sandboxVolumeNamePrefix = "sandbox0-volume-"

type staleMountCleaner func(string) error

func defaultStaleMountCleaner(path string) error {
	if err := unix.Unmount(path, unix.MNT_DETACH); err != nil &&
		!errors.Is(err, unix.EINVAL) &&
		!errors.Is(err, unix.ENOENT) {
		return fmt.Errorf("lazy unmount stale CSI mount: %w", err)
	}
	if err := os.RemoveAll(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale CSI mount: %w", err)
	}
	return nil
}

func (m *Manager) CleanupStaleCSIMounts(ctx context.Context) error {
	if m == nil || strings.TrimSpace(m.kubeletPodsRoot) == "" {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	root := filepath.Clean(m.kubeletPodsRoot)
	cleaner := m.staleMountCleaner
	if cleaner == nil {
		cleaner = defaultStaleMountCleaner
	}

	var firstErr error
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if isSandboxCSIMountPath(root, path) {
				if cleanupErr := cleaner(path); cleanupErr != nil {
					if firstErr == nil {
						firstErr = cleanupErr
					}
					if m.logger != nil {
						m.logger.Warn("ctld stale CSI mount cleanup failed", zap.String("path", path), zap.Error(cleanupErr))
					}
					return filepath.SkipDir
				}
				if m.logger != nil {
					m.logger.Info("ctld cleaned stale CSI mount", zap.String("path", path), zap.Error(err))
				}
				return filepath.SkipDir
			}
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount scan skipped path", zap.String("path", path), zap.Error(err))
			}
			return nil
		}
		if d == nil || !d.IsDir() || filepath.Base(path) != "mount" {
			return nil
		}
		if !isSandboxCSIMountPath(root, path) {
			return nil
		}
		if err := cleaner(path); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount cleanup failed", zap.String("path", path), zap.Error(err))
			}
			return filepath.SkipDir
		}
		if m.logger != nil {
			m.logger.Info("ctld cleaned stale CSI mount", zap.String("path", path))
		}
		return filepath.SkipDir
	})
	if walkErr != nil {
		return walkErr
	}
	return firstErr
}

func isSandboxCSIMountPath(root, path string) bool {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 5 {
		return false
	}
	return parts[1] == "volumes" &&
		parts[2] == kubeletCSIVolumeDir &&
		strings.HasPrefix(parts[3], sandboxVolumeNamePrefix) &&
		parts[4] == "mount"
}
