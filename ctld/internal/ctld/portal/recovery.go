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
type staleMountChecker func(string) (bool, error)

// ActivePodUIDLister returns pod UIDs that still belong to live pods on this node.
type ActivePodUIDLister func(context.Context) (map[string]struct{}, error)

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

func defaultStaleMountChecker(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if errors.Is(err, unix.ENOTCONN) || errors.Is(err, unix.EIO) || errors.Is(err, unix.ESTALE) {
		return true, nil
	}
	return false, fmt.Errorf("check CSI mount target: %w", err)
}

func (m *Manager) cleanUnknownStaleMountTarget(targetPath string) error {
	root := defaultKubeletPodsRoot
	if m != nil && strings.TrimSpace(m.kubeletPodsRoot) != "" {
		root = m.kubeletPodsRoot
	}
	if !isSandboxCSIMountPath(root, targetPath) {
		return fmt.Errorf("refusing to clean unknown CSI target outside sandbox0 kubelet volume paths: %s", targetPath)
	}
	return m.cleanStaleMountTarget(targetPath)
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
	checker := m.staleMountChecker
	if checker == nil {
		checker = defaultStaleMountChecker
	}
	activePods, activeReliable := m.activePodUIDs(ctx)

	var firstErr error
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
			}
			if info, ok := sandboxCSIMountPathInfo(root, path); ok {
				if !shouldCleanSandboxCSIMount(info, activePods, activeReliable, true) {
					return filepath.SkipDir
				}
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
		info, _ := sandboxCSIMountPathInfo(root, path)
		broken, checkErr := checker(path)
		if checkErr != nil {
			if m.logger != nil {
				m.logger.Warn("ctld stale CSI mount health check failed", zap.String("path", path), zap.Error(checkErr))
			}
			return filepath.SkipDir
		}
		if !shouldCleanSandboxCSIMount(info, activePods, activeReliable, broken) {
			return filepath.SkipDir
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

func (m *Manager) activePodUIDs(ctx context.Context) (map[string]struct{}, bool) {
	if m == nil || m.activePodUIDLister == nil {
		return nil, false
	}
	activePods, err := m.activePodUIDLister(ctx)
	if err != nil {
		if m.logger != nil {
			m.logger.Warn("ctld stale CSI mount cleanup could not list active pods", zap.Error(err))
		}
		return nil, false
	}
	if activePods == nil {
		activePods = map[string]struct{}{}
	}
	return activePods, true
}

type csiMountPathInfo struct {
	podUID string
}

func shouldCleanSandboxCSIMount(info csiMountPathInfo, activePods map[string]struct{}, activeReliable bool, broken bool) bool {
	if broken {
		return true
	}
	if info.podUID != "" && activeReliable {
		_, active := activePods[info.podUID]
		return !active
	}
	return false
}

func isSandboxCSIMountPath(root, path string) bool {
	_, ok := sandboxCSIMountPathInfo(root, path)
	return ok
}

func sandboxCSIMountPathInfo(root, path string) (csiMountPathInfo, bool) {
	rel, err := filepath.Rel(filepath.Clean(root), filepath.Clean(path))
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return csiMountPathInfo{}, false
	}
	parts := strings.Split(rel, string(os.PathSeparator))
	if len(parts) != 5 {
		return csiMountPathInfo{}, false
	}
	if parts[1] == "volumes" &&
		parts[2] == kubeletCSIVolumeDir &&
		strings.HasPrefix(parts[3], sandboxVolumeNamePrefix) &&
		parts[4] == "mount" {
		return csiMountPathInfo{podUID: parts[0]}, true
	}
	return csiMountPathInfo{}, false
}
