package rootfs

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

// These paths belong to a fresh sandbox runtime, not its persistent rootfs.
var defaultRootFSSnapshotExcludedPaths = []string{"/procd", "/tmp"}

func (r *ContainerdRuntime) activeOverlayUpperdir(ctx context.Context, client containerdClient, info ctldapi.RootFSInfo) (string, error) {
	if !supportsOverlayRootFS(info.Snapshotter) {
		return "", fmt.Errorf("%w: rootfs checkpoint requires an overlay-compatible snapshotter", ErrBadRequest)
	}
	snapshotter := client.SnapshotService(info.Snapshotter)
	if snapshotter == nil {
		return "", fmt.Errorf("overlay-compatible snapshotter %q is not configured", info.Snapshotter)
	}
	snapshotInfo, err := snapshotter.Stat(ctx, info.SnapshotKey)
	if err != nil {
		return "", fmt.Errorf("inspect overlayfs snapshot: %w", err)
	}
	if snapshotInfo.Kind != snapshots.KindActive {
		return "", fmt.Errorf("%w: rootfs snapshot %s is not active", ErrBadRequest, info.SnapshotKey)
	}
	mounts, err := snapshotter.Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return "", fmt.Errorf("inspect overlayfs mounts: %w", err)
	}
	upperdir, ok := overlayUpperdir(mounts)
	if !ok {
		return "", fmt.Errorf("%w: overlayfs upperdir is not available", ErrBadRequest)
	}
	mountedUpperdir, err := r.mountedContainerdDataPath(upperdir)
	if err != nil {
		return "", err
	}
	return mountedUpperdir, nil
}

func supportsOverlayRootFS(snapshotter string) bool {
	switch strings.TrimSpace(snapshotter) {
	case "overlayfs", rootfshead.SnapshotterName:
		return true
	default:
		return false
	}
}

func overlayUpperdir(mounts []mount.Mount) (string, bool) {
	for _, candidate := range mounts {
		if candidate.Type != "overlay" && candidate.Type != "fuse-overlayfs" {
			continue
		}
		for _, option := range candidate.Options {
			if strings.HasPrefix(option, "upperdir=") {
				upperdir := strings.TrimSpace(strings.TrimPrefix(option, "upperdir="))
				return upperdir, upperdir != ""
			}
		}
	}
	return "", false
}

func (r *ContainerdRuntime) mountedContainerdDataPath(hostPath string) (string, error) {
	hostPath = filepath.Clean(strings.TrimSpace(hostPath))
	if hostPath == "." || hostPath == "" {
		return "", fmt.Errorf("overlayfs upperdir is empty")
	}
	candidates := []string{hostPath}
	if r != nil {
		if mapped, ok := rebasePath(hostPath, r.containerdHostDataRoot, r.containerdDataRoot); ok {
			candidates = append([]string{mapped}, candidates...)
		}
	}
	seen := make(map[string]struct{}, len(candidates))
	var lastErr error
	for _, candidate := range candidates {
		candidate = filepath.Clean(candidate)
		if _, ok := seen[candidate]; ok {
			continue
		}
		seen[candidate] = struct{}{}
		stat, err := os.Stat(candidate)
		if err == nil && stat.IsDir() {
			return candidate, nil
		}
		if err == nil {
			lastErr = fmt.Errorf("%s is not a directory", candidate)
			continue
		}
		lastErr = err
	}
	return "", fmt.Errorf("overlayfs upperdir %s is not readable from ctld: %w", hostPath, lastErr)
}

func rebasePath(value, fromRoot, toRoot string) (string, bool) {
	value = filepath.Clean(strings.TrimSpace(value))
	fromRoot = filepath.Clean(strings.TrimSpace(fromRoot))
	toRoot = filepath.Clean(strings.TrimSpace(toRoot))
	if value == "" || fromRoot == "" || toRoot == "" || fromRoot == "." || toRoot == "." {
		return "", false
	}
	if value != fromRoot && !strings.HasPrefix(value, fromRoot+string(filepath.Separator)) {
		return "", false
	}
	relative, err := filepath.Rel(fromRoot, value)
	if err != nil {
		return "", false
	}
	return filepath.Join(toRoot, relative), true
}

func filterRootFSPortalPaths(paths []ctldapi.RootFSPortalPath, excludedPaths []string) []ctldapi.RootFSPortalPath {
	seen := make(map[string]struct{}, len(paths))
	out := make([]ctldapi.RootFSPortalPath, 0, len(paths))
	for _, portal := range paths {
		mountPath := cleanRootFSPath(portal.MountPath)
		backingPath := filepath.Clean(strings.TrimSpace(portal.BackingPath))
		if mountPath == "/" || backingPath == "" || backingPath == "." || rootFSPathExcluded(mountPath, excludedPaths) {
			continue
		}
		key := mountPath + "\x00" + backingPath
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		portal.MountPath = mountPath
		portal.BackingPath = backingPath
		out = append(out, portal)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].MountPath == out[j].MountPath {
			return out[i].BackingPath < out[j].BackingPath
		}
		return out[i].MountPath < out[j].MountPath
	})
	return out
}

func rootFSExcludedPathsWithPortals(excludedPaths []string, portalPaths []ctldapi.RootFSPortalPath) []string {
	out := append([]string(nil), defaultRootFSSnapshotExcludedPaths...)
	out = append(out, excludedPaths...)
	for _, portal := range filterRootFSPortalPaths(portalPaths, excludedPaths) {
		out = append(out, portal.MountPath)
	}
	return out
}

func rootFSPathExcluded(value string, extra []string) bool {
	value = cleanRootFSPath(value)
	paths := append(append([]string(nil), defaultRootFSSnapshotExcludedPaths...), extra...)
	for _, excluded := range paths {
		excluded = cleanRootFSPath(excluded)
		if excluded == "/" || value == excluded || strings.HasPrefix(value, excluded+"/") {
			return true
		}
	}
	return false
}

func cleanRootFSPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	return path.Clean("/" + strings.TrimPrefix(value, "/"))
}
