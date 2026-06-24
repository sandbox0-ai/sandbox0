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
)

var defaultRootFSSnapshotExcludedPaths = []string{
	"/.wh..wh..opq",
	"/procd",
}

func (r *ContainerdRuntime) activeOverlayUpperdir(ctx context.Context, client containerdClient, info ctldapi.RootFSInfo) (string, error) {
	if strings.TrimSpace(info.Snapshotter) != "overlayfs" {
		return "", fmt.Errorf("%w: rootfs import requires overlayfs snapshotter", ErrBadRequest)
	}
	snapshotter := client.SnapshotService(info.Snapshotter)
	if snapshotter == nil {
		return "", fmt.Errorf("overlayfs snapshotter is not configured")
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

func overlayUpperdir(mounts []mount.Mount) (string, bool) {
	for _, m := range mounts {
		if m.Type != "overlay" && m.Type != "fuse-overlayfs" {
			continue
		}
		for _, option := range m.Options {
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
		st, err := os.Stat(candidate)
		if err == nil && st.IsDir() {
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

func rebasePath(path, fromRoot, toRoot string) (string, bool) {
	path = filepath.Clean(strings.TrimSpace(path))
	fromRoot = filepath.Clean(strings.TrimSpace(fromRoot))
	toRoot = filepath.Clean(strings.TrimSpace(toRoot))
	if path == "" || fromRoot == "" || toRoot == "" || fromRoot == "." || toRoot == "." {
		return "", false
	}
	if path != fromRoot && !strings.HasPrefix(path, fromRoot+string(filepath.Separator)) {
		return "", false
	}
	rel, err := filepath.Rel(fromRoot, path)
	if err != nil {
		return "", false
	}
	return filepath.Join(toRoot, rel), true
}

type rootFSPathFilter struct {
	excluded []string
}

func newRootFSPathFilter(extraPaths []string) rootFSPathFilter {
	seen := make(map[string]struct{}, len(defaultRootFSSnapshotExcludedPaths)+len(extraPaths))
	excluded := make([]string, 0, len(defaultRootFSSnapshotExcludedPaths)+len(extraPaths))
	add := func(value string) {
		clean := cleanRootFSPath(value)
		if clean == "/" {
			return
		}
		if _, ok := seen[clean]; ok {
			return
		}
		seen[clean] = struct{}{}
		excluded = append(excluded, clean)
	}
	for _, value := range defaultRootFSSnapshotExcludedPaths {
		add(value)
	}
	for _, value := range extraPaths {
		add(value)
	}
	return rootFSPathFilter{excluded: excluded}
}

func (f rootFSPathFilter) Excludes(changePath string) bool {
	clean := cleanRootFSPath(changePath)
	for _, excluded := range f.excluded {
		if clean == excluded || strings.HasPrefix(clean, excluded+"/") {
			return true
		}
	}
	return false
}

func filterRootFSPortalPaths(paths []ctldapi.RootFSPortalPath, excludedPaths []string) []ctldapi.RootFSPortalPath {
	if len(paths) == 0 {
		return nil
	}
	filter := newRootFSPathFilter(excludedPaths)
	seen := make(map[string]struct{}, len(paths))
	out := make([]ctldapi.RootFSPortalPath, 0, len(paths))
	for _, portal := range paths {
		mountPath := cleanRootFSPath(portal.MountPath)
		backingPath := filepath.Clean(strings.TrimSpace(portal.BackingPath))
		if mountPath == "/" || backingPath == "" || backingPath == "." || filter.Excludes(mountPath) {
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

func cleanRootFSPath(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "/"
	}
	return path.Clean("/" + strings.TrimPrefix(value, "/"))
}
