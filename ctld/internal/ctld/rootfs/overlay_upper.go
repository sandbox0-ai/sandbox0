package rootfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
	"github.com/sandbox0-ai/sandbox0/pkg/rootfshead"
)

func (r *ContainerdRuntime) activeOverlayUpperdir(ctx context.Context, client containerdClient, info ctldapi.RootFSInfo) (string, error) {
	snapshotterName := strings.TrimSpace(info.Snapshotter)
	if snapshotterName != rootfshead.SnapshotterName {
		return "", fmt.Errorf("%w: rootfs sync requires snapshotter %q", ErrBadRequest, rootfshead.SnapshotterName)
	}
	snapshotter := client.SnapshotService(snapshotterName)
	if snapshotter == nil {
		return "", fmt.Errorf("rootfs snapshotter is not configured")
	}
	snapshotInfo, err := snapshotter.Stat(ctx, info.SnapshotKey)
	if err != nil {
		return "", fmt.Errorf("inspect rootfs snapshot: %w", err)
	}
	if snapshotInfo.Kind != snapshots.KindActive {
		return "", fmt.Errorf("%w: rootfs snapshot %s is not active", ErrBadRequest, info.SnapshotKey)
	}
	mounts, err := snapshotter.Mounts(ctx, info.SnapshotKey)
	if err != nil {
		return "", fmt.Errorf("inspect rootfs mounts: %w", err)
	}
	upperdir, ok := overlayUpperdir(mounts)
	if !ok {
		return "", fmt.Errorf("%w: overlayfs upperdir is not available", ErrBadRequest)
	}
	return r.mountedContainerdDataPath(upperdir)
}

func overlayUpperdir(mounts []mount.Mount) (string, bool) {
	for _, current := range mounts {
		if current.Type != "overlay" && current.Type != "fuse-overlayfs" {
			continue
		}
		for _, option := range current.Options {
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
		info, err := os.Stat(candidate)
		if err == nil && info.IsDir() {
			return candidate, nil
		}
		if err == nil {
			lastErr = fmt.Errorf("%s is not a directory", candidate)
		} else {
			lastErr = err
		}
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
