package rootfs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/moby/sys/mountinfo"
	"github.com/sandbox0-ai/sandbox0/pkg/ctldapi"
)

const containerdTaskRootDirectory = "io.containerd.runtime.v2.task"

// ActiveMergedRoot returns the propagated containerd task mount used as the
// event source for rootfs capture. Content is still read from expectedUpper.
func (r *ContainerdRuntime) ActiveMergedRoot(_ context.Context, info ctldapi.RootFSInfo, expectedUpper string) (string, error) {
	root, err := activeMergedRootPath(r.containerdEndpoint, r.namespace, info.ContainerID)
	if err != nil {
		return "", err
	}
	stat, err := os.Stat(root)
	if err != nil {
		return "", fmt.Errorf("inspect active merged rootfs %s: %w", root, err)
	}
	if !stat.IsDir() {
		return "", fmt.Errorf("active merged rootfs %s is not a directory", root)
	}
	mounts, err := mountinfo.GetMounts(mountinfo.SingleEntryFilter(root))
	if err != nil {
		return "", fmt.Errorf("inspect active merged rootfs mount %s: %w", root, err)
	}
	if len(mounts) != 1 {
		return "", fmt.Errorf("active merged rootfs %s is not propagated into ctld", root)
	}
	if err := r.validateMergedRootMount(mounts[0], expectedUpper); err != nil {
		return "", err
	}
	return root, nil
}

func activeMergedRootPath(containerdEndpoint, namespace, containerID string) (string, error) {
	endpoint := strings.TrimSpace(containerdEndpoint)
	endpoint = strings.TrimPrefix(endpoint, "unix://")
	if endpoint == "" || !filepath.IsAbs(endpoint) {
		return "", fmt.Errorf("containerd endpoint %q is not an absolute path", containerdEndpoint)
	}
	namespace = strings.TrimSpace(namespace)
	containerID = normalizeContainerID(containerID)
	if !safePathComponent(namespace) {
		return "", fmt.Errorf("containerd namespace %q is invalid", namespace)
	}
	if !safePathComponent(containerID) {
		return "", fmt.Errorf("container id %q is invalid", containerID)
	}
	return filepath.Join(filepath.Dir(endpoint), containerdTaskRootDirectory, namespace, containerID, "rootfs"), nil
}

func safePathComponent(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value
}

func (r *ContainerdRuntime) validateMergedRootMount(current *mountinfo.Info, expectedUpper string) error {
	if current == nil {
		return fmt.Errorf("active merged rootfs mount is missing")
	}
	switch current.FSType {
	case "overlay", "fuse-overlayfs", "fuse.fuse-overlayfs":
	default:
		return fmt.Errorf("active merged rootfs %s uses %q instead of overlayfs", current.Mountpoint, current.FSType)
	}
	hostUpper, ok := mountOption(current.VFSOptions, "upperdir")
	if !ok {
		return fmt.Errorf("active merged rootfs %s has no overlayfs upperdir", current.Mountpoint)
	}
	mountedUpper, err := r.mountedContainerdDataPath(hostUpper)
	if err != nil {
		return fmt.Errorf("resolve active merged rootfs upperdir: %w", err)
	}
	expectedInfo, err := os.Stat(filepath.Clean(expectedUpper))
	if err != nil {
		return fmt.Errorf("inspect expected overlayfs upperdir %s: %w", expectedUpper, err)
	}
	mountedInfo, err := os.Stat(mountedUpper)
	if err != nil {
		return fmt.Errorf("inspect active merged rootfs upperdir %s: %w", mountedUpper, err)
	}
	if !os.SameFile(expectedInfo, mountedInfo) {
		return fmt.Errorf("active merged rootfs upperdir %s does not match snapshot upperdir %s", mountedUpper, expectedUpper)
	}
	return nil
}

func mountOption(options, name string) (string, bool) {
	prefix := name + "="
	for _, option := range strings.Split(options, ",") {
		if strings.HasPrefix(option, prefix) {
			value := strings.TrimSpace(strings.TrimPrefix(option, prefix))
			return value, value != ""
		}
	}
	return "", false
}
