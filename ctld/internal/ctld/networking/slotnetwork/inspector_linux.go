//go:build linux

package slotnetwork

import (
	"fmt"
	"net"
	"path/filepath"
	"sort"
	"strings"

	"github.com/containerd/errdefs"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

type namespaceInspector struct {
	root string
}

func newNamespaceInspector(root string) NamespaceInspector {
	return &namespaceInspector{root: root}
}

func (i *namespaceInspector) Inspect(path, expectedIdentity string) (string, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("resolve network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	relative, err := filepath.Rel(i.root, resolved)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("network namespace is outside the configured root: %w", errdefs.ErrPermissionDenied)
	}
	handle, err := netns.GetFromPath(resolved)
	if err != nil {
		return "", fmt.Errorf("open network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	defer handle.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(handle), &stat); err != nil {
		return "", fmt.Errorf("stat network namespace handle: %w", err)
	}
	identity := fmt.Sprintf("netns-v1:%x:%x", uint64(stat.Dev), stat.Ino)
	if identity != expectedIdentity {
		return "", fmt.Errorf("network namespace incarnation changed: %w", errdefs.ErrFailedPrecondition)
	}
	netlinkHandle, err := netlink.NewHandleAt(handle)
	if err != nil {
		return "", fmt.Errorf("open network namespace netlink handle: %w", err)
	}
	defer netlinkHandle.Close()
	links, err := netlinkHandle.LinkList()
	if err != nil {
		return "", fmt.Errorf("list network namespace links: %w", err)
	}
	addresses := make(map[string]struct{})
	for _, link := range links {
		if link == nil || link.Attrs() == nil || link.Attrs().Flags&net.FlagLoopback != 0 {
			continue
		}
		values, err := netlinkHandle.AddrList(link, netlink.FAMILY_V4)
		if err != nil {
			return "", fmt.Errorf("list network namespace addresses: %w", err)
		}
		for _, value := range values {
			if value.IP == nil || !value.IP.IsGlobalUnicast() {
				continue
			}
			addresses[value.IP.String()] = struct{}{}
		}
	}
	return selectRoutableIPv4(addresses)
}

func selectRoutableIPv4(addresses map[string]struct{}) (string, error) {
	values := make([]string, 0, len(addresses))
	for value := range addresses {
		values = append(values, value)
	}
	sort.Strings(values)
	if len(values) == 0 {
		return "", fmt.Errorf("network namespace does not have a routable IPv4 address yet: %w", errdefs.ErrUnavailable)
	}
	if len(values) != 1 {
		return "", fmt.Errorf("network namespace must have exactly one routable IPv4 address, got %v: %w", values, errdefs.ErrFailedPrecondition)
	}
	return values[0], nil
}
