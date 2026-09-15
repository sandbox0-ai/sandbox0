//go:build linux

package slotnetwork

import (
	"fmt"
	"net"
	"os"
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
	return i.inspect(path, expectedIdentity, "")
}

func (i *namespaceInspector) InspectClaimed(path, expectedIdentity, expectedSourceIP string) error {
	_, err := i.inspect(path, expectedIdentity, expectedSourceIP)
	return err
}

func (i *namespaceInspector) inspect(path, expectedIdentity, claimedSourceIP string) (string, error) {
	resolved, err := filepath.EvalSymlinks(filepath.Clean(path))
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("resolve network namespace: %w: %w: %w", err, errExactNamespaceAbsent, errdefs.ErrFailedPrecondition)
		}
		return "", fmt.Errorf("resolve network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	relative, err := filepath.Rel(i.root, resolved)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("network namespace is outside the configured root: %w", errdefs.ErrPermissionDenied)
	}
	handle, err := netns.GetFromPath(resolved)
	if err != nil {
		if os.IsNotExist(err) {
			return "", fmt.Errorf("open network namespace: %w: %w: %w", err, errExactNamespaceAbsent, errdefs.ErrFailedPrecondition)
		}
		return "", fmt.Errorf("open network namespace: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	defer handle.Close()
	var stat unix.Stat_t
	if err := unix.Fstat(int(handle), &stat); err != nil {
		return "", fmt.Errorf("stat network namespace handle: %w", err)
	}
	identity := fmt.Sprintf("netns-v1:%x:%x", uint64(stat.Dev), stat.Ino)
	if identity != expectedIdentity {
		return "", fmt.Errorf("network namespace incarnation changed: %w: %w", errExactNamespaceAbsent, errdefs.ErrFailedPrecondition)
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
	liveLinks := 0
	for _, link := range links {
		if link == nil || link.Attrs() == nil || link.Attrs().Flags&net.FlagLoopback != 0 {
			continue
		}
		if link.Attrs().Flags&net.FlagUp != 0 {
			liveLinks++
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
	if claimedSourceIP != "" {
		return selectClaimedIPv4(addresses, liveLinks, claimedSourceIP)
	}
	return selectRoutableIPv4(addresses)
}

// selectClaimedIPv4 preserves the original address after stock runsc transfers
// it into netstack. It still requires the exact carrier link and rejects any
// conflicting host address; CNI teardown cannot masquerade as a live guest.
func selectClaimedIPv4(addresses map[string]struct{}, liveLinks int, expected string) (string, error) {
	if liveLinks == 0 {
		return "", fmt.Errorf("claimed namespace lost its carrier link: %w: %w", errExactNamespaceUnroutable, errdefs.ErrFailedPrecondition)
	}
	if liveLinks != 1 {
		return "", fmt.Errorf("claimed namespace must have exactly one live carrier link: %w", errdefs.ErrFailedPrecondition)
	}
	if len(addresses) != 0 {
		address, err := selectRoutableIPv4(addresses)
		if err != nil {
			return "", err
		}
		if address != expected {
			return "", fmt.Errorf("claimed namespace source IP changed: %w", errdefs.ErrFailedPrecondition)
		}
	}
	return expected, nil
}

func selectRoutableIPv4(addresses map[string]struct{}) (string, error) {
	values := make([]string, 0, len(addresses))
	for value := range addresses {
		values = append(values, value)
	}
	sort.Strings(values)
	if len(values) == 0 {
		return "", fmt.Errorf(
			"network namespace does not have a routable IPv4 address yet: %w: %w",
			errExactNamespaceUnroutable,
			errdefs.ErrUnavailable,
		)
	}
	if len(values) != 1 {
		return "", fmt.Errorf("network namespace must have exactly one routable IPv4 address, got %v: %w", values, errdefs.ErrFailedPrecondition)
	}
	return values[0], nil
}
