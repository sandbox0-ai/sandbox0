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
	var carrier netlink.Link
	for _, link := range links {
		if link == nil || link.Attrs() == nil || link.Attrs().Flags&net.FlagLoopback != 0 {
			continue
		}
		if link.Attrs().Flags&net.FlagUp != 0 {
			liveLinks++
			carrier = link
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
	address, err := selectRoutableIPv4(addresses)
	if err != nil {
		return "", err
	}
	if liveLinks != 1 {
		return "", fmt.Errorf("unclaimed namespace must have exactly one live carrier link: %w", errdefs.ErrFailedPrecondition)
	}
	if err := inspectRoutedCarrier(handle, carrier); err != nil {
		return "", err
	}
	return address, nil
}

// A bridge's br_netfilter pass can lose the socket selected by UDP TPROXY
// before local delivery. Admit only a directly routed host veth. This check
// applies before a carrier is claimable; legacy claimed namespaces still need
// identity inspection during their authorized drain and physical cleanup.
func inspectRoutedCarrier(namespace netns.NsHandle, carrier netlink.Link) error {
	if carrier == nil || carrier.Type() != "veth" || carrier.Attrs().ParentIndex <= 0 {
		return fmt.Errorf("carrier must use a routed veth network: %w", errdefs.ErrFailedPrecondition)
	}
	host, err := netlink.NewHandle()
	if err != nil {
		return fmt.Errorf("open host network handle: %w", err)
	}
	defer host.Close()
	peer, err := host.LinkByIndex(carrier.Attrs().ParentIndex)
	if err != nil {
		return fmt.Errorf("resolve carrier host peer: %w: %w", err, errdefs.ErrFailedPrecondition)
	}
	namespaceID, err := host.GetNetNsIdByFd(int(namespace))
	if err != nil {
		return fmt.Errorf("resolve carrier peer namespace: %w", err)
	}
	return validateRoutedCarrierPeer(carrier, peer, namespaceID)
}

func validateRoutedCarrierPeer(carrier, peer netlink.Link, namespaceID int) error {
	if carrier == nil || carrier.Attrs() == nil || peer == nil || peer.Attrs() == nil ||
		carrier.Type() != "veth" || peer.Type() != "veth" || namespaceID < 0 ||
		peer.Attrs().Index != carrier.Attrs().ParentIndex ||
		peer.Attrs().ParentIndex != carrier.Attrs().Index || peer.Attrs().NetNsID != namespaceID {
		return fmt.Errorf("carrier host peer does not match the held namespace: %w", errdefs.ErrFailedPrecondition)
	}
	if peer.Attrs().MasterIndex != 0 {
		return fmt.Errorf("bridged or enslaved carrier peers cannot enforce UDP policy; use stock CNI ptp: %w", errdefs.ErrFailedPrecondition)
	}
	if peer.Attrs().Flags&net.FlagUp == 0 {
		return fmt.Errorf("carrier host peer is not up: %w", errdefs.ErrFailedPrecondition)
	}
	return nil
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
