//go:build linux

package slotnetwork

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/containerd/errdefs"
	"github.com/vishvananda/netlink"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// Run only in an isolated privileged Linux test container. No production
// namespace, device or allocation is used by this kernel-level acceptance.
func TestNamespaceInspectorAddressTransferIntegration(t *testing.T) {
	if os.Getenv("SANDBOX0_NETWORK_NAMESPACE_INTEGRATION") != "1" {
		t.Skip("requires an isolated privileged network namespace test container")
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	original, err := netns.Get()
	if err != nil {
		t.Fatal(err)
	}
	defer original.Close()
	name := fmt.Sprintf("s0-netstack-%d-%d", os.Getpid(), time.Now().UnixNano())
	handle, err := netns.NewNamed(name)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := netns.Set(original); err != nil {
			t.Errorf("restore namespace: %v", err)
		}
		_ = handle.Close()
		if err := netns.DeleteNamed(name); err != nil {
			t.Errorf("delete owned namespace: %v", err)
		}
	}()
	if err := netns.Set(original); err != nil {
		t.Fatal(err)
	}
	links, err := netlink.NewHandleAt(handle)
	if err != nil {
		t.Fatal(err)
	}
	defer links.Close()
	if err := links.LinkAdd(&netlink.Veth{LinkAttrs: netlink.LinkAttrs{Name: "s0-test0"}, PeerName: "s0-test1"}); err != nil {
		t.Fatal(err)
	}
	link, err := links.LinkByName("s0-test0")
	if err != nil {
		t.Fatal(err)
	}
	if err := links.LinkSetUp(link); err != nil {
		t.Fatal(err)
	}
	address, err := netlink.ParseAddr("192.0.2.8/24")
	if err != nil {
		t.Fatal(err)
	}
	if err := links.AddrAdd(link, address); err != nil {
		t.Fatal(err)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(int(handle), &stat); err != nil {
		t.Fatal(err)
	}
	identity := fmt.Sprintf("netns-v1:%x:%x", uint64(stat.Dev), stat.Ino)
	root, err := filepath.EvalSymlinks("/var/run/netns")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, name)
	inspector := newNamespaceInspector(root)
	if got, err := inspector.Inspect(path, identity); err != nil || got != "192.0.2.8" {
		t.Fatalf("initial address = %q, %v", got, err)
	}
	// Stock runsc removes this host address while its AF_PACKET carrier stays.
	if err := links.AddrDel(link, address); err != nil {
		t.Fatal(err)
	}
	if _, err := inspector.Inspect(path, identity); !errors.Is(err, errExactNamespaceUnroutable) {
		t.Fatalf("unclaimed empty address = %v", err)
	}
	if err := inspector.InspectClaimed(path, identity, "192.0.2.8"); err != nil {
		t.Fatalf("transferred address = %v", err)
	}
	if err := inspector.InspectClaimed(path, "netns-v1:1:2", "192.0.2.8"); !errors.Is(err, errExactNamespaceAbsent) {
		t.Fatalf("changed identity = %v", err)
	}
	other, err := netlink.ParseAddr("192.0.2.9/24")
	if err != nil {
		t.Fatal(err)
	}
	if err := links.AddrAdd(link, other); err != nil {
		t.Fatal(err)
	}
	if err := inspector.InspectClaimed(path, identity, "192.0.2.8"); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("changed address = %v", err)
	}
	if err := links.LinkDel(link); err != nil {
		t.Fatal(err)
	}
	if err := inspector.InspectClaimed(path, identity, "192.0.2.8"); !errors.Is(err, errExactNamespaceUnroutable) {
		t.Fatalf("removed carrier = %v", err)
	}
}
