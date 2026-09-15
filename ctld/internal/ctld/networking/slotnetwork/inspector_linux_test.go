//go:build linux

package slotnetwork

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/containerd/errdefs"
)

func TestNamespaceInspectorClassifiesMissingExactIncarnation(t *testing.T) {
	root := t.TempDir()
	_, err := newNamespaceInspector(root).Inspect(filepath.Join(root, "missing"), "netns-v1:1:2")
	if !errors.Is(err, errExactNamespaceAbsent) || !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("missing namespace error = %v", err)
	}
}

func TestSelectRoutableIPv4ClassifiesNetworkReadiness(t *testing.T) {
	if _, err := selectRoutableIPv4(nil); !errdefs.IsUnavailable(err) || !errors.Is(err, errExactNamespaceUnroutable) {
		t.Fatalf("missing address error = %v", err)
	}
	if _, err := selectRoutableIPv4(map[string]struct{}{
		"192.0.2.8": {},
		"192.0.2.9": {},
	}); !errdefs.IsFailedPrecondition(err) {
		t.Fatalf("ambiguous address error = %v", err)
	}
	address, err := selectRoutableIPv4(map[string]struct{}{"192.0.2.8": {}})
	if err != nil || address != "192.0.2.8" {
		t.Fatalf("selected address = %q, %v", address, err)
	}
}

func TestSelectClaimedIPv4RequiresCarrierAfterAddressTransfer(t *testing.T) {
	for name, tc := range map[string]struct {
		addresses map[string]struct{}
		links     int
		ok        bool
	}{
		"before transfer":     {map[string]struct{}{"192.0.2.8": {}}, 1, true},
		"netstack transfer":   {nil, 1, true},
		"CNI removed link":    {nil, 0, false},
		"extra link":          {nil, 2, false},
		"address changed":     {map[string]struct{}{"192.0.2.9": {}}, 1, false},
		"ambiguous addresses": {map[string]struct{}{"192.0.2.8": {}, "192.0.2.9": {}}, 1, false},
	} {
		t.Run(name, func(t *testing.T) {
			address, err := selectClaimedIPv4(tc.addresses, tc.links, "192.0.2.8")
			if tc.ok {
				if err != nil || address != "192.0.2.8" {
					t.Fatalf("claimed address = %q, %v", address, err)
				}
			} else if !errdefs.IsFailedPrecondition(err) {
				t.Fatalf("changed carrier = %q, %v", address, err)
			}
		})
	}
}
