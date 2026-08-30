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
	if _, err := selectRoutableIPv4(nil); !errdefs.IsUnavailable(err) {
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
