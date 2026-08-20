//go:build !linux

package slotnetwork

import (
	"fmt"

	"github.com/containerd/errdefs"
)

type unsupportedNamespaceInspector struct{}

func newNamespaceInspector(string) NamespaceInspector {
	return unsupportedNamespaceInspector{}
}

func (unsupportedNamespaceInspector) Inspect(string, string) (string, error) {
	return "", fmt.Errorf("runtime slot network namespace inspection requires Linux: %w", errdefs.ErrNotImplemented)
}
