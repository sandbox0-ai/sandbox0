//go:build !linux

package runtimeslot

import (
	"fmt"

	"github.com/containerd/errdefs"
)

func validateSecureNodeSocket(string, uint32) error {
	return fmt.Errorf("node control sockets require Linux ownership checks: %w", errdefs.ErrNotImplemented)
}
