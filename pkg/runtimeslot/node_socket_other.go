//go:build !linux

package runtimeslot

import (
	"fmt"
	"net"

	"github.com/containerd/errdefs"
)

func validateSecureNodeSocket(string, uint32) error {
	return fmt.Errorf("node control sockets require Linux ownership checks: %w", errdefs.ErrNotImplemented)
}

func validateNodeSocketPeer(net.Conn, uint32) error {
	return fmt.Errorf("node control peer credentials require Linux: %w", errdefs.ErrNotImplemented)
}
