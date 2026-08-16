//go:build !linux

package proxy

import (
	"fmt"
	"net"
)

func originalDst(_ net.Conn) (net.IP, int, error) {
	return nil, 0, fmt.Errorf("original dst is only supported on linux")
}
