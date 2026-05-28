//go:build linux

package proxy

import (
	"context"
	"fmt"
	"net"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

func transparentListenConfig() net.ListenConfig {
	return net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				if err := setTransparentSocketOptions(fd); err != nil {
					controlErr = err
					return
				}
				if strings.HasPrefix(network, "udp") {
					if err := unix.SetsockoptInt(int(fd), unix.SOL_IP, unix.IP_RECVORIGDSTADDR, 1); err != nil {
						controlErr = err
						return
					}
					if err := unix.SetsockoptInt(int(fd), unix.SOL_IP, unix.IP_PKTINFO, 1); err != nil {
						controlErr = err
						return
					}
				}
			})
			if err != nil {
				return err
			}
			return controlErr
		},
	}
}

func setTransparentSocketOptions(fd uintptr) error {
	if err := unix.SetsockoptInt(int(fd), unix.SOL_IP, unix.IP_TRANSPARENT, 1); err != nil {
		return err
	}
	if err := unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return err
	}
	return nil
}

func listenTCPTransparent(addr string) (net.Listener, error) {
	cfg := transparentListenConfig()
	return cfg.Listen(context.Background(), "tcp4", addr)
}

func listenUDPTransparent(addr string) (*net.UDPConn, error) {
	cfg := transparentListenConfig()
	pc, err := cfg.ListenPacket(context.Background(), "udp4", addr)
	if err != nil {
		return nil, err
	}
	udpConn, ok := pc.(*net.UDPConn)
	if !ok {
		_ = pc.Close()
		return nil, fmt.Errorf("listen packet is not udp")
	}
	return udpConn, nil
}

func dialUDPTransparent(local *net.UDPAddr, remote *net.UDPAddr) (udpReplyConn, error) {
	dialer := net.Dialer{
		LocalAddr: local,
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			err := c.Control(func(fd uintptr) {
				controlErr = setTransparentSocketOptions(fd)
			})
			if err != nil {
				return err
			}
			return controlErr
		},
	}
	conn, err := dialer.Dial("udp4", remote.String())
	if err != nil {
		return nil, err
	}
	udpConn, ok := conn.(*net.UDPConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("transparent udp dial returned %T", conn)
	}
	return udpConn, nil
}
