//go:build !linux

package proxy

import (
	"context"
	"net"
)

func listenTCPTransparent(addr string) (net.Listener, error) {
	cfg := net.ListenConfig{}
	return cfg.Listen(context.Background(), "tcp", addr)
}

func listenUDPTransparent(addr string) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP("udp", udpAddr)
}
