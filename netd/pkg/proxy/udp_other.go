//go:build !linux

package proxy

import "net"

func readUDPDatagram(conn *net.UDPConn, buf []byte) (int, *net.UDPAddr, net.IP, int, error) {
	n, src, err := conn.ReadFromUDP(buf)
	if err != nil {
		return 0, nil, nil, 0, err
	}
	return n, src, nil, 0, nil
}
