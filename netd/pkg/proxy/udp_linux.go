//go:build linux

package proxy

import (
	"encoding/binary"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

func readUDPDatagram(conn *net.UDPConn, buf []byte) (int, *net.UDPAddr, net.IP, int, error) {
	oob := make([]byte, 128)
	n, oobn, _, src, err := conn.ReadMsgUDP(buf, oob)
	if err != nil {
		return 0, nil, nil, 0, err
	}
	dstIP, dstPort := parseOriginalDstFromOOB(oob[:oobn])
	return n, src, dstIP, dstPort, nil
}

func parseOriginalDstFromOOB(oob []byte) (net.IP, int) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return nil, 0
	}
	for _, msg := range msgs {
		if msg.Header.Level != unix.SOL_IP {
			continue
		}
		switch msg.Header.Type {
		case unix.IP_RECVORIGDSTADDR:
			if ip, port, ok := parseIPv4OriginalDst(msg.Data); ok {
				return ip, port
			}
		}
	}
	return nil, 0
}

func parseIPv4OriginalDst(data []byte) (net.IP, int, bool) {
	if len(data) < unix.SizeofSockaddrInet4 {
		return nil, 0, false
	}
	addr := (*unix.RawSockaddrInet4)(unsafe.Pointer(&data[0]))
	ip := net.IP(addr.Addr[:])
	port := int(binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&addr.Port))[:]))
	if ip == nil || port <= 0 {
		return nil, 0, false
	}
	return ip, port, true
}

func parseIPv4PktInfo(data []byte) (net.IP, bool) {
	if len(data) < unix.SizeofInet4Pktinfo {
		return nil, false
	}
	info := (*unix.Inet4Pktinfo)(unsafe.Pointer(&data[0]))
	ip := net.IP(info.Spec_dst[:])
	if ip == nil || ip.IsUnspecified() {
		ip = net.IP(info.Addr[:])
	}
	if ip == nil || ip.IsUnspecified() {
		return nil, false
	}
	return ip, true
}
