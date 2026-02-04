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
		if msg.Header.Level != unix.SOL_IP || msg.Header.Type != unix.IP_RECVORIGDSTADDR {
			continue
		}
		if len(msg.Data) < unix.SizeofSockaddrInet4 {
			continue
		}
		addr := (*unix.RawSockaddrInet4)(unsafe.Pointer(&msg.Data[0]))
		ip := net.IP(addr.Addr[:])
		port := int(binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&addr.Port))[:]))
		return ip, port
	}
	return nil, 0
}
