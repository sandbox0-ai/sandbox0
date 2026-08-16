//go:build linux

package proxy

import (
	"encoding/binary"
	"fmt"
	"net"
	"unsafe"

	"golang.org/x/sys/unix"
)

func originalDst(conn net.Conn) (net.IP, int, error) {
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		return nil, 0, fmt.Errorf("connection is not tcp")
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		return nil, 0, err
	}
	var (
		ip   net.IP
		port int
	)
	var controlErr error
	err = rawConn.Control(func(fd uintptr) {
		addr, portValue, err := getOriginalDstIPv4(fd, unix.IPPROTO_IP)
		if err != nil {
			controlErr = err
			return
		}
		ip = addr
		port = portValue
	})
	if err != nil {
		return nil, 0, err
	}
	if controlErr != nil {
		return nil, 0, controlErr
	}
	if ip == nil {
		return nil, 0, fmt.Errorf("original destination not found")
	}
	return ip, port, nil
}

func getOriginalDstIPv4(fd uintptr, level int) (net.IP, int, error) {
	var addr unix.RawSockaddrInet4
	addrLen := uint32(unsafe.Sizeof(addr))
	_, _, errno := unix.Syscall6(
		unix.SYS_GETSOCKOPT,
		fd,
		uintptr(level),
		uintptr(unix.SO_ORIGINAL_DST),
		uintptr(unsafe.Pointer(&addr)),
		uintptr(unsafe.Pointer(&addrLen)),
		0,
	)
	if errno != 0 {
		return nil, 0, errno
	}
	ip := net.IP(addr.Addr[:])
	port := int(binary.BigEndian.Uint16((*[2]byte)(unsafe.Pointer(&addr.Port))[:]))
	return ip, port, nil
}
