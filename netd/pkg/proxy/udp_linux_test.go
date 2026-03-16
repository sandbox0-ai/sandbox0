//go:build linux

package proxy

import (
	"encoding/binary"
	"net"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

func TestParseIPv4OriginalDst(t *testing.T) {
	data := make([]byte, unix.SizeofSockaddrInet4)
	addr := (*unix.RawSockaddrInet4)(unsafePointer(&data[0]))
	addr.Family = unix.AF_INET
	addr.Port = htons(443)
	copy(addr.Addr[:], net.IPv4(8, 8, 8, 8).To4())

	ip, port, ok := parseIPv4OriginalDst(data)
	if !ok {
		t.Fatalf("expected parseIPv4OriginalDst to succeed")
	}
	if got := ip.String(); got != "8.8.8.8" {
		t.Fatalf("ip = %q, want 8.8.8.8", got)
	}
	if port != 443 {
		t.Fatalf("port = %d, want 443", port)
	}
}

func TestParseIPv4PktInfo(t *testing.T) {
	data := make([]byte, unix.SizeofInet4Pktinfo)
	info := (*unix.Inet4Pktinfo)(unsafePointer(&data[0]))
	copy(info.Spec_dst[:], net.IPv4(1, 1, 1, 1).To4())

	ip, ok := parseIPv4PktInfo(data)
	if !ok {
		t.Fatalf("expected parseIPv4PktInfo to succeed")
	}
	if got := ip.String(); got != "1.1.1.1" {
		t.Fatalf("ip = %q, want 1.1.1.1", got)
	}
}

func TestParseOriginalDstFromOOBFallsBackToPktInfoAndListenerPort(t *testing.T) {
	data := make([]byte, unix.SizeofInet4Pktinfo)
	info := (*unix.Inet4Pktinfo)(unsafePointer(&data[0]))
	copy(info.Spec_dst[:], net.IPv4(1, 1, 1, 1).To4())

	raw := make([]byte, unix.CmsgSpace(len(data)))
	header := (*unix.Cmsghdr)(unsafePointer(&raw[0]))
	header.Level = unix.SOL_IP
	header.Type = unix.IP_PKTINFO
	header.SetLen(unix.CmsgLen(len(data)))
	copy(raw[unix.CmsgLen(0):], data)

	ip, port := parseOriginalDstFromOOB(raw, 18443)
	if got := ip.String(); got != "1.1.1.1" {
		t.Fatalf("ip = %q, want 1.1.1.1", got)
	}
	if port != 18443 {
		t.Fatalf("port = %d, want 18443", port)
	}
}

func htons(port uint16) uint16 {
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], port)
	return *(*uint16)(unsafePointer(&buf[0]))
}

func unsafePointer[T any](ptr *T) unsafe.Pointer { return unsafe.Pointer(ptr) }
