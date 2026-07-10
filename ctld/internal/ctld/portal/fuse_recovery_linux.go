//go:build linux

package portal

import (
	"fmt"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

const (
	fuseRecoveryTagMax       = 128
	fuseCapabilityRecovery   = uint64(1) << 57
	fuseDeviceIOCTLRecover   = uintptr(0xC088E5C9)
	fuseDeviceIOCTLStatus    = uintptr(0x8020E5CC)
	fuseRecoveryAttachmentSz = 136
	fuseRecoveryStatusSz     = 32
)

type fuseRecoveryAttachment struct {
	Tag [fuseRecoveryTagMax]byte
	Dev uint64
}

type fuseRecoveryStatus struct {
	ResendPending uint64
	Reserved      [3]uint64
}

// recoverFUSEConnection asks an ANCK recovery-capable kernel to attach a new
// /dev/fuse descriptor to the initialized connection identified by tag. The
// returned descriptor is owned by the caller.
func recoverFUSEConnection(tag string) (int, uint64, uint64, error) {
	tag = strings.TrimSpace(tag)
	if tag == "" {
		return -1, 0, 0, fmt.Errorf("FUSE recovery tag is required")
	}
	if len(tag) >= fuseRecoveryTagMax || strings.IndexByte(tag, 0) >= 0 {
		return -1, 0, 0, fmt.Errorf("invalid FUSE recovery tag length")
	}

	fd, err := unix.Open("/dev/fuse", unix.O_RDWR|unix.O_CLOEXEC, 0)
	if err != nil {
		return -1, 0, 0, fmt.Errorf("open /dev/fuse for recovery: %w", err)
	}
	attachment := fuseRecoveryAttachment{}
	copy(attachment.Tag[:], tag)
	_, _, errno := unix.Syscall(unix.SYS_IOCTL, uintptr(fd), fuseDeviceIOCTLRecover, uintptr(unsafe.Pointer(&attachment)))
	if errno != 0 {
		_ = unix.Close(fd)
		return -1, 0, 0, fmt.Errorf("recover FUSE connection %q: %w", tag, errno)
	}
	status := fuseRecoveryStatus{}
	_, _, errno = unix.Syscall(unix.SYS_IOCTL, uintptr(fd), fuseDeviceIOCTLStatus, uintptr(unsafe.Pointer(&status)))
	if errno != 0 {
		_ = unix.Close(fd)
		return -1, 0, 0, fmt.Errorf("read recovered FUSE connection %q status: %w", tag, errno)
	}
	return fd, attachment.Dev, status.ResendPending, nil
}
