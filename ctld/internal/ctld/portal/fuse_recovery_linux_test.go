//go:build linux

package portal

import (
	"testing"
	"unsafe"
)

func TestFUSERecoveryUAPILayout(t *testing.T) {
	if got := unsafe.Sizeof(fuseRecoveryAttachment{}); got != fuseRecoveryAttachmentSz {
		t.Fatalf("fuse recovery attachment size = %d, want %d", got, fuseRecoveryAttachmentSz)
	}
	if fuseDeviceIOCTLRecover != 0xC088E5C9 {
		t.Fatalf("FUSE_DEV_IOC_RECOVER = %#x", fuseDeviceIOCTLRecover)
	}
	if got := unsafe.Sizeof(fuseRecoveryStatus{}); got != fuseRecoveryStatusSz {
		t.Fatalf("fuse recovery status size = %d, want %d", got, fuseRecoveryStatusSz)
	}
	if fuseDeviceIOCTLStatus != 0x8020E5CC {
		t.Fatalf("FUSE_DEV_IOC_RECOVERY_STATUS = %#x", fuseDeviceIOCTLStatus)
	}
}

func TestRecoverFUSEConnectionRejectsInvalidTagsBeforeOpen(t *testing.T) {
	for _, tag := range []string{"", "   ", string(make([]byte, fuseRecoveryTagMax))} {
		if fd, _, _, err := recoverFUSEConnection(tag); err == nil || fd != -1 {
			t.Fatalf("recoverFUSEConnection(%q) = fd %d, err %v", tag, fd, err)
		}
	}
}
