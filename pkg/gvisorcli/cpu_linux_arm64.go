package gvisorcli

import (
	"encoding/binary"
	"github.com/opencontainers/go-digest"
)

// ARM64 runsc uses a fixed FPSIMD frame and compares its known HWCAP feature
// set. It has no x86 cache-line or XSAVE compatibility fields.
func nativeCPUStateLayout() (uint32, string, error) { return 0, "", nil }

func nativeCPUCapabilityFingerprint() (string, error) {
	hwcap, err := nativeCPUHWCap()
	if err != nil {
		return "", err
	}
	var payload [16]byte
	binary.LittleEndian.PutUint64(payload[:8], hwcap[0])
	binary.LittleEndian.PutUint64(payload[8:], hwcap[1])
	return digest.FromBytes(payload[:]).String(), nil
}
