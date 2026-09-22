//go:build linux && (amd64 || arm64)

package gvisorcli

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// Read the same raw ELF auxiliary-vector capabilities used by pinned stock
// runsc. On ARM64 Linux defines these as the system-wide userspace ISA contract.
// Unknown auxv fields are irrelevant; duplicate/truncated capability entries
// and an absent HWCAP record cannot certify a cached observation.
func nativeCPUHWCap() ([2]uint64, error) {
	var result [2]uint64
	file, err := os.Open("/proc/self/auxv")
	if err != nil {
		return result, err
	}
	defer file.Close()
	payload, err := io.ReadAll(io.LimitReader(file, 8193))
	if err != nil {
		return result, err
	}
	return parseCPUHWCap(payload)
}

func parseCPUHWCap(payload []byte) ([2]uint64, error) {
	var result [2]uint64
	if len(payload) == 0 || len(payload) > 8192 || len(payload)%16 != 0 {
		return result, fmt.Errorf("invalid CPU auxiliary vector size")
	}
	var seen [2]bool
	terminated := false
	for offset := 0; offset < len(payload); offset += 16 {
		kind, value := binary.LittleEndian.Uint64(payload[offset:]), binary.LittleEndian.Uint64(payload[offset+8:])
		if kind == 0 {
			terminated = true
			break
		}
		index := -1
		if kind == 16 {
			index = 0
		}
		if kind == 26 {
			index = 1
		}
		if index < 0 {
			continue
		}
		if seen[index] {
			return result, fmt.Errorf("duplicate CPU auxiliary capability")
		}
		seen[index], result[index] = true, value
	}
	if !terminated || !seen[0] {
		return result, fmt.Errorf("incomplete CPU auxiliary capability")
	}
	return result, nil
}
