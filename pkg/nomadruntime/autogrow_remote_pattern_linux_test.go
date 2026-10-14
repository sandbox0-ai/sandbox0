//go:build linux

package nomadruntime

import (
	"bytes"
	"encoding/binary"
)

// Independently reconstruct the guest's deterministic byte stream after XFS
// writes and remote restore. Every chunk and aligned 4 KiB block differs.
func autogrowChunkPattern(index int, stress, final bool) []byte {
	if final && stress && index%11 == 0 {
		return make([]byte, 1<<20)
	}
	if final && stress && index%7 == 0 {
		index += 600
	}
	if !stress {
		return bytes.Repeat([]byte{byte(index % 251)}, 1<<20)
	}
	chunk := make([]byte, 1<<20)
	state := uint32(0x9e3779b9) ^ uint32(index+1)
	for offset := 0; offset < len(chunk); offset += 4 {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		binary.LittleEndian.PutUint32(chunk[offset:offset+4], state)
	}
	for offset := 0; offset < len(chunk); offset += 4096 {
		binary.LittleEndian.PutUint64(chunk[offset:offset+8], uint64(uint32(index))<<32|uint64(offset/4096))
	}
	return chunk
}
