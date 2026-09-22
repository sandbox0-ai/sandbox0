package gvisorcli

import (
	"encoding/json"
	"fmt"
	"runtime"

	"github.com/opencontainers/go-digest"
)

//go:noescape
func nativeCPUID(leaf, index uint32) (a, b, c, d uint32)

//go:noescape
func nativeXCR0() (low, high uint32)

type cpuStateLeaf struct {
	Index uint32 `json:"index"`
	A     uint32 `json:"a"`
	B     uint32 `json:"b"`
	C     uint32 `json:"c"`
	D     uint32 `json:"d"`
}

func nativeCPUStateLayout() (uint32, string, error) {
	// Do not let the goroutine move OS threads between XCR0 and CPUID reads.
	// Node-level coverage of all eligible CPUs is a separate preflight gate.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	maxLeaf, _, _, _ := nativeCPUID(0, 0)
	if maxLeaf < 1 {
		return 0, "", fmt.Errorf("CPU lacks CPUID feature information")
	}
	_, b, c, _ := nativeCPUID(1, 0)
	// Match the pinned stock gVisor CacheLine() interpretation exactly.
	cacheLine := 8 * (b >> 8) & 0xff
	state := struct {
		Version int            `json:"version"`
		XSave   bool           `json:"xsave"`
		XCR0    uint64         `json:"xcr0"`
		Leaves  []cpuStateLeaf `json:"leaves"`
	}{Version: 1}
	state.XSave = c&(1<<26) != 0 && c&(1<<27) != 0
	if state.XSave {
		if maxLeaf < 0xd {
			return 0, "", fmt.Errorf("CPU reports XSAVE without layout information")
		}
		low, high := nativeXCR0()
		state.XCR0 = uint64(high)<<32 | uint64(low)
		var supported uint64
		for index := uint32(0); index < 64; index++ {
			if index > 1 && supported&(uint64(1)<<index) == 0 {
				continue
			}
			a, b, c, d := nativeCPUID(0xd, index)
			state.Leaves = append(state.Leaves, cpuStateLeaf{Index: index, A: a, B: b, C: c, D: d})
			if index == 0 {
				supported = uint64(d)<<32 | uint64(a)
			}
			if index == 1 {
				supported |= uint64(d)<<32 | uint64(c)
			}
		}
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return 0, "", err
	}
	return cacheLine, digest.FromBytes(payload).String(), nil
}

// This fingerprint covers all eight feature blocks read by the qualified
// runsc release, plus its cache-line and XSAVE/XCR0 interpretation. It is a
// per-CPU cache guard, not a cross-host identity: APIC/topology bits may differ
// between cores. Qualify this input set again before accepting another release.
func nativeCPUCapabilityFingerprint() (string, error) {
	hwcap, err := nativeCPUHWCap()
	if err != nil {
		return "", err
	}
	cacheLine, layout, err := nativeCPUStateLayout()
	if err != nil {
		return "", err
	}
	values := struct {
		HWCap     [2]uint64
		CacheLine uint32
		Layout    string
		Leaves    [][6]uint32
	}{HWCap: hwcap, CacheLine: cacheLine, Layout: layout}
	for _, input := range [][2]uint32{{0, 0}, {1, 0}, {7, 0}, {0x80000000, 0}, {0x80000001, 0}, {0xd, 1}} {
		a, b, c, d := nativeCPUID(input[0], input[1])
		values.Leaves = append(values.Leaves, [6]uint32{input[0], input[1], a, b, c, d})
	}
	payload, err := json.Marshal(values)
	if err != nil {
		return "", err
	}
	return digest.FromBytes(payload).String(), nil
}
