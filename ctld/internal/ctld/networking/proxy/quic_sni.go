package proxy

import (
	"bytes"
	"strings"
)

func parseSNIFromTLSHandshake(data []byte) string {
	offset := bytes.IndexByte(data, 0x01) // ClientHello
	if offset == -1 || offset+4 >= len(data) {
		return ""
	}
	length := int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
	if offset+4+length > len(data) {
		return ""
	}
	hello := data[offset+4 : offset+4+length]
	// Skip: legacy_version(2) + random(32)
	if len(hello) < 34 {
		return ""
	}
	idx := 34
	if idx >= len(hello) {
		return ""
	}
	sessionLen := int(hello[idx])
	idx += 1 + sessionLen
	if idx+2 > len(hello) {
		return ""
	}
	cipherLen := int(hello[idx])<<8 | int(hello[idx+1])
	idx += 2 + cipherLen
	if idx >= len(hello) {
		return ""
	}
	compLen := int(hello[idx])
	idx += 1 + compLen
	if idx+2 > len(hello) {
		return ""
	}
	extLen := int(hello[idx])<<8 | int(hello[idx+1])
	idx += 2
	end := idx + extLen
	if end > len(hello) {
		return ""
	}
	for idx+4 <= end {
		extType := int(hello[idx])<<8 | int(hello[idx+1])
		extSize := int(hello[idx+2])<<8 | int(hello[idx+3])
		idx += 4
		if idx+extSize > end {
			return ""
		}
		if extType == 0x00 {
			if idx+2 > end {
				return ""
			}
			listLen := int(hello[idx])<<8 | int(hello[idx+1])
			idx += 2
			listEnd := idx + listLen
			for idx+3 <= listEnd {
				nameType := hello[idx]
				nameLen := int(hello[idx+1])<<8 | int(hello[idx+2])
				idx += 3
				if nameType == 0 && idx+nameLen <= listEnd {
					return strings.ToLower(string(hello[idx : idx+nameLen]))
				}
				idx += nameLen
			}
			return ""
		}
		idx += extSize
	}
	return ""
}
