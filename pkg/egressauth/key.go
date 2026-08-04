package egressauth

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
)

// NormalizeAES256Key accepts raw, base64, or hex-encoded 32-byte keys.
func NormalizeAES256Key(raw []byte) ([]byte, error) {
	if len(raw) == 32 {
		return append([]byte(nil), raw...), nil
	}
	key := []byte(strings.TrimSpace(string(raw)))
	if decoded, err := base64.StdEncoding.DecodeString(string(key)); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := base64.RawStdEncoding.DecodeString(string(key)); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	if decoded, err := hex.DecodeString(string(key)); err == nil && len(decoded) == 32 {
		return decoded, nil
	}
	return nil, fmt.Errorf("AES-256 key must be 32 raw bytes, base64, or hex")
}
