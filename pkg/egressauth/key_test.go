package egressauth

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"testing"
)

func TestNormalizeAES256Key(t *testing.T) {
	key := bytes.Repeat([]byte{9}, 32)
	for name, raw := range map[string][]byte{
		"raw":    key,
		"base64": []byte(base64.StdEncoding.EncodeToString(key)),
		"hex":    []byte(hex.EncodeToString(key)),
	} {
		t.Run(name, func(t *testing.T) {
			got, err := NormalizeAES256Key(raw)
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			if !bytes.Equal(got, key) {
				t.Fatalf("normalized key mismatch")
			}
		})
	}
}
