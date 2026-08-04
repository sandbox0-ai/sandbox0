package handlers

import (
	"crypto/sha256"
	"encoding/hex"
)

func inlineFunctionDigest(code string) string {
	sum := sha256.Sum256([]byte(code))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// legacyInlineFunctionDigest preserves the pre-filename-removal digest so a
// rolling upgrade can execute functions materialized by an older procd.
func legacyInlineFunctionDigest(filename, code string) string {
	sum := sha256.Sum256([]byte(filename + "\x00" + code))
	return "sha256:" + hex.EncodeToString(sum[:])
}
