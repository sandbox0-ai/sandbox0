package service

import (
	"crypto/sha256"
	"encoding/hex"
)

func policyAnnotationHash(annotation string) string {
	if annotation == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(annotation))
	return hex.EncodeToString(sum[:])
}
