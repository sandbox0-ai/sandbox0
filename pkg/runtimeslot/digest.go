package runtimeslot

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// digestJSON preserves the wire encoding used by regional authority digests.
// Callers validate their own request or proof before hashing it.
func digestJSON(value any) (string, error) {
	payload, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:]), nil
}
