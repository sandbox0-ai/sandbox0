package operationid

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// FromIdempotencyKey derives a non-secret operation identity. The scope and
// resource coordinates prevent the same caller key from aliasing another
// mutation while retries of one request remain stable across gateways.
func FromIdempotencyKey(scope, teamID, userID, resourceID, key string) string {
	key = strings.TrimSpace(key)
	if key == "" || len(key) > 255 {
		return ""
	}
	digest := sha256.Sum256([]byte(strings.Join([]string{
		"sandbox0-operation-v1",
		scope,
		strings.TrimSpace(teamID),
		strings.TrimSpace(userID),
		strings.TrimSpace(resourceID),
		key,
	}, "\x00")))
	return "idem-" + hex.EncodeToString(digest[:])
}
