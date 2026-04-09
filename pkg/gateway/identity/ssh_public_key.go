package identity

import (
	"fmt"
	"strings"
	"time"

	"golang.org/x/crypto/ssh"
)

// UserSSHPublicKey stores one normalized SSH public key uploaded by a user.
type UserSSHPublicKey struct {
	ID                string    `json:"id"`
	UserID            string    `json:"user_id"`
	Name              string    `json:"name"`
	PublicKey         string    `json:"public_key"`
	KeyType           string    `json:"key_type"`
	FingerprintSHA256 string    `json:"fingerprint_sha256"`
	Comment           string    `json:"comment,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// NormalizeAuthorizedSSHPublicKey parses one authorized_keys entry and returns
// the normalized key material plus metadata needed for storage and lookup.
func NormalizeAuthorizedSSHPublicKey(raw string) (publicKey, keyType, fingerprint, comment string, err error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", "", "", fmt.Errorf("ssh public key is required")
	}

	parsedKey, parsedComment, _, _, err := ssh.ParseAuthorizedKey([]byte(trimmed))
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse ssh public key: %w", err)
	}

	normalized := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(parsedKey)))
	return normalized, parsedKey.Type(), ssh.FingerprintSHA256(parsedKey), strings.TrimSpace(parsedComment), nil
}
