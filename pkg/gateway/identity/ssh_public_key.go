package identity

import (
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/resourceguard"
	"golang.org/x/crypto/ssh"
)

const (
	// MaxSSHPublicKeyRequestBytes is the hard JSON body limit for one key.
	MaxSSHPublicKeyRequestBytes int64 = 32 << 10
	// MaxSSHPublicKeyNameBytes bounds a user-provided key name.
	MaxSSHPublicKeyNameBytes int64 = 128
	// MaxAuthorizedSSHPublicKeyBytes bounds both raw and normalized key data.
	MaxAuthorizedSSHPublicKeyBytes int64 = 16 << 10
	// MaxSSHPublicKeyCommentBytes bounds the parsed authorized_keys comment.
	MaxSSHPublicKeyCommentBytes int64 = 1 << 10
)

// UserSSHPublicKey stores one normalized SSH public key uploaded by a user.
type UserSSHPublicKey struct {
	ID                string    `json:"id"`
	TeamID            string    `json:"team_id"`
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
	if err := resourceguard.String(
		"raw ssh public key",
		raw,
		MaxAuthorizedSSHPublicKeyBytes,
	); err != nil {
		return "", "", "", "", err
	}
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", "", "", "", fmt.Errorf("ssh public key is required")
	}

	parsedKey, parsedComment, _, _, err := ssh.ParseAuthorizedKey([]byte(trimmed))
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse ssh public key: %w", err)
	}

	normalized := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(parsedKey)))
	comment = strings.TrimSpace(parsedComment)
	if err := resourceguard.String(
		"normalized ssh public key",
		normalized,
		MaxAuthorizedSSHPublicKeyBytes,
	); err != nil {
		return "", "", "", "", err
	}
	if err := resourceguard.String(
		"ssh public key comment",
		comment,
		MaxSSHPublicKeyCommentBytes,
	); err != nil {
		return "", "", "", "", err
	}
	return normalized, parsedKey.Type(), ssh.FingerprintSHA256(parsedKey), comment, nil
}

// ValidateUserSSHPublicKey protects the repository from oversized internal
// calls that bypass authorized_keys normalization in the HTTP handler.
func ValidateUserSSHPublicKey(key *UserSSHPublicKey) error {
	if key == nil {
		return fmt.Errorf("ssh public key is required")
	}
	if strings.TrimSpace(key.Name) == "" {
		return fmt.Errorf("ssh public key name is required")
	}
	if err := resourceguard.String(
		"ssh public key name",
		key.Name,
		MaxSSHPublicKeyNameBytes,
	); err != nil {
		return err
	}
	if err := resourceguard.String(
		"normalized ssh public key",
		key.PublicKey,
		MaxAuthorizedSSHPublicKeyBytes,
	); err != nil {
		return err
	}
	return resourceguard.String(
		"ssh public key comment",
		key.Comment,
		MaxSSHPublicKeyCommentBytes,
	)
}
