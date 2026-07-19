package egressauth

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

const encryptedPayloadVersion = 1

// SecretCodec encrypts and decrypts credential source specs before they are
// persisted in PostgreSQL.
type SecretCodec interface {
	Encrypt(ctx context.Context, aad []byte, spec CredentialSourceSecretSpec) (json.RawMessage, error)
	Decrypt(ctx context.Context, aad []byte, payload json.RawMessage) (CredentialSourceSecretSpec, error)
}

// AESGCMCodec stores encrypted credential source specs as a compact JSON envelope.
type AESGCMCodec struct {
	activeKeyID string
	keys        map[string][]byte
}

type encryptedSpecEnvelope struct {
	Version    int    `json:"version"`
	Algorithm  string `json:"algorithm"`
	KeyID      string `json:"keyId"`
	Nonce      string `json:"nonce"`
	Ciphertext string `json:"ciphertext"`
}

func NewAESGCMCodec(activeKeyID string, keys map[string][]byte) (*AESGCMCodec, error) {
	activeKeyID = strings.TrimSpace(activeKeyID)
	if activeKeyID == "" {
		return nil, fmt.Errorf("active key id is required")
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("at least one encryption key is required")
	}
	normalized := make(map[string][]byte, len(keys))
	for keyID, key := range keys {
		keyID = strings.TrimSpace(keyID)
		if keyID == "" {
			return nil, fmt.Errorf("encryption key id is required")
		}
		parsed, err := NormalizeAES256Key(key)
		if err != nil {
			return nil, fmt.Errorf("normalize encryption key %q: %w", keyID, err)
		}
		normalized[keyID] = parsed
	}
	if _, ok := normalized[activeKeyID]; !ok {
		return nil, fmt.Errorf("active encryption key %q not configured", activeKeyID)
	}
	return &AESGCMCodec{activeKeyID: activeKeyID, keys: normalized}, nil
}

func (c *AESGCMCodec) Encrypt(_ context.Context, aad []byte, spec CredentialSourceSecretSpec) (json.RawMessage, error) {
	if c == nil {
		return nil, fmt.Errorf("secret codec is not configured")
	}
	plaintext, err := CanonicalCredentialSourceSpec(spec)
	if err != nil {
		return nil, err
	}
	key := c.keys[c.activeKeyID]
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create aes-gcm: %w", err)
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generate nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, plaintext, aad)
	envelope := encryptedSpecEnvelope{
		Version:    encryptedPayloadVersion,
		Algorithm:  "AES-256-GCM",
		KeyID:      c.activeKeyID,
		Nonce:      base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawStdEncoding.EncodeToString(ciphertext),
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, fmt.Errorf("marshal encrypted payload: %w", err)
	}
	if err := ValidateCredentialEnvelope(payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func (c *AESGCMCodec) Decrypt(_ context.Context, aad []byte, payload json.RawMessage) (CredentialSourceSecretSpec, error) {
	if c == nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("secret codec is not configured")
	}
	if err := ValidateCredentialEnvelope(payload); err != nil {
		return CredentialSourceSecretSpec{}, err
	}
	var envelope encryptedSpecEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("unmarshal encrypted payload: %w", err)
	}
	if envelope.Version != encryptedPayloadVersion {
		return CredentialSourceSecretSpec{}, fmt.Errorf("unsupported encrypted payload version %d", envelope.Version)
	}
	if envelope.Algorithm != "AES-256-GCM" {
		return CredentialSourceSecretSpec{}, fmt.Errorf("unsupported encrypted payload algorithm %q", envelope.Algorithm)
	}
	key, ok := c.keys[envelope.KeyID]
	if !ok {
		return CredentialSourceSecretSpec{}, fmt.Errorf("encryption key %q not configured", envelope.KeyID)
	}
	nonce, err := base64.RawStdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("decode nonce: %w", err)
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("decode ciphertext: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("create aes-gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("decrypt credential source spec: %w", err)
	}
	var spec CredentialSourceSecretSpec
	if err := json.Unmarshal(plaintext, &spec); err != nil {
		return CredentialSourceSecretSpec{}, fmt.Errorf("unmarshal credential source spec: %w", err)
	}
	if err := ValidateCredentialSourceSpecSize(spec); err != nil {
		return CredentialSourceSecretSpec{}, err
	}
	return spec, nil
}

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

func credentialSourceAAD(teamID string, sourceID, version int64, resolverKind string) []byte {
	return []byte(fmt.Sprintf("sandbox0:egressauth:credential-source:v1:%s:%d:%d:%s", teamID, sourceID, version, resolverKind))
}
