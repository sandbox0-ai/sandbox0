package volume

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
)

const (
	s3BackendCredentialEnvelopeVersion = 1
	s3BackendCredentialAlgorithm       = "AES-256-GCM"
)

type S3BackendCredentialCodec struct {
	activeKeyID string
	keys        map[string][]byte
}

type s3BackendCredentialPayload struct {
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
}

func NewS3BackendCredentialCodec(activeKeyID string, keys map[string][]byte) (*S3BackendCredentialCodec, error) {
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
		parsed, err := egressauth.NormalizeAES256Key(key)
		if err != nil {
			return nil, fmt.Errorf("normalize encryption key %q: %w", keyID, err)
		}
		normalized[keyID] = parsed
	}
	if _, ok := normalized[activeKeyID]; !ok {
		return nil, fmt.Errorf("active encryption key %q not configured", activeKeyID)
	}
	return &S3BackendCredentialCodec{activeKeyID: activeKeyID, keys: normalized}, nil
}

func NewS3BackendCredentialCodecFromConfig(cfg *config.StorageProxyConfig) (*S3BackendCredentialCodec, error) {
	if cfg == nil {
		return nil, nil
	}
	encryptedPG := cfg.CredentialStore.EncryptedPG
	keyID := strings.TrimSpace(encryptedPG.KeyID)
	if keyID == "" {
		keyID = "default"
	}
	key := []byte(strings.TrimSpace(encryptedPG.Key))
	if len(key) == 0 && strings.TrimSpace(encryptedPG.KeyFile) != "" {
		raw, err := os.ReadFile(strings.TrimSpace(encryptedPG.KeyFile))
		if err != nil {
			return nil, fmt.Errorf("read s3 backend credential encryption key: %w", err)
		}
		key = raw
	}
	if len(key) == 0 {
		return nil, nil
	}
	return NewS3BackendCredentialCodec(keyID, map[string][]byte{keyID: key})
}

func (c *S3BackendCredentialCodec) EncryptS3BackendCredentials(ctx context.Context, teamID, volumeID string, cfg S3BackendConfig) (S3BackendConfig, error) {
	cfg = NormalizeS3BackendConfig(cfg)
	if c == nil {
		return S3BackendConfig{}, fmt.Errorf("s3 backend credential encryption is not configured")
	}
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return S3BackendConfig{}, fmt.Errorf("s3.access_key and s3.secret_key are required")
	}
	payload, err := json.Marshal(s3BackendCredentialPayload{
		AccessKey:    cfg.AccessKey,
		SecretKey:    cfg.SecretKey,
		SessionToken: cfg.SessionToken,
	})
	if err != nil {
		return S3BackendConfig{}, fmt.Errorf("marshal s3 backend credentials: %w", err)
	}
	envelope, err := c.encrypt(ctx, s3BackendCredentialAAD(teamID, volumeID), payload)
	if err != nil {
		return S3BackendConfig{}, err
	}
	cfg.AccessKey = ""
	cfg.SecretKey = ""
	cfg.SessionToken = ""
	cfg.EncryptedCredentials = envelope
	return cfg, nil
}

func (c *S3BackendCredentialCodec) DecryptS3BackendCredentials(ctx context.Context, teamID, volumeID string, cfg S3BackendConfig) (S3BackendConfig, error) {
	cfg = NormalizeS3BackendConfig(cfg)
	if !hasS3BackendEncryptedCredentials(cfg) {
		return S3BackendConfig{}, fmt.Errorf("s3 backend encrypted credentials are required")
	}
	if c == nil {
		return S3BackendConfig{}, fmt.Errorf("s3 backend credential encryption is not configured")
	}
	plaintext, err := c.decrypt(ctx, s3BackendCredentialAAD(teamID, volumeID), cfg.EncryptedCredentials)
	if err != nil {
		return S3BackendConfig{}, err
	}
	var payload s3BackendCredentialPayload
	if err := json.Unmarshal(plaintext, &payload); err != nil {
		return S3BackendConfig{}, fmt.Errorf("unmarshal s3 backend credentials: %w", err)
	}
	cfg.AccessKey = strings.TrimSpace(payload.AccessKey)
	cfg.SecretKey = strings.TrimSpace(payload.SecretKey)
	cfg.SessionToken = strings.TrimSpace(payload.SessionToken)
	cfg.EncryptedCredentials = nil
	if cfg.AccessKey == "" || cfg.SecretKey == "" {
		return S3BackendConfig{}, fmt.Errorf("decrypted s3 backend credentials are incomplete")
	}
	return cfg, nil
}

func MarshalEncryptedS3BackendConfig(ctx context.Context, teamID, volumeID string, cfg S3BackendConfig, codec *S3BackendCredentialCodec) (json.RawMessage, error) {
	if codec == nil {
		return nil, fmt.Errorf("s3 backend credential encryption is not configured")
	}
	encrypted, err := codec.EncryptS3BackendCredentials(ctx, teamID, volumeID, cfg)
	if err != nil {
		return nil, err
	}
	return MarshalS3BackendConfig(encrypted)
}

func DecodeS3BackendConfigWithCredentials(ctx context.Context, teamID, volumeID string, raw json.RawMessage, codec *S3BackendCredentialCodec) (S3BackendConfig, error) {
	cfg, err := decodeS3BackendConfig(raw, false)
	if err != nil {
		return S3BackendConfig{}, err
	}
	cfg, err = codec.DecryptS3BackendCredentials(ctx, teamID, volumeID, cfg)
	if err != nil {
		return S3BackendConfig{}, err
	}
	if err := ValidateS3BackendConfig(cfg); err != nil {
		return S3BackendConfig{}, err
	}
	return cfg, nil
}

func (c *S3BackendCredentialCodec) encrypt(_ context.Context, aad []byte, plaintext []byte) (*S3BackendEncryptedCredentials, error) {
	if c == nil {
		return nil, fmt.Errorf("s3 backend credential encryption is not configured")
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
	return &S3BackendEncryptedCredentials{
		Version:    s3BackendCredentialEnvelopeVersion,
		Algorithm:  s3BackendCredentialAlgorithm,
		KeyID:      c.activeKeyID,
		Nonce:      base64.RawStdEncoding.EncodeToString(nonce),
		Ciphertext: base64.RawStdEncoding.EncodeToString(gcm.Seal(nil, nonce, plaintext, aad)),
	}, nil
}

func (c *S3BackendCredentialCodec) decrypt(_ context.Context, aad []byte, envelope *S3BackendEncryptedCredentials) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("s3 backend credential encryption is not configured")
	}
	if envelope == nil {
		return nil, fmt.Errorf("s3 backend encrypted credentials are required")
	}
	if envelope.Version != s3BackendCredentialEnvelopeVersion {
		return nil, fmt.Errorf("unsupported s3 backend credential version %d", envelope.Version)
	}
	if envelope.Algorithm != s3BackendCredentialAlgorithm {
		return nil, fmt.Errorf("unsupported s3 backend credential algorithm %q", envelope.Algorithm)
	}
	key, ok := c.keys[envelope.KeyID]
	if !ok {
		return nil, fmt.Errorf("s3 backend credential encryption key %q not configured", envelope.KeyID)
	}
	nonce, err := base64.RawStdEncoding.DecodeString(envelope.Nonce)
	if err != nil {
		return nil, fmt.Errorf("decode s3 backend credential nonce: %w", err)
	}
	ciphertext, err := base64.RawStdEncoding.DecodeString(envelope.Ciphertext)
	if err != nil {
		return nil, fmt.Errorf("decode s3 backend credential ciphertext: %w", err)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("create aes cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("create aes-gcm: %w", err)
	}
	plaintext, err := gcm.Open(nil, nonce, ciphertext, aad)
	if err != nil {
		return nil, fmt.Errorf("decrypt s3 backend credentials: %w", err)
	}
	return plaintext, nil
}

func s3BackendCredentialAAD(teamID, volumeID string) []byte {
	return []byte(fmt.Sprintf("sandbox0:volume-backend:s3:v1:%s:%s", teamID, volumeID))
}
