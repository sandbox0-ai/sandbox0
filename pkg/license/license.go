package license

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
)

const (
	FeatureMultiCluster = "multi_cluster"
	FeatureSSO          = "sso"
)

var (
	ErrLicensePathRequired  = errors.New("license path is required")
	ErrInvalidLicenseFormat = errors.New("invalid license format")
	ErrUnknownKeyID         = errors.New("unknown license key id")
	ErrInvalidSignature     = errors.New("invalid license signature")
	ErrLicenseExpired       = errors.New("license expired")
	ErrLicenseNotYetValid   = errors.New("license is not valid yet")
)

// Envelope is the signed license envelope.
type Envelope struct {
	KeyID     string `json:"kid"`
	Payload   string `json:"payload"`
	Signature string `json:"signature"`
}

// Claims is the license payload.
type Claims struct {
	Version   string   `json:"version,omitempty"`
	Subject   string   `json:"subject,omitempty"`
	IssuedAt  int64    `json:"issued_at,omitempty"`
	NotBefore int64    `json:"not_before,omitempty"`
	ExpiresAt int64    `json:"expires_at,omitempty"`
	Features  []string `json:"features,omitempty"`
}

// Checker validates and exposes licensed features.
type Checker struct {
	keyID    string
	claims   Claims
	features map[string]struct{}
}

// LoadFromFile loads and validates a signed license from file using embedded keys.
func LoadFromFile(licensePath string) (*Checker, error) {
	return loadFromFileAt(licensePath, time.Now())
}

func loadFromFileAt(licensePath string, now time.Time) (*Checker, error) {
	if strings.TrimSpace(licensePath) == "" {
		return nil, ErrLicensePathRequired
	}

	rawLicense, err := os.ReadFile(licensePath)
	if err != nil {
		return nil, fmt.Errorf("read license file: %w", err)
	}

	return loadAt(TrustedPublicKeys(), rawLicense, now)
}

func loadAt(keys map[string]ed25519.PublicKey, rawLicense []byte, now time.Time) (*Checker, error) {
	var env Envelope
	if err := json.Unmarshal(rawLicense, &env); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidLicenseFormat, err)
	}
	if strings.TrimSpace(env.KeyID) == "" || strings.TrimSpace(env.Payload) == "" || strings.TrimSpace(env.Signature) == "" {
		return nil, ErrInvalidLicenseFormat
	}

	publicKey, ok := keys[env.KeyID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrUnknownKeyID, env.KeyID)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(env.Payload)
	if err != nil {
		return nil, fmt.Errorf("%w: decode payload: %v", ErrInvalidLicenseFormat, err)
	}
	signature, err := base64.RawURLEncoding.DecodeString(env.Signature)
	if err != nil {
		return nil, fmt.Errorf("%w: decode signature: %v", ErrInvalidLicenseFormat, err)
	}

	if !ed25519.Verify(publicKey, payloadBytes, signature) {
		return nil, ErrInvalidSignature
	}

	var claims Claims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("%w: decode claims: %v", ErrInvalidLicenseFormat, err)
	}

	if claims.NotBefore > 0 && now.Before(time.Unix(claims.NotBefore, 0)) {
		return nil, ErrLicenseNotYetValid
	}
	if claims.ExpiresAt > 0 && now.After(time.Unix(claims.ExpiresAt, 0)) {
		return nil, ErrLicenseExpired
	}

	featureSet := make(map[string]struct{}, len(claims.Features))
	for _, feature := range claims.Features {
		trimmed := strings.TrimSpace(feature)
		if trimmed == "" {
			continue
		}
		featureSet[trimmed] = struct{}{}
	}

	return &Checker{
		keyID:    env.KeyID,
		claims:   claims,
		features: featureSet,
	}, nil
}

// KeyID returns the key id used to verify this license.
func (c *Checker) KeyID() string {
	return c.keyID
}

// Claims returns the validated claims.
func (c *Checker) Claims() Claims {
	return c.claims
}

// HasFeature returns true if the feature is present in the license.
func (c *Checker) HasFeature(feature string) bool {
	_, ok := c.features[feature]
	return ok
}
