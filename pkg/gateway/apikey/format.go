package apikey

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
)

const (
	apiKeyRandomBytes     = 24
	apiKeyRandomHexLength = apiKeyRandomBytes * 2
	// MaxAPIKeyValueBytes bounds authentication work before hashing or parsing.
	MaxAPIKeyValueBytes = 256
)

// NewKeyValue creates a new region-routable API key value.
func NewKeyValue(regionID string) (string, error) {
	normalizedRegionID := strings.TrimSpace(regionID)
	if !tenantdir.IsNormalizedRegionID(normalizedRegionID) {
		return "", ErrInvalidKey
	}

	randomBytes := make([]byte, apiKeyRandomBytes)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("generate random: %w", err)
	}

	keyValue := fmt.Sprintf("s0_%s_%s", normalizedRegionID, hex.EncodeToString(randomBytes))
	if len(keyValue) > MaxAPIKeyValueBytes {
		return "", ErrInvalidKey
	}
	return keyValue, nil
}

// ParseRegionIDFromKey extracts the routable region identifier from an API key.
func ParseRegionIDFromKey(keyValue string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(keyValue), "_", 3)
	if len(parts) != 3 || parts[0] != "s0" || parts[1] == "" || parts[2] == "" {
		return "", ErrInvalidKey
	}
	if !tenantdir.IsNormalizedRegionID(parts[1]) {
		return "", ErrInvalidKey
	}
	return parts[1], nil
}

func validateAuthenticationKeyStructure(keyValue string) error {
	if len(keyValue) == 0 || len(keyValue) > MaxAPIKeyValueBytes {
		return ErrInvalidKey
	}
	parts := strings.SplitN(keyValue, "_", 3)
	if len(parts) != 3 || parts[0] != "s0" ||
		!tenantdir.IsNormalizedRegionID(parts[1]) ||
		len(parts[2]) != apiKeyRandomHexLength {
		return ErrInvalidKey
	}
	for _, value := range []byte(parts[2]) {
		if (value < '0' || value > '9') && (value < 'a' || value > 'f') {
			return ErrInvalidKey
		}
	}
	return nil
}
