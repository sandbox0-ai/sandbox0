package apikey

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

var normalizedRegionIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+$`)

// NewKeyValue creates a new region-routable API key value.
func NewKeyValue(regionID string) (string, error) {
	normalizedRegionID := strings.TrimSpace(regionID)
	if !normalizedRegionIDPattern.MatchString(normalizedRegionID) {
		return "", ErrInvalidKey
	}

	randomBytes := make([]byte, 24)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("generate random: %w", err)
	}

	return fmt.Sprintf("s0_%s_%s", normalizedRegionID, hex.EncodeToString(randomBytes)), nil
}

// ParseRegionIDFromKey extracts the routable region identifier from an API key.
func ParseRegionIDFromKey(keyValue string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(keyValue), "_", 3)
	if len(parts) != 3 || parts[0] != "s0" || parts[1] == "" || parts[2] == "" {
		return "", ErrInvalidKey
	}
	if !normalizedRegionIDPattern.MatchString(parts[1]) {
		return "", ErrInvalidKey
	}
	return parts[1], nil
}
