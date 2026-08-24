package naming

import (
	"crypto/sha1"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"
)

const (
	dnsLabelMaxLen = 63
	// Exposure host label format: <sandboxName>--p<port>.
	// Keep sandbox name shorter than full DNS label to reserve suffix budget.
	exposurePortDelimiter = "--p"
	maxPortDigits         = 5
	sandboxNameMaxLen     = dnsLabelMaxLen - len(exposurePortDelimiter) - maxPortDigits
	defaultClusterID      = "default"
	nameHashLength        = 8
	clusterKeyMaxLen      = 32
	clusterIDMaxLen       = 20
	sandboxNamePrefix     = "rs"
)

const DefaultClusterID = defaultClusterID

// ClusterIDMaxLen is the longest cluster ID that can be encoded into sandbox names.
const ClusterIDMaxLen = clusterIDMaxLen

// DNSLabelMaxLen is the DNS-1123 label length limit.
const DNSLabelMaxLen = dnsLabelMaxLen

// ClusterIDOrDefault returns the cluster ID or a default value.
func ClusterIDOrDefault(clusterID *string) string {
	if clusterID != nil && *clusterID != "" {
		return *clusterID
	}
	return defaultClusterID
}

var (
	base32NoPadding = base32.StdEncoding.WithPadding(base32.NoPadding)
)

func validateDNSLabel(name string) error {
	if name == "" || len(name) > dnsLabelMaxLen {
		return fmt.Errorf("invalid DNS-1123 label %q", name)
	}
	for index, char := range name {
		alphanumeric := char >= 'a' && char <= 'z' || char >= '0' && char <= '9'
		if !alphanumeric && char != '-' {
			return fmt.Errorf("invalid DNS-1123 label %q", name)
		}
		if (index == 0 || index == len(name)-1) && !alphanumeric {
			return fmt.Errorf("invalid DNS-1123 label %q", name)
		}
	}
	return nil
}

// ValidateClusterID ensures a cluster ID is safe for routing and sandbox name encoding.
func ValidateClusterID(clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("clusterID is required")
	}
	if len(clusterID) > clusterIDMaxLen {
		return fmt.Errorf("clusterID '%s' is too long (%d > %d)", clusterID, len(clusterID), clusterIDMaxLen)
	}
	if err := validateDNSLabel(clusterID); err != nil {
		return err
	}
	return nil
}

func normalizeToDNSLabel(input string) (string, error) {
	if input == "" {
		return "", fmt.Errorf("input is empty")
	}
	lower := strings.ToLower(input)
	var b strings.Builder
	b.Grow(len(lower))
	prevDash := false
	for _, r := range lower {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevDash = false
			continue
		}
		if !prevDash {
			b.WriteByte('-')
			prevDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		return "", fmt.Errorf("input '%s' normalized to empty label", input)
	}
	return out, nil
}

func shortHash(input string) string {
	sum := sha1.Sum([]byte(input))
	return hex.EncodeToString(sum[:])[:nameHashLength]
}

func truncateWithHash(base, hashInput string, maxLen int) string {
	hash := shortHash(hashInput)
	if maxLen <= len(hash) {
		return hash[:maxLen]
	}
	cut := maxLen - len(hash) - 1
	if cut <= 0 {
		return hash
	}
	if cut > len(base) {
		cut = len(base)
	}
	return base[:cut] + "-" + hash
}

func slugWithHash(input string, maxLen int) (string, error) {
	if maxLen <= 0 {
		return "", fmt.Errorf("maxLen must be > 0")
	}
	slug, err := normalizeToDNSLabel(input)
	if err != nil {
		return "", err
	}
	normalized := strings.ToLower(input)
	changed := slug != normalized
	if len(slug) > maxLen || changed {
		slug = truncateWithHash(slug, input, maxLen)
	}
	if err := validateDNSLabel(slug); err != nil {
		return "", err
	}
	if len(slug) > maxLen {
		return "", fmt.Errorf("generated label '%s' exceeds max length %d", slug, maxLen)
	}
	return slug, nil
}

// DNSLabelWithHash normalizes input into a DNS-1123 label and adds a stable
// hash when normalization or truncation would otherwise lose uniqueness.
func DNSLabelWithHash(input string, maxLen int) (string, error) {
	return slugWithHash(input, maxLen)
}

func encodeClusterID(clusterID string) (string, error) {
	if err := ValidateClusterID(clusterID); err != nil {
		return "", err
	}
	encoded := strings.ToLower(base32NoPadding.EncodeToString([]byte(clusterID)))
	if len(encoded) > clusterKeyMaxLen {
		return "", fmt.Errorf("clusterID '%s' is too long to encode (%d > %d)", clusterID, len(encoded), clusterKeyMaxLen)
	}
	if err := validateDNSLabel(encoded); err != nil {
		return "", fmt.Errorf("encoded cluster key is invalid: %w", err)
	}
	return encoded, nil
}

// ClusterKey encodes a validated cluster ID into the DNS-safe key embedded in
// sandbox workload names.
func ClusterKey(clusterID string) (string, error) {
	return encodeClusterID(clusterID)
}

func decodeClusterKey(clusterKey string) (string, error) {
	if clusterKey == "" {
		return "", fmt.Errorf("cluster key is empty")
	}
	data, err := base32NoPadding.DecodeString(strings.ToUpper(clusterKey))
	if err != nil {
		return "", fmt.Errorf("decode cluster key: %w", err)
	}
	return string(data), nil
}
