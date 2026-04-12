package naming

import (
	"crypto/sha1"
	"encoding/base32"
	"encoding/hex"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	dnsLabelMaxLen   = 63
	podRandSuffixLen = 5
	replicaSetMaxLen = dnsLabelMaxLen - 1 - podRandSuffixLen
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
	if errs := validation.IsDNS1123Label(name); len(errs) > 0 {
		return fmt.Errorf("invalid DNS-1123 label '%s': %v", name, errs)
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
