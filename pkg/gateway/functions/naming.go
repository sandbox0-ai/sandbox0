package functions

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

var dnsLabelPattern = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)

func SlugFromName(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	lastDash := false
	for _, r := range slug {
		valid := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
		if valid {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "function"
	}
	if len(out) > 48 {
		sum := sha1.Sum([]byte(name))
		suffix := hex.EncodeToString(sum[:])[:8]
		out = strings.TrimRight(out[:39], "-") + "-" + suffix
	}
	return out
}

func DomainLabel(slug, teamID string) string {
	sum := sha1.Sum([]byte(strings.TrimSpace(teamID)))
	suffix := hex.EncodeToString(sum[:])[:8]
	base := strings.Trim(strings.ToLower(slug), "-")
	if base == "" {
		base = "function"
	}
	maxBase := 63 - 1 - len(suffix)
	if len(base) > maxBase {
		base = strings.TrimRight(base[:maxBase], "-")
	}
	return fmt.Sprintf("%s-%s", base, suffix)
}

func ValidateAlias(alias string) error {
	if !dnsLabelPattern.MatchString(strings.TrimSpace(alias)) {
		return fmt.Errorf("alias must be a DNS label")
	}
	return nil
}
