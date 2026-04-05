package tenantdir

import "strings"

// CanonicalRegionID normalizes a region identifier to provider/region form.
func CanonicalRegionID(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if strings.Contains(trimmed, "/") {
		return trimmed
	}
	parts := strings.SplitN(trimmed, "-", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return trimmed
	}
	return parts[0] + "/" + parts[1]
}

// SameRegionID reports whether two region identifiers resolve to the same canonical value.
func SameRegionID(a, b string) bool {
	canonicalA := CanonicalRegionID(a)
	canonicalB := CanonicalRegionID(b)
	return canonicalA != "" && canonicalA == canonicalB
}
