package tenantdir

import "regexp"

var normalizedRegionIDPattern = regexp.MustCompile(`^[a-z0-9]+(?:-[a-z0-9]+)+$`)

// IsNormalizedRegionID reports whether value uses the canonical provider-region format.
func IsNormalizedRegionID(value string) bool {
	return normalizedRegionIDPattern.MatchString(value)
}
