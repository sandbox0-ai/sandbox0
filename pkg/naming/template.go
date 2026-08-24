package naming

import (
	"fmt"
	"strings"
)

const (
	ScopePublic = "public"
	ScopeTeam   = "team"
)

// TenantKey returns a stable short key for a team ID.
func TenantKey(teamID string) string {
	return shortHash(teamID)
}

// TeamKey is an alias for TenantKey to keep naming consistent.
func TeamKey(teamID string) string {
	return TenantKey(teamID)
}

// CanonicalTemplateID normalizes template_id to a canonical lowercase form.
func CanonicalTemplateID(templateID string) (string, error) {
	trimmed := strings.TrimSpace(templateID)
	if trimmed == "" {
		return "", fmt.Errorf("template_id is required")
	}
	if len(trimmed) > 255 {
		return "", fmt.Errorf("template_id is too long (%d > 255)", len(trimmed))
	}
	if strings.Contains(trimmed, "/") {
		return "", fmt.Errorf("template_id cannot contain '/'")
	}
	return strings.ToLower(trimmed), nil
}
