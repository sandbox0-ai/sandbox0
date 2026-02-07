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
// It is intentionally short to keep derived Kubernetes resource names within limits.
func TenantKey(teamID string) string {
	return shortHash(teamID)
}

// TeamKey is an alias for TenantKey to keep naming consistent.
func TeamKey(teamID string) string {
	return TenantKey(teamID)
}

// TemplateNameForCluster returns a Kubernetes-safe name for storing a template in a cluster.
//
// For public templates, the name is the templateID.
// For team-scoped templates, the name includes a stable team key to avoid cross-tenant collisions.
func TemplateNameForCluster(scope, teamID, templateID string) string {
	templateID = strings.ToLower(templateID)
	if scope != ScopeTeam {
		name, err := slugWithHash(templateID, dnsLabelMaxLen)
		if err != nil {
			return fmt.Sprintf("tpl-%s", shortHash(templateID))
		}
		return name
	}

	teamKey := TeamKey(teamID)
	prefix := fmt.Sprintf("t-%s-", teamKey)
	remaining := dnsLabelMaxLen - len(prefix)
	if remaining <= 0 {
		return fmt.Sprintf("t-%s-%s", teamKey, shortHash(templateID))
	}
	name, err := slugWithHash(templateID, remaining)
	if err != nil {
		return fmt.Sprintf("t-%s-%s", teamKey, shortHash(templateID))
	}
	return prefix + name
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

// ValidateTemplateID ensures template_id is non-empty and safe for storage.
func ValidateTemplateID(templateID string) error {
	_, err := CanonicalTemplateID(templateID)
	return err
}

// TemplateNamespaceForBuiltin generates a namespace for a builtin template ID.
func TemplateNamespaceForBuiltin(templateID string) (string, error) {
	canonical, err := CanonicalTemplateID(templateID)
	if err != nil {
		return "", err
	}
	const prefix = "tpl-"
	remaining := dnsLabelMaxLen - len(prefix)
	if remaining <= 0 {
		return "", fmt.Errorf("builtin template namespace prefix too long")
	}
	slug, err := slugWithHash(canonical, remaining)
	if err != nil {
		return "", err
	}
	name := prefix + slug
	if err := validateDNSLabel(name); err != nil {
		return "", err
	}
	return name, nil
}

// TemplateNamespaceForTeam generates a namespace for a team-scoped template.
func TemplateNamespaceForTeam(teamID string) (string, error) {
	trimmed := strings.TrimSpace(teamID)
	if trimmed == "" {
		return "", fmt.Errorf("team_id is required")
	}
	name := fmt.Sprintf("t-%s", TeamKey(trimmed))
	if err := validateDNSLabel(name); err != nil {
		return "", err
	}
	return name, nil
}
