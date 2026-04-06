package naming

import (
	"fmt"
	"strings"
)

const localhostRegistryHost = "localhost"

// NormalizeRegistryHost strips scheme and surrounding slashes from a registry host.
func NormalizeRegistryHost(raw string) string {
	value := strings.TrimSpace(raw)
	value = strings.TrimPrefix(value, "https://")
	value = strings.TrimPrefix(value, "http://")
	return strings.Trim(value, "/")
}

// TeamImageRepositoryPrefix returns the repository prefix reserved for a team.
func TeamImageRepositoryPrefix(teamID string) string {
	trimmed := strings.TrimSpace(teamID)
	if trimmed == "" {
		return ""
	}
	return fmt.Sprintf("t-%s", TeamKey(trimmed))
}

// TeamScopedImageRegistry returns a registry host/path scoped to the team prefix.
func TeamScopedImageRegistry(registryHost, teamID string) string {
	host := NormalizeRegistryHost(registryHost)
	prefix := TeamImageRepositoryPrefix(teamID)
	if host == "" || prefix == "" {
		return host
	}
	return host + "/" + prefix
}

// SplitImageReference separates an image reference into registry host and repository path.
func SplitImageReference(imageRef string) (string, string, error) {
	trimmed := strings.TrimSpace(imageRef)
	if trimmed == "" {
		return "", "", fmt.Errorf("image reference is required")
	}

	registryHost := ""
	repository := trimmed
	if slash := strings.Index(trimmed, "/"); slash > 0 {
		candidate := trimmed[:slash]
		if isRegistryHost(candidate) {
			registryHost = NormalizeRegistryHost(candidate)
			repository = trimmed[slash+1:]
		}
	}

	repository = stripImageTagOrDigest(repository)
	if repository == "" {
		return "", "", fmt.Errorf("image reference %q must include a repository", imageRef)
	}
	return registryHost, repository, nil
}

// ValidateTeamScopedImageReference ensures private-registry image refs stay under the team's reserved prefix.
func ValidateTeamScopedImageReference(imageRef, teamID string, privateRegistryHosts []string) error {
	prefix := TeamImageRepositoryPrefix(teamID)
	if prefix == "" || len(privateRegistryHosts) == 0 {
		return nil
	}

	registryHost, repository, err := SplitImageReference(imageRef)
	if err != nil {
		return err
	}
	if !registryHostMatchesAny(registryHost, privateRegistryHosts) {
		return nil
	}
	if repository == prefix || strings.HasPrefix(repository, prefix+"/") {
		return nil
	}

	return fmt.Errorf("must use team registry prefix %q for private registry %q", prefix, registryHost)
}

func registryHostMatchesAny(actual string, allowedHosts []string) bool {
	normalizedActual := NormalizeRegistryHost(actual)
	if normalizedActual == "" {
		return false
	}
	for _, allowed := range allowedHosts {
		if normalizedActual == NormalizeRegistryHost(allowed) {
			return true
		}
	}
	return false
}

func isRegistryHost(candidate string) bool {
	if candidate == localhostRegistryHost {
		return true
	}
	return strings.Contains(candidate, ".") || strings.Contains(candidate, ":")
}

func stripImageTagOrDigest(repository string) string {
	trimmed := strings.TrimSpace(repository)
	if trimmed == "" {
		return ""
	}
	if at := strings.Index(trimmed, "@"); at >= 0 {
		trimmed = trimmed[:at]
	}
	lastSlash := strings.LastIndex(trimmed, "/")
	if colon := strings.LastIndex(trimmed, ":"); colon > lastSlash {
		trimmed = trimmed[:colon]
	}
	return strings.Trim(trimmed, "/")
}
