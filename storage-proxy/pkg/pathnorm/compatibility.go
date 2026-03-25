package pathnorm

import (
	"path"
	"slices"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/unicode/norm"
)

const (
	IssueCodeCasefoldCollision       = "casefold_collision"
	IssueCodeWindowsReservedName     = "windows_reserved_name"
	IssueCodeWindowsTrailingDotSpace = "windows_trailing_dot_space"
	IssueCodeWindowsForbiddenRune    = "windows_forbidden_character"
	IssueCodeWindowsControlCharacter = "windows_control_character"
)

type FilesystemCapabilities struct {
	CaseSensitive                   bool `json:"case_sensitive"`
	UnicodeNormalizationInsensitive bool `json:"unicode_normalization_insensitive"`
	WindowsCompatiblePaths          bool `json:"windows_compatible_paths"`
}

type CompatibilityIssue struct {
	Code           string   `json:"code"`
	Path           string   `json:"path,omitempty"`
	NormalizedPath string   `json:"normalized_path,omitempty"`
	Paths          []string `json:"paths,omitempty"`
	Segment        string   `json:"segment,omitempty"`
	Message        string   `json:"message,omitempty"`
}

func DefaultFilesystemCapabilities(platform string, legacyCaseSensitive bool) FilesystemCapabilities {
	caps := FilesystemCapabilities{
		CaseSensitive: legacyCaseSensitive,
	}
	if !legacyCaseSensitive {
		caps.UnicodeNormalizationInsensitive = true
	}

	switch strings.ToLower(strings.TrimSpace(platform)) {
	case "darwin", "macos":
		caps.CaseSensitive = false
		caps.UnicodeNormalizationInsensitive = true
	case "windows", "win32":
		caps.CaseSensitive = false
		caps.UnicodeNormalizationInsensitive = true
		caps.WindowsCompatiblePaths = true
	}

	return caps
}

func NormalizeFilesystemCapabilities(platform string, legacyCaseSensitive bool, provided *FilesystemCapabilities) FilesystemCapabilities {
	if provided == nil {
		return DefaultFilesystemCapabilities(platform, legacyCaseSensitive)
	}

	caps := *provided
	if !caps.CaseSensitive {
		caps.UnicodeNormalizationInsensitive = true
	}
	return caps
}

func MergeFilesystemCapabilities(existing, incoming FilesystemCapabilities) FilesystemCapabilities {
	merged := FilesystemCapabilities{
		CaseSensitive:                   existing.CaseSensitive && incoming.CaseSensitive,
		UnicodeNormalizationInsensitive: existing.UnicodeNormalizationInsensitive || incoming.UnicodeNormalizationInsensitive,
		WindowsCompatiblePaths:          existing.WindowsCompatiblePaths || incoming.WindowsCompatiblePaths,
	}
	if !merged.CaseSensitive {
		merged.UnicodeNormalizationInsensitive = true
	}
	return merged
}

func CompatibilityPathKey(raw string, caps FilesystemCapabilities) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	cleaned := path.Clean("/" + strings.TrimSpace(raw))
	if cleaned == "." {
		return "/"
	}
	parts := strings.Split(cleaned, "/")
	for i, part := range parts {
		if i == 0 || part == "" {
			continue
		}
		if caps.UnicodeNormalizationInsensitive {
			part = norm.NFD.String(part)
		}
		if !caps.CaseSensitive {
			part = strings.ToLower(part)
		}
		parts[i] = part
	}
	return strings.Join(parts, "/")
}

func RequiresPortableNameAudit(caps FilesystemCapabilities) bool {
	if caps == (FilesystemCapabilities{}) {
		return false
	}
	return !caps.CaseSensitive || caps.UnicodeNormalizationInsensitive || caps.WindowsCompatiblePaths
}

func ValidatePathCompatibility(raw string, caps FilesystemCapabilities) []CompatibilityIssue {
	cleaned := path.Clean("/" + strings.TrimSpace(raw))
	if cleaned == "." || cleaned == "/" {
		return nil
	}

	issues := make([]CompatibilityIssue, 0)
	if !caps.WindowsCompatiblePaths {
		return issues
	}

	for _, segment := range strings.Split(strings.TrimPrefix(cleaned, "/"), "/") {
		if segment == "" {
			continue
		}
		if hasControlCharacter(segment) {
			issues = append(issues, CompatibilityIssue{
				Code:    IssueCodeWindowsControlCharacter,
				Path:    cleaned,
				Segment: segment,
				Message: "path segment contains a Windows control character",
			})
		}
		if strings.ContainsAny(segment, "<>:\"\\|?*") {
			issues = append(issues, CompatibilityIssue{
				Code:    IssueCodeWindowsForbiddenRune,
				Path:    cleaned,
				Segment: segment,
				Message: "path segment contains a Windows-forbidden character",
			})
		}
		if strings.TrimRight(segment, ". ") != segment {
			issues = append(issues, CompatibilityIssue{
				Code:    IssueCodeWindowsTrailingDotSpace,
				Path:    cleaned,
				Segment: segment,
				Message: "path segment ends with a dot or space and is not portable to Windows",
			})
		}
		if isWindowsReservedName(segment) {
			issues = append(issues, CompatibilityIssue{
				Code:    IssueCodeWindowsReservedName,
				Path:    cleaned,
				Segment: segment,
				Message: "path segment uses a Windows reserved device name",
			})
		}
	}

	return DeduplicateIssues(issues)
}

func BuildCasefoldCollisionIssue(normalizedPath string, paths []string) CompatibilityIssue {
	clone := slices.Clone(paths)
	slices.Sort(clone)
	return CompatibilityIssue{
		Code:           IssueCodeCasefoldCollision,
		NormalizedPath: normalizedPath,
		Paths:          clone,
		Message:        "logical paths collide under the requested filesystem capabilities",
	}
}

func DeduplicateIssues(issues []CompatibilityIssue) []CompatibilityIssue {
	if len(issues) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(issues))
	out := make([]CompatibilityIssue, 0, len(issues))
	for _, issue := range issues {
		key := issue.Code + "\x00" + issue.Path + "\x00" + issue.NormalizedPath + "\x00" + issue.Segment + "\x00" + strings.Join(issue.Paths, "\x00")
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, issue)
	}
	return out
}

func hasControlCharacter(v string) bool {
	for len(v) > 0 {
		r, size := utf8.DecodeRuneInString(v)
		if r < 0x20 {
			return true
		}
		v = v[size:]
	}
	return false
}

func isWindowsReservedName(segment string) bool {
	base := segment
	if idx := strings.IndexRune(base, '.'); idx >= 0 {
		base = base[:idx]
	}
	switch strings.ToUpper(base) {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return true
	default:
		return false
	}
}
