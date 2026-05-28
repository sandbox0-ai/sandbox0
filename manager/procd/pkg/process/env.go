package process

import (
	"sort"
	"strings"
)

// CloneEnvVars returns a shallow copy of environment variables.
func CloneEnvVars(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for key, value := range src {
		if normalized, ok := normalizeEnvKey(key); ok {
			out[normalized] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// MergeEnvVars merges environment maps with later maps overriding earlier maps.
func MergeEnvVars(layers ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, layer := range layers {
		for key, value := range layer {
			if normalized, ok := normalizeEnvKey(key); ok {
				out[normalized] = value
			}
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// MergeEnvironment overlays environment maps onto a base environment slice.
// Precedence is left to right: base < layers[0] < layers[1]...
func MergeEnvironment(base []string, layers ...map[string]string) []string {
	values := make(map[string]string, len(base))
	order := make([]string, 0, len(base))
	seen := make(map[string]struct{}, len(base))
	for _, item := range base {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		if normalized, ok := normalizeEnvKey(key); ok {
			key = normalized
		} else {
			continue
		}
		if _, exists := seen[key]; !exists {
			order = append(order, key)
			seen[key] = struct{}{}
		}
		values[key] = value
	}

	for _, layer := range layers {
		normalizedValues := make(map[string]string, len(layer))
		keys := make([]string, 0, len(layer))
		for key, value := range layer {
			if normalized, ok := normalizeEnvKey(key); ok {
				if _, exists := normalizedValues[normalized]; !exists {
					keys = append(keys, normalized)
				}
				normalizedValues[normalized] = value
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			if _, exists := seen[key]; !exists {
				order = append(order, key)
				seen[key] = struct{}{}
			}
			values[key] = normalizedValues[key]
		}
	}

	out := make([]string, 0, len(order))
	for _, key := range order {
		out = append(out, key+"="+values[key])
	}
	return out
}

func normalizeEnvKey(key string) (string, bool) {
	key = strings.TrimSpace(key)
	if key == "" || strings.Contains(key, "=") {
		return "", false
	}
	return key, true
}
