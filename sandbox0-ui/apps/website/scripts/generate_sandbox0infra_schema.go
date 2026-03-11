package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type outputDoc struct {
	GeneratedFrom string          `json:"generatedFrom"`
	Sections      []outputSection `json:"sections"`
}

type outputSection struct {
	Key         string        `json:"key"`
	Title       string        `json:"title"`
	Description string        `json:"description,omitempty"`
	Entries     []outputEntry `json:"entries"`
}

type outputEntry struct {
	Path        string   `json:"path"`
	Type        string   `json:"type"`
	Required    bool     `json:"required"`
	Default     string   `json:"default,omitempty"`
	Enum        []string `json:"enum,omitempty"`
	Description string   `json:"description,omitempty"`
}

type sectionSpec struct {
	Key   string
	Title string
}

var sectionSpecs = []sectionSpec{
	{Key: "spec.database", Title: "Database"},
	{Key: "spec.juicefsDatabase", Title: "JuiceFS Metadata Database"},
	{Key: "spec.storage", Title: "Storage"},
	{Key: "spec.registry", Title: "Registry"},
	{Key: "spec.controlPlane", Title: "Control Plane"},
	{Key: "spec.internalAuth", Title: "Internal Auth"},
	{Key: "spec.publicExposure", Title: "Public Exposure"},
	{Key: "spec.cluster", Title: "Cluster"},
	{Key: "spec.initUser", Title: "Initial Admin User"},
	{Key: "spec.builtinTemplates", Title: "Builtin Templates"},
	{Key: "spec.sandboxNodePlacement", Title: "Sandbox Node Placement"},
	{Key: "spec.services.edgeGateway", Title: "Service: edgeGateway"},
	{Key: "spec.services.scheduler", Title: "Service: scheduler"},
	{Key: "spec.services.internalGateway", Title: "Service: internalGateway"},
	{Key: "spec.services.manager", Title: "Service: manager"},
	{Key: "spec.services.storageProxy", Title: "Service: storageProxy"},
	{Key: "spec.services.netd", Title: "Service: netd"},
}

func main() {
	repoRoot, err := os.Getwd()
	if err != nil {
		fail(err)
	}

	crdPath := filepath.Join(repoRoot, "infra-operator", "chart", "crds", "infra.sandbox0.ai_sandbox0infras.yaml")
	outPath := filepath.Join(repoRoot, "sandbox0-ui", "apps", "website", "src", "generated", "docs", "sandbox0infra-schema.json")

	data, err := os.ReadFile(crdPath)
	if err != nil {
		fail(err)
	}

	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		fail(err)
	}

	specNode, err := lookupMap(doc,
		"spec", "versions", 0,
		"schema", "openAPIV3Schema", "properties", "spec",
	)
	if err != nil {
		fail(err)
	}

	out := outputDoc{GeneratedFrom: sourcePathForOutput(repoRoot, crdPath)}
	for _, sec := range sectionSpecs {
		node, err := lookupPath(specNode, strings.TrimPrefix(sec.Key, "spec."))
		if err != nil {
			continue
		}
		entries := flattenNode(sec.Key, node, false)
		if len(entries) == 0 {
			continue
		}
		out.Sections = append(out.Sections, outputSection{
			Key:         sec.Key,
			Title:       sec.Title,
			Description: stringValue(node["description"]),
			Entries:     entries,
		})
	}

	encoded, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fail(err)
	}
	encoded = append(encoded, '\n')

	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		fail(err)
	}

	if err := os.WriteFile(outPath, encoded, 0o644); err != nil {
		fail(err)
	}

	fmt.Printf("wrote %s\n", outPath)
}

func sourcePathForOutput(repoRoot, sourcePath string) string {
	rel, err := filepath.Rel(repoRoot, sourcePath)
	if err != nil {
		return filepath.Base(sourcePath)
	}
	if rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return filepath.Base(sourcePath)
	}
	return filepath.ToSlash(rel)
}

func flattenNode(path string, node map[string]any, required bool) []outputEntry {
	entry := outputEntry{
		Path:        path,
		Type:        describeType(node),
		Required:    required,
		Default:     renderValue(node["default"]),
		Enum:        renderEnum(node["enum"]),
		Description: cleanDescription(stringValue(node["description"])),
	}

	entries := []outputEntry{entry}
	if itemNode, ok := nodeMap(node["items"]); ok {
		childPath := path + "[]"
		entries = append(entries, flattenNode(childPath, itemNode, false)...)
	}

	properties, ok := nodeMap(node["properties"])
	if !ok {
		return entries
	}

	requiredSet := make(map[string]bool)
	if rawRequired, ok := node["required"].([]any); ok {
		for _, item := range rawRequired {
			requiredSet[fmt.Sprint(item)] = true
		}
	}

	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		childNode, ok := nodeMap(properties[key])
		if !ok {
			continue
		}
		childPath := path + "." + key
		entries = append(entries, flattenNode(childPath, childNode, requiredSet[key])...)
	}
	return entries
}

func describeType(node map[string]any) string {
	if t := stringValue(node["type"]); t != "" {
		if t == "array" {
			if itemNode, ok := nodeMap(node["items"]); ok {
				return "array<" + describeType(itemNode) + ">"
			}
			return "array"
		}
		return t
	}

	if anyOf, ok := node["anyOf"].([]any); ok {
		parts := make([]string, 0, len(anyOf))
		for _, item := range anyOf {
			child, ok := nodeMap(item)
			if !ok {
				continue
			}
			part := stringValue(child["type"])
			if part != "" {
				parts = append(parts, part)
			}
		}
		if len(parts) > 0 {
			return strings.Join(parts, "|")
		}
	}

	if _, ok := node["properties"]; ok {
		return "object"
	}
	return "unknown"
}

func renderEnum(value any) []string {
	raw, ok := value.([]any)
	if !ok || len(raw) == 0 {
		return nil
	}
	out := make([]string, 0, len(raw))
	for _, item := range raw {
		out = append(out, fmt.Sprint(item))
	}
	return out
}

func renderValue(value any) string {
	switch v := value.(type) {
	case nil:
		return ""
	case string:
		return v
	case []any:
		parts := make([]string, 0, len(v))
		for _, item := range v {
			parts = append(parts, renderValue(item))
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, key := range keys {
			parts = append(parts, key+": "+renderValue(v[key]))
		}
		return "{" + strings.Join(parts, ", ") + "}"
	default:
		return fmt.Sprint(v)
	}
}

func cleanDescription(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	return strings.Join(strings.Fields(text), " ")
}

func lookupMap(root map[string]any, path ...any) (map[string]any, error) {
	current := any(root)
	for _, step := range path {
		switch s := step.(type) {
		case string:
			m, ok := current.(map[string]any)
			if !ok {
				return nil, fmt.Errorf("path %v is not an object", step)
			}
			current = m[s]
		case int:
			arr, ok := current.([]any)
			if !ok || s >= len(arr) {
				return nil, fmt.Errorf("path %v is not an array index", step)
			}
			current = arr[s]
		default:
			return nil, fmt.Errorf("unsupported path step %T", step)
		}
	}

	m, ok := current.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("target is not an object")
	}
	return m, nil
}

func lookupPath(root map[string]any, path string) (map[string]any, error) {
	current := root
	if path == "" {
		return current, nil
	}

	parts := strings.Split(path, ".")
	for _, part := range parts {
		properties, ok := nodeMap(current["properties"])
		if !ok {
			return nil, fmt.Errorf("missing properties for %s", part)
		}
		next, ok := nodeMap(properties[part])
		if !ok {
			return nil, fmt.Errorf("missing section %s", part)
		}
		current = next
	}
	return current, nil
}

func nodeMap(value any) (map[string]any, bool) {
	m, ok := value.(map[string]any)
	return m, ok
}

func stringValue(value any) string {
	s, _ := value.(string)
	return s
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
