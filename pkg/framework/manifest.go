package framework

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"

	yamlv3 "gopkg.in/yaml.v3"
)

// RewriteManifestNamespace creates a temporary manifest with the infra namespace
// rewritten to targetNamespace. It returns the original path when no rewrite is needed.
func RewriteManifestNamespace(manifestPath, targetNamespace string) (string, func(), error) {
	targetNamespace = strings.TrimSpace(targetNamespace)
	if manifestPath == "" {
		return "", nil, fmt.Errorf("manifest path is required")
	}
	if targetNamespace == "" {
		return "", nil, fmt.Errorf("target namespace is required")
	}

	content, err := os.ReadFile(manifestPath)
	if err != nil {
		return "", nil, fmt.Errorf("read manifest %q: %w", manifestPath, err)
	}
	sourceNamespace, err := manifestInfraNamespace(content)
	if err != nil {
		return "", nil, err
	}
	if sourceNamespace == targetNamespace {
		return manifestPath, nil, nil
	}

	rewritten, err := rewriteManifestNamespace(content, sourceNamespace, targetNamespace)
	if err != nil {
		return "", nil, fmt.Errorf("rewrite manifest namespace: %w", err)
	}

	file, err := os.CreateTemp("", "sandbox0-e2e-manifest-*.yaml")
	if err != nil {
		return "", nil, fmt.Errorf("create temporary manifest: %w", err)
	}
	path := file.Name()
	if _, err := file.Write(rewritten); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("write temporary manifest %q: %w", path, err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", nil, fmt.Errorf("close temporary manifest %q: %w", path, err)
	}

	return path, func() { _ = os.Remove(path) }, nil
}

func manifestInfraNamespace(content []byte) (string, error) {
	infra, err := extractSandbox0Infra(content)
	if err != nil {
		return "", err
	}
	namespace := strings.TrimSpace(infra.Namespace)
	if namespace == "" {
		return "", fmt.Errorf("Sandbox0Infra metadata.namespace is required for namespace rewrite")
	}
	return namespace, nil
}

func rewriteManifestNamespace(content []byte, sourceNamespace, targetNamespace string) ([]byte, error) {
	decoder := yamlv3.NewDecoder(bytes.NewReader(content))
	var output bytes.Buffer
	encoder := yamlv3.NewEncoder(&output)
	encoder.SetIndent(2)
	defer encoder.Close()

	for {
		var doc yamlv3.Node
		if err := decoder.Decode(&doc); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if isEmptyYAMLDocument(&doc) {
			continue
		}
		rewriteNamespaceNode(&doc, sourceNamespace, targetNamespace)
		if err := encoder.Encode(&doc); err != nil {
			return nil, err
		}
	}
	return output.Bytes(), nil
}

func rewriteNamespaceNode(node *yamlv3.Node, sourceNamespace, targetNamespace string) {
	if node == nil {
		return
	}
	if node.Kind == yamlv3.MappingNode {
		for i := 0; i+1 < len(node.Content); i += 2 {
			key := node.Content[i]
			value := node.Content[i+1]
			if key.Value == "namespace" && value.Kind == yamlv3.ScalarNode && value.Value == sourceNamespace {
				value.Value = targetNamespace
			}
			if key.Value == "kind" && value.Kind == yamlv3.ScalarNode && value.Value == "Namespace" {
				rewriteNamespaceResourceName(node, sourceNamespace, targetNamespace)
			}
			rewriteNamespaceNode(value, sourceNamespace, targetNamespace)
		}
		return
	}
	for _, child := range node.Content {
		rewriteNamespaceNode(child, sourceNamespace, targetNamespace)
	}
}

func rewriteNamespaceResourceName(node *yamlv3.Node, sourceNamespace, targetNamespace string) {
	metadata := mappingValue(node, "metadata")
	if metadata == nil || metadata.Kind != yamlv3.MappingNode {
		return
	}
	name := mappingValue(metadata, "name")
	if name != nil && name.Kind == yamlv3.ScalarNode && name.Value == sourceNamespace {
		name.Value = targetNamespace
	}
}

func mappingValue(node *yamlv3.Node, key string) *yamlv3.Node {
	if node == nil || node.Kind != yamlv3.MappingNode {
		return nil
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
}

func isEmptyYAMLDocument(node *yamlv3.Node) bool {
	return node == nil || len(node.Content) == 0 || (len(node.Content) == 1 && node.Content[0].Kind == 0)
}
