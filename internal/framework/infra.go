package framework

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
)

// InfraConfig identifies a Sandbox0Infra instance for E2E tests.
type InfraConfig struct {
	Name      string
	Namespace string
}

type SecretSpec struct {
	Name       string
	Namespace  string
	Type       string
	StringData map[string]string
}

// WaitForSandbox0InfraReady waits for the Ready condition.
func WaitForSandbox0InfraReady(ctx context.Context, kubeconfig string, infra InfraConfig, timeout string) error {
	fmt.Printf("Waiting for Sandbox0Infra %q in namespace %q to reach Ready state (timeout: %s)...\n", infra.Name, infra.Namespace, timeout)
	if infra.Name == "" || infra.Namespace == "" {
		return fmt.Errorf("infra name and namespace are required")
	}
	return KubectlWaitForCondition(ctx, kubeconfig, infra.Namespace, "sandbox0infra", infra.Name, "Ready", timeout)
}

func EnsureNamespace(ctx context.Context, kubeconfig, namespace string) error {
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	fmt.Printf("Ensuring namespace %q exists...\n", namespace)
	manifest := fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", namespace)
	return ApplyManifestContent(ctx, kubeconfig, "sandbox0-e2e-namespace-", manifest)
}

func ApplySecret(ctx context.Context, kubeconfig string, spec SecretSpec) error {
	if spec.Name == "" || spec.Namespace == "" {
		return fmt.Errorf("secret name and namespace are required")
	}
	if len(spec.StringData) == 0 {
		return fmt.Errorf("secret stringData is required")
	}
	secretType := strings.TrimSpace(spec.Type)
	if secretType == "" {
		secretType = "Opaque"
	}
	fmt.Printf("Applying secret %q in namespace %q...\n", spec.Name, spec.Namespace)

	manifest := strings.Builder{}
	manifest.WriteString("apiVersion: v1\nkind: Secret\nmetadata:\n")
	manifest.WriteString(fmt.Sprintf("  name: %s\n", spec.Name))
	manifest.WriteString(fmt.Sprintf("  namespace: %s\n", spec.Namespace))
	manifest.WriteString(fmt.Sprintf("type: %s\n", secretType))
	manifest.WriteString("stringData:\n")

	keys := make([]string, 0, len(spec.StringData))
	for key := range spec.StringData {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		manifest.WriteString(fmt.Sprintf("  %s: %q\n", key, spec.StringData[key]))
	}

	return ApplyManifestContent(ctx, kubeconfig, "sandbox0-e2e-secret-", manifest.String())
}

func ApplyManifestContent(ctx context.Context, kubeconfig, prefix, content string) error {
	file, err := os.CreateTemp("", prefix)
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())

	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}

	return ApplyManifest(ctx, kubeconfig, file.Name())
}

func randomHex(bytesLen int) string {
	if bytesLen <= 0 {
		return ""
	}
	buffer := make([]byte, bytesLen)
	if _, err := rand.Read(buffer); err != nil {
		return ""
	}
	return hex.EncodeToString(buffer)
}
