package framework

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// PatchScenarioSandboxRuntimeClass points scenario sandbox Pods at the requested RuntimeClass.
func PatchScenarioSandboxRuntimeClass(ctx context.Context, kubeconfig string, infra InfraConfig, runtimeClassName string) error {
	runtimeClassName = strings.TrimSpace(runtimeClassName)
	if runtimeClassName == "" {
		return nil
	}
	if infra.Name == "" || infra.Namespace == "" {
		return fmt.Errorf("infra name and namespace are required")
	}

	patch, err := sandboxRuntimeClassPatch(runtimeClassName)
	if err != nil {
		return err
	}
	return KubectlPatch(ctx, kubeconfig, infra.Namespace, "sandbox0infra", infra.Name, patch)
}

func sandboxRuntimeClassPatch(runtimeClassName string) (string, error) {
	patch := map[string]any{
		"spec": map[string]any{
			"services": map[string]any{
				"manager": map[string]any{
					"config": map[string]any{
						"sandboxRuntimeClassName": runtimeClassName,
					},
				},
			},
		},
	}
	content, err := json.Marshal(patch)
	if err != nil {
		return "", fmt.Errorf("marshal sandbox runtime class patch: %w", err)
	}
	return string(content), nil
}
