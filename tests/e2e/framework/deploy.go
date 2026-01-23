package framework

import (
	"context"
	"fmt"
)

// ApplyManifest applies a YAML manifest to the cluster using kubectl.
func ApplyManifest(ctx context.Context, kubeconfig, manifestPath string) error {
	fmt.Printf("Applying manifest from file %q...\n", manifestPath)
	if manifestPath == "" {
		return fmt.Errorf("manifest path is required")
	}

	args := []string{"apply", "-f", manifestPath}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}

	return RunCommand(ctx, "kubectl", args...)
}

// WaitForDeployment waits until a deployment is ready.
func WaitForDeployment(ctx context.Context, kubeconfig, namespace, name string, timeout string) error {
	fmt.Printf("Waiting for deployment %q in namespace %q to be ready (timeout: %s)...\n", name, namespace, timeout)
	if name == "" {
		return fmt.Errorf("deployment name is required")
	}

	resource := fmt.Sprintf("deployment/%s", name)
	return KubectlRolloutStatus(ctx, kubeconfig, namespace, resource, timeout)
}
