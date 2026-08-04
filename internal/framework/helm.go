package framework

import (
	"context"
	"fmt"
)

// HelmUpgradeInstall installs or upgrades a release using helm.
func HelmUpgradeInstall(ctx context.Context, releaseName, chartPath, namespace, kubeconfig, valuesPath string, setValues []string) error {
	fmt.Printf("Installing or upgrading Helm release %q from chart %q in namespace %q with values path %q and set values %v...\n", releaseName, chartPath, namespace, valuesPath, setValues)
	if releaseName == "" {
		return fmt.Errorf("release name is required")
	}
	if chartPath == "" {
		return fmt.Errorf("chart path is required")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	args := []string{
		"upgrade",
		"--install",
		releaseName,
		chartPath,
		"--namespace",
		namespace,
		"--create-namespace",
	}
	if valuesPath != "" {
		args = append(args, "--values", valuesPath)
	}
	for _, value := range setValues {
		if value == "" {
			continue
		}
		args = append(args, "--set", value)
	}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}

	return RunCommand(ctx, "helm", args...)
}

// HelmUninstall removes a release using helm.
func HelmUninstall(ctx context.Context, releaseName, namespace, kubeconfig string) error {
	fmt.Printf("Uninstalling Helm release %q from namespace %q...\n", releaseName, namespace)
	if releaseName == "" {
		return fmt.Errorf("release name is required")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}

	args := []string{
		"uninstall",
		releaseName,
		"--namespace",
		namespace,
	}
	if kubeconfig != "" {
		args = append(args, "--kubeconfig", kubeconfig)
	}

	return RunCommand(ctx, "helm", args...)
}
