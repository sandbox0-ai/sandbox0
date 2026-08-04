package framework

import (
	"context"
	"fmt"
)

// InstallOperator installs or upgrades the infra-operator chart.
func InstallOperator(ctx context.Context, cfg Config) error {
	fmt.Printf("Installing infra-operator into namespace %q...\n", cfg.OperatorNamespace)
	if cfg.OperatorChartPath == "" {
		return fmt.Errorf("operator chart path is required")
	}

	setValues := []string{}
	if cfg.OperatorImageRepo != "" {
		setValues = append(setValues, fmt.Sprintf("image.repository=%s", cfg.OperatorImageRepo))
	}
	if cfg.OperatorImageTag != "" {
		setValues = append(setValues, fmt.Sprintf("image.tag=%s", cfg.OperatorImageTag))
	}
	if cfg.OperatorImagePullPolicy != "" {
		setValues = append(setValues, fmt.Sprintf("config.imagePullPolicy=%s", cfg.OperatorImagePullPolicy))
	}

	return HelmUpgradeInstall(
		ctx,
		cfg.OperatorReleaseName,
		cfg.OperatorChartPath,
		cfg.OperatorNamespace,
		cfg.Kubeconfig,
		cfg.OperatorValuesPath,
		setValues,
	)
}

// UninstallOperator removes the infra-operator release.
func UninstallOperator(ctx context.Context, cfg Config) error {
	fmt.Printf("Uninstalling infra-operator from namespace %q...\n", cfg.OperatorNamespace)
	return HelmUninstall(ctx, cfg.OperatorReleaseName, cfg.OperatorNamespace, cfg.Kubeconfig)
}

// WaitForOperatorReady waits until the operator deployment is ready.
func WaitForOperatorReady(ctx context.Context, cfg Config, timeout string) error {
	fmt.Printf("Waiting for infra-operator deployment %q to be ready (timeout: %s)...\n", cfg.OperatorDeploymentName, timeout)
	return WaitForDeployment(ctx, cfg.Kubeconfig, cfg.OperatorNamespace, cfg.OperatorDeploymentName, timeout)
}
