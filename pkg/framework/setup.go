package framework

import (
	"context"
	"fmt"
	"os"
	"time"
)

const managerOwnedNamespaceLabelSelector = "app.kubernetes.io/managed-by=sandbox0-manager"

// SetupScenario provisions a cluster, installs the operator, and applies a sample manifest.
func SetupScenario(cfg Config, scenario Scenario) (*ScenarioEnv, func(), error) {
	workingCfg := cfg
	if scenario.ManifestPath == "" {
		return nil, nil, fmt.Errorf("scenario manifest path is required")
	}
	if scenario.InfraNamespace == "" {
		scenario.InfraNamespace = cfg.InfraNamespace
	}
	if scenario.InfraName == "" {
		return nil, nil, fmt.Errorf("scenario infra name is required")
	}

	cluster := NewCluster(cfg.ClusterName)
	testCtx := NewTestContext(cluster)
	env := &ScenarioEnv{
		Config:   workingCfg,
		Scenario: scenario,
		TestCtx:  testCtx,
		Infra: InfraConfig{
			Name:      scenario.InfraName,
			Namespace: scenario.InfraNamespace,
		},
	}

	cleanupFns := []func(){}
	appendCleanup := func(fn func()) {
		if fn != nil {
			cleanupFns = append(cleanupFns, fn)
		}
	}
	cleanup := func() {
		for i := len(cleanupFns) - 1; i >= 0; i-- {
			cleanupFns[i]()
		}
	}

	lockRelease, err := AcquireE2ELock(testCtx.Context, cfg.ClusterName, 0)
	if err != nil {
		return nil, nil, err
	}
	appendCleanup(lockRelease)

	if !cfg.UseExistingCluster {
		fmt.Printf("Setting up Kind cluster %q...\n", cfg.ClusterName)
		if err := cluster.CreateKind(testCtx.Context, cfg.KindConfigPath); err != nil {
			cleanup()
			return nil, nil, err
		}

		if err := cluster.LoadDockerImage(testCtx.Context, "sandbox0ai/infra:latest"); err != nil {
			cleanup()
			return nil, nil, err
		}

		if !cfg.SkipClusterDelete {
			appendCleanup(func() {
				fmt.Printf("Deleting Kind cluster %q...\n", cfg.ClusterName)
				_ = testCtx.Cluster.DeleteKind(testCtx.Context)
			})
		}
	}

	if workingCfg.Kubeconfig == "" {
		kubeconfig, err := cluster.ExportKubeconfig(testCtx.Context)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		workingCfg.Kubeconfig = kubeconfig
		env.Config = workingCfg
		appendCleanup(func() {
			fmt.Printf("Cleaning up temporary kubeconfig %q...\n", kubeconfig)
			_ = os.Remove(kubeconfig)
		})
	}

	if err := CleanupManagerOwnedNamespaces(testCtx.Context, workingCfg.Kubeconfig, "3m"); err != nil {
		cleanup()
		return nil, nil, err
	}

	if !workingCfg.SkipOperatorInstall {
		fmt.Printf("Installing infra-operator...\n")
		if err := InstallOperator(testCtx.Context, workingCfg); err != nil {
			cleanup()
			return nil, nil, err
		}
		if err := WaitForOperatorReady(testCtx.Context, workingCfg, "5m"); err != nil {
			cleanup()
			return nil, nil, err
		}
		if !workingCfg.SkipOperatorUninstall {
			appendCleanup(func() {
				fmt.Printf("Uninstalling infra-operator...\n")
				_ = UninstallOperator(testCtx.Context, workingCfg)
				time.Sleep(2 * time.Second)
			})
		}
	}

	if err := EnsureNamespace(testCtx.Context, workingCfg.Kubeconfig, scenario.InfraNamespace); err != nil {
		cleanup()
		return nil, nil, err
	}

	for _, spec := range scenario.Secrets {
		if err := ApplySecret(testCtx.Context, workingCfg.Kubeconfig, spec); err != nil {
			cleanup()
			return nil, nil, err
		}
	}

	manifestPath := scenario.ManifestPath
	if scenario.InfraNamespace != "" {
		namespacedManifest, removeNamespacedManifest, err := RewriteManifestNamespace(scenario.ManifestPath, scenario.InfraNamespace)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		if namespacedManifest != scenario.ManifestPath {
			manifestPath = namespacedManifest
			appendCleanup(removeNamespacedManifest)
		}
	}

	if err := ApplyManifest(testCtx.Context, workingCfg.Kubeconfig, manifestPath); err != nil {
		cleanup()
		return nil, nil, err
	}
	appendCleanup(func() {
		_ = KubectlDeleteManifest(testCtx.Context, workingCfg.Kubeconfig, manifestPath)
		if err := CleanupManagerOwnedNamespaces(testCtx.Context, workingCfg.Kubeconfig, "3m"); err != nil {
			fmt.Printf("Failed to clean up manager-owned namespaces: %v\n", err)
		}
	})

	return env, cleanup, nil
}

// CleanupManagerOwnedNamespaces removes namespaces created by sandbox managers during e2e scenarios.
func CleanupManagerOwnedNamespaces(ctx context.Context, kubeconfig, timeout string) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if err := KubectlDeleteNamespacesByLabel(ctx, kubeconfig, managerOwnedNamespaceLabelSelector); err != nil {
		return err
	}
	return KubectlWaitForNamespacesDeletedByLabel(ctx, kubeconfig, managerOwnedNamespaceLabelSelector, timeout)
}
