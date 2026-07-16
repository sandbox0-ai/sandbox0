package framework

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

const managerOwnedNamespaceLabelSelector = "app.kubernetes.io/managed-by=sandbox0-manager"
const managerOwnedNamespaceCleanupTimeout = "6m"

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
	preserveInfraForNamespaceCleanup := false
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

		if !cfg.SkipClusterDelete && !workingCfg.PreserveScenario {
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

	namespaceCleanupErr, managerStopErr := cleanupManagerNamespacesBeforeStoppingManager(
		func() error {
			return KubectlQuiesceManagerTemplateReconcilers(testCtx.Context, workingCfg.Kubeconfig, scenario.InfraNamespace, "30s")
		},
		func() error {
			return CleanupManagerOwnedNamespaces(testCtx.Context, workingCfg.Kubeconfig, managerOwnedNamespaceCleanupTimeout)
		},
		func() error {
			return ScaleScenarioManagerToZero(testCtx.Context, workingCfg.Kubeconfig, env.Infra)
		},
	)
	if managerStopErr != nil {
		fmt.Printf("Failed to scale scenario manager down after setup namespace cleanup: %v\n", managerStopErr)
	}
	if namespaceCleanupErr != nil {
		cleanup()
		return nil, nil, namespaceCleanupErr
	}
	if err := CleanupNamespace(testCtx.Context, workingCfg.Kubeconfig, scenario.InfraNamespace, "3m"); err != nil {
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
		if !workingCfg.SkipOperatorUninstall && !workingCfg.PreserveScenario {
			appendCleanup(func() {
				if preserveInfraForNamespaceCleanup {
					fmt.Printf("Skipping infra-operator uninstall because manager-owned namespaces are still terminating; preserving CSI drivers for the next cleanup pass.\n")
					return
				}
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
	if err := PatchScenarioSandboxRuntimeClass(testCtx.Context, workingCfg.Kubeconfig, env.Infra, workingCfg.SandboxRuntimeClassName); err != nil {
		cleanup()
		return nil, nil, err
	}
	for _, rollout := range scenario.Rollouts {
		resourceID, err := rollout.ResourceID()
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		timeout := rollout.Timeout
		if timeout == "" {
			timeout = scenario.ReadyTimeout
		}
		if timeout == "" {
			timeout = "5m"
		}
		if err := KubectlRolloutStatus(testCtx.Context, workingCfg.Kubeconfig, rollout.Namespace, resourceID, timeout); err != nil {
			cleanup()
			return nil, nil, err
		}
	}
	if workingCfg.PreserveScenario {
		fmt.Printf("Preserving scenario %q for follow-up tests.\n", scenario.Name)
	} else {
		appendCleanup(func() {
			// Pods can mount sandbox0 CSI volumes. Keep manager and its embedded storage
			// API running until namespace deletion completes kubelet unpublish. Quiesce
			// template syncing first so it cannot recreate namespaces during teardown.
			namespaceCleanupErr, managerStopErr := cleanupManagerNamespacesBeforeStoppingManager(
				func() error {
					return KubectlQuiesceManagerTemplateReconcilers(testCtx.Context, workingCfg.Kubeconfig, scenario.InfraNamespace, "30s")
				},
				func() error {
					return CleanupManagerOwnedNamespaces(testCtx.Context, workingCfg.Kubeconfig, managerOwnedNamespaceCleanupTimeout)
				},
				func() error {
					return ScaleScenarioManagerToZero(testCtx.Context, workingCfg.Kubeconfig, env.Infra)
				},
			)
			if managerStopErr != nil {
				fmt.Printf("Failed to scale scenario manager down after namespace cleanup: %v\n", managerStopErr)
			}
			if namespaceCleanupErr != nil {
				preserveInfraForNamespaceCleanup = true
				fmt.Printf("Failed to clean up manager-owned namespaces: %v\n", namespaceCleanupErr)
				fmt.Printf("Preserving scenario infra so CSI drivers remain available for the next cleanup pass.\n")
				return
			}
			if err := KubectlDeleteManifest(testCtx.Context, workingCfg.Kubeconfig, manifestPath); err != nil {
				fmt.Printf("Failed to delete scenario manifest: %v\n", err)
			}
			if err := KubectlWaitForNamespaceDeleted(testCtx.Context, workingCfg.Kubeconfig, scenario.InfraNamespace, "3m"); err != nil {
				fmt.Printf("Failed to clean up infra namespace %q: %v\n", scenario.InfraNamespace, err)
			}
		})
	}

	return env, cleanup, nil
}

// cleanupManagerNamespacesBeforeStoppingManager quiesces template syncing while
// preserving manager-hosted storage until kubelet unpublish operations complete.
// If namespace cleanup fails, stopManager is deliberately not called so a later
// cleanup pass can continue using the storage API.
func cleanupManagerNamespacesBeforeStoppingManager(quiesceManager, cleanupNamespaces, stopManager func() error) (namespaceCleanupErr, managerStopErr error) {
	if err := quiesceManager(); err != nil {
		return fmt.Errorf("quiesce manager template reconciliation: %w", err), nil
	}
	if err := cleanupNamespaces(); err != nil {
		return err, nil
	}
	return nil, stopManager()
}

// ScaleScenarioManagerToZero stops manager reconciliation and its embedded storage API.
// Call it only after manager-owned namespaces have finished terminating.
func ScaleScenarioManagerToZero(ctx context.Context, kubeconfig string, infra InfraConfig) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if infra.Name == "" || infra.Namespace == "" {
		return fmt.Errorf("infra name and namespace are required")
	}

	name := infra.Name + "-manager"
	err := Kubectl(
		ctx,
		kubeconfig,
		"scale",
		"deployment/"+name,
		"--replicas=0",
		"--namespace",
		infra.Namespace,
	)
	if err == nil {
		return nil
	}

	output, getErr := KubectlOutput(
		ctx,
		kubeconfig,
		"get",
		"deployment",
		name,
		"--namespace",
		infra.Namespace,
		"--ignore-not-found=true",
		"-o",
		"name",
	)
	if getErr == nil && strings.TrimSpace(output) == "" {
		return nil
	}
	return err
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

// CleanupNamespace removes a scenario namespace and waits until Kubernetes finalizes it.
func CleanupNamespace(ctx context.Context, kubeconfig, namespace, timeout string) error {
	if ctx == nil {
		return fmt.Errorf("context is required")
	}
	if namespace == "" {
		return fmt.Errorf("namespace is required")
	}
	if err := KubectlDeleteNamespace(ctx, kubeconfig, namespace); err != nil {
		return err
	}
	return KubectlWaitForNamespaceDeleted(ctx, kubeconfig, namespace, timeout)
}
