package suites

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/infra/tests/e2e/framework"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Operator entrypoint", Ordered, func() {
	var (
		controlPlane framework.InfraConfig
		dataPlane    framework.InfraConfig
		allInOne     framework.InfraConfig
	)

	BeforeAll(func() {
		controlPlane = framework.InfraConfig{
			Name:      cfg.InfraControlPlaneName,
			Namespace: cfg.InfraNamespace,
		}
		dataPlane = framework.InfraConfig{
			Name:      cfg.InfraDataPlaneName,
			Namespace: cfg.InfraNamespace,
		}
		allInOne = framework.InfraConfig{
			Name:      cfg.InfraAllName,
			Namespace: cfg.InfraNamespace,
		}
	})

	shouldRun := func(target string) bool {
		mode := strings.TrimSpace(strings.ToLower(cfg.TestMode))
		if mode == "" || mode == "all" {
			return true
		}
		return mode == target
	}

	Context("installation", func() {
		It("installs CRDs and reconciles control-plane components", func() {
			if !shouldRun("control-plane") {
				fmt.Printf("E2E_TEST_MODE does not include control-plane, skipping...\n")
				Skip("E2E_TEST_MODE does not include control-plane")
			}

			err := framework.Kubectl(testCtx.Context, cfg.Kubeconfig, "get", "crd", "sandbox0infras.infra.sandbox0.ai")
			Expect(err).NotTo(HaveOccurred())

			err = framework.WaitForDeployment(testCtx.Context, cfg.Kubeconfig, cfg.OperatorNamespace, cfg.OperatorDeploymentName, "3m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.EnsureNamespace(testCtx.Context, cfg.Kubeconfig, cfg.InfraNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.ApplyInfraControlPlaneSecrets(testCtx.Context, cfg.Kubeconfig, cfg.InfraNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.ApplyManifest(testCtx.Context, cfg.Kubeconfig, cfg.InfraControlPlaneManifestPath)
			Expect(err).NotTo(HaveOccurred())

			err = framework.WaitForSandbox0InfraReady(testCtx.Context, cfg.Kubeconfig, controlPlane, "15m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, fmt.Sprintf("deployment/%s-edge-gateway", controlPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, fmt.Sprintf("deployment/%s-scheduler", controlPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("upgrade and rollback", func() {
		It("applies configuration changes with rolling updates", func() {
			if !shouldRun("control-plane") {
				fmt.Printf("E2E_TEST_MODE does not include control-plane, skipping...\n")
				Skip("E2E_TEST_MODE does not include control-plane")
			}

			patch := `{"spec":{"services":{"edgeGateway":{"replicas":2}}}}`
			err := framework.KubectlPatch(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "sandbox0infra", controlPlane.Name, patch)
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, fmt.Sprintf("deployment/%s-edge-gateway", controlPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			replicas, err := framework.KubectlGetJSONPath(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "deployment", fmt.Sprintf("%s-edge-gateway", controlPlane.Name), "{.status.availableReplicas}")
			Expect(err).NotTo(HaveOccurred())
			Expect(replicas).To(Equal("2"))
		})

		It("rolls back to last stable configuration", func() {
			if !shouldRun("control-plane") {
				fmt.Printf("E2E_TEST_MODE does not include control-plane, skipping...\n")
				Skip("E2E_TEST_MODE does not include control-plane")
			}

			patch := `{"spec":{"services":{"edgeGateway":{"replicas":1}}}}`
			err := framework.KubectlPatch(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "sandbox0infra", controlPlane.Name, patch)
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, fmt.Sprintf("deployment/%s-edge-gateway", controlPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			replicas, err := framework.KubectlGetJSONPath(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "deployment", fmt.Sprintf("%s-edge-gateway", controlPlane.Name), "{.status.availableReplicas}")
			Expect(err).NotTo(HaveOccurred())
			Expect(replicas).To(Equal("1"))
		})
	})

	Context("data-plane mode", func() {
		It("reconciles data-plane components with external dependencies", func() {
			if !shouldRun("data-plane") {
				fmt.Printf("E2E_TEST_MODE does not include data-plane, skipping...\n")
				Skip("E2E_TEST_MODE does not include data-plane")
			}

			err := framework.EnsureNamespace(testCtx.Context, cfg.Kubeconfig, cfg.InfraNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.ApplyInfraDataPlaneSecrets(testCtx.Context, cfg.Kubeconfig, cfg.InfraNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.ApplyManifest(testCtx.Context, cfg.Kubeconfig, cfg.InfraDataPlaneManifestPath)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				_ = framework.KubectlDeleteManifest(testCtx.Context, cfg.Kubeconfig, cfg.InfraDataPlaneManifestPath)
			})

			err = framework.WaitForSandbox0InfraReady(testCtx.Context, cfg.Kubeconfig, dataPlane, "20m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, dataPlane.Namespace, fmt.Sprintf("deployment/%s-internal-gateway", dataPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, dataPlane.Namespace, fmt.Sprintf("deployment/%s-manager", dataPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, dataPlane.Namespace, fmt.Sprintf("deployment/%s-storage-proxy", dataPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, dataPlane.Namespace, fmt.Sprintf("daemonset/%s-netd", dataPlane.Name), "5m")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("lifecycle and recovery", func() {
		It("recovers after operator restart without duplication", func() {
			if !shouldRun("control-plane") {
				fmt.Printf("E2E_TEST_MODE does not include control-plane, skipping...\n")
				Skip("E2E_TEST_MODE does not include control-plane")
			}

			err := framework.Kubectl(testCtx.Context, cfg.Kubeconfig, "rollout", "restart", fmt.Sprintf("deployment/%s", cfg.OperatorDeploymentName), "--namespace", cfg.OperatorNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, cfg.OperatorNamespace, fmt.Sprintf("deployment/%s", cfg.OperatorDeploymentName), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.WaitForSandbox0InfraReady(testCtx.Context, cfg.Kubeconfig, controlPlane, "10m")
			Expect(err).NotTo(HaveOccurred())
		})

		It("cleans up resources on uninstall", func() {
			if !shouldRun("control-plane") {
				fmt.Printf("E2E_TEST_MODE does not include control-plane, skipping...\n")
				Skip("E2E_TEST_MODE does not include control-plane")
			}

			err := framework.KubectlDeleteManifest(testCtx.Context, cfg.Kubeconfig, cfg.InfraControlPlaneManifestPath)
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlWaitForDelete(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "sandbox0infra", controlPlane.Name, "10m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlWaitForDelete(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "deployment", fmt.Sprintf("%s-edge-gateway", controlPlane.Name), "10m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlWaitForDelete(testCtx.Context, cfg.Kubeconfig, controlPlane.Namespace, "deployment", fmt.Sprintf("%s-scheduler", controlPlane.Name), "10m")
			Expect(err).NotTo(HaveOccurred())
		})
	})

	Context("combined install", func() {
		It("installs control-plane and data-plane in a single CR", func() {
			if !shouldRun("combined") {
				fmt.Printf("E2E_TEST_MODE does not include combined, skipping...\n")
				Skip("E2E_TEST_MODE does not include combined")
			}

			err := framework.EnsureNamespace(testCtx.Context, cfg.Kubeconfig, cfg.InfraNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.ApplyInfraAllSecrets(testCtx.Context, cfg.Kubeconfig, cfg.InfraNamespace)
			Expect(err).NotTo(HaveOccurred())

			err = framework.ApplyManifest(testCtx.Context, cfg.Kubeconfig, cfg.InfraAllManifestPath)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				_ = framework.KubectlDeleteManifest(testCtx.Context, cfg.Kubeconfig, cfg.InfraAllManifestPath)
			})

			err = framework.WaitForSandbox0InfraReady(testCtx.Context, cfg.Kubeconfig, allInOne, "20m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("deployment/%s-edge-gateway", allInOne.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("deployment/%s-scheduler", allInOne.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("deployment/%s-internal-gateway", allInOne.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("deployment/%s-manager", allInOne.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("deployment/%s-storage-proxy", allInOne.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("daemonset/%s-netd", allInOne.Name), "5m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("statefulset/%s-postgres", allInOne.Name), "10m")
			Expect(err).NotTo(HaveOccurred())

			err = framework.KubectlRolloutStatus(testCtx.Context, cfg.Kubeconfig, allInOne.Namespace, fmt.Sprintf("statefulset/%s-rustfs", allInOne.Name), "10m")
			Expect(err).NotTo(HaveOccurred())
		})
	})
})
