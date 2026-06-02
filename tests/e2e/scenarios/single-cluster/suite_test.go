package singlecluster

import (
	"testing"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/tests/e2e/cases"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var (
	baseCfg   framework.Config
	scenarios []framework.Scenario
)

func TestSingleCluster(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "E2E Single-Cluster Suite")
}

func init() {
	var loadErr error
	baseCfg, loadErr = framework.LoadConfig()
	if loadErr != nil {
		panic(loadErr)
	}

	scenarios, loadErr = LoadScenarios(baseCfg)
	if loadErr != nil {
		panic(loadErr)
	}

	for _, scenario := range scenarios {
		scenario := scenario
		Describe("Scenario "+scenario.Name, Ordered, func() {
			var (
				env     *framework.ScenarioEnv
				cleanup func()
			)

			BeforeAll(func() {
				var setupErr error
				env, cleanup, setupErr = framework.SetupScenario(baseCfg, scenario)
				Expect(setupErr).NotTo(HaveOccurred())
				DeferCleanup(cleanup)
				if env.Infra.Name == "rootfs-persistence" && !env.Config.UseExistingCluster {
					Expect(framework.KubectlRolloutStatus(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "daemonset/"+env.Infra.Name+"-ctld", "3m")).To(Succeed())
					DeferCleanup(func() {
						if err := framework.RestoreKindContainerdConfig(env.TestCtx.Context, env.Config.ClusterName); err != nil {
							GinkgoWriter.Printf("restore kind containerd config failed: %v\n", err)
						}
					})
					Expect(framework.ConfigureKindRootFSSnapshotter(env.TestCtx.Context, env.Config.ClusterName)).To(Succeed())
					Expect(framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "wait", "node", "--all", "--for=condition=Ready", "--timeout=2m")).To(Succeed())
					Expect(framework.KubectlRolloutStatus(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "daemonset/"+env.Infra.Name+"-ctld", "3m")).To(Succeed())
					Expect(framework.KubectlRolloutStatus(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "deployment/"+env.Infra.Name+"-manager", "3m")).To(Succeed())
				}
			})

			AfterAll(func() {
				GinkgoWriter.Printf("Scenario %s teardown completed.\n", scenario.Name)
			})

			cases.RegisterOperatorSuite(func() *framework.ScenarioEnv { return env })
			cases.RegisterApiSuite(func() *framework.ScenarioEnv { return env })
			if scenario.Name == "fullmode" {
				cases.RegisterInfraOperatorLifecycleSuite(func() *framework.ScenarioEnv { return env })
			}
		})
	}
}
