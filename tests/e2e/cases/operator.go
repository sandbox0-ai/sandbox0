package cases

import (
	"fmt"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// RegisterOperatorSuite validates operator reconciliation for a scenario.
func RegisterOperatorSuite(envProvider func() *framework.ScenarioEnv) {
	Describe("Operator reconciliation", Ordered, func() {
		It("installs CRDs and operator deployment", func() {
			env := envProvider()
			Expect(env).NotTo(BeNil())

			err := framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "get", "crd", "sandbox0infras.infra.sandbox0.ai")
			Expect(err).NotTo(HaveOccurred())

			err = framework.WaitForDeployment(env.TestCtx.Context, env.Config.Kubeconfig, env.Config.OperatorNamespace, env.Config.OperatorDeploymentName, "3m")
			Expect(err).NotTo(HaveOccurred())
		})

		It("reconciles Sandbox0Infra to Ready", func() {
			env := envProvider()
			Expect(env).NotTo(BeNil())

			timeout := env.Scenario.ReadyTimeout
			if timeout == "" {
				timeout = "20m"
			}
			err := framework.WaitForSandbox0InfraReady(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra, timeout)
			Expect(err).NotTo(HaveOccurred())
		})

		It("rolls out required components", func() {
			env := envProvider()
			Expect(env).NotTo(BeNil())

			if len(env.Scenario.Rollouts) == 0 {
				Skip("no rollout targets configured for scenario")
			}

			for _, target := range env.Scenario.Rollouts {
				resource, err := target.ResourceID()
				Expect(err).NotTo(HaveOccurred())
				timeout := target.Timeout
				if timeout == "" {
					timeout = "5m"
				}
				err = framework.KubectlRolloutStatus(env.TestCtx.Context, env.Config.Kubeconfig, target.Namespace, resource, timeout)
				Expect(err).NotTo(HaveOccurred(), fmt.Sprintf("rollout failed for %s", resource))
			}
		})
	})
}
