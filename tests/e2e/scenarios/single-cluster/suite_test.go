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
			})

			AfterAll(func() {
				GinkgoWriter.Printf("Scenario %s teardown completed.\n", scenario.Name)
			})

			cases.RegisterOperatorSuite(func() *framework.ScenarioEnv { return env })
			cases.RegisterApiSuite(func() *framework.ScenarioEnv { return env })
			cases.RegisterInfraOperatorLifecycleSuite(func() *framework.ScenarioEnv { return env })
		})
	}
}
