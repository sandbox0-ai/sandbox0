package cases

import (
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
)

func registerApiNetworkPolicySuite(envProvider func() *framework.ScenarioEnv) {
	registerApiModeSuite(envProvider, apiModeSuiteOptions{
		name:                     "network-policy",
		describe:                 "API network policy mode",
		templateNamePrefix:       "e2e-network",
		fileContent:              "hello network",
		includeNetworkPolicy:     true,
		expectStorageUnavailable: true,
	})
}
