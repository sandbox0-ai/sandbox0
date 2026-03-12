package cases

import "github.com/sandbox0-ai/sandbox0/pkg/framework"

func registerApiFullModeSuite(envProvider func() *framework.ScenarioEnv) {
	registerApiModeSuite(envProvider, apiModeSuiteOptions{
		name:                      "fullmode",
		describe:                  "API full mode",
		templateNamePrefix:        "e2e-fullmode",
		fileContent:               "hello fullmode",
		includeTemplateStatus:     true,
		includeNetworkPolicy:      true,
		includeVolumeLifecycle:    true,
		includeMeteringAssertions: true,
	})
}
