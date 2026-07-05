package cases

import "github.com/sandbox0-ai/sandbox0/pkg/framework"

func registerApiVolumesSuite(envProvider func() *framework.ScenarioEnv) {
	registerApiModeSuite(envProvider, apiModeSuiteOptions{
		name:                      "volumes",
		describe:                  "API volumes mode",
		templateNamePrefix:        "e2e-volumes",
		fileContent:               "hello volumes",
		includeVolumeLifecycle:    true,
		includeMountpointS3Compat: true,
		expectNetworkUnavailable:  true,
	})
}
