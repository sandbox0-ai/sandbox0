package cases

import "github.com/sandbox0-ai/sandbox0/pkg/framework"

func registerApiFilesystemsSuite(envProvider func() *framework.ScenarioEnv) {
	registerApiModeSuite(envProvider, apiModeSuiteOptions{
		name:                       "filesystems",
		describe:                   "API filesystems mode",
		templateNamePrefix:         "e2e-filesystems",
		fileContent:                "hello filesystems",
		includeFilesystemLifecycle: true,
		expectNetworkUnavailable:   true,
	})
}
