package cases

import "github.com/sandbox0-ai/sandbox0/pkg/framework"

func registerApiFullModeSuite(envProvider func() *framework.ScenarioEnv) {
	registerApiModeSuite(envProvider, apiModeSuiteOptions{
		name:                        "fullmode",
		describe:                    "API fullmode",
		templateNamePrefix:          "e2e-fullmode",
		fileContent:                 "hello fullmode",
		includeSandboxListTests:     true,
		includeTemplateStatus:       true,
		includePoolReadinessGate:    true,
		includeNetworkPolicy:        true,
		includeVolumeLifecycle:      true,
		includeMountpointS3Compat:   true,
		includeObjectEncryption:     true,
		includeWebhookLifecycle:     true,
		includeRootFSPauseResume:    true,
		includeTemplateFromSandbox:  true,
		includeUsageQuotaAssertions: true,
	})
}
