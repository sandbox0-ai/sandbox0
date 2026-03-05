package cases

import (
	"fmt"
	"strings"

	"github.com/sandbox0-ai/sandbox0/pkg/framework"

	. "github.com/onsi/ginkgo/v2"
)

// RegisterApiSuite defines API coverage for a scenario.
func RegisterApiSuite(envProvider func() *framework.ScenarioEnv) {
	Describe("API entrypoint", func() {
		registerApiMinimalSuite(envProvider)
		registerApiNetworkPolicySuite(envProvider)
		registerApiVolumesSuite(envProvider)
		registerApiFullModeSuite(envProvider)
		registerApiUnknownSuite(envProvider)
	})
}

var knownApiScenarios = map[string]struct{}{
	"minimal":        {},
	"network-policy": {},
	"volumes":        {},
	"fullmode":       {},
}

func normalizeScenarioName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func isKnownApiScenario(name string) bool {
	_, ok := knownApiScenarios[normalizeScenarioName(name)]
	return ok
}

func shouldRunApiScenario(envProvider func() *framework.ScenarioEnv, expected string) *framework.ScenarioEnv {
	env := envProvider()
	if env == nil {
		Skip("scenario env is nil")
	}
	actual := normalizeScenarioName(env.Infra.Name)
	if actual != expected {
		Skip(fmt.Sprintf("skip API suite %q for scenario %q", expected, env.Infra.Name))
	}
	return env
}

func registerApiUnknownSuite(envProvider func() *framework.ScenarioEnv) {
	Describe("API entrypoint for unknown scenario", func() {
		It("skips until scenario-specific tests exist", func() {
			env := envProvider()
			if env == nil {
				Skip("scenario env is nil")
			}
			if isKnownApiScenario(env.Infra.Name) {
				Skip("scenario-specific API suite exists: " + env.Infra.Name)
			}
			Skip("no API suite registered for Sandbox0Infra name: " + env.Infra.Name)
		})
	})
}
