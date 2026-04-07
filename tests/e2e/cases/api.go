package cases

import (
	"fmt"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
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
		return nil
	}
	actual := normalizeScenarioName(env.Infra.Name)
	if actual != expected {
		Skip(fmt.Sprintf("skip API suite %q for scenario %q", expected, env.Infra.Name))
		return nil
	}
	return env
}

func registerApiUnknownSuite(envProvider func() *framework.ScenarioEnv) {
	Describe("API entrypoint for unknown scenario", func() {
		It("skips until scenario-specific tests exist", func() {
			env := envProvider()
			if env == nil {
				Skip("scenario env is nil")
				return
			}
			if isKnownApiScenario(env.Infra.Name) {
				Skip("scenario-specific API suite exists: " + env.Infra.Name)
				return
			}
			Skip("no API suite registered for Sandbox0Infra name: " + env.Infra.Name)
		})
	})
}

func waitForDefaultTemplateReady(env *framework.ScenarioEnv, session *e2eutils.Session) {
	Eventually(func() error {
		templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
		if err != nil {
			return err
		}
		if len(templates) == 0 {
			return fmt.Errorf("no templates found")
		}

		tpl, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
		if err != nil {
			return err
		}
		if tpl.Status == nil || tpl.Status.IdleCount == nil {
			return fmt.Errorf("default template status is not ready")
		}
		if *tpl.Status.IdleCount < 1 {
			return fmt.Errorf("default template idle pool is not ready: idleCount=%d", *tpl.Status.IdleCount)
		}
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}

func ensureSharedVolumeRuntimeClass(env *framework.ScenarioEnv, name string) {
	manifest := fmt.Sprintf(`apiVersion: node.k8s.io/v1
kind: RuntimeClass
metadata:
  name: %s
handler: runc
`, name)
	Expect(framework.ApplyManifestContent(env.TestCtx.Context, env.Config.Kubeconfig, "sandbox0-e2e-runtimeclass-", manifest)).To(Succeed())
	DeferCleanup(func() {
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "runtimeclass", name, "--ignore-not-found=true")
	})

	Eventually(func() error {
		_, err := framework.KubectlOutput(env.TestCtx.Context, env.Config.Kubeconfig, "get", "runtimeclass", name, "-o", "name")
		return err
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
}

func waitForTeamTemplateProjectionEventually(env *framework.ScenarioEnv, teamID, templateID string) {
	templateNamespace, err := naming.TemplateNamespaceForTeam(teamID)
	Expect(err).NotTo(HaveOccurred())
	clusterTemplateID := naming.TemplateNameForCluster(naming.ScopeTeam, teamID, templateID)

	Eventually(func() error {
		_, getErr := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get",
			"sandboxtemplates.sandbox0.ai",
			clusterTemplateID,
			"--namespace",
			templateNamespace,
			"-o",
			"name",
		)
		return getErr
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
}

func claimSandboxEventually(env *framework.ScenarioEnv, session *e2eutils.Session, templateID string) *apispec.ClaimResponse {
	var resp *apispec.ClaimResponse
	Eventually(func() error {
		var err error
		resp, err = session.ClaimSandbox(env.TestCtx.Context, GinkgoT(), templateID)
		return err
	}).WithTimeout(2 * time.Minute).WithPolling(3 * time.Second).Should(Succeed())
	return resp
}
