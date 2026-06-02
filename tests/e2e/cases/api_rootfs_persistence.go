package cases

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func registerApiRootFSPersistenceSuite(envProvider func() *framework.ScenarioEnv) {
	Describe("API rootfs persistence mode", Ordered, func() {
		var (
			env     *framework.ScenarioEnv
			session *e2eutils.Session
			cleanup func()
		)

		BeforeAll(func() {
			env = shouldRunApiScenario(envProvider, "rootfs-persistence")

			var err error
			session, cleanup, err = e2eutils.NewAPISession(env, false)
			Expect(err).NotTo(HaveOccurred())

			password, err := framework.GetSecretValue(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "admin-password", "password")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				return session.Login(env.TestCtx.Context, GinkgoT(), "admin@example.com", password)
			}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

			waitForDefaultTemplateRegistered(env, session)
		})

		AfterAll(func() {
			if cleanup != nil {
				cleanup()
			}
		})

		It("restores writable rootfs content after hard TTL clean and resume", func() {
			assertRootFSRestoredAfterClean(env, session)
		})
	})
}

func waitForDefaultTemplateRegistered(env *framework.ScenarioEnv, session *e2eutils.Session) {
	Eventually(func() error {
		tpl, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
		if err != nil {
			return err
		}
		if tpl.TemplateId != "default" {
			return fmt.Errorf("default template not registered")
		}
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
}

func assertRootFSRestoredAfterClean(env *framework.ScenarioEnv, session *e2eutils.Session) {
	hardTTL := int32(12)
	claim := apispec.ClaimRequest{
		Template: ptr("default"),
		Config: &apispec.SandboxConfig{
			HardTtl: &hardTTL,
		},
	}
	resp, status, err := session.ClaimSandboxDetailed(env.TestCtx.Context, GinkgoT(), claim)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(resp).NotTo(BeNil())
	sandboxID := resp.SandboxId
	Expect(sandboxID).NotTo(BeEmpty())
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})

	templateNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	sandbox := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	firstPod := sandbox.PodName
	Expect(firstPod).NotTo(BeEmpty())

	filePath := fmt.Sprintf("/tmp/rootfs-persistence-%d.txt", time.Now().UnixNano())
	content := []byte("rootfs persists through cleaned restore")
	status, err = session.WriteFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath, content, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	waitForSandboxCleanedEventually(env, session, sandboxID)

	resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())

	restored := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	Expect(restored.PodName).NotTo(BeEmpty())
	Expect(restored.PodName).NotTo(Equal(firstPod))

	body, status, err := session.ReadFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(string(body)).To(Equal(string(content)))
}

func waitForSandboxCleanedEventually(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) *apispec.Sandbox {
	var sandbox *apispec.Sandbox
	Eventually(func() error {
		current, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("get sandbox status %d", status)
		}
		if current.Status != apispec.SandboxLifecycleStatusCleaned {
			return fmt.Errorf("sandbox is not cleaned yet: status=%s pod=%s", current.Status, current.PodName)
		}
		sandbox = current
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(3 * time.Second).Should(Succeed())
	return sandbox
}
