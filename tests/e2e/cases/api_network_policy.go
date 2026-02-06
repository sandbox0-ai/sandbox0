package cases

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/sandbox0-ai/infra/pkg/apispec"
	"github.com/sandbox0-ai/infra/pkg/framework"
	e2eutils "github.com/sandbox0-ai/infra/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func registerApiNetworkPolicySuite(envProvider func() *framework.ScenarioEnv) {
	Describe("API network policy mode", Ordered, func() {
		var (
			env       *framework.ScenarioEnv
			session   *e2eutils.Session
			cleanup   func()
			sandboxID string
		)

		BeforeAll(func() {
			env = shouldRunApiScenario(envProvider, "network-policy")

			var err error
			session, cleanup, err = e2eutils.NewAPISession(env, false)
			Expect(err).NotTo(HaveOccurred())

			password, err := framework.GetSecretValue(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "admin-password", "password")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				return session.Login(env.TestCtx.Context, GinkgoT(), "admin@example.com", password)
			}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

			Eventually(func() error {
				templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
				if err != nil {
					return err
				}
				if len(templates) == 0 {
					return fmt.Errorf("no templates found")
				}
				return nil
			}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

			resp, err := session.ClaimSandbox(env.TestCtx.Context, GinkgoT(), "default")
			Expect(err).NotTo(HaveOccurred())
			sandboxID = resp.SandboxId
		})

		AfterAll(func() {
			if session != nil {
				_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
			}
			if cleanup != nil {
				cleanup()
			}
		})

		Context("template lifecycle", func() {
			It("creates, updates, and deletes templates", func() {
				templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
				Expect(err).NotTo(HaveOccurred())
				Expect(templates).NotTo(BeEmpty())

				base := templates[0]
				name := fmt.Sprintf("e2e-network-%d", time.Now().UnixNano())
				newTemplate := e2eutils.CloneTemplateForCreate(base, name)

				created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), newTemplate)
				Expect(err).NotTo(HaveOccurred())
				Expect(created).NotTo(BeNil())
				Expect(created.Metadata).NotTo(BeNil())
				Expect(created.Metadata.Name).NotTo(BeNil())
				Expect(*created.Metadata.Name).To(Equal(name))

				updated := *created
				Expect(updated.Spec).NotTo(BeNil())
				Expect(updated.Spec.Pool).NotTo(BeNil())
				desc := "e2e update"
				updated.Spec.Description = &desc
				updated.Spec.Pool.MaxIdle = updated.Spec.Pool.MaxIdle + 1
				if updated.Spec.Pool.MaxIdle < updated.Spec.Pool.MinIdle {
					updated.Spec.Pool.MaxIdle = updated.Spec.Pool.MinIdle + 1
				}

				updatedResp, err := session.UpdateTemplate(env.TestCtx.Context, GinkgoT(), name, updated)
				Expect(err).NotTo(HaveOccurred())
				Expect(updatedResp).NotTo(BeNil())
				Expect(updatedResp.Spec).NotTo(BeNil())
				Expect(updatedResp.Spec.Description).NotTo(BeNil())
				Expect(*updatedResp.Spec.Description).To(Equal("e2e update"))

				err = session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), name)
				Expect(err).NotTo(HaveOccurred())
			})
		})

		Context("sandbox lifecycle", func() {
			It("fetches status and refreshes sandboxes", func() {
				Expect(sandboxID).NotTo(BeEmpty())

				_, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))

				_, status, err = session.GetSandboxStatus(env.TestCtx.Context, GinkgoT(), sandboxID)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))

				_, status, err = session.RefreshSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
			})
		})

		Context("filesystem and process capabilities", func() {
			It("performs file operations and process management", func() {
				Expect(sandboxID).NotTo(BeEmpty())
				dirPath := fmt.Sprintf("tmp/e2e-network-%d", time.Now().UnixNano())
				filePath := dirPath + "/hello.txt"
				content := []byte("hello network")

				status, err := session.CreateDirectory(env.TestCtx.Context, GinkgoT(), sandboxID, dirPath, true)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusCreated))

				status, err = session.WriteFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath, content, "")
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))

				body, status, err := session.ReadFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				Expect(string(body)).To(Equal(string(content)))

				body, status, err = session.ListFiles(env.TestCtx.Context, GinkgoT(), sandboxID, dirPath)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))

				var listResp struct {
					Entries []map[string]any `json:"entries"`
				}
				Expect(json.Unmarshal(body, &listResp)).To(Succeed())
				Expect(listResp.Entries).NotTo(BeEmpty())

				processType := apispec.Cmd
				command := []string{"/bin/sh", "-c", "sleep 30"}
				ctxReq := apispec.CreateContextRequest{
					Type: &processType,
					Cmd: &apispec.CreateCMDContextRequest{
						Command: &command,
					},
				}
				ctxResp, status, err := session.CreateContext(env.TestCtx.Context, GinkgoT(), sandboxID, ctxReq)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusCreated))
				Expect(ctxResp).NotTo(BeNil())
				Expect(ctxResp.Id).NotTo(BeEmpty())

				_, status, err = session.ListContexts(env.TestCtx.Context, GinkgoT(), sandboxID)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))

				status, err = session.DeleteContext(env.TestCtx.Context, GinkgoT(), sandboxID, ctxResp.Id)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
			})
		})

		Context("network policies", func() {
			It("retrieves network policy", func() {
				Expect(sandboxID).NotTo(BeEmpty())

				_, status, _, err := session.GetNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
			})
		})

		Context("missing services", func() {
			It("returns errors for storage APIs", func() {
				_, status, _, err := session.ListSandboxVolumes(env.TestCtx.Context, GinkgoT())
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusServiceUnavailable))
			})
		})
	})
}
