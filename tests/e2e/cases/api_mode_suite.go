package cases

import (
	"fmt"
	"net/http"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type apiModeSuiteOptions struct {
	name                      string
	describe                  string
	templateNamePrefix        string
	fileContent               string
	includeSandboxListTests   bool
	includeTemplateStatus     bool
	includeNetworkPolicy      bool
	includeVolumeLifecycle    bool
	includeMeteringAssertions bool
	expectStorageUnavailable  bool
	expectNetworkUnavailable  bool
}

func registerApiModeSuite(envProvider func() *framework.ScenarioEnv, opts apiModeSuiteOptions) {
	Describe(opts.describe, Ordered, func() {
		var (
			env       *framework.ScenarioEnv
			session   *e2eutils.Session
			cleanup   func()
			sandboxID string
		)

		BeforeAll(func() {
			env = shouldRunApiScenario(envProvider, opts.name)

			var err error
			session, cleanup, err = e2eutils.NewAPISession(env, false)
			Expect(err).NotTo(HaveOccurred())

			password, err := framework.GetSecretValue(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "admin-password", "password")
			Expect(err).NotTo(HaveOccurred())

			Eventually(func() error {
				return session.Login(env.TestCtx.Context, GinkgoT(), "admin@example.com", password)
			}).WithTimeout(2 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

			waitForDefaultTemplateReady(env, session)

			resp := claimSandboxEventually(env, session, "default")
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
				runTemplateLifecycleAssertions(env, session, opts.templateNamePrefix)
			})

			if opts.includeTemplateStatus {
				It("returns template status with pool counters", func() {
					assertTemplateStatusCountersEventually(env, session)
				})
			}
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

			if opts.includeSandboxListTests {
				It("lists sandboxes", func() {
					assertSandboxListContainsClaimedSandbox(env, session, sandboxID)
				})

				It("lists sandboxes with filters", func() {
					limit := 10
					listResp, status, err := session.ListSandboxes(env.TestCtx.Context, GinkgoT(), &e2eutils.ListSandboxesOptions{
						Limit: &limit,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					Expect(listResp).NotTo(BeNil())
					Expect(len(listResp.Sandboxes)).To(BeNumerically("<=", limit))
				})
			}
		})

		Context("filesystem and process capabilities", func() {
			It("performs file operations and process management", func() {
				assertFilesystemAndProcessCapabilities(env, session, sandboxID, opts.name, opts.fileContent)
			})
		})

		if opts.includeNetworkPolicy {
			Context("network policies", func() {
				It("retrieves network policy", func() {
					Expect(sandboxID).NotTo(BeEmpty())
					_, status, _, err := session.GetNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
				})
			})
		}

		if opts.includeVolumeLifecycle {
			Context("sandbox volumes", func() {
				It("creates volumes and snapshots", func() {
					assertVolumeLifecycle(env, session)
				})
			})
		}

		if opts.expectStorageUnavailable || opts.expectNetworkUnavailable {
			Context("missing services", func() {
				It("returns expected degraded-mode errors", func() {
					Expect(sandboxID).NotTo(BeEmpty())
					if opts.expectStorageUnavailable {
						_, status, _, err := session.ListSandboxVolumes(env.TestCtx.Context, GinkgoT())
						Expect(err).NotTo(HaveOccurred())
						Expect(status).To(Equal(http.StatusServiceUnavailable))
					}
					if opts.expectNetworkUnavailable {
						_, status, _, err := session.GetNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID)
						Expect(err).NotTo(HaveOccurred())
						Expect(status).To(BeNumerically(">=", http.StatusBadRequest))
					}
				})
			})
		}

		if opts.includeMeteringAssertions {
			Context("metering export", func() {
				It("exports sandbox and storage usage facts", func() {
					Expect(sandboxID).NotTo(BeEmpty())

					pausedResp, status, err := session.PauseSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					Expect(pausedResp).NotTo(BeNil())
					Expect(pausedResp.Paused).To(BeTrue())

					resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					Expect(resumeResp).NotTo(BeNil())
					Expect(resumeResp.Resumed).To(BeTrue())

					cacheSize := "512M"
					volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{
						CacheSize: &cacheSize,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusCreated))
					Expect(volume).NotTo(BeNil())
					Expect(volume.Id).NotTo(BeEmpty())

					snapshot, status, err := session.CreateSnapshot(env.TestCtx.Context, GinkgoT(), volume.Id, apispec.CreateSnapshotRequest{
						Name: "e2e-metering-snap",
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusCreated))
					Expect(snapshot).NotTo(BeNil())
					Expect(snapshot.Id).NotTo(BeEmpty())

					status, err = session.RestoreSnapshot(env.TestCtx.Context, GinkgoT(), volume.Id, snapshot.Id)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))

					status, err = session.DeleteSnapshot(env.TestCtx.Context, GinkgoT(), volume.Id, snapshot.Id)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))

					status, err = session.DeleteSandboxVolume(env.TestCtx.Context, GinkgoT(), volume.Id)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))

					Expect(session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)).To(Succeed())
					sandboxID = ""

					Eventually(func() error {
						statusResp, _, err := session.GetMeteringStatus(env.TestCtx.Context)
						if err != nil {
							return err
						}
						if statusResp.LatestEventSequence <= 0 {
							return fmt.Errorf("latest_event_sequence not advanced")
						}
						if statusResp.LatestWindowSequence <= 0 {
							return fmt.Errorf("latest_window_sequence not advanced")
						}
						if statusResp.CompleteBefore == nil {
							return fmt.Errorf("complete_before is nil")
						}

						events, err := session.ListAllMeteringEvents(env.TestCtx.Context, 200)
						if err != nil {
							return err
						}
						if !hasMeteringEvent(events, metering.EventTypeSandboxClaimed, "sandbox", pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.claimed event")
						}
						if !hasMeteringEvent(events, metering.EventTypeSandboxPaused, "sandbox", pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.paused event")
						}
						if !hasMeteringEvent(events, metering.EventTypeSandboxResumed, "sandbox", pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.resumed event")
						}
						if !hasMeteringEvent(events, metering.EventTypeSandboxTerminated, "sandbox", pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.terminated event")
						}
						if !hasMeteringEvent(events, metering.EventTypeVolumeCreated, "volume", volume.Id) {
							return fmt.Errorf("missing volume.created event")
						}
						if !hasMeteringEvent(events, metering.EventTypeVolumeDeleted, "volume", volume.Id) {
							return fmt.Errorf("missing volume.deleted event")
						}
						if !hasMeteringEvent(events, metering.EventTypeSnapshotCreated, "snapshot", snapshot.Id) {
							return fmt.Errorf("missing snapshot.created event")
						}
						if !hasMeteringEvent(events, metering.EventTypeSnapshotRestored, "snapshot", snapshot.Id) {
							return fmt.Errorf("missing snapshot.restored event")
						}
						if !hasMeteringEvent(events, metering.EventTypeSnapshotDeleted, "snapshot", snapshot.Id) {
							return fmt.Errorf("missing snapshot.deleted event")
						}

						windows, err := session.ListAllMeteringWindows(env.TestCtx.Context, 200)
						if err != nil {
							return err
						}
						if !hasMeteringWindow(windows, metering.WindowTypeSandboxActiveSeconds, pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.active_seconds window")
						}
						if !hasMeteringWindow(windows, metering.WindowTypeSandboxPausedSeconds, pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.paused_seconds window")
						}

						return nil
					}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
				})
			})
		}
	})
}

func runTemplateLifecycleAssertions(env *framework.ScenarioEnv, session *e2eutils.Session, templateNamePrefix string) {
	templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
	Expect(err).NotTo(HaveOccurred())
	Expect(templates).NotTo(BeEmpty())

	base := templates[0]
	name := fmt.Sprintf("%s-%d", templateNamePrefix, time.Now().UnixNano())
	newTemplate := e2eutils.CloneTemplateForCreate(base, name)

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), newTemplate)
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	Expect(created.TemplateId).To(Equal(name))

	updated := *created
	Expect(updated.Spec.Pool).NotTo(BeNil())
	desc := "e2e update"
	updated.Spec.Description = &desc
	updated.Spec.Pool.MaxIdle = updated.Spec.Pool.MaxIdle + 1
	if updated.Spec.Pool.MaxIdle < updated.Spec.Pool.MinIdle {
		updated.Spec.Pool.MaxIdle = updated.Spec.Pool.MinIdle + 1
	}

	updatedResp, err := session.UpdateTemplate(env.TestCtx.Context, GinkgoT(), name, apispec.TemplateUpdateRequest{
		Spec: updated.Spec,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(updatedResp).NotTo(BeNil())
	Expect(updatedResp.Spec.Description).NotTo(BeNil())
	Expect(*updatedResp.Spec.Description).To(Equal("e2e update"))

	err = session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), name)
	Expect(err).NotTo(HaveOccurred())
}

func assertTemplateStatusCountersEventually(env *framework.ScenarioEnv, session *e2eutils.Session) {
	templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
	Expect(err).NotTo(HaveOccurred())
	Expect(templates).NotTo(BeEmpty())

	templateID := templates[0].TemplateId
	Expect(templateID).NotTo(BeEmpty())

	Eventually(func() error {
		tpl, getErr := session.GetTemplate(env.TestCtx.Context, GinkgoT(), templateID)
		if getErr != nil {
			return getErr
		}
		if tpl.Status == nil {
			return fmt.Errorf("template %s status not ready", templateID)
		}
		if tpl.Status.IdleCount == nil {
			return fmt.Errorf("template %s idleCount is missing", templateID)
		}
		if *tpl.Status.IdleCount < 0 {
			return fmt.Errorf("template %s idleCount is negative: %d", templateID, *tpl.Status.IdleCount)
		}
		if tpl.Status.ActiveCount == nil {
			return fmt.Errorf("template %s activeCount is missing", templateID)
		}
		if *tpl.Status.ActiveCount < 0 {
			return fmt.Errorf("template %s activeCount is negative: %d", templateID, *tpl.Status.ActiveCount)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func assertSandboxListContainsClaimedSandbox(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	listResp, status, err := session.ListSandboxes(env.TestCtx.Context, GinkgoT(), nil)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(listResp).NotTo(BeNil())
	Expect(listResp.Count).To(BeNumerically(">=", 1))

	found := false
	for _, sb := range listResp.Sandboxes {
		if sb.Id == sandboxID {
			found = true
			Expect(sb.TemplateId).NotTo(BeEmpty())
			Expect(sb.Status).NotTo(BeEmpty())
			break
		}
	}
	Expect(found).To(BeTrue(), "created sandbox should be in the list")
}

func assertFilesystemAndProcessCapabilities(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID, modeName, fileContent string) {
	Expect(sandboxID).NotTo(BeEmpty())
	dirPath := fmt.Sprintf("tmp/e2e-%s-%d", modeName, time.Now().UnixNano())
	filePath := dirPath + "/hello.txt"
	content := []byte(fileContent)

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

	listResp, status, err := session.ListFiles(env.TestCtx.Context, GinkgoT(), sandboxID, dirPath)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(listResp).NotTo(BeNil())
	Expect(bool(listResp.Success)).To(BeTrue())
	Expect(listResp.Data).NotTo(BeNil())
	Expect(listResp.Data.Entries).NotTo(BeNil())
	Expect(*listResp.Data.Entries).NotTo(BeEmpty())

	processType := apispec.Cmd
	command := []string{"/bin/sh", "-c", "sleep 3"}
	ctxReq := apispec.CreateContextRequest{
		Type: &processType,
		Cmd: &apispec.CreateCMDContextRequest{
			Command: command,
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
}

func assertVolumeLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session) {
	cacheSize := "512M"
	createReq := apispec.CreateSandboxVolumeRequest{
		CacheSize: &cacheSize,
	}
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), createReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	Expect(volume.Id).NotTo(BeEmpty())

	snapReq := apispec.CreateSnapshotRequest{
		Name: "e2e-snap",
	}
	snapshot, status, err := session.CreateSnapshot(env.TestCtx.Context, GinkgoT(), volume.Id, snapReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(snapshot).NotTo(BeNil())
	Expect(snapshot.Id).NotTo(BeEmpty())

	_, status, err = session.ListSnapshots(env.TestCtx.Context, GinkgoT(), volume.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	status, err = session.RestoreSnapshot(env.TestCtx.Context, GinkgoT(), volume.Id, snapshot.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	status, err = session.DeleteSnapshot(env.TestCtx.Context, GinkgoT(), volume.Id, snapshot.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	status, err = session.DeleteSandboxVolume(env.TestCtx.Context, GinkgoT(), volume.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
}

func hasMeteringEvent(events []*metering.Event, eventType, subjectType, subjectID string) bool {
	for _, event := range events {
		if event == nil {
			continue
		}
		if event.EventType == eventType && event.SubjectType == subjectType && event.SubjectID == subjectID {
			return true
		}
	}
	return false
}

func hasMeteringWindow(windows []*metering.Window, windowType, sandboxID string) bool {
	for _, window := range windows {
		if window == nil {
			continue
		}
		if window.WindowType == windowType && window.SandboxID == sandboxID {
			return true
		}
	}
	return false
}
