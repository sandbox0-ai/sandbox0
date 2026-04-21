package cases

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	mgrv1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

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
	includePoolReadinessGate  bool
	includeNetworkPolicy      bool
	includeVolumeLifecycle    bool
	includeWebhookLifecycle   bool
	includeMeteringAssertions bool
	expectStorageUnavailable  bool
	expectNetworkUnavailable  bool
}

const (
	templateNamespaceBaselineDenyPolicyName  = "sandbox0-baseline-deny-sandbox-ingress"
	templateNamespaceBaselineAllowPolicyName = "sandbox0-baseline-allow-system-to-sandbox"
	templateNamespaceBaselineProcdPort       = 49983
)

type e2ePodList struct {
	Items []e2ePod `json:"items"`
}

type e2ePod struct {
	Metadata e2ePodMetadata `json:"metadata"`
	Status   e2ePodStatus   `json:"status"`
}

type e2ePodMetadata struct {
	Name              string            `json:"name"`
	Annotations       map[string]string `json:"annotations"`
	DeletionTimestamp *metav1.Time      `json:"deletionTimestamp,omitempty"`
}

type e2ePodStatus struct {
	Conditions []e2ePodCondition `json:"conditions"`
}

type e2ePodCondition struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type idlePoolPodInfo struct {
	Name             string
	TemplateSpecHash string
}

func registerApiModeSuite(envProvider func() *framework.ScenarioEnv, opts apiModeSuiteOptions) {
	Describe(opts.describe, Ordered, func() {
		var (
			env               *framework.ScenarioEnv
			session           *e2eutils.Session
			cleanup           func()
			sandboxID         string
			sshFixtureState   *sshFixture
			sshFixtureCleanup func()
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

			if opts.includeNetworkPolicy {
				sshFixtureState, sshFixtureCleanup = setupSSHFixture(env)
			}

			resp := claimSandboxEventually(env, session, "default")
			sandboxID = resp.SandboxId
		})

		AfterAll(func() {
			if session != nil {
				_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
			}
			if sshFixtureCleanup != nil {
				sshFixtureCleanup()
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

			if opts.includePoolReadinessGate {
				It("gates pooled capacity on sandbox0-managed pod readiness", func() {
					assertTemplatePoolReadinessGate(env, session, opts.templateNamePrefix)
				})

				It("falls back to cold start while stale idle pods drain during template rollout", func() {
					assertTemplateRolloutClaimFallsBackToColdStart(env, session, opts.templateNamePrefix)
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

				It("manages credential sources and binds them through sandbox network policy", func() {
					assertCredentialSourceBindingLifecycle(env, session, sandboxID)
				})

				It("matches SSH app protocols through traffic rules", func() {
					assertSSHAppProtocolTrafficRules(env, session, sandboxID, sshFixtureState)
				})

				It("creates and repairs template namespace ingress baseline policies", func() {
					assertTemplateNamespaceIngressBaselineLifecycle(env, session, opts.templateNamePrefix)
				})

				It("enforces template namespace ingress baseline traffic rules", func() {
					assertTemplateNamespaceIngressBaselineTrafficRules(env)
				})

				It("blocks private sandbox traffic while preserving public exposure and cluster service access", func() {
					assertSandboxNetworkIsolation(env, session)
				})
			})
		}

		if opts.includeVolumeLifecycle {
			Context("sandbox volumes", func() {
				It("creates volumes and snapshots", func() {
					assertVolumeLifecycle(env, session)
				})

				It("bootstraps an existing volume during claim", func() {
					assertClaimBootstrapMountLifecycle(env, session)
				})

				It("keeps claim-mounted volumes writable", func() {
					assertClaimMountedVolumeWritable(env, session)
				})

				It("rejects mounting an active RWO volume into a second sandbox", func() {
					assertClaimMountedRWOVolumeConflict(env, session)
				})

				It("rejects invalid bootstrap mount requests at claim time", func() {
					assertClaimBootstrapMountValidation(env, session)
				})

				It("serves volume sync backend APIs", func() {
					assertVolumeSyncBackendLifecycle(env, session)
				})
			})
		}

		if opts.includeWebhookLifecycle {
			Context("sandbox webhooks", func() {
				It("delivers lifecycle events through durable state volume and pod deletion", func() {
					assertSandboxWebhookDurabilityLifecycle(env, session)
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
					waitForSandboxPowerStateEventually(env, session, sandboxID, apispec.SandboxPowerStateObserved("paused"))

					resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					Expect(resumeResp).NotTo(BeNil())
					Expect(resumeResp.Resumed).To(BeTrue())
					waitForSandboxPowerStateEventually(env, session, sandboxID, apispec.SandboxPowerStateObserved("active"))

					defaultUID := int64(1000)
					defaultGID := int64(1000)
					volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{
						DefaultPosixUid: &defaultUID,
						DefaultPosixGid: &defaultGID,
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusCreated))
					Expect(volume).NotTo(BeNil())
					volumeID := expectStringPtr(volume.Id, "volume id")

					snapshot, status, err := session.CreateSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, apispec.CreateSnapshotRequest{
						Name: "e2e-metering-snap",
					})
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusCreated))
					Expect(snapshot).NotTo(BeNil())
					Expect(snapshot.Id).NotTo(BeEmpty())

					status, err = session.RestoreSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, snapshot.Id)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))

					status, err = session.DeleteSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, snapshot.Id)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))

					status, err = session.DeleteSandboxVolume(env.TestCtx.Context, GinkgoT(), volumeID)
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
						if !hasMeteringEvent(events, metering.EventTypeVolumeCreated, "volume", volumeID) {
							return fmt.Errorf("missing volume.created event")
						}
						if !hasMeteringEvent(events, metering.EventTypeVolumeDeleted, "volume", volumeID) {
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

func assertSandboxWebhookDurabilityLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session) {
	receiverName := "sandbox0-e2e-webhook"
	cleanup := setupWebhookReceiver(env, receiverName)
	defer cleanup()

	webhookURL := fmt.Sprintf("http://%s.%s.svc.cluster.local:8080/events", receiverName, env.Infra.Namespace)
	webhookSecret := "e2e-secret"
	template := "default"
	claimReq := apispec.ClaimRequest{
		Template: &template,
		Config: &apispec.SandboxConfig{
			Webhook: &apispec.WebhookConfig{
				Url:    &webhookURL,
				Secret: &webhookSecret,
			},
		},
	}

	claimResp, err := session.ClaimSandboxWithRequest(env.TestCtx.Context, GinkgoT(), claimReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(claimResp).NotTo(BeNil())
	Expect(claimResp.BootstrapMounts).To(BeNil())
	sandboxID := claimResp.SandboxId
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	}()
	sandboxNamespace, err := naming.TemplateNamespaceForBuiltin(template)
	Expect(err).NotTo(HaveOccurred())

	volumeID, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		sandboxNamespace,
		"pod",
		claimResp.PodName,
		"{.metadata.annotations.sandbox0\\.ai/webhook-state-volume-id}",
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(volumeID)).NotTo(BeEmpty())

	Eventually(func() error {
		events := readWebhookReceiverEvents(env, receiverName)
		if !strings.Contains(events, `"event_type":"sandbox.ready"`) {
			return fmt.Errorf("missing sandbox.ready event: %s", events)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Expect(framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"delete",
		"pod",
		claimResp.PodName,
		"--namespace",
		sandboxNamespace,
		"--wait=true",
	)).To(Succeed())
	sandboxID = ""

	Eventually(func() error {
		events := readWebhookReceiverEvents(env, receiverName)
		if !strings.Contains(events, `"event_type":"sandbox.deleted"`) {
			return fmt.Errorf("missing sandbox.deleted event: %s", events)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func setupWebhookReceiver(env *framework.ScenarioEnv, name string) func() {
	manifest := fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: %[1]s-script
  namespace: %[2]s
data:
  server.py: |
    from http.server import BaseHTTPRequestHandler, HTTPServer
    from pathlib import Path

    events = Path("/data/events.jsonl")
    events.parent.mkdir(parents=True, exist_ok=True)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length)
            with events.open("ab") as f:
                f.write(body + b"\n")
            self.send_response(204)
            self.end_headers()

        def log_message(self, fmt, *args):
            return

    HTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  replicas: 1
  selector:
    matchLabels:
      app: %[1]s
  template:
    metadata:
      labels:
        app: %[1]s
    spec:
      containers:
        - name: receiver
          image: python:3.12-alpine
          imagePullPolicy: IfNotPresent
          command: ["python", "/app/server.py"]
          ports:
            - containerPort: 8080
          volumeMounts:
            - name: script
              mountPath: /app
              readOnly: true
            - name: data
              mountPath: /data
      volumes:
        - name: script
          configMap:
            name: %[1]s-script
        - name: data
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  selector:
    app: %[1]s
  ports:
    - name: http
      port: 8080
      targetPort: 8080
`, name, env.Infra.Namespace)
	Expect(framework.ApplyManifestContent(env.TestCtx.Context, env.Config.Kubeconfig, "sandbox0-e2e-webhook-", manifest)).To(Succeed())
	Expect(framework.WaitForDeployment(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, name, "3m")).To(Succeed())
	return func() {
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "service", name, "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "deployment", name, "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "configmap", name+"-script", "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
	}
}

func readWebhookReceiverEvents(env *framework.ScenarioEnv, name string) string {
	podName, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		"pod",
		"-l=app="+name,
		"{.items[0].metadata.name}",
	)
	Expect(err).NotTo(HaveOccurred())
	output, err := framework.KubectlExecOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		strings.TrimSpace(podName),
		"sh",
		"-c",
		"cat /data/events.jsonl 2>/dev/null || true",
	)
	Expect(err).NotTo(HaveOccurred())
	return output
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

func assertTemplatePoolReadinessGate(env *framework.ScenarioEnv, session *e2eutils.Session, templateNamePrefix string) {
	templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
	Expect(err).NotTo(HaveOccurred())
	Expect(templates).NotTo(BeEmpty())

	name := fmt.Sprintf("%s-ready-gate-%d", templateNamePrefix, time.Now().UnixNano())
	templateReq := e2eutils.CloneTemplateForCreate(templates[0], name)
	Expect(templateReq.Spec.Pool).NotTo(BeNil())
	Expect(templateReq.Spec.MainContainer).NotTo(BeNil())
	templateReq.Spec.MainContainer.Resources = apispec.ResourceQuota{
		Cpu:    ptr("500m"),
		Memory: ptr("2Gi"),
	}
	templateReq.Spec.Pool.MinIdle = 1
	templateReq.Spec.Pool.MaxIdle = 1
	templateReq.Spec.WarmProcesses = &[]apispec.WarmProcessSpec{{
		Type:    "cmd",
		Command: ptr([]string{"/bin/sh", "-lc", "touch /tmp/started; sleep 3600"}),
	}}

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), templateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	defer func() {
		Expect(session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), name)).To(Succeed())
	}()

	templateNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(created.TeamId, "team id"))
	Expect(err).NotTo(HaveOccurred())
	templateNameForCluster := naming.TemplateNameForCluster(naming.ScopeTeam, expectStringPtr(created.TeamId, "team id"), name)

	Eventually(func() error {
		output, outputErr := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "pods",
			"--namespace", templateNamespace,
			"--selector", fmt.Sprintf("sandbox0.ai/template-id=%s,sandbox0.ai/pool-type=idle", templateNameForCluster),
			"-o", `jsonpath={range .items[*]}{.metadata.name}{"|"}{range .spec.readinessGates[*]}{.conditionType}{","}{end}{"|"}{range .status.conditions[?(@.type=="sandbox0.ai/ready")]}{.status}{end}{"\n"}{end}`,
		)
		if outputErr != nil {
			return outputErr
		}
		output = strings.TrimSpace(output)
		if output == "" {
			return fmt.Errorf("idle pool pod not created yet")
		}
		if !strings.Contains(output, "sandbox0.ai/ready") {
			return fmt.Errorf("idle pool pod missing sandbox readiness gate: %s", output)
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		tpl, getErr := session.GetTemplate(env.TestCtx.Context, GinkgoT(), name)
		if getErr != nil {
			return getErr
		}
		if tpl.Status == nil || tpl.Status.IdleCount == nil {
			return fmt.Errorf("template status not ready")
		}
		if *tpl.Status.IdleCount != 1 {
			return fmt.Errorf("idleCount=%d, want 1 after readiness passes", *tpl.Status.IdleCount)
		}
		output, outputErr := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "pods",
			"--namespace", templateNamespace,
			"--selector", fmt.Sprintf("sandbox0.ai/template-id=%s,sandbox0.ai/pool-type=idle", templateNameForCluster),
			"-o", `jsonpath={range .items[*]}{range .status.conditions[?(@.type=="sandbox0.ai/ready")]}{.status}{end}{"\n"}{end}`,
		)
		if outputErr != nil {
			return outputErr
		}
		if !strings.Contains(strings.TrimSpace(output), "True") {
			return fmt.Errorf("sandbox readiness condition not true yet: %s", output)
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(3 * time.Second).Should(Succeed())
}

func assertTemplateRolloutClaimFallsBackToColdStart(env *framework.ScenarioEnv, session *e2eutils.Session, templateNamePrefix string) {
	templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
	Expect(err).NotTo(HaveOccurred())
	Expect(templates).NotTo(BeEmpty())

	name := fmt.Sprintf("%s-rc-%d", templateNamePrefix, time.Now().UnixNano()%1_000_000_000)
	templateReq := e2eutils.CloneTemplateForCreate(templates[0], name)
	Expect(templateReq.Spec.Pool).NotTo(BeNil())
	Expect(templateReq.Spec.MainContainer).NotTo(BeNil())
	templateReq.Spec.MainContainer.Resources = apispec.ResourceQuota{
		Cpu:    ptr("500m"),
		Memory: ptr("2Gi"),
	}
	templateReq.Spec.Pool.MinIdle = 1
	templateReq.Spec.Pool.MaxIdle = 1
	templateReq.Spec.WarmProcesses = ptr(rolloutClaimWarmProcesses("before"))

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), templateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	defer func() {
		Expect(session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), name)).To(Succeed())
	}()

	templateNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(created.TeamId, "team id"))
	Expect(err).NotTo(HaveOccurred())
	templateNameForCluster := naming.TemplateNameForCluster(naming.ScopeTeam, expectStringPtr(created.TeamId, "team id"), name)
	staleIdlePod := waitForReadyIdlePoolPodEventually(env, templateNamespace, templateNameForCluster)
	Expect(staleIdlePod.TemplateSpecHash).NotTo(BeEmpty())

	updated := *created
	Expect(updated.Spec.Pool).NotTo(BeNil())
	updated.Spec.Pool.MinIdle = 0
	updated.Spec.Pool.MaxIdle = 0
	updated.Spec.WarmProcesses = ptr(rolloutClaimWarmProcesses("after"))
	updatedResp, err := session.UpdateTemplate(env.TestCtx.Context, GinkgoT(), name, apispec.TemplateUpdateRequest{
		Spec: updated.Spec,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(updatedResp).NotTo(BeNil())

	waitForPodDeletingOrGoneEventually(env, templateNamespace, staleIdlePod.Name)

	claimResp, err := session.ClaimSandbox(env.TestCtx.Context, GinkgoT(), name)
	Expect(err).NotTo(HaveOccurred())
	Expect(claimResp).NotTo(BeNil())
	defer func() {
		Expect(session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), claimResp.SandboxId)).To(Succeed())
	}()

	sandbox := waitForSandboxPodReadyEventually(env, session, claimResp.SandboxId, templateNamespace)
	Expect(sandbox.PodName).NotTo(Equal(staleIdlePod.Name))
	Expect(podAnnotationEventually(env, templateNamespace, sandbox.PodName, "sandbox0.ai/claim-type")).To(Equal("cold"))
}

func rolloutClaimWarmProcesses(marker string) []apispec.WarmProcessSpec {
	return []apispec.WarmProcessSpec{{
		Alias:   ptr("rollout-cold-start"),
		Type:    apispec.WarmProcessSpecTypeCmd,
		Command: ptr([]string{"/bin/sh", "-lc", "touch /tmp/rollout-warm; sleep 3600"}),
		EnvVars: ptr(map[string]string{"E2E_ROLLOUT_MARKER": marker}),
	}}
}

func waitForReadyIdlePoolPodEventually(env *framework.ScenarioEnv, namespace, templateNameForCluster string) idlePoolPodInfo {
	var selected idlePoolPodInfo
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "pods",
			"--namespace", namespace,
			"--selector", fmt.Sprintf("sandbox0.ai/template-id=%s,sandbox0.ai/pool-type=idle", templateNameForCluster),
			"-o", "json",
		)
		if err != nil {
			return err
		}

		var pods e2ePodList
		if err := json.Unmarshal([]byte(output), &pods); err != nil {
			return err
		}
		for _, pod := range pods.Items {
			if pod.Metadata.DeletionTimestamp != nil || !podHasCondition(pod, "sandbox0.ai/ready", "True") {
				continue
			}
			templateHash := pod.Metadata.Annotations["sandbox0.ai/template-spec-hash"]
			if strings.TrimSpace(templateHash) == "" {
				return fmt.Errorf("ready idle pod %s is missing template spec hash", pod.Metadata.Name)
			}
			selected = idlePoolPodInfo{Name: pod.Metadata.Name, TemplateSpecHash: templateHash}
			return nil
		}
		return fmt.Errorf("ready idle pod for template %s not found", templateNameForCluster)
	}).WithTimeout(2 * time.Minute).WithPolling(3 * time.Second).Should(Succeed())
	return selected
}

func waitForPodDeletingOrGoneEventually(env *framework.ScenarioEnv, namespace, podName string) {
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "pod", podName,
			"--namespace", namespace,
			"--ignore-not-found=true",
			"-o", "json",
		)
		if err != nil {
			return err
		}
		if strings.TrimSpace(output) == "" {
			return nil
		}

		var pod e2ePod
		if err := json.Unmarshal([]byte(output), &pod); err != nil {
			return err
		}
		if pod.Metadata.DeletionTimestamp == nil {
			return fmt.Errorf("pod %s is not deleting yet", podName)
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(500 * time.Millisecond).Should(Succeed())
}

func podAnnotationEventually(env *framework.ScenarioEnv, namespace, podName, annotation string) string {
	var value string
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "pod", podName,
			"--namespace", namespace,
			"-o", "json",
		)
		if err != nil {
			return err
		}

		var pod e2ePod
		if err := json.Unmarshal([]byte(output), &pod); err != nil {
			return err
		}
		value = strings.TrimSpace(pod.Metadata.Annotations[annotation])
		if value == "" {
			return fmt.Errorf("pod %s annotation %s is not set", podName, annotation)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(time.Second).Should(Succeed())
	return value
}

func podHasCondition(pod e2ePod, conditionType, status string) bool {
	for _, condition := range pod.Status.Conditions {
		if condition.Type == conditionType && condition.Status == status {
			return true
		}
	}
	return false
}

func assertTemplateNamespaceIngressBaselineLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session, templateNamePrefix string) {
	templates, err := session.ListTemplates(env.TestCtx.Context, GinkgoT())
	Expect(err).NotTo(HaveOccurred())
	Expect(templates).NotTo(BeEmpty())

	name := fmt.Sprintf("%s-baseline-%d", templateNamePrefix, time.Now().UnixNano())
	templateReq := e2eutils.CloneTemplateForCreate(templates[0], name)

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), templateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	defer func() {
		Expect(session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), name)).To(Succeed())
	}()

	templateNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(created.TeamId, "team id"))
	Expect(err).NotTo(HaveOccurred())

	assertTemplateNamespaceBaselinePoliciesEventually(env, templateNamespace)

	for _, policyName := range []string{
		templateNamespaceBaselineDenyPolicyName,
		templateNamespaceBaselineAllowPolicyName,
	} {
		originalUID := templateNamespaceNetworkPolicyUID(env, templateNamespace, policyName)
		Expect(framework.Kubectl(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"delete",
			"networkpolicy",
			policyName,
			"--namespace",
			templateNamespace,
			"--ignore-not-found=false",
		)).To(Succeed())
		assertTemplateNamespaceNetworkPolicyOriginalUIDGoneEventually(env, templateNamespace, policyName, originalUID)
	}

	updated := *created
	desc := fmt.Sprintf("baseline repaired %d", time.Now().UnixNano())
	updated.Spec.Description = &desc
	updatedResp, err := session.UpdateTemplate(env.TestCtx.Context, GinkgoT(), name, apispec.TemplateUpdateRequest{
		Spec: updated.Spec,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(updatedResp).NotTo(BeNil())
	Expect(updatedResp.Spec.Description).NotTo(BeNil())
	Expect(*updatedResp.Spec.Description).To(Equal(desc))

	assertTemplateNamespaceBaselinePoliciesEventually(env, templateNamespace)
}

func templateNamespaceNetworkPolicyUID(env *framework.ScenarioEnv, namespace, policyName string) string {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get",
		"networkpolicy",
		policyName,
		"--namespace",
		namespace,
		"-o",
		"jsonpath={.metadata.uid}",
	)
	Expect(err).NotTo(HaveOccurred())
	uid := strings.TrimSpace(output)
	Expect(uid).NotTo(BeEmpty(), "expected networkpolicy %s/%s to have a uid", namespace, policyName)
	return uid
}

func assertTemplateNamespaceNetworkPolicyOriginalUIDGoneEventually(env *framework.ScenarioEnv, namespace, policyName, originalUID string) {
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get",
			"networkpolicy",
			policyName,
			"--namespace",
			namespace,
			"--ignore-not-found=true",
			"-o",
			"jsonpath={.metadata.uid}",
		)
		if err != nil {
			return err
		}

		currentUID := strings.TrimSpace(output)
		if currentUID == originalUID {
			return fmt.Errorf("networkpolicy %s/%s still has original uid %s", namespace, policyName, originalUID)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(1 * time.Second).Should(Succeed())
}

func assertTemplateNamespaceBaselinePoliciesEventually(env *framework.ScenarioEnv, namespace string) {
	expectedNames := []string{
		templateNamespaceBaselineDenyPolicyName,
		templateNamespaceBaselineAllowPolicyName,
	}

	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get",
			"networkpolicy",
			"--namespace",
			namespace,
			"-o",
			`jsonpath={range .items[*]}{.metadata.name}{"\n"}{end}`,
		)
		if err != nil {
			return err
		}

		existing := map[string]struct{}{}
		for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
			name := strings.TrimSpace(line)
			if name == "" {
				continue
			}
			existing[name] = struct{}{}
		}
		for _, expectedName := range expectedNames {
			if _, ok := existing[expectedName]; !ok {
				return fmt.Errorf("networkpolicy %s missing in namespace %s", expectedName, namespace)
			}
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func assertTemplateNamespaceIngressBaselineTrafficRules(env *framework.ScenarioEnv) {
	testNamespace := fmt.Sprintf("e2e-baseline-np-%d", time.Now().UnixNano())
	manifestFile, err := os.CreateTemp("", "sandbox0-e2e-baseline-networkpolicy-*.yaml")
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		_ = framework.KubectlDeleteManifest(env.TestCtx.Context, env.Config.Kubeconfig, manifestFile.Name())
		_ = os.Remove(manifestFile.Name())
	}()

	manifest := fmt.Sprintf(`
apiVersion: v1
kind: Namespace
metadata:
  name: %s
---
apiVersion: v1
kind: Pod
metadata:
  name: baseline-server
  namespace: %s
  labels:
    sandbox0.ai/sandbox-id: baseline-server
spec:
  restartPolicy: Never
  containers:
    - name: server
      image: busybox:1.36
      command:
        - sh
        - -lc
        - |
          set -eu
          mkdir -p /srv/http-80 /srv/http-49983
          printf 'baseline-server-80\n' >/srv/http-80/index.html
          printf 'baseline-server-procd\n' >/srv/http-49983/index.html
          httpd -f -p 80 -h /srv/http-80 &
          exec httpd -f -p %d -h /srv/http-49983
---
apiVersion: v1
kind: Pod
metadata:
  name: same-namespace-client
  namespace: %s
  labels:
    sandbox0.ai/sandbox-id: same-namespace-client
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: busybox:1.36
      command: ["sh", "-lc", "sleep 3600"]
---
apiVersion: v1
kind: Pod
metadata:
  name: baseline-cluster-gateway
  namespace: %s
  labels:
    app.kubernetes.io/name: cluster-gateway
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: busybox:1.36
      command: ["sh", "-lc", "sleep 3600"]
---
apiVersion: v1
kind: Pod
metadata:
  name: baseline-manager
  namespace: %s
  labels:
    app.kubernetes.io/name: manager
spec:
  restartPolicy: Never
  containers:
    - name: client
      image: busybox:1.36
      command: ["sh", "-lc", "sleep 3600"]
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
spec:
  podSelector:
    matchExpressions:
      - key: sandbox0.ai/sandbox-id
        operator: Exists
  policyTypes:
    - Ingress
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: %s
  namespace: %s
spec:
  podSelector:
    matchExpressions:
      - key: sandbox0.ai/sandbox-id
        operator: Exists
  policyTypes:
    - Ingress
  ingress:
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: %s
          podSelector:
            matchLabels:
              app.kubernetes.io/name: manager
      ports:
        - protocol: TCP
          port: %d
    - from:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: %s
          podSelector:
            matchLabels:
              app.kubernetes.io/name: cluster-gateway
`, testNamespace, testNamespace, templateNamespaceBaselineProcdPort, testNamespace, env.Infra.Namespace, env.Infra.Namespace, templateNamespaceBaselineDenyPolicyName, testNamespace, templateNamespaceBaselineAllowPolicyName, testNamespace, env.Infra.Namespace, templateNamespaceBaselineProcdPort, env.Infra.Namespace)
	Expect(os.WriteFile(manifestFile.Name(), []byte(strings.TrimSpace(manifest)), 0o600)).To(Succeed())
	Expect(framework.ApplyManifest(env.TestCtx.Context, env.Config.Kubeconfig, manifestFile.Name())).To(Succeed())

	Expect(framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, testNamespace, "pod", "baseline-server", "Ready", "3m")).To(Succeed())
	Expect(framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, testNamespace, "pod", "same-namespace-client", "Ready", "3m")).To(Succeed())
	Expect(framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "pod", "baseline-cluster-gateway", "Ready", "3m")).To(Succeed())
	Expect(framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "pod", "baseline-manager", "Ready", "3m")).To(Succeed())

	serverIP, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, testNamespace, "pod", "baseline-server", "{.status.podIP}")
	Expect(err).NotTo(HaveOccurred())
	serverIP = strings.TrimSpace(serverIP)
	Expect(serverIP).NotTo(BeEmpty())

	Eventually(func() error {
		body, execErr := framework.KubectlExecOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			env.Infra.Namespace,
			"baseline-cluster-gateway",
			"sh", "-lc", fmt.Sprintf("wget -qO- --timeout=5 http://%s:80/", serverIP),
		)
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(body) != "baseline-server-80" {
			return fmt.Errorf("unexpected cluster-gateway body: %q", body)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		body, execErr := framework.KubectlExecOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			env.Infra.Namespace,
			"baseline-manager",
			"sh", "-lc", fmt.Sprintf("wget -qO- --timeout=5 http://%s:%d/", serverIP, templateNamespaceBaselineProcdPort),
		)
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(body) != "baseline-server-procd" {
			return fmt.Errorf("unexpected manager procd body: %q", body)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		_, execErr := framework.KubectlExecOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			testNamespace,
			"same-namespace-client",
			"sh", "-lc", fmt.Sprintf("wget -qO- --timeout=2 http://%s:80/", serverIP),
		)
		if execErr == nil {
			return fmt.Errorf("expected same-namespace sandbox traffic to be denied")
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		_, execErr := framework.KubectlExecOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			env.Infra.Namespace,
			"baseline-manager",
			"sh", "-lc", fmt.Sprintf("wget -qO- --timeout=2 http://%s:80/", serverIP),
		)
		if execErr == nil {
			return fmt.Errorf("expected manager access to non-procd port to be denied")
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
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

	processType := apispec.ProcessTypeCmd
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

func assertSandboxNetworkIsolation(env *framework.ScenarioEnv, session *e2eutils.Session) {
	workerNodes, err := listWorkerNodes(env)
	Expect(err).NotTo(HaveOccurred())
	if len(workerNodes) < 2 {
		Skip("network isolation e2e requires at least two worker nodes")
	}

	baseTemplate, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
	Expect(err).NotTo(HaveOccurred())

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	templateAID := "e2e-net-a-" + suffix
	templateBID := "e2e-net-b-" + suffix
	templateANamespace, err := naming.TemplateNamespaceForBuiltin(templateAID)
	Expect(err).NotTo(HaveOccurred())
	templateBNamespace, err := naming.TemplateNamespaceForBuiltin(templateBID)
	Expect(err).NotTo(HaveOccurred())

	err = applyPinnedTemplate(env, *baseTemplate, templateAID, workerNodes[0])
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		_ = deleteTemplateCR(env, templateAID)
	}()

	err = applyPinnedTemplate(env, *baseTemplate, templateBID, workerNodes[1])
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		_ = deleteTemplateCR(env, templateBID)
	}()

	sandboxAID := claimSandboxEventually(env, session, templateAID).SandboxId
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxAID)
	}()

	sandboxBID := claimSandboxEventually(env, session, templateBID).SandboxId
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxBID)
	}()

	sandboxA := waitForSandboxPodReadyEventually(env, session, sandboxAID, templateANamespace)
	sandboxB := waitForSandboxPodReadyEventually(env, session, sandboxBID, templateBNamespace)

	nodeA, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, templateANamespace, "pod", sandboxA.PodName, "{.spec.nodeName}")
	Expect(err).NotTo(HaveOccurred())
	nodeB, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, templateBNamespace, "pod", sandboxB.PodName, "{.spec.nodeName}")
	Expect(err).NotTo(HaveOccurred())
	Expect(nodeA).To(Equal(workerNodes[0]))
	Expect(nodeB).To(Equal(workerNodes[1]))
	Expect(nodeA).NotTo(Equal(nodeB))

	const exposedPort int32 = 18080
	const expectedBody = "sandbox public route works\n"

	startSandboxHTTPServer(env, templateBNamespace, sandboxB.PodName, exposedPort, expectedBody)
	Eventually(func() error {
		body, execErr := execInSandboxPod(env, templateBNamespace, sandboxB.PodName, fmt.Sprintf("curl -fsS --max-time 5 http://127.0.0.1:%d/", exposedPort))
		if execErr != nil {
			return execErr
		}
		if body != expectedBody {
			return fmt.Errorf("unexpected local server body: %q", body)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

	podIPB, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, templateBNamespace, "pod", sandboxB.PodName, "{.status.podIP}")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(podIPB)).NotTo(BeEmpty())

	Eventually(func() error {
		_, execErr := execInSandboxPod(env, templateANamespace, sandboxA.PodName, fmt.Sprintf("curl -fsS --max-time 5 http://%s:%d/", strings.TrimSpace(podIPB), exposedPort))
		if execErr == nil {
			return fmt.Errorf("expected private sandbox-to-sandbox request to fail")
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	clusterGatewayPort, err := framework.GetServicePort(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, env.Infra.Name+"-cluster-gateway")
	Expect(err).NotTo(HaveOccurred())
	clusterGatewayBaseURL := fmt.Sprintf("http://%s-cluster-gateway.%s.svc.cluster.local:%d", env.Infra.Name, env.Infra.Namespace, clusterGatewayPort)

	Eventually(func() error {
		body, execErr := execInSandboxPod(env, templateANamespace, sandboxA.PodName, fmt.Sprintf("curl -fsS --max-time 5 %s/healthz", clusterGatewayBaseURL))
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(body) == "" {
			return fmt.Errorf("cluster-gateway healthz returned empty body")
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

	updatedPorts, status, err := session.UpdateExposedPorts(env.TestCtx.Context, GinkgoT(), sandboxBID, []apispec.ExposedPortConfig{{
		Port:   exposedPort,
		Resume: true,
	}})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	publicHost := publicExposureHostForPort(updatedPorts, exposedPort)
	Expect(publicHost).NotTo(BeEmpty())

	Eventually(func() error {
		body, execErr := execInSandboxPod(env, templateANamespace, sandboxA.PodName, fmt.Sprintf("curl -fsS --max-time 10 -H 'Host: %s' %s/", publicHost, clusterGatewayBaseURL))
		if execErr != nil {
			return execErr
		}
		if body != expectedBody {
			return fmt.Errorf("unexpected public exposure body: %q", body)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func assertCredentialSourceBindingLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	Expect(sandboxID).NotTo(BeEmpty())

	sourceName := fmt.Sprintf("e2e-headers-%d", time.Now().UnixNano())
	refName := "api-egress-auth"
	ruleName := "api-egress-auth-rule"
	domains := []string{"httpbin.org"}
	headers := map[string]string{"token": "initial-token"}

	created, err := session.CreateCredentialSource(env.TestCtx.Context, GinkgoT(), apispec.CredentialSourceWriteRequest{
		Name:         sourceName,
		ResolverKind: apispec.StaticHeaders,
		Spec: apispec.CredentialSourceWriteSpec{
			StaticHeaders: &apispec.StaticHeadersSourceSpec{
				Values: &headers,
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	Expect(created.Name).To(Equal(sourceName))

	cleanupSource := true
	defer func() {
		clearPolicy := apispec.SandboxNetworkPolicy{
			Mode:               apispec.AllowAll,
			CredentialBindings: &[]apispec.CredentialBinding{},
		}
		_, _, _, _ = session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, clearPolicy)
		if !cleanupSource {
			return
		}
		status, _, deleteErr := session.DeleteCredentialSource(env.TestCtx.Context, sourceName)
		Expect(deleteErr).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
	}()

	records, err := session.ListCredentialSources(env.TestCtx.Context, GinkgoT())
	Expect(err).NotTo(HaveOccurred())
	found := false
	for _, record := range records {
		if record.Name == sourceName {
			found = true
			break
		}
	}
	Expect(found).To(BeTrue())

	fetched, status, apiErr, err := session.GetCredentialSource(env.TestCtx.Context, GinkgoT(), sourceName)
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
	Expect(fetched).NotTo(BeNil())
	Expect(fetched.Name).To(Equal(sourceName))

	updatedHeaders := map[string]string{"token": "updated-token"}
	updatedSource, err := session.UpdateCredentialSource(env.TestCtx.Context, GinkgoT(), sourceName, apispec.CredentialSourceWriteRequest{
		ResolverKind: apispec.StaticHeaders,
		Spec: apispec.CredentialSourceWriteSpec{
			StaticHeaders: &apispec.StaticHeadersSourceSpec{
				Values: &updatedHeaders,
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(updatedSource).NotTo(BeNil())
	Expect(updatedSource.Name).To(Equal(sourceName))
	Expect(updatedSource.CurrentVersion).NotTo(BeNil())

	policy, status, apiErr, err := session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxNetworkPolicy{
		Mode: apispec.AllowAll,
		Egress: &apispec.NetworkEgressPolicy{
			CredentialRules: &[]apispec.EgressCredentialRule{{
				Name:          &ruleName,
				CredentialRef: refName,
				Domains:       &domains,
				Protocol:      ptrTo(apispec.EgressAuthProtocolHttps),
				Rollout:       ptrTo(apispec.Enabled),
			}},
		},
		CredentialBindings: &[]apispec.CredentialBinding{{
			Ref:       refName,
			SourceRef: sourceName,
			Projection: apispec.ProjectionSpec{
				Type: apispec.HttpHeaders,
				HttpHeaders: &apispec.HTTPHeadersProjection{
					Headers: &[]apispec.ProjectedHeader{{
						Name:          "Authorization",
						ValueTemplate: "Bearer {{ .token }}",
					}},
				},
			},
		}},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
	Expect(policy).NotTo(BeNil())
	Expect(policy.CredentialBindings).NotTo(BeNil())
	Expect(*policy.CredentialBindings).To(HaveLen(1))
	Expect((*policy.CredentialBindings)[0].Ref).To(Equal(refName))
	Expect((*policy.CredentialBindings)[0].SourceRef).To(Equal(sourceName))
	Expect(policy.Egress).NotTo(BeNil())
	Expect(policy.Egress.CredentialRules).NotTo(BeNil())
	Expect(*policy.Egress.CredentialRules).To(HaveLen(1))
	Expect((*policy.Egress.CredentialRules)[0].CredentialRef).To(Equal(refName))

	effective, status, apiErr, err := session.GetNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
	Expect(effective).NotTo(BeNil())
	Expect(effective.CredentialBindings).NotTo(BeNil())
	Expect(*effective.CredentialBindings).To(HaveLen(1))
	Expect((*effective.CredentialBindings)[0].Ref).To(Equal(refName))
	Expect((*effective.CredentialBindings)[0].SourceRef).To(Equal(sourceName))

	status, apiErr, err = session.DeleteCredentialSource(env.TestCtx.Context, sourceName)
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).NotTo(BeNil())
	Expect(status).To(Equal(http.StatusConflict))

	clearPolicy, status, apiErr, err := session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxNetworkPolicy{
		Mode:               apispec.AllowAll,
		CredentialBindings: &[]apispec.CredentialBinding{},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
	Expect(clearPolicy).NotTo(BeNil())
	Expect(clearPolicy.CredentialBindings).To(BeNil())
	cleanupSource = false

	status, apiErr, err = session.DeleteCredentialSource(env.TestCtx.Context, sourceName)
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
}

func ptrTo[T any](value T) *T {
	return &value
}

func applyPinnedTemplate(env *framework.ScenarioEnv, base apispec.Template, templateID, nodeName string) error {
	templateCR, err := buildPinnedTemplateCR(base, templateID, nodeName)
	if err != nil {
		return err
	}
	if err := framework.EnsureNamespace(env.TestCtx.Context, env.Config.Kubeconfig, templateCR.Namespace); err != nil {
		return err
	}
	raw, err := yaml.Marshal(templateCR)
	if err != nil {
		return err
	}
	file, err := os.CreateTemp("", "sandbox0-e2e-template-*.yaml")
	if err != nil {
		return err
	}
	defer os.Remove(file.Name())
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	return framework.ApplyManifest(env.TestCtx.Context, env.Config.Kubeconfig, file.Name())
}

func buildPinnedTemplateCR(base apispec.Template, templateID, nodeName string) (*mgrv1alpha1.SandboxTemplate, error) {
	raw, err := json.Marshal(base.Spec)
	if err != nil {
		return nil, err
	}

	var spec mgrv1alpha1.SandboxTemplateSpec
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, err
	}

	spec.Description = "E2E network isolation template pinned to " + nodeName
	spec.DisplayName = "E2E network isolation " + nodeName
	spec.Pool = mgrv1alpha1.PoolStrategy{
		MinIdle: 0,
		MaxIdle: 0,
	}
	if spec.Pod == nil {
		spec.Pod = &mgrv1alpha1.PodSpecOverride{}
	}
	nodeSelector := map[string]string{}
	for key, value := range spec.Pod.NodeSelector {
		nodeSelector[key] = value
	}
	nodeSelector["kubernetes.io/hostname"] = nodeName
	spec.Pod.NodeSelector = nodeSelector
	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	if err != nil {
		return nil, err
	}

	return &mgrv1alpha1.SandboxTemplate{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "sandbox0.ai/v1alpha1",
			Kind:       "SandboxTemplate",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      templateID,
			Namespace: namespace,
			Labels: map[string]string{
				"sandbox0.ai/template-scope":      naming.ScopePublic,
				"sandbox0.ai/template-logical-id": templateID,
			},
		},
		Spec: spec,
	}, nil
}

func deleteTemplateCR(env *framework.ScenarioEnv, templateID string) error {
	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	if err != nil {
		return err
	}
	return framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"delete",
		"sandboxtemplate",
		templateID,
		"--namespace",
		namespace,
		"--ignore-not-found=true",
	)
}

func waitForSandboxPodReadyEventually(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID, namespace string) *apispec.Sandbox {
	var sandbox *apispec.Sandbox
	Eventually(func() error {
		current, _, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
		if err != nil {
			return err
		}
		if strings.TrimSpace(current.PodName) == "" {
			return fmt.Errorf("sandbox %s pod name not assigned", sandboxID)
		}
		if err := framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, namespace, "pod", current.PodName, "Ready", "10s"); err != nil {
			describe, describeErr := framework.KubectlOutput(
				env.TestCtx.Context,
				env.Config.Kubeconfig,
				"-n", namespace,
				"describe", "pod", current.PodName,
			)
			if describeErr != nil {
				return fmt.Errorf("wait for pod %s ready: %w (describe failed: %v)", current.PodName, err, describeErr)
			}
			return fmt.Errorf("wait for pod %s ready: %w\n%s", current.PodName, err, strings.TrimSpace(describe))
		}
		sandbox = current
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	return sandbox
}

func waitForSandboxPowerStateEventually(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string, observed apispec.SandboxPowerStateObserved) *apispec.Sandbox {
	var sandbox *apispec.Sandbox
	Eventually(func() error {
		resp, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("get sandbox status %d", status)
		}
		if resp == nil {
			return fmt.Errorf("sandbox response missing")
		}
		if resp.PowerState.Observed != observed || resp.PowerState.Phase != apispec.SandboxPowerStatePhase("stable") {
			return fmt.Errorf("sandbox power state not stable at %s yet: observed=%s phase=%s", observed, resp.PowerState.Observed, resp.PowerState.Phase)
		}
		sandbox = resp
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
	return sandbox
}

func listWorkerNodes(env *framework.ScenarioEnv) ([]string, error) {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get", "nodes",
		"--selector=!node-role.kubernetes.io/control-plane",
		"-o", "jsonpath={range .items[*]}{.metadata.name}{\"\\n\"}{end}",
	)
	if err != nil {
		return nil, err
	}
	nodes := strings.Fields(strings.TrimSpace(output))
	return nodes, nil
}

func startSandboxHTTPServer(env *framework.ScenarioEnv, namespace, podName string, port int32, body string) {
	script := fmt.Sprintf(
		"set -eu; dir=/tmp/s0-e2e-http-%d; rm -rf \"$dir\"; mkdir -p \"$dir\"; cat <<'EOF' > \"$dir/index.html\"\n%sEOF\nnohup python3 -m http.server %d --bind 0.0.0.0 -d \"$dir\" >/tmp/s0-e2e-http.log 2>&1 &\n",
		port,
		body,
		port,
	)
	_, err := execInSandboxPod(env, namespace, podName, script)
	Expect(err).NotTo(HaveOccurred())
}

func execInSandboxPod(env *framework.ScenarioEnv, namespace, podName, script string) (string, error) {
	output, err := framework.KubectlExecContainerOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		namespace,
		podName,
		"procd",
		"/bin/sh", "-lc", script,
	)
	return strings.ReplaceAll(output, "\r\n", "\n"), err
}

func createVolumePortalTemplate(env *framework.ScenarioEnv, session *e2eutils.Session, mountPath string) string {
	templateID := fmt.Sprintf("volume-portal-%d", time.Now().UnixNano())
	base, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
	Expect(err).NotTo(HaveOccurred())

	req := e2eutils.CloneTemplateForCreate(*base, templateID)
	req.Spec.VolumeMounts = &[]apispec.VolumeMountSpec{{
		Name:      "data",
		MountPath: mountPath,
	}}

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), req)
	Expect(err).NotTo(HaveOccurred())
	templateNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(created.TeamId, "team id"))
	Expect(err).NotTo(HaveOccurred())

	DeferCleanup(func() {
		deleteTeamTemplateAndWaitForNamespaceCleanup(env, session, templateID, templateNamespace)
	})

	Eventually(func() error {
		tpl, getErr := session.GetTemplate(env.TestCtx.Context, GinkgoT(), templateID)
		if getErr != nil {
			return getErr
		}
		if tpl.Status == nil || tpl.Status.IdleCount == nil || *tpl.Status.IdleCount < 1 {
			return fmt.Errorf("template %s idle pool is not ready", templateID)
		}
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

	return templateID
}

func deleteTeamTemplateAndWaitForNamespaceCleanup(env *framework.ScenarioEnv, session *e2eutils.Session, templateID, namespace string) {
	if err := session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), templateID); err != nil {
		GinkgoWriter.Printf("delete template %q through API failed during cleanup, falling back to kubectl cleanup: %v\n", templateID, err)
		_, _ = framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"delete", "sandboxtemplate", templateID,
			"--namespace", namespace,
			"--ignore-not-found=true",
		)
		_, _ = framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"delete", "namespace", namespace,
			"--ignore-not-found=true",
			"--wait=false",
		)
	}
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"get", "namespace", namespace,
			"--ignore-not-found",
		)
		if err != nil {
			return err
		}
		if strings.TrimSpace(output) != "" {
			return fmt.Errorf("namespace %s still exists", namespace)
		}
		return nil
	}).WithTimeout(2 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
}

func publicExposureHostForPort(ports []apispec.ExposedPortConfig, port int32) string {
	for _, item := range ports {
		if item.Port != port || item.PublicUrl == nil {
			continue
		}
		host := strings.TrimSpace(*item.PublicUrl)
		host = strings.TrimPrefix(host, "https://")
		host = strings.TrimPrefix(host, "http://")
		host = strings.TrimSuffix(host, "/")
		if host != "" {
			return host
		}
	}
	return ""
}

func assertVolumeLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session) {
	defaultUID := int64(1000)
	defaultGID := int64(1000)
	createReq := apispec.CreateSandboxVolumeRequest{
		DefaultPosixUid: &defaultUID,
		DefaultPosixGid: &defaultGID,
	}
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), createReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "volume id")

	directFilePath := "/direct-e2e/hello.txt"
	directContent := []byte("hello direct volume api")
	status, err = session.WriteVolumeFile(env.TestCtx.Context, GinkgoT(), volumeID, directFilePath, directContent, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	directBody, status, err := session.ReadVolumeFile(env.TestCtx.Context, GinkgoT(), volumeID, directFilePath)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(directBody).To(Equal(directContent))

	snapReq := apispec.CreateSnapshotRequest{
		Name: "e2e-snap",
	}
	snapshot, status, err := session.CreateSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, snapReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(snapshot).NotTo(BeNil())
	Expect(snapshot.Id).NotTo(BeEmpty())

	_, status, err = session.ListSnapshots(env.TestCtx.Context, GinkgoT(), volumeID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	status, err = session.RestoreSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, snapshot.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	status, err = session.DeleteSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, snapshot.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	status, err = session.DeleteSandboxVolume(env.TestCtx.Context, GinkgoT(), volumeID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
}

func assertClaimBootstrapMountLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session) {
	createReq := apispec.CreateSandboxVolumeRequest{}
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), createReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "volume id")
	DeferCleanup(func() {
		deleteSandboxVolumeForCleanup(env, session, volumeID)
	})

	seedPath := "/claim-bootstrap/hello.txt"
	seedContent := []byte("hello bootstrap claim mount")
	status, err = session.WriteVolumeFile(env.TestCtx.Context, GinkgoT(), volumeID, seedPath, seedContent, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	mountPoint := "/workspace/bootstrap-data"
	templateID := createVolumePortalTemplate(env, session, mountPoint)
	claimReq := apispec.ClaimRequest{
		Template: &templateID,
		Mounts: &[]apispec.ClaimMountRequest{{
			SandboxvolumeId: volumeID,
			MountPoint:      mountPoint,
		}},
	}
	claimResp, err := session.ClaimSandboxWithRequest(env.TestCtx.Context, GinkgoT(), claimReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(claimResp).NotTo(BeNil())
	sandboxID := claimResp.SandboxId
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})
	Expect(claimResp.BootstrapMounts).NotTo(BeNil())
	Expect(*claimResp.BootstrapMounts).NotTo(BeEmpty())
	Expect((*claimResp.BootstrapMounts)[0].State).To(Equal(apispec.MountStatusStateMounted))

	Eventually(func() ([]byte, error) {
		body, _, readErr := session.ReadFile(env.TestCtx.Context, GinkgoT(), sandboxID, mountPoint+seedPath)
		return body, readErr
	}).WithTimeout(20 * time.Second).WithPolling(1 * time.Second).Should(Equal(seedContent))
}

func assertClaimMountedVolumeWritable(env *framework.ScenarioEnv, session *e2eutils.Session) {
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "volume id")
	DeferCleanup(func() {
		deleteSandboxVolumeForCleanup(env, session, volumeID)
	})

	mountPoint := "/workspace/claim-writable"
	templateID := createVolumePortalTemplate(env, session, mountPoint)
	claimReq := apispec.ClaimRequest{
		Template: &templateID,
		Mounts: &[]apispec.ClaimMountRequest{{
			SandboxvolumeId: volumeID,
			MountPoint:      mountPoint,
		}},
	}
	claimResp, err := session.ClaimSandboxWithRequest(env.TestCtx.Context, GinkgoT(), claimReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(claimResp).NotTo(BeNil())
	sandboxID := claimResp.SandboxId
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})
	Expect(claimResp.BootstrapMounts).NotTo(BeNil())
	Expect(*claimResp.BootstrapMounts).NotTo(BeEmpty())
	Expect((*claimResp.BootstrapMounts)[0].State).To(Equal(apispec.MountStatusStateMounted))

	apiPath := "/api-after-claim.txt"
	apiContent := []byte(fmt.Sprintf("api write after claim %d", time.Now().UnixNano()))
	status, err = session.WriteVolumeFile(env.TestCtx.Context, GinkgoT(), volumeID, apiPath, apiContent, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	Eventually(func() ([]byte, error) {
		body, _, readErr := session.ReadFile(env.TestCtx.Context, GinkgoT(), sandboxID, mountPoint+apiPath)
		return body, readErr
	}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Equal(apiContent))

	latePath := "/late-after-claim.txt"
	lateContent := fmt.Sprintf("late write after claim %d", time.Now().UnixNano())
	processType := apispec.ProcessTypeCmd
	ttlSec := int32(120)
	envVars := map[string]string{"S0_LATE_CONTENT": lateContent}
	ctxReq := apispec.CreateContextRequest{
		Type: &processType,
		Cmd: &apispec.CreateCMDContextRequest{
			Command: []string{
				"/bin/sh",
				"-lc",
				"printf '%s' \"$S0_LATE_CONTENT\" > /workspace/claim-writable/late-after-claim.txt; sync",
			},
		},
		EnvVars: &envVars,
		TtlSec:  &ttlSec,
	}
	ctxResp, status, err := session.CreateContext(env.TestCtx.Context, GinkgoT(), sandboxID, ctxReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(ctxResp).NotTo(BeNil())
	DeferCleanup(func() {
		_, _ = session.DeleteContext(env.TestCtx.Context, GinkgoT(), sandboxID, ctxResp.Id)
	})

	Eventually(func() ([]byte, error) {
		body, _, readErr := session.ReadVolumeFile(env.TestCtx.Context, GinkgoT(), volumeID, latePath)
		return body, readErr
	}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Equal([]byte(lateContent)))

	batchReq := apispec.CreateContextRequest{
		Type: &processType,
		Cmd: &apispec.CreateCMDContextRequest{
			Command: []string{
				"/bin/sh",
				"-lc",
				`set -eu; mkdir -p /workspace/claim-writable/small-batch; i=0; while [ "$i" -lt 16 ]; do name=$(printf "file-%02d.txt" "$i"); body=$(printf "batch-%02d" "$i"); printf "%s" "$body" > "/workspace/claim-writable/small-batch/$name"; i=$((i + 1)); done; sync`,
			},
		},
		TtlSec: &ttlSec,
	}
	batchCtxResp, status, err := session.CreateContext(env.TestCtx.Context, GinkgoT(), sandboxID, batchReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(batchCtxResp).NotTo(BeNil())
	DeferCleanup(func() {
		_, _ = session.DeleteContext(env.TestCtx.Context, GinkgoT(), sandboxID, batchCtxResp.Id)
	})

	for _, index := range []int{0, 7, 15} {
		path := fmt.Sprintf("/small-batch/file-%02d.txt", index)
		expected := []byte(fmt.Sprintf("batch-%02d", index))
		Eventually(func() ([]byte, error) {
			body, _, readErr := session.ReadVolumeFile(env.TestCtx.Context, GinkgoT(), volumeID, path)
			return body, readErr
		}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Equal(expected))
	}
}

func deleteSandboxVolumeForCleanup(env *framework.ScenarioEnv, session *e2eutils.Session, volumeID string) {
	if err := session.DeleteSandboxVolumeEventually(env.TestCtx.Context, GinkgoT(), volumeID, 30*time.Second); err != nil {
		GinkgoWriter.Printf("delete sandbox volume %q failed during cleanup: %v\n", volumeID, err)
	}
}

func assertClaimBootstrapMountValidation(env *framework.ScenarioEnv, session *e2eutils.Session) {
	templateID := "default"
	claimReq := apispec.ClaimRequest{
		Template: &templateID,
		Mounts: &[]apispec.ClaimMountRequest{
			{SandboxvolumeId: "vol-a", MountPoint: "/workspace/data"},
			{SandboxvolumeId: "vol-b", MountPoint: "/workspace/data"},
		},
	}
	_, status, err := session.ClaimSandboxDetailed(env.TestCtx.Context, GinkgoT(), claimReq)
	Expect(err).To(HaveOccurred())
	Expect(status).To(Equal(http.StatusBadRequest))
}

func assertClaimMountedRWOVolumeConflict(env *framework.ScenarioEnv, session *e2eutils.Session) {
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "volume id")
	DeferCleanup(func() {
		deleteSandboxVolumeForCleanup(env, session, volumeID)
	})

	mountPoint := "/workspace/claim-rwo-conflict"
	templateID := createVolumePortalTemplate(env, session, mountPoint)
	claimReq := apispec.ClaimRequest{
		Template: &templateID,
		Mounts: &[]apispec.ClaimMountRequest{{
			SandboxvolumeId: volumeID,
			MountPoint:      mountPoint,
		}},
	}

	firstClaim, err := session.ClaimSandboxWithRequest(env.TestCtx.Context, GinkgoT(), claimReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(firstClaim).NotTo(BeNil())
	firstSandboxID := firstClaim.SandboxId
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), firstSandboxID)
	})

	_, status, err = session.ClaimSandboxDetailed(env.TestCtx.Context, GinkgoT(), claimReq)
	Expect(err).To(HaveOccurred())
	Expect(status).To(Equal(http.StatusConflict))
}

func assertVolumeSyncBackendLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session) {
	defaultUID := int64(1000)
	defaultGID := int64(1000)
	volume, status, err := session.CreateSandboxVolume(env.TestCtx.Context, GinkgoT(), apispec.CreateSandboxVolumeRequest{
		DefaultPosixUid: &defaultUID,
		DefaultPosixGid: &defaultGID,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(volume).NotTo(BeNil())
	volumeID := expectStringPtr(volume.Id, "volume id")

	defer func() {
		status, err := session.DeleteSandboxVolume(env.TestCtx.Context, GinkgoT(), volumeID)
		Expect(err).NotTo(HaveOccurred())
		Expect(status).To(Equal(http.StatusOK))
	}()

	displayName := "Linux Laptop"
	platform := "linux"
	rootPath := "/workspace"
	caseSensitive := true
	replica, status, err := session.UpsertSyncReplica(env.TestCtx.Context, GinkgoT(), volumeID, "replica-linux", apispec.UpsertSyncReplicaRequest{
		DisplayName:   &displayName,
		Platform:      &platform,
		RootPath:      &rootPath,
		CaseSensitive: &caseSensitive,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(replica).NotTo(BeNil())
	Expect(replica.HeadSeq).To(Equal(int64(0)))

	appendResp, status, err := session.AppendSyncReplicaChanges(env.TestCtx.Context, GinkgoT(), volumeID, "replica-linux", apispec.AppendReplicaChangesRequest{
		RequestId: "req-e2e-sync-1",
		BaseSeq:   0,
		Changes: []apispec.ChangeRequest{{
			EventType:     apispec.SyncEventType("write"),
			Path:          ptr("volume-sync-e2e/main.go"),
			ContentBase64: ptr("cGFja2FnZSBtYWluCg=="),
		}},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(appendResp).NotTo(BeNil())
	Expect(appendResp.Accepted).To(HaveLen(1))
	Expect(appendResp.Conflicts).To(BeEmpty())
	Expect(appendResp.Accepted[0].Seq).NotTo(BeNil())
	firstSeq := *appendResp.Accepted[0].Seq
	Expect(appendResp.HeadSeq).To(Equal(firstSeq))
	Expect(appendResp.Accepted[0].Path).NotTo(BeNil())
	Expect(*appendResp.Accepted[0].Path).To(Equal("volume-sync-e2e/main.go"))

	changesResp, status, err := session.ListSyncChanges(env.TestCtx.Context, GinkgoT(), volumeID, 0, nil)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(changesResp).NotTo(BeNil())
	Expect(changesResp.HeadSeq).To(Equal(firstSeq))
	Expect(changesResp.Changes).To(HaveLen(1))
	Expect(changesResp.Changes[0].Path).NotTo(BeNil())
	Expect(*changesResp.Changes[0].Path).To(Equal("volume-sync-e2e/main.go"))

	replica, status, err = session.UpdateSyncReplicaCursor(env.TestCtx.Context, GinkgoT(), volumeID, "replica-linux", apispec.UpdateSyncReplicaCursorRequest{
		LastAppliedSeq: firstSeq,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(replica.Replica.LastAppliedSeq).NotTo(BeNil())
	Expect(*replica.Replica.LastAppliedSeq).To(Equal(firstSeq))

	replica, status, err = session.GetSyncReplica(env.TestCtx.Context, GinkgoT(), volumeID, "replica-linux")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(replica.Replica.LastAppliedSeq).NotTo(BeNil())
	Expect(*replica.Replica.LastAppliedSeq).To(Equal(firstSeq))

	bootstrapName := "e2e-sync-bootstrap"
	bootstrapResp, status, bootstrapConflict, err := session.CreateSyncBootstrap(env.TestCtx.Context, GinkgoT(), volumeID, &apispec.CreateVolumeSyncBootstrapRequest{
		SnapshotName: &bootstrapName,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(bootstrapConflict).To(BeNil())
	Expect(bootstrapResp).NotTo(BeNil())
	Expect(bootstrapResp.Snapshot.Id).NotTo(BeEmpty())
	Expect(strings.TrimSpace(bootstrapResp.ArchiveDownloadPath)).NotTo(BeEmpty())

	archiveBody, status, err := session.DownloadSyncBootstrapArchive(env.TestCtx.Context, GinkgoT(), volumeID, bootstrapResp.Snapshot.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(len(archiveBody)).To(BeNumerically(">", 0))

	status, err = session.DeleteSnapshot(env.TestCtx.Context, GinkgoT(), volumeID, bootstrapResp.Snapshot.Id)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	windowsDisplayName := "Windows Laptop"
	windowsPlatform := "windows"
	windowsRootPath := "C:/workspace"
	windowsCaseSensitive := false
	windowsReplica, status, err := session.UpsertSyncReplica(env.TestCtx.Context, GinkgoT(), volumeID, "replica-windows", apispec.UpsertSyncReplicaRequest{
		DisplayName:   &windowsDisplayName,
		Platform:      &windowsPlatform,
		RootPath:      &windowsRootPath,
		CaseSensitive: &windowsCaseSensitive,
		Capabilities: &apispec.VolumeSyncFilesystemCapabilities{
			CaseSensitive:                   false,
			UnicodeNormalizationInsensitive: true,
			WindowsCompatiblePaths:          true,
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(windowsReplica).NotTo(BeNil())

	windowsAppendResp, status, err := session.AppendSyncReplicaChanges(env.TestCtx.Context, GinkgoT(), volumeID, "replica-windows", apispec.AppendReplicaChangesRequest{
		RequestId: "req-e2e-sync-win-1",
		BaseSeq:   firstSeq,
		Changes: []apispec.ChangeRequest{{
			EventType:     apispec.SyncEventType("write"),
			Path:          ptr("volume-sync-e2e/CON.txt"),
			ContentBase64: ptr("d2luZG93cyBjb25mbGljdAo="),
		}},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(windowsAppendResp.Accepted).To(BeEmpty())
	Expect(windowsAppendResp.Conflicts).To(HaveLen(1))
	Expect(windowsAppendResp.Conflicts[0].Reason).NotTo(BeNil())
	Expect(*windowsAppendResp.Conflicts[0].Reason).To(Equal("windows_reserved_name"))

	conflictsResp, status, err := session.ListSyncConflicts(env.TestCtx.Context, GinkgoT(), volumeID, "open", nil)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(conflictsResp).NotTo(BeNil())
	Expect(conflictsResp.Conflicts).NotTo(BeEmpty())
	conflictID := expectStringPtr(conflictsResp.Conflicts[0].Id, "conflict id")

	resolution := "keep_remote"
	note := "resolved by e2e"
	resolvedConflict, status, err := session.ResolveSyncConflict(env.TestCtx.Context, GinkgoT(), volumeID, conflictID, apispec.ResolveVolumeSyncConflictRequest{
		Status:     apispec.ResolveVolumeSyncConflictRequestStatus("resolved"),
		Resolution: &resolution,
		Note:       &note,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resolvedConflict).NotTo(BeNil())
	Expect(resolvedConflict.Status).NotTo(BeNil())
	Expect(*resolvedConflict.Status).To(Equal("resolved"))
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

func expectStringPtr(value any, label string) string {
	switch v := value.(type) {
	case string:
		Expect(strings.TrimSpace(v)).NotTo(BeEmpty(), "%s should not be empty", label)
		return v
	case *string:
		Expect(v).NotTo(BeNil(), "%s should not be nil", label)
		Expect(strings.TrimSpace(*v)).NotTo(BeEmpty(), "%s should not be empty", label)
		return *v
	default:
		Fail(fmt.Sprintf("%s should be a string or *string, got %T", label, value))
		return ""
	}
}

func ptr[T any](value T) *T {
	return &value
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
