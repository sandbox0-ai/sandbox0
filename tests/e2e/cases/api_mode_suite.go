package cases

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/internal/framework"
	mgrv1alpha1 "github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/metering"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/quota"
	"github.com/sandbox0-ai/sandbox0/pkg/runtimecontrol"
	e2eframework "github.com/sandbox0-ai/sandbox0/tests/e2e/internal/framework"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type apiModeSuiteOptions struct {
	name                        string
	describe                    string
	templateNamePrefix          string
	fileContent                 string
	includeSandboxListTests     bool
	includeTemplateStatus       bool
	includePoolReadinessGate    bool
	includeNetworkPolicy        bool
	includeObjectEncryption     bool
	includeWebhookLifecycle     bool
	includeRootFSPauseResume    bool
	includeRuntimeControl       bool
	includeTemplateFromSandbox  bool
	includeMeteringAssertions   bool
	includeUsageQuotaAssertions bool
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
	Spec     e2ePodSpec     `json:"spec"`
	Status   e2ePodStatus   `json:"status"`
}

type e2ePodMetadata struct {
	Name              string            `json:"name"`
	UID               string            `json:"uid"`
	Annotations       map[string]string `json:"annotations"`
	DeletionTimestamp *metav1.Time      `json:"deletionTimestamp,omitempty"`
}

type e2ePodSpec struct {
	NodeName string `json:"nodeName"`
}

type e2ePodStatus struct {
	Phase             string               `json:"phase"`
	Conditions        []e2ePodCondition    `json:"conditions"`
	ContainerStatuses []e2eContainerStatus `json:"containerStatuses"`
}

type e2eContainerStatus struct {
	Name  string            `json:"name"`
	State e2eContainerState `json:"state"`
}

type e2eContainerState struct {
	Terminated *e2eContainerStateTerminated `json:"terminated"`
}

type e2eContainerStateTerminated struct {
	ExitCode int32 `json:"exitCode"`
}

type e2ePodCondition struct {
	Type               string      `json:"type"`
	Status             string      `json:"status"`
	Reason             string      `json:"reason"`
	LastTransitionTime metav1.Time `json:"lastTransitionTime"`
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
			adminPassword     string
			sshFixtureState   *sshFixture
			sshFixtureCleanup func()
		)

		BeforeAll(func() {
			env = shouldRunApiScenario(envProvider, opts.name)

			var err error
			session, cleanup, err = e2eutils.NewAPISession(env, false)
			Expect(err).NotTo(HaveOccurred())
			// Keep the API tunnel alive for spec-level deferred cleanups.
			DeferCleanup(func() {
				if cleanup != nil {
					cleanup()
				}
			})

			password, err := e2eframework.GetSecretValue(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "admin-password", "password")
			Expect(err).NotTo(HaveOccurred())
			adminPassword = password

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

			if opts.includeTemplateFromSandbox {
				It("creates a claimable template from a sandbox rootfs", func() {
					assertTemplateFromSandboxLifecycle(env, session, opts.templateNamePrefix)
				})
			}
		})

		Context("sandbox lifecycle", func() {
			It("enforces active sandbox quota", func() {
				_, status, err := session.PutTeamQuota(env.TestCtx.Context, env, quota.DimensionActiveSandboxes, 1)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				defer func() {
					_, _ = session.DeleteTeamQuota(env.TestCtx.Context, env, quota.DimensionActiveSandboxes)
				}()

				_, status, err = session.ClaimSandboxDetailed(env.TestCtx.Context, GinkgoT(), apispec.ClaimRequest{Template: ptr("default")})
				Expect(err).To(HaveOccurred())
				Expect(status).To(Equal(http.StatusTooManyRequests))
			})

			It("enforces sandbox claim rate quota", func() {
				_, status, err := session.PutTeamRateQuota(
					env.TestCtx.Context,
					env,
					quota.DimensionSandboxClaims,
					0,
					1000,
					0,
				)
				Expect(err).NotTo(HaveOccurred())
				Expect(status).To(Equal(http.StatusOK))
				defer func() {
					_, _ = session.DeleteTeamQuota(env.TestCtx.Context, env, quota.DimensionSandboxClaims)
				}()

				Eventually(func() int {
					_, observedStatus, _ := session.ClaimSandboxDetailed(
						env.TestCtx.Context,
						GinkgoT(),
						apispec.ClaimRequest{Template: ptr("default")},
					)
					return observedStatus
				}).WithTimeout(15 * time.Second).WithPolling(500 * time.Millisecond).
					Should(Equal(http.StatusTooManyRequests))
			})

			if opts.includeUsageQuotaAssertions {
				It("enforces API request quota", func() {
					_, status, err := session.PutTeamRateQuota(
						env.TestCtx.Context,
						env,
						quota.DimensionAPIRequests,
						0,
						1000,
						0,
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					defer func() {
						_, _ = session.DeleteTeamQuota(env.TestCtx.Context, env, quota.DimensionAPIRequests)
					}()

					Eventually(func() int {
						_, observedStatus, _ := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
						return observedStatus
					}).WithTimeout(15 * time.Second).WithPolling(500 * time.Millisecond).
						Should(Equal(http.StatusTooManyRequests))
				})
			}

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

			if opts.includeRootFSPauseResume {
				It("persists rootfs changes across pause and resume", func() {
					assertSandboxRootFSPersistsAcrossPauseResume(env, session)
				})

				It("reconstructs a missing runtime without duplicating generations", Label("runtime-reconciliation"), func() {
					assertSandboxRuntimeReconcilesUnexpectedPodDeletion(env, session)
				})

				It("snapshots restores and forks sandbox rootfs", func() {
					assertSandboxRootFSSnapshotRestoreFork(env, session)
				})

				if opts.includeObjectEncryption {
					It("keeps encrypted rootfs objects and local cache opaque", func() {
						assertObjectEncryptionLifecycle(env, session, sandboxID)
					})
				}
			}

			if opts.includeRuntimeControl {
				It("keeps activation failures unroutable without reconstructing the live runtime", Label("runtime-control"), func() {
					assertRuntimeActivationFailureRecovery(env, session)
				})

				It("reconnects the same runtime after ctld primary failover", Label("runtime-control"), func() {
					assertRuntimeControlCtldFailover(env, session)
				})

				It("reconstructs a terminated procd from durable manifest state", Label("runtime-control"), func() {
					assertTerminatedProcdAutonomousRecovery(env, session)
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

				It("proxies SSH egress auth without exposing upstream private keys to the sandbox", func() {
					assertSSHTransparentEgressAuthProxy(env, session, sandboxID, sshFixtureState)
				})

				It("matches SSH app protocols through traffic rules", func() {
					assertSSHAppProtocolTrafficRules(env, session, sandboxID, sshFixtureState)
				})

				It("routes allowed TCP egress through a sandbox egress proxy", func() {
					assertSandboxEgressProxy(env, session, sandboxID)
				})

				It("enforces transparent TCP egress through the ctld network runtime", func() {
					assertNetdTransparentEgressPolicy(env, session, sandboxID)
				})

				It("resolves cluster DNS over UDP with the ctld network runtime active", func() {
					assertNetdClusterDNSUDP(env, session, sandboxID)
				})

				It("enforces Redis-backed team bandwidth through the ctld network runtime", func() {
					assertNetdRedisTeamBandwidthLimit(env, session, adminPassword)
				})

				if opts.includeUsageQuotaAssertions {
					It("enforces network egress quota", func() {
						assertNetworkEgressQuota(env, session, sandboxID)
					})

					It("enforces network ingress quota", func() {
						assertNetworkIngressQuota(env, session, sandboxID)
					})
				}

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

		if opts.includeWebhookLifecycle {
			Context("sandbox webhooks", func() {
				It("delivers lifecycle events through the durable rootfs outbox", func() {
					assertSandboxWebhookDurabilityLifecycle(env, session)
				})
			})
		}

		if opts.includeMeteringAssertions {
			Context("metering export", func() {
				It("exports sandbox usage facts", func() {
					Expect(sandboxID).NotTo(BeEmpty())

					pausedResp, status, err := session.PauseSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					Expect(pausedResp).NotTo(BeNil())
					Expect(pausedResp.Paused).To(BeFalse())
					Expect(pausedResp.Status).NotTo(BeNil())
					Expect(*pausedResp.Status).To(Equal(apispec.SandboxLifecycleStatusRunning))
					waitForSandboxLifecycleStatusEventually(env, session, sandboxID, apispec.SandboxLifecycleStatusPaused)

					resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
					Expect(err).NotTo(HaveOccurred())
					Expect(status).To(Equal(http.StatusOK))
					Expect(resumeResp).NotTo(BeNil())
					Expect(resumeResp.Resumed).To(BeTrue())
					waitForSandboxLifecycleStatusEventually(env, session, sandboxID, apispec.SandboxLifecycleStatusRunning)
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

						windows, err := session.ListAllMeteringWindows(env.TestCtx.Context, 200)
						if err != nil {
							return err
						}
						if !hasMeteringWindow(windows, metering.WindowTypeSandboxRuntimeMiBMilliseconds, pausedResp.SandboxId) {
							return fmt.Errorf("missing sandbox.runtime_mib_milliseconds window")
						}

						return nil
					}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
				})
			})
		}
	})
}

func getDefaultTemplate(env *framework.ScenarioEnv, session *e2eutils.Session) apispec.Template {
	GinkgoHelper()
	template, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
	Expect(err).NotTo(HaveOccurred())
	Expect(template).NotTo(BeNil())
	return *template
}

func runTemplateLifecycleAssertions(env *framework.ScenarioEnv, session *e2eutils.Session, templateNamePrefix string) {
	base := getDefaultTemplate(env, session)
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
	const rejectedDeletionAttempts = 3
	cleanup := setupWebhookReceiver(env, receiverName, rejectedDeletionAttempts)
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
	sandboxID := claimResp.SandboxId
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	}()
	Eventually(func() error {
		events := readWebhookReceiverEvents(env, receiverName)
		if !strings.Contains(events, `"event_type":"sandbox.ready"`) {
			return fmt.Errorf("missing sandbox.ready event: %s", events)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Expect(session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)).To(Succeed())
	sandboxID = ""

	Eventually(func() error {
		events := readWebhookReceiverEvents(env, receiverName)
		if !strings.Contains(events, `"event_type":"sandbox.deleted"`) {
			return fmt.Errorf("missing sandbox.deleted event: %s", events)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	attempts := readWebhookReceiverFile(env, receiverName, "delete-attempts.jsonl")
	Expect(strings.Count(attempts, `"event_type":"sandbox.deleted"`)).To(BeNumerically(">=", rejectedDeletionAttempts+1))
}

func setupWebhookReceiver(env *framework.ScenarioEnv, name string, rejectedDeletionAttempts int) func() {
	manifest := fmt.Sprintf(`
apiVersion: v1
kind: ConfigMap
metadata:
  name: %[1]s-script
  namespace: %[2]s
data:
  server.py: |
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
    from pathlib import Path

    events = Path("/data/events.jsonl")
    delete_attempts = Path("/data/delete-attempts.jsonl")
    events.parent.mkdir(parents=True, exist_ok=True)

    class Handler(BaseHTTPRequestHandler):
        deleted_requests = 0

        def respond(self, status):
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.flush()
            self.close_connection = True

        def do_POST(self):
            length = int(self.headers.get("content-length", "0"))
            body = self.rfile.read(length)
            if b'"event_type":"sandbox.deleted"' in body:
                Handler.deleted_requests += 1
                with delete_attempts.open("ab") as f:
                    f.write(body + b"\n")
                if Handler.deleted_requests <= %[3]d:
                    self.respond(503)
                    return
            with events.open("ab") as f:
                f.write(body + b"\n")
            self.respond(204)

        def log_message(self, fmt, *args):
            return

    ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
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
`, name, env.Infra.Namespace, rejectedDeletionAttempts)
	Expect(framework.ApplyManifestContent(env.TestCtx.Context, env.Config.Kubeconfig, "sandbox0-e2e-webhook-", manifest)).To(Succeed())
	Expect(framework.WaitForDeployment(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, name, "3m")).To(Succeed())
	return func() {
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "service", name, "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "deployment", name, "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
		_ = framework.Kubectl(env.TestCtx.Context, env.Config.Kubeconfig, "delete", "configmap", name+"-script", "--namespace", env.Infra.Namespace, "--ignore-not-found=true")
	}
}

func readWebhookReceiverEvents(env *framework.ScenarioEnv, name string) string {
	return readWebhookReceiverFile(env, name, "events.jsonl")
}

func readWebhookReceiverFile(env *framework.ScenarioEnv, name, filename string) string {
	podName, err := e2eframework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		"pod",
		"-l=app="+name,
		"{.items[0].metadata.name}",
	)
	Expect(err).NotTo(HaveOccurred())
	output, err := e2eframework.KubectlExecOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		strings.TrimSpace(podName),
		"sh",
		"-c",
		"cat /data/"+filename+" 2>/dev/null || true",
	)
	Expect(err).NotTo(HaveOccurred())
	return output
}

func assertTemplateStatusCountersEventually(env *framework.ScenarioEnv, session *e2eutils.Session) {
	templateID := getDefaultTemplate(env, session).TemplateId

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
	name := fmt.Sprintf("%s-ready-gate-%d", templateNamePrefix, time.Now().UnixNano())
	templateReq := e2eutils.CloneTemplateForCreate(getDefaultTemplate(env, session), name)
	Expect(templateReq.Spec.Pool).NotTo(BeNil())
	Expect(templateReq.Spec.MainContainer).NotTo(BeNil())
	templateReq.Spec.MainContainer.Resources = apispec.ResourceQuota{
		Memory: "2Gi",
	}
	templateReq.Spec.Pool.MinIdle = 1
	templateReq.Spec.Pool.MaxIdle = 1

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
	name := fmt.Sprintf("%s-rc-%d", templateNamePrefix, time.Now().UnixNano()%1_000_000_000)
	templateReq := e2eutils.CloneTemplateForCreate(getDefaultTemplate(env, session), name)
	Expect(templateReq.Spec.Pool).NotTo(BeNil())
	Expect(templateReq.Spec.MainContainer).NotTo(BeNil())
	templateReq.Spec.MainContainer.Resources = apispec.ResourceQuota{
		Memory: "2Gi",
	}
	templateReq.Spec.Pool.MinIdle = 1
	templateReq.Spec.Pool.MaxIdle = 1
	templateReq.Spec.EnvVars = ptr(map[string]string{"E2E_ROLLOUT_MARKER": "before"})

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
	updated.Spec.EnvVars = ptr(map[string]string{"E2E_ROLLOUT_MARKER": "after"})
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
	name := fmt.Sprintf("%s-baseline-%d", templateNamePrefix, time.Now().UnixNano())
	templateReq := e2eutils.CloneTemplateForCreate(getDefaultTemplate(env, session), name)

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

	serverIP, err := e2eframework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, testNamespace, "pod", "baseline-server", "{.status.podIP}")
	Expect(err).NotTo(HaveOccurred())
	serverIP = strings.TrimSpace(serverIP)
	Expect(serverIP).NotTo(BeEmpty())

	Eventually(func() error {
		body, execErr := e2eframework.KubectlExecOutput(
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
		body, execErr := e2eframework.KubectlExecOutput(
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
		_, execErr := e2eframework.KubectlExecOutput(
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
		_, execErr := e2eframework.KubectlExecOutput(
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

func assertSandboxRootFSPersistsAcrossPauseResume(env *framework.ScenarioEnv, session *e2eutils.Session) {
	claimResp := claimSandboxEventually(env, session, "default")
	sandboxID := claimResp.SandboxId
	Expect(sandboxID).NotTo(BeEmpty())
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})

	templateNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	sandbox := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)

	marker := fmt.Sprintf("s0-rootfs-pause-resume-%d", time.Now().UnixNano())
	rootDir := "/root/" + marker
	filePath := rootDir + "/marker.txt"
	nestedPath := rootDir + "/nested/value.txt"
	linkPath := rootDir + "/marker.link"
	tmpPath := "/tmp/" + marker + ".txt"
	varTmpPath := "/var/tmp/" + marker + ".txt"
	content := "rootfs checkpoint " + marker

	writeScript := fmt.Sprintf(`set -eu
rm -rf %s
mkdir -p %s
printf %%s %s > %s
printf %%s nested > %s
printf %%s ephemeral > %s
printf %%s persistent-var-tmp > %s
ln -sf %s %s
chmod 751 %s
test "$(cat %s)" = %s
test "$(cat %s)" = nested
test "$(cat %s)" = %s
test "$(stat -c %%a %s)" = 751
`,
		shellQuote(rootDir),
		shellQuote(filepathDir(nestedPath)),
		shellQuote(content),
		shellQuote(filePath),
		shellQuote(nestedPath),
		shellQuote(tmpPath),
		shellQuote(varTmpPath),
		shellQuote(filePath),
		shellQuote(linkPath),
		shellQuote(rootDir),
		shellQuote(filePath),
		shellQuote(content),
		shellQuote(nestedPath),
		shellQuote(linkPath),
		shellQuote(content),
		shellQuote(rootDir),
	)
	_, err = execInSandboxPod(env, templateNamespace, sandbox.PodName, writeScript)
	Expect(err).NotTo(HaveOccurred())

	ttl := int32(1)
	_, status, err := session.UpdateSandbox(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxUpdateConfig{
		Ttl: &ttl,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	waitForSandboxLifecycleStatusEventually(env, session, sandboxID, apispec.SandboxLifecycleStatusPaused)

	disabledTTL := int32(0)
	_, status, err = session.UpdateSandbox(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxUpdateConfig{
		Ttl: &disabledTTL,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())

	restored := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	verifyScript := fmt.Sprintf(`set -eu
test "$(cat %s)" = %s
test "$(cat %s)" = nested
test "$(cat %s)" = %s
test "$(stat -c %%a %s)" = 751
test ! -e %s
test "$(cat %s)" = persistent-var-tmp
`,
		shellQuote(filePath),
		shellQuote(content),
		shellQuote(nestedPath),
		shellQuote(linkPath),
		shellQuote(content),
		shellQuote(rootDir),
		shellQuote(tmpPath),
		shellQuote(varTmpPath),
	)
	_, err = execInSandboxPod(env, templateNamespace, restored.PodName, verifyScript)
	Expect(err).NotTo(HaveOccurred())
}

func assertSandboxRuntimeReconcilesUnexpectedPodDeletion(env *framework.ScenarioEnv, session *e2eutils.Session) {
	claimResp := claimSandboxEventually(env, session, "default")
	sandboxID := claimResp.SandboxId
	Expect(sandboxID).NotTo(BeEmpty())
	DeferCleanup(func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	})

	templateNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	current := waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	uncommittedPath := fmt.Sprintf("/root/s0-runtime-uncommitted-%d", time.Now().UnixNano())
	_, err = execInSandboxPod(env, templateNamespace, current.PodName, fmt.Sprintf(
		"printf uncommitted > %s",
		shellQuote(uncommittedPath),
	))
	Expect(err).NotTo(HaveOccurred())

	current = forceDeleteSandboxRuntimeAndWaitForReplacement(env, session, sandboxID, templateNamespace, current)
	_, err = execInSandboxPod(env, templateNamespace, current.PodName, fmt.Sprintf(
		"test ! -e %s",
		shellQuote(uncommittedPath),
	))
	Expect(err).NotTo(HaveOccurred(), "a runtime lost before its first checkpoint must restart from the template baseline")

	marker := fmt.Sprintf("s0-runtime-reconcile-%d", time.Now().UnixNano())
	markerPath := "/root/" + marker
	markerContent := "last committed rootfs " + marker
	_, err = execInSandboxPod(env, templateNamespace, current.PodName, fmt.Sprintf(
		"printf %%s %s > %s",
		shellQuote(markerContent),
		shellQuote(markerPath),
	))
	Expect(err).NotTo(HaveOccurred())

	_, status, err := session.PauseSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	waitForSandboxLifecycleStatusEventually(env, session, sandboxID, apispec.SandboxLifecycleStatusPaused)

	resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())
	current = waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)

	for iteration := 0; iteration < 3; iteration++ {
		current = forceDeleteSandboxRuntimeAndWaitForReplacement(env, session, sandboxID, templateNamespace, current)
		_, err = execInSandboxPod(env, templateNamespace, current.PodName, fmt.Sprintf(
			"test \"$(cat %s)\" = %s",
			shellQuote(markerPath),
			shellQuote(markerContent),
		))
		Expect(err).NotTo(HaveOccurred())
	}
}

func forceDeleteSandboxRuntimeAndWaitForReplacement(
	env *framework.ScenarioEnv,
	session *e2eutils.Session,
	sandboxID string,
	templateNamespace string,
	current *apispec.Sandbox,
) *apispec.Sandbox {
	oldPodName := current.PodName
	oldGeneration := current.RuntimeGeneration
	Expect(framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"-n", templateNamespace,
		"delete", "pod", oldPodName,
		"--force", "--grace-period=0", "--wait=false",
	)).To(Succeed())

	Eventually(func() error {
		observed, observedStatus, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
		if err != nil {
			return err
		}
		if observedStatus != http.StatusOK {
			return fmt.Errorf("get sandbox returned status %d", observedStatus)
		}
		if observed == nil {
			return fmt.Errorf("sandbox response missing")
		}
		if observed.Status != apispec.SandboxLifecycleStatusRunning {
			return fmt.Errorf("sandbox status is %s", observed.Status)
		}
		if observed.PodName == "" || observed.PodName == oldPodName {
			return fmt.Errorf("replacement pod is not published")
		}
		if observed.RuntimeGeneration <= oldGeneration {
			return fmt.Errorf("runtime generation did not advance: old=%d current=%d", oldGeneration, observed.RuntimeGeneration)
		}
		current = observed
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())

	current = waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)
	Eventually(func() error {
		podListJSON, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"-n", templateNamespace,
			"get", "pods",
			"-l", "sandbox0.ai/sandbox-id="+sandboxID,
			"-o", "json",
		)
		if err != nil {
			return err
		}
		var podList e2ePodList
		if err := json.Unmarshal([]byte(podListJSON), &podList); err != nil {
			return err
		}
		var activePod *e2ePod
		for _, pod := range podList.Items {
			if pod.Metadata.DeletionTimestamp == nil {
				if activePod != nil {
					return fmt.Errorf("multiple active runtime pods remain")
				}
				pod := pod
				activePod = &pod
			}
		}
		if len(podList.Items) != 1 || activePod == nil {
			return fmt.Errorf("runtime pod set has not converged: total=%d", len(podList.Items))
		}
		if activePod.Metadata.Name != current.PodName {
			return fmt.Errorf("API pod %s does not match Kubernetes pod %s", current.PodName, activePod.Metadata.Name)
		}
		generation, err := strconv.ParseInt(strings.TrimSpace(activePod.Metadata.Annotations[runtimecontrol.AnnotationRuntimeGeneration]), 10, 64)
		if err != nil {
			return fmt.Errorf("parse runtime generation annotation: %w", err)
		}
		if generation != current.RuntimeGeneration {
			return fmt.Errorf("API generation %d does not match Kubernetes generation %d", current.RuntimeGeneration, generation)
		}
		return nil
	}).WithTimeout(3 * time.Minute).WithPolling(2 * time.Second).Should(Succeed())
	return current
}

func assertTemplateFromSandboxLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session, templateNamePrefix string) {
	derivedTemplateID := fmt.Sprintf("%s-from-sandbox-%d", templateNamePrefix, time.Now().UnixNano()%1_000_000_000)
	sourceTemplateID := derivedTemplateID + "-source"
	sourceSandboxID := ""
	derivedSandboxID := ""
	DeferCleanup(func() {
		if derivedSandboxID != "" {
			_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), derivedSandboxID)
		}
		_ = session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), derivedTemplateID)
		if sourceSandboxID != "" {
			_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
		}
		_ = session.DeleteSystemTemplate(env.TestCtx.Context, env, sourceTemplateID)
	})

	base, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
	Expect(err).NotTo(HaveOccurred())
	sourceTemplateRequest := e2eutils.CloneTemplateForCreate(*base, sourceTemplateID)
	Expect(sourceTemplateRequest.Spec.MainContainer).NotTo(BeNil())
	Expect(sourceTemplateRequest.Spec.Pool).NotTo(BeNil())
	// The default image is loaded into kind from a Docker archive for most
	// tests. Force this source pod to resolve the registry manifest so the
	// captured base digest remains remotely fetchable by the image publisher.
	sourceTemplateRequest.Spec.MainContainer.ImagePullPolicy = ptr("Always")
	sourceTemplateRequest.Spec.Pool.MinIdle = 0
	sourceTemplateRequest.Spec.Pool.MaxIdle = 0
	sourceTemplate, err := session.CreateSystemTemplate(env.TestCtx.Context, env, sourceTemplateRequest)
	Expect(err).NotTo(HaveOccurred())
	Expect(sourceTemplate).NotTo(BeNil())

	sourceNamespace, err := naming.TemplateNamespaceForBuiltin(sourceTemplateID)
	Expect(err).NotTo(HaveOccurred())
	sourceClaim := claimSandboxEventually(env, session, sourceTemplateID)
	sourceSandboxID = sourceClaim.SandboxId
	Expect(sourceSandboxID).NotTo(BeEmpty())
	source := waitForSandboxPodReadyEventually(env, session, sourceSandboxID, sourceNamespace)

	marker := fmt.Sprintf("sandbox-template-marker-%d", time.Now().UnixNano())
	markerPath := "/root/" + marker + ".txt"
	markerContent := "template-from-sandbox " + marker
	_, err = execInSandboxPod(env, sourceNamespace, source.PodName, fmt.Sprintf(
		"set -eu; printf %%s %s > %s; test \"$(cat %s)\" = %s",
		shellQuote(markerContent),
		shellQuote(markerPath),
		shellQuote(markerPath),
		shellQuote(markerContent),
	))
	Expect(err).NotTo(HaveOccurred())

	request := e2eutils.TemplateFromSandboxCreateRequest{
		TemplateID: derivedTemplateID,
		SandboxID:  sourceSandboxID,
	}
	idempotencyKey := "e2e-template-from-sandbox-" + derivedTemplateID
	created, status, err := session.CreateTemplateFromSandboxDetailed(
		env.TestCtx.Context,
		GinkgoT(),
		request,
		idempotencyKey,
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusAccepted))
	Expect(created).NotTo(BeNil())
	Expect(created.TemplateID).To(Equal(derivedTemplateID))
	Expect(created.Status).NotTo(BeNil())
	Expect(created.Status.Creation).NotTo(BeNil())

	replayed, replayStatus, err := session.CreateTemplateFromSandboxDetailed(
		env.TestCtx.Context,
		GinkgoT(),
		request,
		idempotencyKey,
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(replayStatus).To(Equal(http.StatusAccepted))
	Expect(replayed).NotTo(BeNil())
	Expect(replayed.TemplateID).To(Equal(derivedTemplateID))

	if created.Status.Creation.State == "creating" {
		_, claimStatus, claimErr := session.ClaimSandboxDetailed(
			env.TestCtx.Context,
			GinkgoT(),
			apispec.ClaimRequest{Template: ptr(derivedTemplateID)},
		)
		Expect(claimErr).To(HaveOccurred())
		Expect(claimStatus).To(Equal(http.StatusConflict))
	}

	var ready *e2eutils.TemplateFromSandboxView
	Eventually(func() error {
		view, getStatus, getErr := session.GetTemplateFromSandboxView(env.TestCtx.Context, GinkgoT(), derivedTemplateID)
		if getErr != nil {
			return getErr
		}
		if getStatus != http.StatusOK || view == nil || view.Status == nil || view.Status.Creation == nil {
			return fmt.Errorf("template creation status is unavailable")
		}
		switch view.Status.Creation.State {
		case "ready":
			ready = view
			return nil
		case "failed":
			return fmt.Errorf(
				"template creation failed: reason=%s message=%s",
				view.Status.Creation.Reason,
				view.Status.Creation.Message,
			)
		default:
			return fmt.Errorf(
				"template creation is %s/%s",
				view.Status.Creation.State,
				view.Status.Creation.Stage,
			)
		}
	}).WithTimeout(5 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())

	Expect(ready).NotTo(BeNil())
	Expect(ready.Status.Creation.OutputImage).To(ContainSubstring("@sha256:"))
	Expect(ready.TeamID).NotTo(BeNil())
	derivedNamespace, err := naming.TemplateNamespaceForTeam(expectStringPtr(ready.TeamID, "team id"))
	Expect(err).NotTo(HaveOccurred())

	derivedClaim := claimSandboxEventually(env, session, derivedTemplateID)
	derivedSandboxID = derivedClaim.SandboxId
	Expect(derivedSandboxID).NotTo(BeEmpty())
	derived := waitForSandboxPodReadyEventually(env, session, derivedSandboxID, derivedNamespace)
	_, err = execInSandboxPod(env, derivedNamespace, derived.PodName, fmt.Sprintf(
		"set -eu; test \"$(cat %s)\" = %s",
		shellQuote(markerPath),
		shellQuote(markerContent),
	))
	Expect(err).NotTo(HaveOccurred())
}

func assertSandboxRootFSSnapshotRestoreFork(env *framework.ScenarioEnv, session *e2eutils.Session) {
	claimResp := claimSandboxEventually(env, session, "default")
	sourceSandboxID := claimResp.SandboxId
	Expect(sourceSandboxID).NotTo(BeEmpty())
	forkSandboxID := ""
	runningForkSandboxID := ""
	runningSnapshotID := ""
	pausedSnapshotID := ""
	DeferCleanup(func() {
		if runningSnapshotID != "" {
			_, _ = session.DeleteSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), runningSnapshotID)
		}
		if pausedSnapshotID != "" {
			_, _ = session.DeleteSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), pausedSnapshotID)
		}
		if forkSandboxID != "" {
			_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), forkSandboxID)
		}
		if runningForkSandboxID != "" {
			_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), runningForkSandboxID)
		}
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	})

	templateNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	source := waitForSandboxPodReadyEventually(env, session, sourceSandboxID, templateNamespace)

	marker := fmt.Sprintf("s0-rootfs-snapshot-%d", time.Now().UnixNano())
	rootDir := "/root/" + marker
	filePath := rootDir + "/marker.txt"
	nestedPath := rootDir + "/nested/value.txt"
	tmpPath := "/tmp/" + marker + ".txt"
	v1Content := "snapshot v1 " + marker
	v2Content := "snapshot v2 " + marker

	writeV1Script := fmt.Sprintf(`set -eu
rm -rf %s
mkdir -p %s
printf %%s %s > %s
printf %%s nested-v1 > %s
printf %%s ephemeral-v1 > %s
test "$(cat %s)" = %s
test "$(cat %s)" = nested-v1
`,
		shellQuote(rootDir),
		shellQuote(filepathDir(nestedPath)),
		shellQuote(v1Content),
		shellQuote(filePath),
		shellQuote(nestedPath),
		shellQuote(tmpPath),
		shellQuote(filePath),
		shellQuote(v1Content),
		shellQuote(nestedPath),
	)
	_, err = execInSandboxPod(env, templateNamespace, source.PodName, writeV1Script)
	Expect(err).NotTo(HaveOccurred())

	runningSnapshotName := "e2e-running-rootfs-" + marker
	runningSnapshot, status, err := session.CreateSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), sourceSandboxID, apispec.CreateSandboxRootFSSnapshotRequest{
		Name: ptr(runningSnapshotName),
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(runningSnapshot).NotTo(BeNil())
	Expect(runningSnapshot.Id).NotTo(BeEmpty())
	runningSnapshotID = runningSnapshot.Id

	sourceAfterRunningSnapshot := waitForSandboxLifecycleStatusEventually(env, session, sourceSandboxID, apispec.SandboxLifecycleStatusRunning)
	Expect(sourceAfterRunningSnapshot.PodName).To(Equal(source.PodName))

	listResp, status, err := session.ListSandboxRootFSSnapshots(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(listResp).NotTo(BeNil())
	Expect(listResp.Count).To(BeNumerically(">=", 1))

	loaded, status, err := session.GetSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), runningSnapshotID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(loaded).NotTo(BeNil())
	Expect(loaded.Id).To(Equal(runningSnapshotID))

	writeV2Script := fmt.Sprintf(`set -eu
printf %%s %s > %s
printf %%s nested-v2 > %s
printf %%s ephemeral-v2 > %s
test "$(cat %s)" = %s
test "$(cat %s)" = nested-v2
`,
		shellQuote(v2Content),
		shellQuote(filePath),
		shellQuote(nestedPath),
		shellQuote(tmpPath),
		shellQuote(filePath),
		shellQuote(v2Content),
		shellQuote(nestedPath),
	)
	_, err = execInSandboxPod(env, templateNamespace, source.PodName, writeV2Script)
	Expect(err).NotTo(HaveOccurred())

	runningForkResp, status, err := session.ForkSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(runningForkResp).NotTo(BeNil())
	Expect(runningForkResp.SourceSandboxId).To(Equal(sourceSandboxID))
	Expect(runningForkResp.Sandbox.Id).NotTo(BeEmpty())
	Expect(runningForkResp.Sandbox.Id).NotTo(Equal(sourceSandboxID))
	Expect(runningForkResp.Sandbox.Status).To(Equal(apispec.SandboxLifecycleStatusPaused))
	runningForkSandboxID = runningForkResp.Sandbox.Id

	sourceAfterRunningFork := waitForSandboxLifecycleStatusEventually(env, session, sourceSandboxID, apispec.SandboxLifecycleStatusRunning)
	Expect(sourceAfterRunningFork.PodName).To(Equal(source.PodName))

	resumeResp, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), runningForkSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())
	runningForked := waitForSandboxPodReadyEventually(env, session, runningForkSandboxID, templateNamespace)
	verifyV2Script := fmt.Sprintf(`set -eu
test "$(cat %s)" = %s
test "$(cat %s)" = nested-v2
test ! -e %s
`,
		shellQuote(filePath),
		shellQuote(v2Content),
		shellQuote(nestedPath),
		shellQuote(tmpPath),
	)
	_, err = execInSandboxPod(env, templateNamespace, runningForked.PodName, verifyV2Script)
	Expect(err).NotTo(HaveOccurred())

	pausedResp, status, err := session.PauseSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(pausedResp).NotTo(BeNil())
	waitForSandboxLifecycleStatusEventually(env, session, sourceSandboxID, apispec.SandboxLifecycleStatusPaused)

	pausedSnapshotName := "e2e-paused-rootfs-" + marker
	pausedSnapshot, status, err := session.CreateSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), sourceSandboxID, apispec.CreateSandboxRootFSSnapshotRequest{
		Name: ptr(pausedSnapshotName),
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(pausedSnapshot).NotTo(BeNil())
	Expect(pausedSnapshot.Id).NotTo(BeEmpty())
	pausedSnapshotID = pausedSnapshot.Id

	restoreResp, status, err := session.RestoreSandboxRootFS(env.TestCtx.Context, GinkgoT(), sourceSandboxID, apispec.RestoreSandboxRootFSRequest{
		SnapshotId: runningSnapshotID,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(restoreResp).NotTo(BeNil())
	Expect(restoreResp.Status).To(Equal(apispec.SandboxLifecycleStatusPaused))

	resumeResp, status, err = session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())
	source = waitForSandboxPodReadyEventually(env, session, sourceSandboxID, templateNamespace)

	verifyV1Script := fmt.Sprintf(`set -eu
test "$(cat %s)" = %s
test "$(cat %s)" = nested-v1
test ! -e %s
`,
		shellQuote(filePath),
		shellQuote(v1Content),
		shellQuote(nestedPath),
		shellQuote(tmpPath),
	)
	_, err = execInSandboxPod(env, templateNamespace, source.PodName, verifyV1Script)
	Expect(err).NotTo(HaveOccurred())

	pausedResp, status, err = session.PauseSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(pausedResp).NotTo(BeNil())
	waitForSandboxLifecycleStatusEventually(env, session, sourceSandboxID, apispec.SandboxLifecycleStatusPaused)

	restoreResp, status, err = session.RestoreSandboxRootFS(env.TestCtx.Context, GinkgoT(), sourceSandboxID, apispec.RestoreSandboxRootFSRequest{
		SnapshotId: pausedSnapshotID,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(restoreResp).NotTo(BeNil())
	Expect(restoreResp.Status).To(Equal(apispec.SandboxLifecycleStatusPaused))

	forkResp, status, err := session.ForkSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(forkResp).NotTo(BeNil())
	Expect(forkResp.SourceSandboxId).To(Equal(sourceSandboxID))
	Expect(forkResp.Sandbox.Id).NotTo(BeEmpty())
	Expect(forkResp.Sandbox.Id).NotTo(Equal(sourceSandboxID))
	Expect(forkResp.Sandbox.Status).To(Equal(apispec.SandboxLifecycleStatusPaused))
	forkSandboxID = forkResp.Sandbox.Id

	status, err = session.DeleteSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), runningSnapshotID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	runningSnapshotID = ""

	status, err = session.DeleteSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), pausedSnapshotID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	pausedSnapshotID = ""

	resumeResp, status, err = session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sourceSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())
	source = waitForSandboxPodReadyEventually(env, session, sourceSandboxID, templateNamespace)

	_, err = execInSandboxPod(env, templateNamespace, source.PodName, verifyV2Script)
	Expect(err).NotTo(HaveOccurred())

	resumeResp, status, err = session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), forkSandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumeResp).NotTo(BeNil())
	Expect(resumeResp.Resumed).To(BeTrue())
	forked := waitForSandboxPodReadyEventually(env, session, forkSandboxID, templateNamespace)
	_, err = execInSandboxPod(env, templateNamespace, forked.PodName, verifyV2Script)
	Expect(err).NotTo(HaveOccurred())
}

func waitForSandboxLifecycleStatusEventually(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string, want apispec.SandboxLifecycleStatus) *apispec.Sandbox {
	var sandbox *apispec.Sandbox
	Eventually(func() error {
		current, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("get sandbox status %d", status)
		}
		if current == nil {
			return fmt.Errorf("sandbox response missing")
		}
		if current.Status != want {
			return fmt.Errorf("sandbox status = %s, want %s", current.Status, want)
		}
		sandbox = current
		return nil
	}).WithTimeout(8 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	return sandbox
}

func filepathDir(path string) string {
	idx := strings.LastIndex(path, "/")
	if idx <= 0 {
		return "."
	}
	return path[:idx]
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

	nodeA, err := e2eframework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, templateANamespace, "pod", sandboxA.PodName, "{.spec.nodeName}")
	Expect(err).NotTo(HaveOccurred())
	nodeB, err := e2eframework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, templateBNamespace, "pod", sandboxB.PodName, "{.spec.nodeName}")
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

	podIPB, err := e2eframework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, templateBNamespace, "pod", sandboxB.PodName, "{.status.podIP}")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(podIPB)).NotTo(BeEmpty())

	Eventually(func() error {
		_, execErr := execInSandboxPod(env, templateANamespace, sandboxA.PodName, fmt.Sprintf("curl -fsS --max-time 5 http://%s:%d/", strings.TrimSpace(podIPB), exposedPort))
		if execErr == nil {
			return fmt.Errorf("expected private sandbox-to-sandbox request to fail")
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	clusterGatewayPort, err := e2eframework.GetServicePort(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, env.Infra.Name+"-cluster-gateway")
	Expect(err).NotTo(HaveOccurred())
	clusterGatewayIP, err := e2eframework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, "service", env.Infra.Name+"-cluster-gateway", "{.spec.clusterIP}")
	Expect(err).NotTo(HaveOccurred())
	clusterGatewayIP = strings.TrimSpace(clusterGatewayIP)
	Expect(clusterGatewayIP).NotTo(BeEmpty())
	Expect(strings.EqualFold(clusterGatewayIP, "None")).To(BeFalse())
	clusterGatewayBaseURL := "http://" + net.JoinHostPort(clusterGatewayIP, strconv.Itoa(clusterGatewayPort))

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

	routes := []apispec.SandboxAppServiceRoute{{
		Id:         "web",
		PathPrefix: ptr("/"),
		Resume:     false,
	}}
	_, exposureDomain, status, err := session.UpdateSandboxServices(env.TestCtx.Context, GinkgoT(), sandboxBID, []apispec.SandboxAppService{{
		Id:   "web",
		Port: ptr(exposedPort),
		Ingress: apispec.SandboxAppServiceIngress{
			Public: true,
			Routes: &routes,
		},
	}})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	publicHost := publicExposureHostForRoute(sandboxBID, exposedPort, exposureDomain)
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

func assertNetworkEgressQuota(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	assertNetworkQuota(env, session, sandboxID, quota.DimensionNetworkEgress)
}

func assertNetworkIngressQuota(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	assertNetworkQuota(env, session, sandboxID, quota.DimensionNetworkIngress)
}

func assertNetworkQuota(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string, dimension quota.Dimension) {
	sandbox, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(sandbox).NotTo(BeNil())
	templateNamespace, err := naming.TemplateNamespaceForBuiltin(sandbox.TemplateId)
	Expect(err).NotTo(HaveOccurred())
	waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)

	_, status, err = session.PutTeamRateQuota(env.TestCtx.Context, env, dimension, 0, 1000, 0)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	DeferCleanup(func() {
		_, _ = session.DeleteTeamQuota(env.TestCtx.Context, env, dimension)
	})

	Eventually(func() bool {
		_, execErr := execInSandboxPod(env, templateNamespace, sandbox.PodName, "curl -fsS --max-time 5 http://example.com/")
		return execErr != nil
	}).WithTimeout(15 * time.Second).WithPolling(500 * time.Millisecond).Should(BeTrue())
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
				Rollout:       ptrTo(apispec.EgressAuthRolloutModeEnabled),
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

func publicExposureHostForRoute(sandboxID string, port int32, exposureDomain string) string {
	sandboxID = strings.TrimSpace(sandboxID)
	exposureDomain = strings.Trim(strings.TrimSpace(exposureDomain), ".")
	if sandboxID == "" || port <= 0 || exposureDomain == "" {
		return ""
	}
	return fmt.Sprintf("%s--p%d.%s", sandboxID, port, exposureDomain)
}

func assertObjectEncryptionLifecycle(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	secretName := env.Infra.Name + "-object-encryption-key"
	privateKey := getSecretValueWithEscapedKey(env, secretName, "private\\.key")
	Expect(privateKey).To(ContainSubstring("BEGIN"))

	assertWorkloadConfigMapContains(env, "deployment", env.Infra.Name+"-manager", "config", "object_encryption_enabled: true")
	for _, slot := range []string{"a", "b"} {
		assertWorkloadConfigMapContains(env, "daemonset", env.Infra.Name+"-ctld-"+slot, "config", "object_encryption_enabled: true")
	}

	sentinel := fmt.Sprintf("ROOTFS_OBJECT_ENCRYPTION_E2E_%d", time.Now().UnixNano())
	beforeContent := []byte(sentinel + "_before\n" + strings.Repeat("a", 128*1024))
	afterContent := []byte(sentinel + "_after\n" + strings.Repeat("b", 96*1024))
	filePath := fmt.Sprintf("tmp/object-encryption-%d/sentinel.txt", time.Now().UnixNano())

	status, err := session.WriteFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath, beforeContent, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	snapshot, status, err := session.CreateSandboxRootFSSnapshot(
		env.TestCtx.Context,
		GinkgoT(),
		sandboxID,
		apispec.CreateSandboxRootFSSnapshotRequest{Name: ptr("object-encryption-e2e")},
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(snapshot).NotTo(BeNil())
	Expect(snapshot.Id).NotTo(BeEmpty())
	DeferCleanup(func() {
		_, _ = session.DeleteSandboxRootFSSnapshot(env.TestCtx.Context, GinkgoT(), snapshot.Id)
	})

	status, err = session.WriteFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath, afterContent, "")
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	paused, status, err := session.PauseSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(paused).NotTo(BeNil())
	waitForSandboxLifecycleStatusEventually(env, session, sandboxID, apispec.SandboxLifecycleStatusPaused)

	_, status, err = session.RestoreSandboxRootFS(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.RestoreSandboxRootFSRequest{
		SnapshotId: snapshot.Id,
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))

	resumed, status, err := session.ResumeSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(resumed).NotTo(BeNil())
	waitForSandboxLifecycleStatusEventually(env, session, sandboxID, apispec.SandboxLifecycleStatusRunning)

	body, status, err := session.ReadFile(env.TestCtx.Context, GinkgoT(), sandboxID, filePath)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(body).To(Equal(beforeContent))

	for _, slot := range []string{"a", "b"} {
		assertNoPlaintextInStorage(env, "daemonset/"+env.Infra.Name+"-ctld-"+slot, "/var/lib/sandbox0/ctld/rootfs", sentinel)
	}
	assertNoPlaintextInStorage(env, "pod/"+env.Infra.Name+"-rustfs-0", "/data", sentinel)
}

func getSecretValueWithEscapedKey(env *framework.ScenarioEnv, secretName, escapedKey string) string {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get",
		"secret",
		secretName,
		"-o",
		fmt.Sprintf("jsonpath={.data.%s}", escapedKey),
		"--namespace",
		env.Infra.Namespace,
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(output)).NotTo(BeEmpty())
	decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(output))
	Expect(err).NotTo(HaveOccurred())
	return string(decoded)
}

func assertConfigMapContains(env *framework.ScenarioEnv, configMapName, expected string) {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get",
		"configmap",
		configMapName,
		"-o",
		"yaml",
		"--namespace",
		env.Infra.Namespace,
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(output).To(ContainSubstring(expected))
}

func assertWorkloadConfigMapContains(env *framework.ScenarioEnv, workloadKind, workloadName, volumeName, expected string) {
	configMapName := workloadConfigMapName(env, workloadKind, workloadName, volumeName)
	assertConfigMapContains(env, configMapName, expected)
}

func workloadConfigMapName(env *framework.ScenarioEnv, workloadKind, workloadName, volumeName string) string {
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get",
		workloadKind,
		workloadName,
		"-o",
		fmt.Sprintf(`jsonpath={.spec.template.spec.volumes[?(@.name=="%s")].configMap.name}`, volumeName),
		"--namespace",
		env.Infra.Namespace,
	)
	Expect(err).NotTo(HaveOccurred())
	name := strings.TrimSpace(output)
	Expect(name).NotTo(BeEmpty())
	return name
}

func assertNoPlaintextInStorage(env *framework.ScenarioEnv, target, root, sentinel string) {
	command := fmt.Sprintf("grep -R -a -n -- %s %s || true", shellQuote(sentinel), shellQuote(root))
	output, err := e2eframework.KubectlExecOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		target,
		"sh",
		"-lc",
		command,
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(output)).To(BeEmpty())
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\\''") + "'"
}

func hasMeteringEvent(events []*metering.Event, eventType, subjectType, subjectID string) bool {
	for _, event := range events {
		if event == nil {
			continue
		}
		if event.EventType == eventType && event.SubjectType == subjectType && event.SubjectID == subjectID && event.Sequence > 0 {
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
		if window.WindowType == windowType && window.SandboxID == sandboxID && window.Sequence > 0 {
			return true
		}
	}
	return false
}
