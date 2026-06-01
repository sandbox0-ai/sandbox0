package cases

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	netdHTTPFixtureImageEnvVar     = "E2E_NETD_HTTP_FIXTURE_IMAGE"
	defaultNetdHTTPFixtureImageRef = "sandbox0ai/otemplates:default-v0.2.0"
	netdHTTPFixturePort            = 8080
)

type netdHTTPFixture struct {
	Namespace string
	AllowIP   string
	DenyIP    string
}

func assertNetdTransparentEgressPolicy(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	Expect(sandboxID).NotTo(BeEmpty())

	sandbox, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(sandbox).NotTo(BeNil())

	templateNamespace, err := naming.TemplateNamespaceForBuiltin(sandbox.TemplateId)
	Expect(err).NotTo(HaveOccurred())
	sandbox = waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)

	fixture := setupNetdHTTPFixture(env, templateNamespace, sandbox.PodName)
	clearPolicy := apispec.SandboxNetworkPolicy{Mode: apispec.AllowAll}
	defer func() {
		_, _, _, _ = session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, clearPolicy)
	}()

	rules := []apispec.TrafficRule{
		buildNetdHTTPRule("allow-http-fixture", apispec.Allow, fixture.AllowIP),
	}
	policy, status, apiErr, err := session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxNetworkPolicy{
		Mode: apispec.BlockAll,
		Egress: &apispec.NetworkEgressPolicy{
			TrafficRules: &rules,
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
	Expect(policy).NotTo(BeNil())
	waitForSandboxNetworkPolicyApplied(env, templateNamespace, sandbox.PodName)

	assertNetdHTTPFixtureEventuallySucceeds(env, templateNamespace, sandbox.PodName, fixture.AllowIP, "allow")
	assertNetdHTTPFixtureEventuallyFails(env, templateNamespace, sandbox.PodName, fixture.DenyIP)
}

func setupNetdHTTPFixture(env *framework.ScenarioEnv, sandboxNamespace, sandboxPodName string) *netdHTTPFixture {
	imageRef := strings.TrimSpace(os.Getenv(netdHTTPFixtureImageEnvVar))
	if imageRef == "" {
		imageRef = defaultNetdHTTPFixtureImageRef
	}

	namespace := fmt.Sprintf("sandbox0-e2e-netd-%d", time.Now().UnixNano())
	nodeName := selectNetdHTTPFixtureNode(env, sandboxNamespace, sandboxPodName)

	Expect(framework.ApplyManifestContent(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"sandbox0-e2e-netd-",
		buildNetdHTTPFixtureManifest(namespace, imageRef, nodeName),
	)).To(Succeed())
	DeferCleanup(func() {
		_ = framework.Kubectl(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"delete",
			"namespace",
			namespace,
			"--ignore-not-found=true",
			"--wait=false",
		)
	})

	Expect(framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, namespace, "pod", "allow-http", "Ready", "2m")).To(Succeed())
	Expect(framework.KubectlWaitForCondition(env.TestCtx.Context, env.Config.Kubeconfig, namespace, "pod", "deny-http", "Ready", "2m")).To(Succeed())

	allowIP, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, namespace, "pod", "allow-http", "{.status.podIP}")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(allowIP)).NotTo(BeEmpty())

	denyIP, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, namespace, "pod", "deny-http", "{.status.podIP}")
	Expect(err).NotTo(HaveOccurred())
	Expect(strings.TrimSpace(denyIP)).NotTo(BeEmpty())

	return &netdHTTPFixture{
		Namespace: namespace,
		AllowIP:   strings.TrimSpace(allowIP),
		DenyIP:    strings.TrimSpace(denyIP),
	}
}

func selectNetdHTTPFixtureNode(env *framework.ScenarioEnv, sandboxNamespace, sandboxPodName string) string {
	sandboxNode, err := framework.KubectlGetJSONPath(env.TestCtx.Context, env.Config.Kubeconfig, sandboxNamespace, "pod", sandboxPodName, "{.spec.nodeName}")
	Expect(err).NotTo(HaveOccurred())
	sandboxNode = strings.TrimSpace(sandboxNode)

	workerNodes, err := listWorkerNodes(env)
	Expect(err).NotTo(HaveOccurred())
	for _, node := range workerNodes {
		if node != sandboxNode {
			return node
		}
	}
	if len(workerNodes) > 0 {
		return workerNodes[0]
	}
	return ""
}

func buildNetdHTTPFixtureManifest(namespace, imageRef, nodeName string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %s
---
%s
---
%s
`, namespace, buildNetdHTTPFixturePodManifest(namespace, imageRef, "allow-http", "allow", nodeName), buildNetdHTTPFixturePodManifest(namespace, imageRef, "deny-http", "deny", nodeName))
}

func buildNetdHTTPFixturePodManifest(namespace, imageRef, name, body, nodeName string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[3]s
  namespace: %[1]s
  labels:
    app.kubernetes.io/name: sandbox0-e2e-netd-http
spec:
%[5]s  containers:
    - name: http
      image: %[2]q
      imagePullPolicy: IfNotPresent
      command:
        - /bin/sh
        - -lc
        - |
          set -eu
          dir=/tmp/sandbox0-e2e-netd-http
          rm -rf "$dir"
          mkdir -p "$dir"
          printf '%[4]s\n' > "$dir/index.html"
          exec python3 -m http.server %[6]d --bind 0.0.0.0 -d "$dir"
      ports:
        - name: http
          containerPort: %[6]d
          protocol: TCP
      readinessProbe:
        httpGet:
          path: /
          port: http
        initialDelaySeconds: 1
        periodSeconds: 2
`, namespace, imageRef, name, body, netdHTTPFixtureNodeNameYAML(nodeName), netdHTTPFixturePort)
}

func netdHTTPFixtureNodeNameYAML(nodeName string) string {
	nodeName = strings.TrimSpace(nodeName)
	if nodeName == "" {
		return ""
	}
	return fmt.Sprintf("  nodeName: %q\n", nodeName)
}

func buildNetdHTTPRule(name string, action apispec.TrafficRuleAction, ip string) apispec.TrafficRule {
	cidrs := []string{ip + "/32"}
	ports := []apispec.PortSpec{{
		Port:     netdHTTPFixturePort,
		Protocol: ptrTo("tcp"),
	}}
	return apispec.TrafficRule{
		Name:   ptrTo(name),
		Action: action,
		Cidrs:  &cidrs,
		Ports:  &ports,
	}
}

func waitForSandboxNetworkPolicyApplied(env *framework.ScenarioEnv, namespace, podName string) {
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
		hash := strings.TrimSpace(pod.Metadata.Annotations[controller.AnnotationNetworkPolicyHash])
		appliedHash := strings.TrimSpace(pod.Metadata.Annotations[controller.AnnotationNetworkPolicyAppliedHash])
		if hash == "" {
			return fmt.Errorf("pod %s network policy hash is not set", podName)
		}
		if appliedHash != hash {
			return fmt.Errorf("pod %s network policy hash is not applied yet: hash=%s applied=%s", podName, hash, appliedHash)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
}

func assertNetdHTTPFixtureEventuallySucceeds(env *framework.ScenarioEnv, namespace, podName, ip, wantBody string) {
	Eventually(func() error {
		body, err := execInSandboxPod(env, namespace, podName, netdHTTPFixtureCurlCommand(ip))
		if err != nil {
			return err
		}
		if strings.TrimSpace(body) != wantBody {
			return fmt.Errorf("unexpected response body from %s: %q", ip, body)
		}
		return nil
	}).WithTimeout(60 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func assertNetdHTTPFixtureEventuallyFails(env *framework.ScenarioEnv, namespace, podName, ip string) {
	Eventually(func() error {
		if body, err := execInSandboxPod(env, namespace, podName, netdHTTPFixtureCurlCommand(ip)); err == nil {
			return fmt.Errorf("request to denied fixture %s succeeded with body %q", ip, body)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func netdHTTPFixtureCurlCommand(ip string) string {
	return fmt.Sprintf("curl -4 -fsS --connect-timeout 2 --max-time 5 http://%s:%d/", ip, netdHTTPFixturePort)
}
