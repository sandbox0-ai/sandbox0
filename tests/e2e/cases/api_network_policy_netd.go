package cases

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	apiconfig "github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/rediscache"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	netdHTTPFixtureImageEnvVar     = "E2E_NETD_HTTP_FIXTURE_IMAGE"
	defaultNetdHTTPFixtureImageRef = "sandbox0ai/otemplates:default-v0.2.0"
	netdHTTPFixturePort            = 8080
	netdHTTPFixtureLargeBytes      = 256 * 1024
	netdRedisBandwidthMinElapsed   = 3 * time.Second
	defaultNetdBandwidthKeyPrefix  = "sandbox0:netd:bandwidth"
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

func assertNetdRedisTeamBandwidthLimit(env *framework.ScenarioEnv, session *e2eutils.Session, adminPassword string) {
	if !netdRedisTeamBandwidthConfigured(env) {
		Skip("netd Redis team bandwidth limit is not configured for this scenario")
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	team, status, err := session.CreateTeam(
		env.TestCtx.Context,
		GinkgoT(),
		"E2E bandwidth "+suffix,
		"e2e-bandwidth-"+suffix,
		nil,
	)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusCreated))
	Expect(team).NotTo(BeNil())

	originalTeamID := session.SelectedTeamID()
	var sandboxIDs []string
	DeferCleanup(func() {
		defer session.SelectTeam(originalTeamID)
		session.SelectTeam(team.Id)
		var cleanupErrs []error
		for _, id := range sandboxIDs {
			if err := session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), id); err != nil {
				cleanupErrs = append(cleanupErrs, err)
			}
		}
		if err := clearNetdRedisTeamBandwidthKeys(env, team.Id); err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
		if _, err := session.DeleteTeam(env.TestCtx.Context, GinkgoT(), team.Id); err != nil {
			cleanupErrs = append(cleanupErrs, err)
		}
		Expect(errors.Join(cleanupErrs...)).NotTo(HaveOccurred())
	})

	Expect(session.Login(env.TestCtx.Context, GinkgoT(), "admin@example.com", adminPassword)).To(Succeed())
	session.SelectTeam(team.Id)

	first := claimSandboxEventually(env, session, "default")
	sandboxIDs = append(sandboxIDs, first.SandboxId)
	second := claimSandboxEventually(env, session, "default")
	sandboxIDs = append(sandboxIDs, second.SandboxId)

	sandbox, status, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), first.SandboxId)
	Expect(err).NotTo(HaveOccurred())
	Expect(status).To(Equal(http.StatusOK))
	Expect(sandbox).NotTo(BeNil())
	templateNamespace, err := naming.TemplateNamespaceForBuiltin(sandbox.TemplateId)
	Expect(err).NotTo(HaveOccurred())
	sandbox = waitForSandboxPodReadyEventually(env, session, first.SandboxId, templateNamespace)

	secondSandbox := waitForSandboxPodReadyEventually(env, session, second.SandboxId, templateNamespace)

	fixture := setupNetdHTTPFixture(env, templateNamespace, sandbox.PodName)
	clearPolicy := apispec.SandboxNetworkPolicy{Mode: apispec.AllowAll}
	DeferCleanup(func() {
		_, _, _, _ = session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), first.SandboxId, clearPolicy)
		_, _, _, _ = session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), second.SandboxId, clearPolicy)
	})

	applyNetdFixtureAllowPolicy(env, session, first.SandboxId, templateNamespace, sandbox.PodName, fixture.AllowIP)
	applyNetdFixtureAllowPolicy(env, session, second.SandboxId, templateNamespace, secondSandbox.PodName, fixture.AllowIP)

	started := time.Now()
	errCh := make(chan error, 2)
	for _, podName := range []string{sandbox.PodName, secondSandbox.PodName} {
		podName := podName
		go func() {
			_, err := execInSandboxPod(env, templateNamespace, podName, netdHTTPFixtureLargeCurlCommand(fixture.AllowIP))
			errCh <- err
		}()
	}
	for i := 0; i < 2; i++ {
		Expect(<-errCh).NotTo(HaveOccurred())
	}
	elapsed := time.Since(started)
	Expect(elapsed).To(BeNumerically(">=", netdRedisBandwidthMinElapsed), "expected two same-team downloads to share the cluster-scoped bandwidth bucket")
}

func netdRedisTeamBandwidthConfigured(env *framework.ScenarioEnv) bool {
	cfg, ok := netdRedisTeamBandwidthConfig(env)
	return ok && strings.TrimSpace(cfg.RedisURL) != "" &&
		(cfg.TeamEgressBandwidthBytesPerSecond > 0 || cfg.TeamIngressBandwidthBytesPerSecond > 0)
}

func netdRedisTeamBandwidthConfig(env *framework.ScenarioEnv) (*apiconfig.NetdConfig, bool) {
	if env == nil || env.Infra.Name == "" || env.Infra.Namespace == "" {
		return nil, false
	}
	output, err := framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get", "daemonset", env.Infra.Name+"-netd",
		"--namespace", env.Infra.Namespace,
		"-o", "json",
	)
	if err != nil {
		return nil, false
	}
	var ds appsv1.DaemonSet
	if err := json.Unmarshal([]byte(output), &ds); err != nil {
		return nil, false
	}
	configMapName := ""
	for _, volume := range ds.Spec.Template.Spec.Volumes {
		if volume.Name == "config" && volume.ConfigMap != nil {
			configMapName = volume.ConfigMap.Name
			break
		}
	}
	if configMapName == "" {
		return nil, false
	}
	output, err = framework.KubectlOutput(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"get", "configmap", configMapName,
		"--namespace", env.Infra.Namespace,
		"-o", "json",
	)
	if err != nil {
		return nil, false
	}
	var cm corev1.ConfigMap
	if err := json.Unmarshal([]byte(output), &cm); err != nil {
		return nil, false
	}
	var cfg apiconfig.NetdConfig
	if err := yaml.Unmarshal([]byte(cm.Data["config.yaml"]), &cfg); err != nil {
		return nil, false
	}
	return &cfg, true
}

func clearNetdRedisTeamBandwidthKeys(env *framework.ScenarioEnv, teamID string) error {
	cfg, ok := netdRedisTeamBandwidthConfig(env)
	if !ok || strings.TrimSpace(cfg.RedisURL) == "" || strings.TrimSpace(teamID) == "" {
		return nil
	}

	serviceName := env.Infra.Name + "-redis"
	port, err := framework.GetServicePort(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName)
	if err != nil {
		return err
	}

	localURL, cleanup, err := framework.PortForwardService(env.TestCtx.Context, env.Config.Kubeconfig, env.Infra.Namespace, serviceName, port)
	if err != nil {
		return err
	}
	defer cleanup()

	forwarded, err := url.Parse(localURL)
	if err != nil {
		return err
	}
	if forwarded.Host == "" {
		return fmt.Errorf("redis port-forward did not return a host")
	}

	options, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return err
	}
	options.Addr = forwarded.Host

	client := redis.NewClient(options)
	defer client.Close()

	ctx, cancel := context.WithTimeout(env.TestCtx.Context, 10*time.Second)
	defer cancel()
	keys := netdRedisTeamBandwidthKeys(cfg, teamID)
	if err := client.Del(ctx, keys...).Err(); err != nil {
		return err
	}
	return nil
}

func netdRedisTeamBandwidthKeys(cfg *apiconfig.NetdConfig, teamID string) []string {
	return []string{
		netdRedisTeamBandwidthKey(cfg, teamID, "egress"),
		netdRedisTeamBandwidthKey(cfg, teamID, "ingress"),
	}
}

func netdRedisTeamBandwidthKey(cfg *apiconfig.NetdConfig, teamID, direction string) string {
	keyPrefix := defaultNetdBandwidthKeyPrefix
	if cfg != nil {
		basePrefix := strings.TrimSpace(cfg.RedisKeyPrefix)
		if basePrefix == "" {
			basePrefix = rediscache.DefaultKeyPrefix
		}
		keyPrefix = rediscache.JoinKeyPrefix(basePrefix, "netd", "bandwidth")
		if keyPrefix == "" {
			keyPrefix = defaultNetdBandwidthKeyPrefix
		}
	}
	raw := rediscache.JoinKeyPrefix(
		"region", netdRedisValueOrUnknown(cfgRegionID(cfg)),
		"cluster", netdRedisValueOrUnknown(cfgClusterID(cfg)),
		"team", teamID,
		"direction", direction,
	)
	return rediscache.HashedKey(keyPrefix, raw)
}

func cfgRegionID(cfg *apiconfig.NetdConfig) string {
	if cfg == nil {
		return ""
	}
	return cfg.RegionID
}

func cfgClusterID(cfg *apiconfig.NetdConfig) string {
	if cfg == nil {
		return ""
	}
	return cfg.ClusterID
}

func netdRedisValueOrUnknown(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "unknown"
	}
	return value
}

func applyNetdFixtureAllowPolicy(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID, namespace, podName, allowIP string) {
	rules := []apispec.TrafficRule{
		buildNetdHTTPRule("allow-http-fixture", apispec.Allow, allowIP),
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
	waitForSandboxNetworkPolicyApplied(env, namespace, podName)
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
          python3 -c 'from pathlib import Path; Path("/tmp/sandbox0-e2e-netd-http/large.bin").write_bytes(b"x" * %[7]d)'
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
`, namespace, imageRef, name, body, netdHTTPFixtureNodeNameYAML(nodeName), netdHTTPFixturePort, netdHTTPFixtureLargeBytes)
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

func netdHTTPFixtureLargeCurlCommand(ip string) string {
	return fmt.Sprintf("curl -4 -fsS --connect-timeout 2 --max-time 30 -o /dev/null http://%s:%d/large.bin", ip, netdHTTPFixturePort)
}
