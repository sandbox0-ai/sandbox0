package cases

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/pkg/apispec"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	sshFixtureNamespace       = "sandbox0-e2e-ssh-fixture"
	sshFixtureDeploymentName  = "ssh-server"
	sshFixtureServiceName     = "ssh-server"
	sshFixtureUserName        = "e2e"
	sshFixtureImageEnvVar     = "E2E_SSH_FIXTURE_IMAGE"
	defaultSSHFixtureImageRef = "lscr.io/linuxserver/openssh-server@sha256:68b605929e83b2efe000da09269688f6d82a44579e8a18e2d9e8c8d272917cf7"
)

const sshFixturePrivateKey = `-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACCeHS2S2TgP2rGj/uUDurfNY823uRuI6yNhfo24y+k2PAAAALC/eI5Ov3iO
TgAAAAtzc2gtZWQyNTUxOQAAACCeHS2S2TgP2rGj/uUDurfNY823uRuI6yNhfo24y+k2PA
AAAEAMJIibgNCaX3VtNbI9iCpYHFRLs4FaeGbeP+BVxhEhQ54dLZLZOA/asaP+5QO6t81j
zbe5G4jrI2F+jbjL6TY8AAAALGh1YW5nemhpaGFvQGh1YW5nemhpaGFvZGVNYWNCb29rLV
Byby0yLmxvY2FsAQ==
-----END OPENSSH PRIVATE KEY-----`

const sshFixturePublicKey = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIJ4dLZLZOA/asaP+5QO6t81jzbe5G4jrI2F+jbjL6TY8 sandbox0-e2e"

type sshFixture struct {
	Namespace string
	ServiceIP string
	UserName  string
	ImageRef  string
}

func setupSSHFixture(env *framework.ScenarioEnv) (*sshFixture, func()) {
	imageRef := strings.TrimSpace(os.Getenv(sshFixtureImageEnvVar))
	if imageRef == "" {
		imageRef = defaultSSHFixtureImageRef
	}

	if !env.Config.UseExistingCluster && env.TestCtx != nil && env.TestCtx.Cluster != nil {
		Expect(preloadSSHFixtureImage(env, imageRef)).To(Succeed())
	}

	Expect(framework.EnsureNamespace(env.TestCtx.Context, env.Config.Kubeconfig, sshFixtureNamespace)).To(Succeed())
	Expect(framework.ApplyManifestContent(env.TestCtx.Context, env.Config.Kubeconfig, "sandbox0-e2e-ssh-fixture-", buildSSHFixtureManifest(imageRef))).To(Succeed())
	Expect(framework.WaitForDeployment(env.TestCtx.Context, env.Config.Kubeconfig, sshFixtureNamespace, sshFixtureDeploymentName, "3m")).To(Succeed())

	var serviceIP string
	Eventually(func() string {
		ip, err := framework.KubectlGetJSONPath(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			sshFixtureNamespace,
			"service",
			sshFixtureServiceName,
			"{.spec.clusterIP}",
		)
		if err != nil {
			return ""
		}
		return strings.TrimSpace(ip)
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).ShouldNot(BeEmpty())

	serviceIP, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		sshFixtureNamespace,
		"service",
		sshFixtureServiceName,
		"{.spec.clusterIP}",
	)
	Expect(err).NotTo(HaveOccurred())

	fixture := &sshFixture{
		Namespace: sshFixtureNamespace,
		ServiceIP: strings.TrimSpace(serviceIP),
		UserName:  sshFixtureUserName,
		ImageRef:  imageRef,
	}

	cleanup := func() {
		_ = framework.Kubectl(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"delete",
			"namespace",
			sshFixtureNamespace,
			"--ignore-not-found=true",
			"--wait=false",
		)
	}

	return fixture, cleanup
}

func buildSSHFixtureManifest(imageRef string) string {
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %s
  namespace: %s
spec:
  replicas: 1
  selector:
    matchLabels:
      app.kubernetes.io/name: %s
  template:
    metadata:
      labels:
        app.kubernetes.io/name: %s
    spec:
      containers:
        - name: sshd
          image: %s
          imagePullPolicy: IfNotPresent
          env:
            - name: PUID
              value: "1000"
            - name: PGID
              value: "1000"
            - name: TZ
              value: "Etc/UTC"
            - name: USER_NAME
              value: %q
            - name: PUBLIC_KEY
              value: %q
            - name: PASSWORD_ACCESS
              value: "false"
            - name: LOG_STDOUT
              value: "true"
          ports:
            - name: ssh
              containerPort: 2222
          readinessProbe:
            tcpSocket:
              port: 2222
            initialDelaySeconds: 2
            periodSeconds: 2
          volumeMounts:
            - name: config
              mountPath: /config
      volumes:
        - name: config
          emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: %s
  namespace: %s
spec:
  selector:
    app.kubernetes.io/name: %s
  ports:
    - name: ssh
      port: 22
      targetPort: 2222
      protocol: TCP
`, sshFixtureDeploymentName, sshFixtureNamespace, sshFixtureDeploymentName, sshFixtureDeploymentName, imageRef, sshFixtureUserName, sshFixturePublicKey, sshFixtureServiceName, sshFixtureNamespace, sshFixtureDeploymentName)
}

func preloadSSHFixtureImage(env *framework.ScenarioEnv, imageRef string) error {
	if _, err := framework.RunCommandOutput(env.TestCtx.Context, "docker", "image", "inspect", imageRef); err != nil {
		if err := framework.RunCommand(env.TestCtx.Context, "docker", "pull", imageRef); err != nil {
			return err
		}
	}
	return env.TestCtx.Cluster.LoadDockerImage(env.TestCtx.Context, imageRef)
}

func assertSSHAppProtocolTrafficRules(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string, fixture *sshFixture) {
	Expect(sandboxID).NotTo(BeEmpty())
	Expect(fixture).NotTo(BeNil())

	sandbox, _, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	templateNamespace, err := naming.TemplateNamespaceForBuiltin(sandbox.TemplateId)
	Expect(err).NotTo(HaveOccurred())

	sandbox = waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)

	Expect(installSSHFixturePrivateKey(env, templateNamespace, sandbox.PodName)).To(Succeed())

	clearPolicy := apispec.SandboxNetworkPolicy{
		Mode: apispec.SandboxNetworkPolicyModeAllowAll,
	}
	defer func() {
		_, _, _, _ = session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, clearPolicy)
	}()

	serviceCIDR := fixture.ServiceIP + "/32"
	sshCommand := buildSSHFixtureCommand(fixture)

	_, status, apiErr, err := session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, clearPolicy)
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(200))
	assertSSHFixtureCommandEventuallySucceeds(env, templateNamespace, sandbox.PodName, sshCommand)

	Expect(updateSandboxTrafficRulesPolicy(
		env,
		session,
		sandboxID,
		apispec.SandboxNetworkPolicyModeBlockAll,
		buildTrafficRule("allow-tls-only", apispec.Allow, serviceCIDR, apispec.TrafficRuleAppProtocolTls),
	)).To(Succeed())
	assertSSHFixtureCommandEventuallyFails(env, templateNamespace, sandbox.PodName, sshCommand)

	Expect(updateSandboxTrafficRulesPolicy(
		env,
		session,
		sandboxID,
		apispec.SandboxNetworkPolicyModeBlockAll,
		buildTrafficRule("allow-ssh", apispec.Allow, serviceCIDR, apispec.TrafficRuleAppProtocolSsh),
	)).To(Succeed())
	assertSSHFixtureCommandEventuallySucceeds(env, templateNamespace, sandbox.PodName, sshCommand)

	Expect(updateSandboxTrafficRulesPolicy(
		env,
		session,
		sandboxID,
		apispec.SandboxNetworkPolicyModeAllowAll,
		buildTrafficRule("deny-ssh", apispec.Deny, serviceCIDR, apispec.TrafficRuleAppProtocolSsh),
	)).To(Succeed())
	assertSSHFixtureCommandEventuallyFails(env, templateNamespace, sandbox.PodName, sshCommand)
}

func installSSHFixturePrivateKey(env *framework.ScenarioEnv, namespace, podName string) error {
	_, err := execInSandboxPod(env, namespace, podName, fmt.Sprintf(`set -eu
command -v ssh >/dev/null
cat <<'EOF' >/tmp/sandbox0-e2e-ssh-key
%s
EOF
chmod 600 /tmp/sandbox0-e2e-ssh-key
`, strings.TrimSpace(sshFixturePrivateKey)))
	return err
}

func buildSSHFixtureCommand(fixture *sshFixture) string {
	return fmt.Sprintf(
		"ssh -F /dev/null -i /tmp/sandbox0-e2e-ssh-key -o BatchMode=yes -o ConnectTimeout=5 -o LogLevel=ERROR -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -p 22 %s@%s 'printf ok'",
		fixture.UserName,
		fixture.ServiceIP,
	)
}

func buildTrafficRule(name string, action apispec.TrafficRuleAction, cidr string, appProtocol apispec.TrafficRuleAppProtocol) apispec.TrafficRule {
	cidrs := []string{cidr}
	appProtocols := []apispec.TrafficRuleAppProtocol{appProtocol}
	ports := []apispec.PortSpec{{
		Port:     22,
		Protocol: ptrTo("tcp"),
	}}
	return apispec.TrafficRule{
		Name:         ptrTo(name),
		Action:       action,
		Cidrs:        &cidrs,
		Ports:        &ports,
		AppProtocols: &appProtocols,
	}
}

func updateSandboxTrafficRulesPolicy(
	env *framework.ScenarioEnv,
	session *e2eutils.Session,
	sandboxID string,
	mode apispec.SandboxNetworkPolicyMode,
	rule apispec.TrafficRule,
) error {
	rules := []apispec.TrafficRule{rule}
	policy, status, apiErr, err := session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxNetworkPolicy{
		Mode: mode,
		Egress: &apispec.NetworkEgressPolicy{
			TrafficRules: &rules,
		},
	})
	if err != nil {
		return err
	}
	if apiErr != nil {
		return fmt.Errorf("update network policy returned API error: %s", apiErr.Error.Message)
	}
	if status != 200 {
		return fmt.Errorf("update network policy returned status %d", status)
	}
	if policy == nil || policy.Egress == nil || policy.Egress.TrafficRules == nil || len(*policy.Egress.TrafficRules) != 1 {
		return fmt.Errorf("updated network policy did not persist the expected traffic rule")
	}
	return nil
}

func assertSSHFixtureCommandEventuallySucceeds(env *framework.ScenarioEnv, namespace, podName, command string) {
	Eventually(func() error {
		output, err := execInSandboxPod(env, namespace, podName, command)
		if err != nil {
			return err
		}
		if sshFixtureSuccessOutput(output) != "ok" {
			return fmt.Errorf("unexpected ssh output: %q", output)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func assertSSHFixtureCommandEventuallyFails(env *framework.ScenarioEnv, namespace, podName, command string) {
	Eventually(func() error {
		output, err := execInSandboxPod(env, namespace, podName, command)
		if err == nil {
			return fmt.Errorf("ssh unexpectedly succeeded with output %q", output)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func sshFixtureSuccessOutput(output string) string {
	lines := strings.Split(strings.ReplaceAll(output, "\r\n", "\n"), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line != "" {
			return line
		}
	}
	return ""
}
