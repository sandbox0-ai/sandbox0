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
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	issue36HTTPPort   int32 = 18081
	issue36OpaquePort int32 = 18082
	issue36UDPPort    int32 = 18083
	issue36HTTPBody         = "issue36-http-ok\n"
)

func assertAuditableEgressInterception(env *framework.ScenarioEnv, session *e2eutils.Session) {
	baseTemplate, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
	Expect(err).NotTo(HaveOccurred())
	Expect(baseTemplate).NotTo(BeNil())
	Expect(baseTemplate.Spec.MainContainer).NotTo(BeNil())
	Expect(strings.TrimSpace(baseTemplate.Spec.MainContainer.Image)).NotTo(BeEmpty())

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	helperPodName := "issue36-helper-" + suffix
	helperManifestPath, err := applyIssue36HelperPod(env, helperPodName, baseTemplate.Spec.MainContainer.Image)
	Expect(err).NotTo(HaveOccurred())
	defer func() {
		_ = framework.KubectlDeleteManifest(env.TestCtx.Context, env.Config.Kubeconfig, helperManifestPath)
		_ = os.Remove(helperManifestPath)
	}()

	Expect(framework.KubectlWaitForCondition(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		"pod",
		helperPodName,
		"Ready",
		"2m",
	)).To(Succeed())

	helperIP, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		env.Infra.Namespace,
		"pod",
		helperPodName,
		"{.status.podIP}",
	)
	Expect(err).NotTo(HaveOccurred())
	helperIP = strings.TrimSpace(helperIP)
	Expect(helperIP).NotTo(BeEmpty())

	assertIssue36TCPClassificationBehavior(env, session, *baseTemplate, helperIP, suffix)
	assertIssue36UDPSessionBehavior(env, session, *baseTemplate, helperIP, suffix)
}

func assertIssue36TCPClassificationBehavior(env *framework.ScenarioEnv, session *e2eutils.Session, base apispec.Template, helperIP, suffix string) {
	allowedDomains := []string{helperIP}
	templateID := "e2e-issue36-tcp-" + suffix
	templateReq := buildIssue36TemplateCreateRequest(base, templateID, &apispec.TplSandboxNetworkPolicy{
		Mode: apispec.BlockAll,
		Egress: &apispec.NetworkEgressPolicy{
			AllowedDomains: &allowedDomains,
		},
	})

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), templateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	defer func() {
		_ = session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), templateID)
	}()

	sandboxID := claimSandboxEventually(env, session, templateID).SandboxId
	Expect(sandboxID).NotTo(BeEmpty())
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	}()

	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	Expect(err).NotTo(HaveOccurred())
	sandbox := waitForSandboxPodReadyEventually(env, session, sandboxID, namespace)

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, namespace, sandbox.PodName, issue36FragmentedHTTPRequestCommand(helperIP))
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(output) != "ok" {
			return fmt.Errorf("unexpected fragmented http result: %q", output)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, namespace, sandbox.PodName, issue36OpaqueTCPCommand(helperIP))
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(output) != "denied" {
			return fmt.Errorf("expected opaque tcp traffic to be denied, got %q", output)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func assertIssue36UDPSessionBehavior(env *framework.ScenarioEnv, session *e2eutils.Session, base apispec.Template, helperIP, suffix string) {
	allowedCIDRs := []string{helperIP + "/32"}
	udpProtocol := "udp"
	allowedPorts := []apispec.PortSpec{{
		Port:     issue36UDPPort,
		Protocol: &udpProtocol,
	}}
	templateID := "e2e-issue36-udp-" + suffix
	templateReq := buildIssue36TemplateCreateRequest(base, templateID, &apispec.TplSandboxNetworkPolicy{
		Mode: apispec.BlockAll,
		Egress: &apispec.NetworkEgressPolicy{
			AllowedCidrs: &allowedCIDRs,
			AllowedPorts: &allowedPorts,
		},
	})

	created, err := session.CreateTemplate(env.TestCtx.Context, GinkgoT(), templateReq)
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())
	defer func() {
		_ = session.DeleteTemplate(env.TestCtx.Context, GinkgoT(), templateID)
	}()

	sandboxID := claimSandboxEventually(env, session, templateID).SandboxId
	Expect(sandboxID).NotTo(BeEmpty())
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	}()

	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	Expect(err).NotTo(HaveOccurred())
	sandbox := waitForSandboxPodReadyEventually(env, session, sandboxID, namespace)

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, namespace, sandbox.PodName, issue36UDPSessionCommand(helperIP, templateID))
		if execErr != nil {
			return execErr
		}
		lines := strings.Split(strings.TrimSpace(output), "\n")
		if len(lines) == 0 {
			return fmt.Errorf("udp session command returned no output")
		}
		last := lines[len(lines)-1]
		if !strings.Contains(last, "ports=1") || !strings.Contains(last, "same=true") {
			return fmt.Errorf("unexpected udp session response: %q", output)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func buildIssue36TemplateCreateRequest(base apispec.Template, templateID string, network *apispec.TplSandboxNetworkPolicy) apispec.TemplateCreateRequest {
	req := e2eutils.CloneTemplateForCreate(base, templateID)
	req.Spec.Pool = &apispec.PoolStrategy{
		MinIdle: 0,
		MaxIdle: 0,
	}
	req.Spec.Network = network
	description := "Issue36 auditable egress e2e template"
	displayName := "Issue36 E2E " + templateID
	req.Spec.Description = &description
	req.Spec.DisplayName = &displayName
	return req
}

func applyIssue36HelperPod(env *framework.ScenarioEnv, podName, image string) (string, error) {
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: env.Infra.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name": "issue36-helper",
				"app.kubernetes.io/part": "sandbox0-e2e",
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyAlways,
			Containers: []corev1.Container{{
				Name:    "helper",
				Image:   image,
				Command: []string{"/bin/sh", "-lc", issue36HelperPodCommand()},
			}},
		},
	}

	raw, err := yaml.Marshal(pod)
	if err != nil {
		return "", err
	}
	file, err := os.CreateTemp("", "sandbox0-issue36-helper-*.yaml")
	if err != nil {
		return "", err
	}
	if _, err := file.Write(raw); err != nil {
		_ = file.Close()
		_ = os.Remove(file.Name())
		return "", err
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(file.Name())
		return "", err
	}
	if err := framework.ApplyManifest(env.TestCtx.Context, env.Config.Kubeconfig, file.Name()); err != nil {
		_ = os.Remove(file.Name())
		return "", err
	}
	return file.Name(), nil
}

func issue36HelperPodCommand() string {
	return fmt.Sprintf(`set -eu
cat <<'PY' >/tmp/issue36-helper.py
import http.server
import socket
import socketserver
import threading
import time

HTTP_PORT = %d
OPAQUE_PORT = %d
UDP_PORT = %d
HTTP_BODY = %q.encode("utf-8")
udp_seen = {}
udp_lock = threading.Lock()

class QuietHTTPHandler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Length", str(len(HTTP_BODY)))
        self.end_headers()
        self.wfile.write(HTTP_BODY)

    def log_message(self, format, *args):
        return

class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True

class OpaqueTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(5.0)
        try:
            payload = self.request.recv(4096)
        except OSError:
            return
        if payload:
            try:
                self.request.sendall(b"opaque-ok\n")
            except OSError:
                pass

def udp_loop():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", UDP_PORT))
    while True:
        payload, addr = sock.recvfrom(4096)
        token = payload.decode("utf-8", "replace").strip() or "empty"
        with udp_lock:
            ports = udp_seen.setdefault(token, set())
            ports.add(addr[1])
            count = len(ports)
        reply = f"token={token};ports={count};same={'true' if count == 1 else 'false'}\n".encode("utf-8")
        sock.sendto(reply, addr)

http_server = ThreadingTCPServer(("0.0.0.0", HTTP_PORT), QuietHTTPHandler)
opaque_server = ThreadingTCPServer(("0.0.0.0", OPAQUE_PORT), OpaqueTCPHandler)

threading.Thread(target=http_server.serve_forever, daemon=True).start()
threading.Thread(target=opaque_server.serve_forever, daemon=True).start()
threading.Thread(target=udp_loop, daemon=True).start()

while True:
    time.sleep(60)
PY
exec python3 /tmp/issue36-helper.py
`, issue36HTTPPort, issue36OpaquePort, issue36UDPPort, issue36HTTPBody)
}

func issue36FragmentedHTTPRequestCommand(helperIP string) string {
	return fmt.Sprintf(`python3 - <<'PY'
import socket
import time

addr = (%q, %d)
sock = socket.create_connection(addr, timeout=5)
sock.settimeout(5)
sock.sendall(b"GET / HTTP/1.1\r\nHo")
time.sleep(0.25)
sock.sendall(b"st: %s\r\nConnection: close\r\n\r\n")
chunks = []
while True:
    try:
        chunk = sock.recv(4096)
    except socket.timeout:
        break
    if not chunk:
        break
    chunks.append(chunk)
sock.close()
body = b"".join(chunks).decode("utf-8", "replace")
if %q not in body:
    raise SystemExit(body)
print("ok")
PY`, helperIP, issue36HTTPPort, helperIP, issue36HTTPBody)
}

func issue36OpaqueTCPCommand(helperIP string) string {
	return fmt.Sprintf(`python3 - <<'PY'
import socket

addr = (%q, %d)
try:
    sock = socket.create_connection(addr, timeout=5)
    sock.settimeout(1.5)
    sock.sendall(b"\x01\x02\x03opaque")
    reply = sock.recv(64)
except OSError:
    print("denied")
    raise SystemExit(0)
finally:
    try:
        sock.close()
    except Exception:
        pass

if not reply:
    print("denied")
    raise SystemExit(0)
raise SystemExit("unexpected response: %%r" %% (reply,))
PY`, helperIP, issue36OpaquePort)
}

func issue36UDPSessionCommand(helperIP, tokenSeed string) string {
	return fmt.Sprintf(`python3 - <<'PY'
import socket

server = (%q, %d)
token = %q
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(5)
try:
    sock.sendto(token.encode("utf-8"), server)
    first = sock.recv(256).decode("utf-8", "replace").strip()
    sock.sendto(token.encode("utf-8"), server)
    second = sock.recv(256).decode("utf-8", "replace").strip()
finally:
    sock.close()

print(first)
print(second)
if "ports=1" not in second or "same=true" not in second:
    raise SystemExit(second)
PY`, helperIP, issue36UDPPort, "issue36-"+tokenSeed)
}
