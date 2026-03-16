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

const (
	issue36HTTPPort   int32 = 18081
	issue36OpaquePort int32 = 18082
	issue36UDPPort    int32 = 18083
	issue36HTTPBody         = "issue36-http-ok\n"
)

func registerApiNetworkPolicySuite(envProvider func() *framework.ScenarioEnv) {
	registerApiModeSuite(envProvider, apiModeSuiteOptions{
		name:                     "network-policy",
		describe:                 "API network policy mode",
		templateNamePrefix:       "e2e-network",
		fileContent:              "hello network",
		includeNetworkPolicy:     true,
		expectStorageUnavailable: true,
	})
}

func assertAuditableEgressInterception(env *framework.ScenarioEnv, session *e2eutils.Session, helperSandboxID string) {
	baseTemplate, err := session.GetTemplate(env.TestCtx.Context, GinkgoT(), "default")
	Expect(err).NotTo(HaveOccurred())
	Expect(baseTemplate).NotTo(BeNil())

	helperNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	helperSandbox := waitForSandboxPodReadyEventually(env, session, helperSandboxID, helperNamespace)

	Expect(execIssue36HelperServices(env, helperNamespace, helperSandbox.PodName)).To(Succeed())

	helperIP, err := framework.KubectlGetJSONPath(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		helperNamespace,
		"pod",
		helperSandbox.PodName,
		"{.status.podIP}",
	)
	Expect(err).NotTo(HaveOccurred())
	helperIP = strings.TrimSpace(helperIP)
	Expect(helperIP).NotTo(BeEmpty())

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, helperNamespace, helperSandbox.PodName, fmt.Sprintf("curl -fsS --max-time 5 http://127.0.0.1:%d/", issue36HTTPPort))
		if execErr != nil {
			return execErr
		}
		if output != issue36HTTPBody {
			return fmt.Errorf("unexpected helper http body: %q", output)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
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

	namespace, podName := waitForIssue36IdlePodEventually(env, templateID)

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, namespace, podName, issue36FragmentedHTTPRequestCommand(helperIP))
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(output) != "ok" {
			return fmt.Errorf("unexpected fragmented http result: %q", output)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, namespace, podName, issue36OpaqueTCPCommand(helperIP))
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

	namespace, podName := waitForIssue36IdlePodEventually(env, templateID)

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, namespace, podName, issue36UDPSessionCommand(helperIP, templateID))
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
		MinIdle: 1,
		MaxIdle: 1,
	}
	req.Spec.Network = network
	description := "Issue36 auditable egress e2e template"
	displayName := "Issue36 E2E " + templateID
	req.Spec.Description = &description
	req.Spec.DisplayName = &displayName
	return req
}

func waitForIssue36IdlePodEventually(env *framework.ScenarioEnv, templateID string) (string, string) {
	namespace, err := naming.TemplateNamespaceForBuiltin(templateID)
	Expect(err).NotTo(HaveOccurred())

	var podName string
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"-n", namespace,
			"get", "pods",
			"-l", fmt.Sprintf("sandbox0.ai/template-id=%s,sandbox0.ai/pool-type=idle", templateID),
			"--field-selector", "status.phase=Running",
			"-o", "jsonpath={.items[0].metadata.name}",
		)
		if err != nil {
			return err
		}
		podName = strings.TrimSpace(output)
		if podName == "" {
			return fmt.Errorf("template %s has no running idle pod yet", templateID)
		}
		return nil
	}).WithTimeout(5 * time.Minute).WithPolling(5 * time.Second).Should(Succeed())
	return namespace, podName
}

func execIssue36HelperServices(env *framework.ScenarioEnv, namespace, podName string) error {
	_, err := execInSandboxPod(env, namespace, podName, issue36HelperServicesCommand())
	return err
}

func issue36HelperServicesCommand() string {
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
HTTP_BODY = %q.encode()


class ReusableTCPServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True


class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(HTTP_BODY)))
        self.end_headers()
        self.wfile.write(HTTP_BODY)

    def log_message(self, fmt, *args):
        return


def run_http():
    with ReusableTCPServer(("0.0.0.0", HTTP_PORT), Handler) as server:
        server.serve_forever()


def run_opaque():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(("0.0.0.0", OPAQUE_PORT))
        server.listen()
        while True:
            conn, _ = server.accept()
            with conn:
                conn.sendall(b"opaque-ready\n")
                time.sleep(0.2)


def run_udp():
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as server:
        server.bind(("0.0.0.0", UDP_PORT))
        sessions = {}
        next_port = 20000
        while True:
            data, addr = server.recvfrom(2048)
            token = data.decode(errors="ignore").strip()
            if not token:
                continue
            if token not in sessions:
                sessions[token] = next_port
                next_port += 1
            payload = f"token={token} port={sessions[token]}".encode()
            server.sendto(payload, addr)


for target in (run_http, run_opaque, run_udp):
    thread = threading.Thread(target=target, daemon=True)
    thread.start()

while True:
    time.sleep(1)
PY
nohup python3 /tmp/issue36-helper.py >/tmp/issue36-helper.log 2>&1 &
`, issue36HTTPPort, issue36OpaquePort, issue36UDPPort, issue36HTTPBody)
}

func issue36FragmentedHTTPRequestCommand(helperIP string) string {
	return fmt.Sprintf(`cat <<'PY' | python3
import socket

sock = socket.create_connection((%q, %d), timeout=5)
try:
    sock.sendall(b"GET / HT")
    sock.sendall(b"TP/1.1\r\nHost: %s\r\nConnection: close\r\n\r\n")
    data = []
    while True:
        chunk = sock.recv(4096)
        if not chunk:
            break
        data.append(chunk)
finally:
    sock.close()

body = b"".join(data).decode(errors="ignore")
if %q not in body:
    raise SystemExit(body)
print("ok")
PY`, helperIP, issue36HTTPPort, helperIP, issue36HTTPBody)
}

func issue36OpaqueTCPCommand(helperIP string) string {
	return fmt.Sprintf(`cat <<'PY' | python3
import socket
try:
    sock = socket.create_connection((%q, %d), timeout=5)
    sock.settimeout(3)
    try:
        sock.sendall(b"opaque")
        sock.recv(32)
    finally:
        sock.close()
    raise SystemExit("unexpected success")
except (TimeoutError, OSError):
    pass
print("denied")
PY`, helperIP, issue36OpaquePort)
}

func issue36UDPSessionCommand(helperIP, tokenSeed string) string {
	return fmt.Sprintf(`cat <<'PY' | python3
import socket

token = %q
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.settimeout(5)
sock.sendto(token.encode(), (%q, %d))
first = sock.recv(1024).decode()
sock.sendto(token.encode(), (%q, %d))
second = sock.recv(1024).decode()
sock.close()

def parse_port(message):
    for part in message.split():
        if part.startswith("port="):
            return part.split("=", 1)[1]
    raise SystemExit(message)

port1 = parse_port(first)
port2 = parse_port(second)
print(f"ports={1 if port1 == port2 else 2} same={'true' if port1 == port2 else 'false'}")
PY`, "issue36-"+tokenSeed, helperIP, issue36UDPPort, helperIP, issue36UDPPort)
}
