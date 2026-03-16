package cases

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/controller"
	"github.com/sandbox0-ai/sandbox0/pkg/framework"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	e2eutils "github.com/sandbox0-ai/sandbox0/tests/e2e/utils"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

const (
	auditableEgressHTTPPort   int32 = 18081
	auditableEgressOpaquePort int32 = 18082
	auditableEgressUDPPort    int32 = 18083
	auditableEgressHTTPBody         = "auditable-egress-http-ok\n"
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
	helperNamespace, err := naming.TemplateNamespaceForBuiltin("default")
	Expect(err).NotTo(HaveOccurred())
	helperSandbox := waitForSandboxPodReadyEventually(env, session, helperSandboxID, helperNamespace)

	Expect(execAuditableEgressHelperServices(env, helperNamespace, helperSandbox.PodName)).To(Succeed())

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
	helperServiceHost := upsertAuditableEgressHelperService(env, helperNamespace, helperSandbox.PodName)
	defer deleteAuditableEgressHelperService(env, helperNamespace)

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, helperNamespace, helperSandbox.PodName, fmt.Sprintf("curl -fsS --max-time 5 http://127.0.0.1:%d/", auditableEgressHTTPPort))
		if execErr != nil {
			return execErr
		}
		if output != auditableEgressHTTPBody {
			return fmt.Errorf("unexpected helper http body: %q", output)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())

	targetSandboxID := claimSandboxEventually(env, session, "default").SandboxId
	Expect(targetSandboxID).NotTo(BeEmpty())
	defer func() {
		_ = session.DeleteSandbox(env.TestCtx.Context, GinkgoT(), targetSandboxID)
	}()
	targetSandbox := waitForSandboxPodReadyEventually(env, session, targetSandboxID, helperNamespace)

	applyAuditableEgressNetworkPolicyToSandboxPod(env, helperNamespace, targetSandboxID, targetSandbox.PodName, &v1alpha1.TplSandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			AllowedDomains: []string{helperServiceHost},
		},
	})

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, helperNamespace, targetSandbox.PodName, auditableEgressFragmentedHTTPRequestCommand(helperServiceHost))
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(output) != "ok" {
			return fmt.Errorf("unexpected fragmented http result: %q", output)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, helperNamespace, targetSandbox.PodName, auditableEgressOpaqueTCPCommand(helperIP))
		if execErr != nil {
			return execErr
		}
		if strings.TrimSpace(output) != "denied" {
			return fmt.Errorf("expected opaque tcp traffic to be denied, got %q", output)
		}
		return nil
	}).WithTimeout(45 * time.Second).WithPolling(3 * time.Second).Should(Succeed())

	applyAuditableEgressNetworkPolicyToSandboxPod(env, helperNamespace, targetSandboxID, targetSandbox.PodName, &v1alpha1.TplSandboxNetworkPolicy{
		Mode: v1alpha1.NetworkModeBlockAll,
		Egress: &v1alpha1.NetworkEgressPolicy{
			AllowedCIDRs: []string{helperIP + "/32"},
			AllowedPorts: []v1alpha1.PortSpec{{
				Port:     auditableEgressUDPPort,
				Protocol: "udp",
			}},
		},
	})

	Eventually(func() error {
		output, execErr := execInSandboxPod(env, helperNamespace, targetSandbox.PodName, auditableEgressUDPSessionCommand(helperIP, targetSandboxID))
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

func applyAuditableEgressNetworkPolicyToSandboxPod(env *framework.ScenarioEnv, namespace, sandboxID, podName string, policyTpl *v1alpha1.TplSandboxNetworkPolicy) {
	annotations := getSandboxPodAnnotationsEventually(env, namespace, podName)
	existingAnnotation := annotations[controller.AnnotationNetworkPolicy]
	existingSpec, err := v1alpha1.ParseNetworkPolicyFromAnnotation(existingAnnotation)
	Expect(err).NotTo(HaveOccurred())
	if existingSpec == nil {
		existingSpec = &v1alpha1.NetworkPolicySpec{
			Version:   "v1",
			SandboxID: sandboxID,
		}
	}
	existingSpec.Mode = policyTpl.Mode
	existingSpec.Egress = v1alpha1.BuildEgressSpec(policyTpl)

	annotation, err := v1alpha1.NetworkPolicyToAnnotation(existingSpec)
	Expect(err).NotTo(HaveOccurred())
	hash := auditableEgressPolicyAnnotationHash(annotation)
	patchBytes, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]any{
				controller.AnnotationNetworkPolicy:            annotation,
				controller.AnnotationNetworkPolicyHash:        hash,
				controller.AnnotationNetworkPolicyAppliedHash: nil,
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"patch",
		"pod",
		podName,
		"--namespace",
		namespace,
		"--type",
		"merge",
		"-p",
		string(patchBytes),
	)).To(Succeed())

	Eventually(func() error {
		current := getSandboxPodAnnotationsEventually(env, namespace, podName)
		if current[controller.AnnotationNetworkPolicyHash] != hash {
			return fmt.Errorf("sandbox %s network policy hash not updated yet", sandboxID)
		}
		if current[controller.AnnotationNetworkPolicyAppliedHash] != hash {
			return fmt.Errorf("sandbox %s network policy not applied yet", sandboxID)
		}
		return nil
	}).WithTimeout(90 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}

func getSandboxPodAnnotationsEventually(env *framework.ScenarioEnv, namespace, podName string) map[string]string {
	var annotations map[string]string
	Eventually(func() error {
		output, err := framework.KubectlOutput(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"-n", namespace,
			"get", "pod", podName,
			"-o", "json",
		)
		if err != nil {
			return err
		}
		var payload struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		}
		if err := json.Unmarshal([]byte(output), &payload); err != nil {
			return err
		}
		annotations = payload.Metadata.Annotations
		if annotations == nil {
			return fmt.Errorf("pod %s annotations not available", podName)
		}
		return nil
	}).WithTimeout(30 * time.Second).WithPolling(2 * time.Second).Should(Succeed())
	return annotations
}

func auditableEgressPolicyAnnotationHash(annotation string) string {
	if annotation == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(annotation))
	return hex.EncodeToString(sum[:])
}

func execAuditableEgressHelperServices(env *framework.ScenarioEnv, namespace, podName string) error {
	_, err := execInSandboxPod(env, namespace, podName, auditableEgressHelperServicesCommand())
	return err
}

func upsertAuditableEgressHelperService(env *framework.ScenarioEnv, namespace, sandboxID string) string {
	file, err := os.CreateTemp("", "sandbox0-e2e-auditable-egress-service-*.yaml")
	Expect(err).NotTo(HaveOccurred())
	defer os.Remove(file.Name())

	manifest := fmt.Sprintf(`apiVersion: v1
kind: Service
metadata:
  name: auditable-egress-helper
  namespace: %s
spec:
  clusterIP: None
  selector:
    %s: %s
  ports:
    - name: http
      port: %d
      protocol: TCP
      targetPort: %d
`, namespace, controller.LabelSandboxID, sandboxID, auditableEgressHTTPPort, auditableEgressHTTPPort)
	_, err = file.WriteString(manifest)
	Expect(err).NotTo(HaveOccurred())
	Expect(file.Close()).To(Succeed())
	Expect(framework.ApplyManifest(env.TestCtx.Context, env.Config.Kubeconfig, file.Name())).To(Succeed())
	return fmt.Sprintf("auditable-egress-helper.%s.svc.cluster.local", namespace)
}

func deleteAuditableEgressHelperService(env *framework.ScenarioEnv, namespace string) {
	_ = framework.Kubectl(
		env.TestCtx.Context,
		env.Config.Kubeconfig,
		"delete",
		"service",
		"auditable-egress-helper",
		"--namespace",
		namespace,
		"--ignore-not-found=true",
	)
}

func auditableEgressHelperServicesCommand() string {
	return fmt.Sprintf(`set -eu
cat <<'PY' >/tmp/auditable-egress-helper.py
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
nohup python3 /tmp/auditable-egress-helper.py >/tmp/auditable-egress-helper.log 2>&1 &
`, auditableEgressHTTPPort, auditableEgressOpaquePort, auditableEgressUDPPort, auditableEgressHTTPBody)
}

func auditableEgressFragmentedHTTPRequestCommand(helperIP string) string {
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
PY`, helperIP, auditableEgressHTTPPort, helperIP, auditableEgressHTTPBody)
}

func auditableEgressOpaqueTCPCommand(helperIP string) string {
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
PY`, helperIP, auditableEgressOpaquePort)
}

func auditableEgressUDPSessionCommand(helperIP, tokenSeed string) string {
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
PY`, "auditable-egress-"+tokenSeed, helperIP, auditableEgressUDPPort, helperIP, auditableEgressUDPPort)
}
