package cases

import (
	"fmt"
	"net/http"
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
	egressProxyFixtureNamespace       = "sandbox0-e2e-egress-proxy"
	egressProxyFixtureDeploymentName  = "egress-proxy"
	egressProxyFixtureServiceName     = "egress-proxy"
	egressProxyFixtureImageEnvVar     = "E2E_EGRESS_PROXY_FIXTURE_IMAGE"
	defaultEgressProxyFixtureImageRef = "python:3.12-alpine"
	egressProxyFixtureUsername        = "proxy-user"
	egressProxyFixturePassword        = "proxy-pass"
	egressProxyTargetAddress          = "203.0.113.10"
	egressProxyTargetPort             = 18080
)

type egressProxyFixture struct {
	Namespace string
	ServiceIP string
	ImageRef  string
}

func setupEgressProxyFixture(env *framework.ScenarioEnv) (*egressProxyFixture, func()) {
	imageRef := strings.TrimSpace(os.Getenv(egressProxyFixtureImageEnvVar))
	if imageRef == "" {
		imageRef = defaultEgressProxyFixtureImageRef
	}

	if env.TestCtx != nil && env.TestCtx.Cluster != nil {
		Expect(preloadEgressProxyFixtureImage(env, imageRef)).To(Succeed())
	}

	Expect(framework.EnsureNamespace(env.TestCtx.Context, env.Config.Kubeconfig, egressProxyFixtureNamespace)).To(Succeed())
	Expect(framework.ApplyManifestContent(env.TestCtx.Context, env.Config.Kubeconfig, "sandbox0-e2e-egress-proxy-", buildEgressProxyFixtureManifest(imageRef))).To(Succeed())
	Expect(framework.WaitForDeployment(env.TestCtx.Context, env.Config.Kubeconfig, egressProxyFixtureNamespace, egressProxyFixtureDeploymentName, "3m")).To(Succeed())

	var serviceIP string
	Eventually(func() string {
		ip, err := framework.KubectlGetJSONPath(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			egressProxyFixtureNamespace,
			"service",
			egressProxyFixtureServiceName,
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
		egressProxyFixtureNamespace,
		"service",
		egressProxyFixtureServiceName,
		"{.spec.clusterIP}",
	)
	Expect(err).NotTo(HaveOccurred())

	fixture := &egressProxyFixture{
		Namespace: egressProxyFixtureNamespace,
		ServiceIP: strings.TrimSpace(serviceIP),
		ImageRef:  imageRef,
	}

	cleanup := func() {
		_ = framework.Kubectl(
			env.TestCtx.Context,
			env.Config.Kubeconfig,
			"delete",
			"namespace",
			egressProxyFixtureNamespace,
			"--ignore-not-found=true",
			"--wait=false",
		)
	}

	return fixture, cleanup
}

func preloadEgressProxyFixtureImage(env *framework.ScenarioEnv, imageRef string) error {
	if _, err := framework.RunCommandOutput(env.TestCtx.Context, "docker", "image", "inspect", imageRef); err != nil {
		if err := framework.RunCommand(env.TestCtx.Context, "docker", "pull", imageRef); err != nil {
			return err
		}
	}
	return env.TestCtx.Cluster.LoadDockerImage(env.TestCtx.Context, imageRef)
}

func buildEgressProxyFixtureManifest(imageRef string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s-script
  namespace: %s
data:
  proxy.py: |
    import http.server
    import select
    import socket
    import socketserver
    import struct
    import threading

    USERNAME = %q
    PASSWORD = %q
    RESPONSE_BODY = b"proxied through socks5\n"

    def recv_exact(conn, size):
        chunks = []
        remaining = size
        while remaining:
            chunk = conn.recv(remaining)
            if not chunk:
                raise OSError("unexpected EOF")
            chunks.append(chunk)
            remaining -= len(chunk)
        return b"".join(chunks)

    class HTTPHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header("content-type", "text/plain")
            self.send_header("content-length", str(len(RESPONSE_BODY)))
            self.end_headers()
            self.wfile.write(RESPONSE_BODY)

        def log_message(self, format, *args):
            return

    class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
        allow_reuse_address = True
        daemon_threads = True

    class SocksHandler(socketserver.BaseRequestHandler):
        def handle(self):
            conn = self.request
            header = recv_exact(conn, 2)
            if header[0] != 5:
                return
            methods = recv_exact(conn, header[1])
            if 2 not in methods:
                conn.sendall(b"\x05\xff")
                return
            conn.sendall(b"\x05\x02")

            auth_header = recv_exact(conn, 2)
            username = recv_exact(conn, auth_header[1]).decode()
            password_len = recv_exact(conn, 1)[0]
            password = recv_exact(conn, password_len).decode()
            if auth_header[0] != 1 or username != USERNAME or password != PASSWORD:
                conn.sendall(b"\x01\x01")
                return
            conn.sendall(b"\x01\x00")

            request = recv_exact(conn, 4)
            if request[0] != 5 or request[1] != 1:
                conn.sendall(b"\x05\x07\x00\x01\x00\x00\x00\x00\x00\x00")
                return
            atyp = request[3]
            if atyp == 1:
                recv_exact(conn, 4)
            elif atyp == 3:
                recv_exact(conn, recv_exact(conn, 1)[0])
            elif atyp == 4:
                recv_exact(conn, 16)
            else:
                conn.sendall(b"\x05\x08\x00\x01\x00\x00\x00\x00\x00\x00")
                return
            recv_exact(conn, 2)

            upstream = socket.create_connection(("127.0.0.1", 8080), timeout=5)
            conn.sendall(b"\x05\x00\x00\x01\x7f\x00\x00\x01\x00\x00")
            try:
                sockets = [conn, upstream]
                while True:
                    readable, _, _ = select.select(sockets, [], [], 10)
                    if not readable:
                        return
                    for current in readable:
                        data = current.recv(65536)
                        if not data:
                            return
                        other = upstream if current is conn else conn
                        other.sendall(data)
            finally:
                upstream.close()

    if __name__ == "__main__":
        httpd = ThreadingTCPServer(("127.0.0.1", 8080), HTTPHandler)
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        socksd = ThreadingTCPServer(("0.0.0.0", 1080), SocksHandler)
        socksd.serve_forever()
---
apiVersion: apps/v1
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
        - name: proxy
          image: %s
          imagePullPolicy: IfNotPresent
          command: ["python3", "/app/proxy.py"]
          ports:
            - name: socks5
              containerPort: 1080
          readinessProbe:
            tcpSocket:
              port: 1080
            initialDelaySeconds: 2
            periodSeconds: 2
          volumeMounts:
            - name: script
              mountPath: /app
              readOnly: true
      volumes:
        - name: script
          configMap:
            name: %s-script
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
    - name: socks5
      port: 1080
      targetPort: 1080
      protocol: TCP
`, egressProxyFixtureDeploymentName, egressProxyFixtureNamespace, egressProxyFixtureUsername, egressProxyFixturePassword, egressProxyFixtureDeploymentName, egressProxyFixtureNamespace, egressProxyFixtureDeploymentName, egressProxyFixtureDeploymentName, imageRef, egressProxyFixtureDeploymentName, egressProxyFixtureServiceName, egressProxyFixtureNamespace, egressProxyFixtureDeploymentName)
}

func assertSandboxEgressProxy(env *framework.ScenarioEnv, session *e2eutils.Session, sandboxID string) {
	Expect(sandboxID).NotTo(BeEmpty())

	fixture, cleanup := setupEgressProxyFixture(env)
	defer cleanup()

	sandbox, _, err := session.GetSandbox(env.TestCtx.Context, GinkgoT(), sandboxID)
	Expect(err).NotTo(HaveOccurred())
	templateNamespace, err := naming.TemplateNamespaceForBuiltin(sandbox.TemplateId)
	Expect(err).NotTo(HaveOccurred())

	sandbox = waitForSandboxPodReadyEventually(env, session, sandboxID, templateNamespace)

	sourceName := fmt.Sprintf("e2e-socks5-proxy-%d", time.Now().UnixNano())
	refName := "api-egress-proxy"
	targetCIDR := egressProxyTargetAddress + "/32"
	ports := []apispec.PortSpec{{
		Port:     egressProxyTargetPort,
		Protocol: ptrTo("tcp"),
	}}
	rules := []apispec.TrafficRule{{
		Name:   ptrTo("allow-proxied-http"),
		Action: apispec.Allow,
		Cidrs:  &[]string{targetCIDR},
		Ports:  &ports,
	}}

	clearPolicy := apispec.SandboxNetworkPolicy{
		Mode:               apispec.AllowAll,
		CredentialBindings: &[]apispec.CredentialBinding{},
	}
	defer func() {
		_, _, _, _ = session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, clearPolicy)
		status, _, deleteErr := session.DeleteCredentialSource(env.TestCtx.Context, sourceName)
		if deleteErr == nil {
			Expect(status).To(Or(Equal(http.StatusOK), Equal(http.StatusNotFound)))
		}
	}()

	created, err := session.CreateCredentialSource(env.TestCtx.Context, GinkgoT(), apispec.CredentialSourceWriteRequest{
		Name:         sourceName,
		ResolverKind: apispec.StaticUsernamePassword,
		Spec: apispec.CredentialSourceWriteSpec{
			StaticUsernamePassword: &apispec.StaticUsernamePasswordSourceSpec{
				Username: ptrTo(egressProxyFixtureUsername),
				Password: ptrTo(egressProxyFixturePassword),
			},
		},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(created).NotTo(BeNil())

	policy, status, apiErr, err := session.UpdateNetworkPolicy(env.TestCtx.Context, GinkgoT(), sandboxID, apispec.SandboxNetworkPolicy{
		Mode: apispec.BlockAll,
		Egress: &apispec.NetworkEgressPolicy{
			TrafficRules: &rules,
			Proxy: &apispec.EgressProxyPolicy{
				Type:          apispec.EgressProxyTypeSocks5,
				Address:       fmt.Sprintf("%s:1080", fixture.ServiceIP),
				CredentialRef: &refName,
			},
		},
		CredentialBindings: &[]apispec.CredentialBinding{{
			Ref:       refName,
			SourceRef: sourceName,
			Projection: apispec.ProjectionSpec{
				Type:             apispec.UsernamePassword,
				UsernamePassword: &apispec.UsernamePasswordProjection{},
			},
		}},
	})
	Expect(err).NotTo(HaveOccurred())
	Expect(apiErr).To(BeNil())
	Expect(status).To(Equal(http.StatusOK))
	Expect(policy).NotTo(BeNil())
	Expect(policy.Egress).NotTo(BeNil())
	Expect(policy.Egress.Proxy).NotTo(BeNil())

	Eventually(func() error {
		body, execErr := execInSandboxPod(env, templateNamespace, sandbox.PodName, fmt.Sprintf(
			"curl -fsS --max-time 10 http://%s:%d/",
			egressProxyTargetAddress,
			egressProxyTargetPort,
		))
		if execErr != nil {
			return execErr
		}
		if body != "proxied through socks5\n" {
			return fmt.Errorf("unexpected proxied response body: %q", body)
		}
		return nil
	}).WithTimeout(60 * time.Second).WithPolling(3 * time.Second).Should(Succeed())
}
