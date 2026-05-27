package http

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/sandboxfunction"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSandboxServiceProxiesAuthorizedRouteWithRewrite(t *testing.T) {
	upstreamHits := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHits++
		if r.URL.Path != "/v1/users" {
			t.Fatalf("upstream path = %q, want /v1/users", r.URL.Path)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	tokenHash := sha256Hex("secret-token")
	rewritePrefix := "/v1"
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		InternalAddr: "http://127.0.0.1:1",
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:            "api",
					PathPrefix:    "/api",
					Methods:       []string{http.MethodGet},
					RewritePrefix: &rewritePrefix,
					Auth: &mgr.SandboxAppServiceRouteAuth{
						Mode:              mgr.SandboxAppServiceRouteAuthModeBearer,
						BearerTokenSHA256: tokenHash,
					},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req := newGatewayRequest(t, http.MethodGet, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/api/users")
	req.Header.Set("Authorization", "Bearer secret-token")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if upstreamHits != 1 {
		t.Fatalf("upstream hits = %d, want 1", upstreamHits)
	}
}

func TestSandboxServiceRejectsDisallowedMethod(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not be called")
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		InternalAddr: "http://127.0.0.1:1",
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "api",
					PathPrefix: "/api",
					Methods:    []string{http.MethodGet},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req := newGatewayRequest(t, http.MethodPost, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/api/users")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestSandboxServiceHandlesCORSPreflight(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("upstream should not be called")
	}))
	defer upstream.Close()

	port := serverPort(t, upstream.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		InternalAddr: "http://127.0.0.1:1",
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: port,
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "api",
					PathPrefix: "/api",
					Methods:    []string{http.MethodGet},
					CORS: &mgr.SandboxAppServiceRouteCORS{
						AllowedOrigins: []string{"https://app.example"},
						AllowedMethods: []string{http.MethodGet},
						AllowedHeaders: []string{"Authorization"},
						MaxAgeSeconds:  60,
					},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req := newGatewayRequest(t, http.MethodOptions, gatewayServer.URL, fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port), "/api/users")
	req.Header.Set("Origin", "https://app.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodGet)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
}

func TestSandboxFunctionServiceExecutesThroughProcdPort(t *testing.T) {
	var execReq sandboxfunction.ExecuteRequest
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/functions/execute" {
			t.Fatalf("procd path = %q, want /api/v1/functions/execute", r.URL.Path)
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Fatal("missing internal token")
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatalf("read procd body: %v", err)
		}
		if err := json.Unmarshal(body, &execReq); err != nil {
			t.Fatalf("decode execute request: %v", err)
		}
		_ = spec.WriteSuccess(w, http.StatusOK, sandboxfunction.ExecuteResponse{
			Status:     http.StatusCreated,
			Headers:    map[string][]string{"content-type": {"text/plain"}, "x-function": {"ok"}},
			BodyBase64: "ZnVuY3Rpb24gb2s=",
		})
	}))
	defer procd.Close()

	port := serverPort(t, procd.URL)
	gateway := newSandboxServiceExposureTestServer(t, &mgr.Sandbox{
		ID:           "sb-demo",
		TeamID:       "team-1",
		UserID:       "user-1",
		InternalAddr: procd.URL,
		AutoResume:   true,
		Services: []mgr.SandboxAppService{{
			ID:   "webhook",
			Port: port,
			Runtime: &mgr.SandboxAppServiceRuntime{
				Type: mgr.SandboxAppServiceRuntimeFunction,
				Function: &mgr.SandboxFunction{
					Runtime: "python",
					Handler: "handler",
					Source: mgr.SandboxFunctionSource{
						Type:     "inline",
						Filename: "main.py",
						Code:     "def handler(request):\n    return {'status': 201}\n",
					},
				},
			},
			Ingress: mgr.SandboxAppServiceIngress{
				Public: true,
				Routes: []mgr.SandboxAppServiceRoute{{
					ID:         "root",
					PathPrefix: "/events",
					Methods:    []string{http.MethodPost},
				}},
			},
		}},
	})

	gatewayServer := httptest.NewServer(gateway)
	defer gatewayServer.Close()
	req, err := http.NewRequest(http.MethodPost, gatewayServer.URL+"/events/stripe?source=test", strings.NewReader("payload"))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = fmt.Sprintf("sb-demo--p%d.aws-us-east-1.sandbox0.app", port)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("gateway request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}
	if string(body) != "function ok" {
		t.Fatalf("body = %q, want function ok", string(body))
	}
	if resp.Header.Get("x-function") != "ok" {
		t.Fatalf("x-function = %q, want ok", resp.Header.Get("x-function"))
	}
	if execReq.ServiceID != "webhook" || execReq.RouteID != "root" {
		t.Fatalf("execute service/route = %q/%q, want webhook/root", execReq.ServiceID, execReq.RouteID)
	}
	if execReq.Request.Path != "/events/stripe" || execReq.Request.RawQuery != "source=test" {
		t.Fatalf("execute path/query = %q/%q", execReq.Request.Path, execReq.Request.RawQuery)
	}
	decodedBody, err := base64.StdEncoding.DecodeString(execReq.Request.BodyBase64)
	if err != nil {
		t.Fatalf("decode request body: %v", err)
	}
	if string(decodedBody) != "payload" {
		t.Fatalf("execute body = %q, want payload", string(decodedBody))
	}
}

func newSandboxServiceExposureTestServer(t *testing.T, sandbox *mgr.Sandbox) http.Handler {
	t.Helper()
	gin.SetMode(gin.TestMode)

	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/sb-demo" {
			spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, sandbox)
	}))
	t.Cleanup(manager.Close)

	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	gen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})

	s := &Server{
		cfg: &config.ClusterGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicExposureEnabled: true,
				PublicRootDomain:      "sandbox0.app",
				PublicRegionID:        "aws-us-east-1",
			},
			ProxyTimeout: metav1.Duration{Duration: 10 * time.Second},
		},
		logger:                zap.NewNop(),
		managerClient:         client.NewManagerClient(manager.URL, gen, zap.NewNop(), time.Second),
		internalAuthGen:       gen,
		sandboxServiceLimiter: ratelimit.NewMemoryLimiter(ratelimit.MemoryConfig{}),
	}
	router := gin.New()
	router.NoRoute(s.handlePublicExposureNoRoute)
	return router
}

func serverPort(t *testing.T, rawURL string) int {
	t.Helper()
	parsed, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	_, portString, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatalf("split host port: %v", err)
	}
	port, err := strconv.Atoi(portString)
	if err != nil {
		t.Fatalf("parse port: %v", err)
	}
	return port
}

func newGatewayRequest(t *testing.T, method, baseURL, host, path string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(method, baseURL+path, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Host = host
	return req
}

func sha256Hex(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}
