package http

import (
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/ratelimit"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
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
