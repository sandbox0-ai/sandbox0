package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	clustermiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestInternalFunctionRuntimeProxyRoutesToPrivateService(t *testing.T) {
	gin.SetMode(gin.TestMode)

	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(120 * time.Millisecond)
		if r.URL.Path != "/private" {
			t.Errorf("upstream path = %s", r.URL.Path)
		}
		if r.URL.RawQuery != "q=1" {
			t.Errorf("upstream query = %q", r.URL.RawQuery)
		}
		if got := r.Header.Get(internalauth.DefaultTokenHeader); got != "" {
			t.Errorf("upstream received internal token header %q", got)
		}
		if got := r.Header.Get(internalauth.TeamIDHeader); got != "" {
			t.Errorf("upstream received internal team header %q", got)
		}
		if got := r.Header.Get("X-Forwarded-For"); !strings.HasPrefix(got, "203.0.113.10, ") {
			t.Errorf("X-Forwarded-For = %q, want trusted client IP plus proxy hop", got)
		}
		if got := r.Header.Get("X-Forwarded-Host"); got != "fn.example.test" {
			t.Errorf("X-Forwarded-Host = %q, want fn.example.test", got)
		}
		if got := r.Header.Get("X-Forwarded-Proto"); got != "https" {
			t.Errorf("X-Forwarded-Proto = %q, want https", got)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "ok")
	}))
	defer app.Close()
	appPort, err := portFromURL(app.URL)
	if err != nil {
		t.Fatalf("parse app port: %v", err)
	}

	sandbox := &mgr.Sandbox{
		ID:           "sb_test",
		TeamID:       "team-1",
		InternalAddr: testInternalAddrWithPort(t, app.URL, 49983),
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: appPort,
		}},
	}
	clusterGateway, token := newInternalFunctionProxyTestGateway(t, sandbox, 50*time.Millisecond)
	defer clusterGateway.Close()

	req, err := http.NewRequest(http.MethodGet, clusterGateway.URL+"/internal/v1/functions/runtime/sandboxes/sb_test/services/api/proxy/private?q=1", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("X-Forwarded-Host", "fn.example.test")
	req.Header.Set("X-Forwarded-Proto", "https")

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusOK, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "ok" {
		t.Fatalf("body = %q, want ok", string(body))
	}
}

func TestInternalFunctionRuntimeProxySupportsWebSocket(t *testing.T) {
	gin.SetMode(gin.TestMode)

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get(internalauth.DefaultTokenHeader); got != "" {
			t.Errorf("upstream received internal token header %q", got)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("upgrade: %v", err)
			return
		}
		defer conn.Close()
		mt, payload, err := conn.ReadMessage()
		if err != nil {
			t.Errorf("read message: %v", err)
			return
		}
		if err := conn.WriteMessage(mt, append([]byte("echo:"), payload...)); err != nil {
			t.Errorf("write message: %v", err)
		}
	}))
	defer app.Close()
	appPort, err := portFromURL(app.URL)
	if err != nil {
		t.Fatalf("parse app port: %v", err)
	}

	sandbox := &mgr.Sandbox{
		ID:           "sb_test",
		TeamID:       "team-1",
		InternalAddr: testInternalAddrWithPort(t, app.URL, 49983),
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: appPort,
		}},
	}
	clusterGateway, token := newInternalFunctionProxyTestGateway(t, sandbox, time.Second, time.Second)
	defer clusterGateway.Close()

	wsURL := "ws" + strings.TrimPrefix(clusterGateway.URL, "http") + "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/proxy/ws"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{internalauth.DefaultTokenHeader: []string{token}})
	if err != nil {
		if resp != nil && resp.Body != nil {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("dial error = %v, status = %d, body = %s", err, resp.StatusCode, string(body))
		}
		t.Fatalf("dial error = %v", err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write message: %v", err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read message: %v", err)
	}
	if string(payload) != "echo:ping" {
		t.Fatalf("payload = %q", string(payload))
	}
}

func TestInternalFunctionRuntimeReadinessFallsBackToTCP(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var requests int32
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		http.NotFound(w, r)
	}))
	defer app.Close()
	appPort, err := portFromURL(app.URL)
	if err != nil {
		t.Fatalf("parse app port: %v", err)
	}

	sandbox := &mgr.Sandbox{
		ID:           "sb_test",
		TeamID:       "team-1",
		InternalAddr: testInternalAddrWithPort(t, app.URL, 49983),
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: appPort,
		}},
	}
	clusterGateway, token := newInternalFunctionProxyTestGateway(t, sandbox, time.Second, time.Second)
	defer clusterGateway.Close()

	req, err := http.NewRequest(http.MethodGet, clusterGateway.URL+"/internal/v1/functions/runtime/sandboxes/sb_test/services/api/readiness", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := clusterGateway.Client().Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusNoContent, string(body))
	}
	if got := atomic.LoadInt32(&requests); got != 0 {
		t.Fatalf("health requests = %d, want TCP fallback without HTTP requests", got)
	}
}

func TestInternalFunctionRuntimeReadinessUsesHTTPHealth(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var requests int32
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ready" {
			t.Errorf("path = %s, want /ready", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if atomic.AddInt32(&requests, 1) == 1 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer app.Close()
	appPort, err := portFromURL(app.URL)
	if err != nil {
		t.Fatalf("parse app port: %v", err)
	}

	sandbox := &mgr.Sandbox{
		ID:           "sb_test",
		TeamID:       "team-1",
		InternalAddr: testInternalAddrWithPort(t, app.URL, 49983),
		Services: []mgr.SandboxAppService{{
			ID:   "api",
			Port: appPort,
		}},
	}
	clusterGateway, token := newInternalFunctionProxyTestGateway(t, sandbox, time.Second, time.Second)
	defer clusterGateway.Close()

	readinessURL := clusterGateway.URL + "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/readiness?health_path=/ready"
	for attempt, wantStatus := range []int{http.StatusServiceUnavailable, http.StatusNoContent} {
		req, err := http.NewRequest(http.MethodGet, readinessURL, nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}
		req.Header.Set(internalauth.DefaultTokenHeader, token)
		resp, err := clusterGateway.Client().Do(req)
		if err != nil {
			t.Fatalf("Do() attempt %d error = %v", attempt+1, err)
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != wantStatus {
			t.Fatalf("attempt %d status = %d, want %d, body = %s", attempt+1, resp.StatusCode, wantStatus, string(body))
		}
	}
	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Fatalf("health requests = %d, want 2", got)
	}
}

func newInternalFunctionProxyTestGateway(t *testing.T, sandbox *mgr.Sandbox, proxyTimeout time.Duration, writeTimeoutOverride ...time.Duration) (*httptest.Server, string) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	logger := zap.NewNop()
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/"+sandbox.ID {
			http.NotFound(w, r)
			return
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			http.Error(w, "missing internal token", http.StatusUnauthorized)
			return
		}
		_ = spec.WriteSuccess(w, http.StatusOK, sandbox)
	}))
	t.Cleanup(manager.Close)

	managerProxy, err := proxy.NewRouter(manager.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create manager proxy: %v", err)
	}
	internalAuthGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceClusterGateway,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	managerClient := client.NewManagerClient(manager.URL, internalAuthGen, logger, time.Second)
	managerClient.SetHTTPClient(manager.Client())
	clusterValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceClusterGateway,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceFunctionGateway},
		ClockSkewTolerance: 5 * time.Second,
	})
	server := &Server{
		cfg: &config.ClusterGatewayConfig{
			ManagerURL:   manager.URL,
			ProxyTimeout: metav1.Duration{Duration: proxyTimeout},
		},
		proxy2Mgr:       managerProxy,
		managerClient:   managerClient,
		authMiddleware:  clustermiddleware.NewInternalAuthMiddleware(clusterValidator, logger),
		internalAuthGen: internalAuthGen,
		logger:          logger,
		httpClient:      manager.Client(),
	}
	server.router = gin.New()
	server.setupInternalControlPlaneRoutes()

	clusterGateway := httptest.NewUnstartedServer(server.router)
	writeTimeout := 50 * time.Millisecond
	if len(writeTimeoutOverride) > 0 {
		writeTimeout = writeTimeoutOverride[0]
	}
	clusterGateway.Config.WriteTimeout = writeTimeout
	clusterGateway.Start()

	functionGatewayToken, err := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     internalauth.ServiceFunctionGateway,
		PrivateKey: privateKey,
		TTL:        time.Minute,
	}).Generate(internalauth.ServiceClusterGateway, sandbox.TeamID, "user-1", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxRead},
	})
	if err != nil {
		clusterGateway.Close()
		t.Fatalf("generate function-gateway token: %v", err)
	}
	return clusterGateway, functionGatewayToken
}

func testInternalAddrWithPort(t *testing.T, raw string, port int) string {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse URL: %v", err)
	}
	parsed.Host = net.JoinHostPort(parsed.Hostname(), strconv.Itoa(port))
	return parsed.String()
}
