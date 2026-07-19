package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type sandboxObservabilityProxySpy struct {
	mu     sync.Mutex
	path   string
	token  string
	teamID string
}

func (s *sandboxObservabilityProxySpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path = r.URL.Path
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
	s.teamID = r.Header.Get(internalauth.TeamIDHeader)
}

func TestSandboxObservabilityRoutesProxyToOwningCluster(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logger := zap.NewNop()
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "regional-gateway-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
		TraceExporter: observability.TraceExporterConfig{
			Type: "noop",
		},
	})
	if err != nil {
		t.Fatalf("create observability provider: %v", err)
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	clusterSpy := &sandboxObservabilityProxySpy{}
	clusterTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clusterSpy.record(r)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer clusterTarget.Close()

	schedulerTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("scheduler should not receive sandbox observability query: %s", r.URL.Path)
	}))
	defer schedulerTarget.Close()

	defaultClusterTarget := httptest.NewServer(http.NotFoundHandler())
	defer defaultClusterTarget.Close()
	defaultClusterRouter, err := proxy.NewRouter(defaultClusterTarget.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create default cluster proxy: %v", err)
	}
	schedulerRouter, err := proxy.NewRouter(schedulerTarget.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create scheduler proxy: %v", err)
	}

	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	clusterID := "aws-us-east-1"
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: defaultClusterRouter,
		schedulerRouter:      schedulerRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		teamQuotaController:  newAllowingTeamQuotaController(logger),
		requestLogger:        gatewaymiddleware.NewRequestLogger(logger),
		logger:               logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "regional-gateway",
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		obsProvider:           obsProvider,
		jwtIssuer:             jwtIssuer,
		clusterGatewayProxies: make(map[string]*proxy.Router),
		clusterCache:          map[string]string{clusterID: clusterTarget.URL},
	}
	server.setupRoutes()

	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	sandboxName, err := naming.SandboxName(clusterID, "template-a", "abcde")
	if err != nil {
		t.Fatalf("sandbox name: %v", err)
	}
	tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/sandboxes/"+sandboxName+"/observability/logs", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	req.Header.Set(internalauth.TeamIDHeader, "team-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d body=%s", resp.StatusCode, string(body))
	}

	if clusterSpy.path != "/api/v1/sandboxes/"+sandboxName+"/observability/logs" {
		t.Fatalf("cluster path = %q", clusterSpy.path)
	}
	if clusterSpy.token == "" {
		t.Fatal("expected forwarded internal token")
	}
	if clusterSpy.teamID != "team-1" {
		t.Fatalf("team header = %q, want team-1", clusterSpy.teamID)
	}
}
