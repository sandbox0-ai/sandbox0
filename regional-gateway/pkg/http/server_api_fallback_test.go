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
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type apiFallbackSpy struct {
	mu     sync.Mutex
	method string
	path   string
	token  string
}

func (s *apiFallbackSpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.method = r.Method
	s.path = r.URL.Path
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
}

func TestSetupRoutesFallsBackToClusterGatewayForUnmatchedAPIPaths(t *testing.T) {
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

	spy := &apiFallbackSpy{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy.record(r)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer target.Close()

	clusterGatewayRouter, err := proxy.NewRouter(target.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}

	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: clusterGatewayRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		rateLimiter:          gatewaymiddleware.NewRateLimiter(100, 200, time.Minute, logger),
		requestLogger:        gatewaymiddleware.NewRequestLogger(logger),
		logger:               logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "regional-gateway",
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		obsProvider: obsProvider,
		jwtIssuer:   jwtIssuer,
	}

	server.setupRoutes()
	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/workspaces", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusOK, string(body))
	}
	if spy.method != http.MethodGet {
		t.Fatalf("method = %q, want %q", spy.method, http.MethodGet)
	}
	if spy.path != "/api/v1/workspaces" {
		t.Fatalf("path = %q, want %q", spy.path, "/api/v1/workspaces")
	}
	if spy.token == "" {
		t.Fatal("expected forwarded internal token")
	}
}
