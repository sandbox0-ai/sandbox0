package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"go.uber.org/zap"
)

func TestSetupMeteringRoutesMountsInternalEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "")
	server.setupMeteringRoutes()

	if !hasRoute(server.router, "GET", "/internal/v1/metering/status") {
		t.Fatal("expected metering status route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/events") {
		t.Fatal("expected metering events route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/windows") {
		t.Fatal("expected metering windows route to be mounted")
	}
}

func TestSetupRoutesMountsMeteringEndpointsInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/internal/v1/metering/status") {
		t.Fatal("expected metering status route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/events") {
		t.Fatal("expected metering events route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/windows") {
		t.Fatal("expected metering windows route to be mounted")
	}
}

func TestSetupRoutesSkipsControlPlaneEndpointsInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if hasRoute(server.router, "GET", "/internal/v1/sandboxes/:id") {
		t.Fatal("expected public mode to skip internal sandbox metadata route")
	}
	if hasRoute(server.router, "POST", "/internal/v1/sandboxes/:id/resume") {
		t.Fatal("expected public mode to skip internal sandbox resume route")
	}
	if hasRoute(server.router, "PUT", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected public mode to skip internal quota write route")
	}
}

func TestSetupRoutesMountsSandboxObservabilityIngestInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	for _, path := range []string{
		"/internal/v1/sandbox-observability/events",
		"/internal/v1/sandbox-observability/logs",
		"/internal/v1/sandbox-observability/metrics",
	} {
		if !hasRoute(server.router, "POST", path) {
			t.Fatalf("expected public mode to mount sandbox observability ingest route %s", path)
		}
	}
}

func TestSetupRoutesMountsControlPlaneEndpointsInInternalMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/internal/v1/sandboxes/:id") {
		t.Fatal("expected internal mode to mount internal sandbox metadata route")
	}
	if !hasRoute(server.router, "POST", "/internal/v1/sandboxes/:id/resume") {
		t.Fatal("expected internal mode to mount internal sandbox resume route")
	}
	if !hasRoute(server.router, "PUT", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected internal mode to mount internal quota write route")
	}
	if !hasRoute(server.router, "DELETE", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected internal mode to mount internal quota delete route")
	}
}

func TestSetupRoutesMountsMetadataEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/metadata") {
		t.Fatal("expected /metadata route to be mounted")
	}
}

func TestSetupRoutesMountsSandboxLogsEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/api/v1/sandboxes/:id/logs") {
		t.Fatal("expected sandbox logs route to be mounted")
	}
}

func TestSetupRoutesMountsSandboxStatsAndHistoricalObservabilityEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	for _, path := range []string{
		"/api/v1/sandboxes/:id/stats",
		"/api/v1/sandboxes/:id/observability/events",
		"/api/v1/sandboxes/:id/observability/logs",
		"/api/v1/sandboxes/:id/observability/metrics",
		"/api/v1/sandboxes/:id/audit/events",
	} {
		if !hasRoute(server.router, "GET", path) {
			t.Fatalf("expected route %s to be mounted", path)
		}
	}
}

func TestSandboxObservabilityRouteDoesNotRequireManagerUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, authModeInternal)
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()
	if !hasRoute(server.router, "POST", "/internal/v1/sandbox-observability/events") {
		t.Fatal("expected sandbox observability ingest route to be mounted")
	}

	token, err := generator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxRead},
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb-1/observability/events", nil)
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "sandbox observability backend is disabled") {
		t.Fatalf("response = %s, want disabled backend error", rec.Body.String())
	}
}

func TestSetupRoutesExposesQuotaReadOnlyPublicAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/api/v1/quotas/:dimension") {
		t.Fatal("expected public quota read route")
	}
	if hasRoute(server.router, "PUT", "/api/v1/quotas/:dimension") {
		t.Fatal("expected public quota put route to be absent")
	}
	if hasRoute(server.router, "DELETE", "/api/v1/quotas/:dimension") {
		t.Fatal("expected public quota delete route to be absent")
	}
}

func TestSetupMeteringRoutesDoesNotRequireManagerUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, "")
	server.setupMeteringRoutes()

	token, err := generator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSetupMeteringRoutesAllowsSystemAdminInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, issuer := testMeteringRouteServer(t, "public")
	server.setupMeteringRoutes()

	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", true, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSetupMeteringRoutesRejectsNonAdminInBothMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, issuer := testMeteringRouteServer(t, "both")
	server.setupMeteringRoutes()

	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func testMeteringRouteServer(t *testing.T, authMode string) (*Server, *internalauth.Generator, *authn.Issuer) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"regional-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	issuer := authn.NewIssuer("cluster-gateway", "secret", time.Minute, time.Hour)
	publicAuth := gatewaymiddleware.NewAuthMiddleware(nil, "secret", issuer, zap.NewNop())
	internalAuth := middleware.NewInternalAuthMiddleware(validator, zap.NewNop())
	netdValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"netd"},
		ClockSkewTolerance: 5 * time.Second,
	})
	netdAuth := middleware.NewInternalAuthMiddleware(netdValidator, zap.NewNop())

	server := &Server{
		router:               gin.New(),
		cfg:                  &config.ClusterGatewayConfig{AuthMode: authMode},
		authMiddleware:       internalAuth,
		netdAuthMiddleware:   netdAuth,
		publicAuth:           publicAuth,
		compositeAuth:        middleware.NewCompositeAuthMiddleware(internalAuth, publicAuth, zap.NewNop()),
		publicJWT:            issuer,
		logger:               zap.NewNop(),
		meteringHandler:      gatewayhandlers.NewMeteringHandler(nil, "aws-us-east-1", zap.NewNop()),
		observabilityHandler: gatewayhandlers.NewSandboxObservabilityHandler(nil, zap.NewNop()),
	}

	return server, generator, issuer
}

func hasRoute(router *gin.Engine, method, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}

func newTestMeteringObservability(t *testing.T) *observability.Provider {
	t.Helper()

	provider, err := observability.New(observability.Config{
		ServiceName:    "cluster-gateway-metering-route-test",
		Logger:         zap.NewNop(),
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
	return provider
}
