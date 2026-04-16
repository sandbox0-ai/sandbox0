package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"go.uber.org/zap"
)

func TestSetupMeteringRoutesMountsRegionScopedEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testMeteringRouteServer()
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

func TestSetupMeteringRoutesRequiresSystemAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testMeteringRouteServer()
	server.setupMeteringRoutes()

	nonAdminTokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer "+nonAdminTokens.AccessToken)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func TestSetupMeteringRoutesAllowsSystemAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testMeteringRouteServer()
	server.setupMeteringRoutes()

	adminTokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", true, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer "+adminTokens.AccessToken)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSetupMeteringRoutesAllowsPlatformAPIKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testMeteringRouteServer()
	server.authMiddleware = gatewaymiddleware.NewAuthMiddleware(staticAPIKeyValidator{key: &apikey.APIKey{
		ID:        "key-1",
		TeamID:    "team-1",
		CreatedBy: "user-1",
		Scope:     apikey.ScopePlatform,
	}}, "secret", server.jwtIssuer, zap.NewNop())
	server.setupMeteringRoutes()

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer s0_test")
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func testMeteringRouteServer() *Server {
	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	return &Server{
		router:          gin.New(),
		cfg:             &config.RegionalGatewayConfig{},
		authMiddleware:  gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		logger:          logger,
		jwtIssuer:       jwtIssuer,
		meteringHandler: gatewayhandlers.NewMeteringHandler(nil, "aws-us-east-1", logger),
	}
}

type staticAPIKeyValidator struct {
	key *apikey.APIKey
}

func (v staticAPIKeyValidator) ValidateAPIKey(context.Context, string) (*apikey.APIKey, error) {
	return v.key, nil
}
