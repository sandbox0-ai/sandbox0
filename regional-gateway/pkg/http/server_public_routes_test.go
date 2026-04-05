package http

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"go.uber.org/zap"
)

func TestSetupPublicRoutesSelfHostedMountsIdentityAndAPIKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testPublicRouteServer(edgeAuthModeSelfHosted)
	server.setupPublicRoutes()

	if !hasRoute(server.router, "POST", "/auth/login") {
		t.Fatal("expected self-hosted mode to mount /auth/login")
	}
	if !hasRoute(server.router, "GET", "/users/me") {
		t.Fatal("expected self-hosted mode to mount /users/me")
	}
	if !hasRoute(server.router, "GET", "/teams") {
		t.Fatal("expected self-hosted mode to mount /teams")
	}
	if !hasRoute(server.router, "GET", "/api-keys") {
		t.Fatal("expected self-hosted mode to mount /api-keys")
	}
}

func TestSetupPublicRoutesFederatedMountsRegionalAPIKeysOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testPublicRouteServer(edgeAuthModeFederatedGlobal)
	server.setupPublicRoutes()

	if !hasRoute(server.router, "GET", "/api-keys") {
		t.Fatal("expected federated mode to mount /api-keys")
	}
	if hasRoute(server.router, "POST", "/auth/login") {
		t.Fatal("expected federated mode to omit /auth/login")
	}
	if hasRoute(server.router, "GET", "/users/me") {
		t.Fatal("expected federated mode to omit /users/me")
	}
	if hasRoute(server.router, "POST", "/teams") {
		t.Fatal("expected federated mode to omit /teams")
	}
}

func testPublicRouteServer(mode string) *Server {
	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("test", "secret", time.Minute, time.Hour)
	return &Server{
		router:         gin.New(),
		cfg:            &config.RegionalGatewayConfig{AuthMode: mode},
		apiKeyRepo:     &apikey.Repository{},
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		logger:         logger,
		jwtIssuer:      jwtIssuer,
	}
}

func hasRoute(router *gin.Engine, method, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}
