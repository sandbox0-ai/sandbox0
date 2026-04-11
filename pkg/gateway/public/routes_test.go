package public

import (
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"go.uber.org/zap"
)

func TestRegisterIdentityRoutesOmitsRegionalAPIKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterIdentityRoutes(router, testDeps())

	if hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected identity routes to omit /api-keys")
	}
	if !hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected identity routes to include /users/me")
	}
	if !hasRoute(router, "GET", "/teams") {
		t.Fatal("expected identity routes to include /teams")
	}
}

func TestRegisterAPIKeyRoutesOnlyMountsRegionalAPIKeySurface(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterAPIKeyRoutes(router, testDeps())

	if !hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected regional routes to include /api-keys")
	}
	if !hasRoute(router, "GET", "/api-keys/current") {
		t.Fatal("expected regional routes to include /api-keys/current")
	}
	if hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected regional API key routes to omit /users/me")
	}
	if hasRoute(router, "POST", "/auth/login") {
		t.Fatal("expected regional API key routes to omit /auth/login")
	}
}

func testDeps() Deps {
	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("test", "secret", time.Minute, time.Hour)
	return Deps{
		APIKeyRepo:     &apikey.Repository{},
		AuthMiddleware: middleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		JWTIssuer:      jwtIssuer,
		Logger:         logger,
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
