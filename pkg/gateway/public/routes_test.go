package public

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestRegisterRoutesMountsSelfHostedPublicSurface(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterRoutes(router, testDeps())

	if !hasRoute(router, "POST", "/auth/login") {
		t.Fatal("expected full public routes to include /auth/login")
	}
	if !hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected full public routes to include /users/me")
	}
	if !hasRoute(router, "GET", "/teams") {
		t.Fatal("expected full public routes to include /teams")
	}
	if !hasRoute(router, "PUT", "/teams/:id/owner") {
		t.Fatal("expected full public routes to include team owner transfer")
	}
	if !hasRoute(router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected full public routes to include SSH key list")
	}
	if !hasRoute(router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected full public routes to include SSH key create")
	}
	if !hasRoute(router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected full public routes to include SSH key delete")
	}
	if !hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected full public routes to include /api-keys")
	}
}

func TestRegisterIdentityRoutesOmitsRegionalStateRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterIdentityRoutes(router, testDeps())

	if hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected identity routes to omit /api-keys")
	}
	if hasRoute(router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected identity routes to omit SSH key list")
	}
	if hasRoute(router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected identity routes to omit SSH key create")
	}
	if hasRoute(router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected identity routes to omit SSH key delete")
	}
	if !hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected identity routes to include /users/me")
	}
	if !hasRoute(router, "GET", "/teams") {
		t.Fatal("expected identity routes to include /teams")
	}
	if !hasRoute(router, "PUT", "/teams/:id/owner") {
		t.Fatal("expected identity routes to include team owner transfer")
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

func TestRegisterAPIKeyRoutesRejectViewerJWTForManagement(t *testing.T) {
	gin.SetMode(gin.TestMode)

	deps := testDeps()
	router := gin.New()
	RegisterAPIKeyRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair("user-1", "viewer@example.com", "Viewer", false, []authn.TeamGrant{
		{TeamID: "team-1", TeamRole: "viewer", HomeRegionID: "aws-us-east-1"},
	})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	tests := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/api-keys"},
		{method: http.MethodPost, path: "/api-keys", body: `{"name":"viewer-key","roles":["viewer"]}`},
		{method: http.MethodDelete, path: "/api-keys/key-1"},
		{method: http.MethodPost, path: "/api-keys/key-1/deactivate"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
			req.Header.Set(internalauth.TeamIDHeader, "team-1")

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
}

func TestRegisterUserSSHKeyRoutesMountsSSHKeysOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterUserSSHKeyRoutes(router, testDeps())

	if !hasRoute(router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected SSH key routes to include list")
	}
	if !hasRoute(router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected SSH key routes to include create")
	}
	if !hasRoute(router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected SSH key routes to include delete")
	}
	if hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected SSH key routes to omit full user profile")
	}
	if hasRoute(router, "POST", "/auth/login") {
		t.Fatal("expected SSH key routes to omit /auth/login")
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
