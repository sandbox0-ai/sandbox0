package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	registryprovider "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"go.uber.org/zap"
)

func TestRegistryCredentialsRequireTemplateWritePermission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, registrySpy, cleanup := newEdgeRegistryRouteTestServer(t)
	defer cleanup()

	t.Run("forbidden without template write", func(t *testing.T) {
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "team-1", "developer", "user@example.com", "User", false)
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", nil)
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)

		rec := httptest.NewRecorder()
		server.router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if got := registrySpy.callCount(); got != 0 {
			t.Fatalf("registry call count = %d, want 0", got)
		}
	})

	t.Run("allowed with template write", func(t *testing.T) {
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false)
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", nil)
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)

		rec := httptest.NewRecorder()
		server.router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if got := registrySpy.callCount(); got != 1 {
			t.Fatalf("registry call count = %d, want 1", got)
		}
		if got := registrySpy.teamID(); got != "team-1" {
			t.Fatalf("registry team id = %q, want %q", got, "team-1")
		}
	})
}

type registryProviderSpy struct {
	mu      sync.Mutex
	teamIDs []string
}

func (s *registryProviderSpy) GetPushCredentials(_ context.Context, teamID string) (*registryprovider.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.teamIDs = append(s.teamIDs, teamID)
	return &registryprovider.Credential{
		Provider:     "builtin",
		PushRegistry: "registry.example.com",
		Username:     "user",
		Password:     "password",
	}, nil
}

func (s *registryProviderSpy) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.teamIDs)
}

func (s *registryProviderSpy) teamID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.teamIDs) == 0 {
		return ""
	}
	return s.teamIDs[len(s.teamIDs)-1]
}

func newEdgeRegistryRouteTestServer(t *testing.T) (*Server, *registryProviderSpy, func()) {
	t.Helper()

	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("edge-gateway", "secret", time.Minute, time.Hour)
	registrySpy := &registryProviderSpy{}
	server := &Server{
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		logger:         logger,
		registry:       registrySpy,
		jwtIssuer:      jwtIssuer,
	}
	server.router = gin.New()
	api := server.router.Group("/api")
	api.Use(server.authMiddleware.Authenticate())
	registry := api.Group("/v1/registry")
	registry.POST("/credentials", server.authMiddleware.RequirePermission(authn.PermTemplateWrite), server.getRegistryCredentials)

	cleanup := func() {}
	return server, registrySpy, cleanup
}
