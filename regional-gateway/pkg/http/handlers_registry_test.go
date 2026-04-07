package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	registryprovider "github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestRegistryCredentialsRequireRegistryWritePermission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, registrySpy, cleanup := newEdgeRegistryRouteTestServer(t)
	defer cleanup()

	t.Run("forbidden without registry write", func(t *testing.T) {
		registrySpy.reset()
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "viewer"}})
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", nil)
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
		req.Header.Set(internalauth.TeamIDHeader, "team-1")

		rec := httptest.NewRecorder()
		server.router.ServeHTTP(rec, req)

		if rec.Code != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
		}
		if got := registrySpy.callCount(); got != 0 {
			t.Fatalf("registry call count = %d, want 0", got)
		}
	})

	t.Run("allowed with builder role", func(t *testing.T) {
		registrySpy.reset()
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "builder"}})
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", strings.NewReader(`{"targetImage":"my-app:v1"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
		req.Header.Set(internalauth.TeamIDHeader, "team-1")

		rec := httptest.NewRecorder()
		server.router.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if got := registrySpy.callCount(); got != 1 {
			t.Fatalf("registry call count = %d, want 1", got)
		}
	})

	t.Run("allowed with developer role", func(t *testing.T) {
		registrySpy.reset()
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "developer"}})
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", strings.NewReader(`{"targetImage":"my-app:v1"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
		req.Header.Set(internalauth.TeamIDHeader, "team-1")

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
		if got := registrySpy.targetImage(); got != "my-app:v1" {
			t.Fatalf("registry target image = %q, want %q", got, "my-app:v1")
		}
	})

	t.Run("invalid json is rejected", func(t *testing.T) {
		registrySpy.reset()
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "builder"}})
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", strings.NewReader("{"))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
		req.Header.Set(internalauth.TeamIDHeader, "team-1")

		rec := httptest.NewRecorder()
		server.router.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusBadRequest)
		}
	})
}

type registryProviderSpy struct {
	mu       sync.Mutex
	requests []registryprovider.PushCredentialsRequest
}

func (s *registryProviderSpy) GetPushCredentials(_ context.Context, req registryprovider.PushCredentialsRequest) (*registryprovider.Credential, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, req)
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
	return len(s.requests)
}

func (s *registryProviderSpy) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = nil
}

func (s *registryProviderSpy) teamID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return ""
	}
	return s.requests[len(s.requests)-1].TeamID
}

func (s *registryProviderSpy) targetImage() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.requests) == 0 {
		return ""
	}
	return s.requests[len(s.requests)-1].TargetImage
}

func newEdgeRegistryRouteTestServer(t *testing.T) (*Server, *registryProviderSpy, func()) {
	t.Helper()

	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
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
	registry.POST("/credentials", server.authMiddleware.RequirePermission(authn.PermRegistryWrite), server.getRegistryCredentials)

	cleanup := func() {}
	return server, registrySpy, cleanup
}
