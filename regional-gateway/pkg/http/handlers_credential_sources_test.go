package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type edgeStaticAPIKeyValidator struct {
	key *apikey.APIKey
}

func (v edgeStaticAPIKeyValidator) ValidateAPIKey(context.Context, string) (*apikey.APIKey, error) {
	return v.key, nil
}

func TestCredentialSourcesRequireDedicatedPermissionsAtEdge(t *testing.T) {
	gin.SetMode(gin.TestMode)

	baseURL, server, targetSpy, cleanup := newEdgeCredentialSourceRouteTestServer(t)
	defer cleanup()

	t.Run("viewer can read", func(t *testing.T) {
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "viewer"}})
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req, err := http.NewRequest(http.MethodGet, baseURL+"/api/v1/credential-sources/source-a", nil)
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
		_, _ = io.Copy(io.Discard, resp.Body)

		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
		}
		if got := targetSpy.path(); got != "/api/v1/credential-sources/source-a" {
			t.Fatalf("path = %q, want %q", got, "/api/v1/credential-sources/source-a")
		}
		if got := targetSpy.internalToken(); got == "" {
			t.Fatal("expected forwarded internal token")
		}
	})

	t.Run("viewer cannot delete", func(t *testing.T) {
		tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "viewer"}})
		if err != nil {
			t.Fatalf("issue token pair: %v", err)
		}

		req, err := http.NewRequest(http.MethodDelete, baseURL+"/api/v1/credential-sources/source-a", nil)
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
		_, _ = io.Copy(io.Discard, resp.Body)

		if resp.StatusCode != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
		}
	})
}

func TestCredentialSourcesRejectPlatformAPIKeyWithoutSelectedTeam(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logger := zap.NewNop()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	called := false
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	clusterGatewayRouter, err := proxy.NewRouter(target.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}

	server := &Server{
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(edgeStaticAPIKeyValidator{key: &apikey.APIKey{
			ID:        "key-1",
			TeamID:    "platform-owner-team",
			CreatedBy: "user-1",
			Scope:     apikey.ScopePlatform,
		}}, "secret", nil, logger),
		logger:               logger,
		clusterGatewayRouter: clusterGatewayRouter,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "regional-gateway",
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}
	server.router = gin.New()
	api := server.router.Group("/api")
	api.Use(server.authMiddleware.Authenticate())
	credentialSources := api.Group("/v1/credential-sources")
	credentialSources.Use(server.requireTeamContextForTeamScopedAPI())
	credentialSources.GET("/:name", server.authMiddleware.RequirePermission(authn.PermCredentialSourceRead), server.injectInternalToken(), server.clusterGatewayRouter.ProxyToTarget)
	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/credential-sources/source-a", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer s0_test")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
	if called {
		t.Fatal("target should not be called without selected team")
	}
}

type edgeCredentialSourceRequestSpy struct {
	mu    sync.Mutex
	pathV string
	token string
}

func (s *edgeCredentialSourceRequestSpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pathV = r.URL.Path
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
}

func (s *edgeCredentialSourceRequestSpy) path() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pathV
}

func (s *edgeCredentialSourceRequestSpy) internalToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

func newEdgeCredentialSourceRouteTestServer(t *testing.T) (string, *Server, *edgeCredentialSourceRequestSpy, func()) {
	t.Helper()

	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	targetSpy := &edgeCredentialSourceRequestSpy{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetSpy.record(r)
		w.WriteHeader(http.StatusOK)
	}))

	clusterGatewayRouter, err := proxy.NewRouter(target.URL, logger, time.Second)
	if err != nil {
		target.Close()
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}

	server := &Server{
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		logger:               logger,
		jwtIssuer:            jwtIssuer,
		clusterGatewayRouter: clusterGatewayRouter,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "regional-gateway",
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
	}
	server.router = gin.New()
	api := server.router.Group("/api")
	api.Use(server.authMiddleware.Authenticate())
	credentialSources := api.Group("/v1/credential-sources")
	credentialSources.GET("/:name", server.authMiddleware.RequirePermission(authn.PermCredentialSourceRead), server.injectInternalToken(), server.clusterGatewayRouter.ProxyToTarget)
	credentialSources.DELETE("/:name", server.authMiddleware.RequirePermission(authn.PermCredentialSourceDelete), server.injectInternalToken(), server.clusterGatewayRouter.ProxyToTarget)
	gateway := httptest.NewServer(server.router)

	cleanup := func() {
		gateway.Close()
		target.Close()
	}
	return gateway.URL, server, targetSpy, cleanup
}
