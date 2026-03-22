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
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestRegistryCredentialsRequireTemplateWritePermission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	baseURL, incomingGen, managerSpy, cleanup := newRegistryRouteTestServer(t)
	defer cleanup()

	t.Run("forbidden without template write", func(t *testing.T) {
		status := doInternalRegistryRequest(t, baseURL, newInternalRegistryToken(t, incomingGen, gatewayauthn.PermTemplateRead))
		if status != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", status, http.StatusForbidden)
		}
		if got := managerSpy.callCount(); got != 0 {
			t.Fatalf("manager call count = %d, want 0", got)
		}
	})

	t.Run("allowed with template write", func(t *testing.T) {
		status := doInternalRegistryRequest(t, baseURL, newInternalRegistryToken(t, incomingGen, gatewayauthn.PermTemplateWrite))
		if status != http.StatusOK {
			t.Fatalf("status = %d, want %d", status, http.StatusOK)
		}
		if got := managerSpy.callCount(); got != 1 {
			t.Fatalf("manager call count = %d, want 1", got)
		}
		if got := managerSpy.path(); got != "/api/v1/registry/credentials" {
			t.Fatalf("manager path = %q, want %q", got, "/api/v1/registry/credentials")
		}
		if got := managerSpy.teamID(); got != "team-1" {
			t.Fatalf("manager team id = %q, want %q", got, "team-1")
		}
		if got := managerSpy.internalToken(); got == "" {
			t.Fatal("expected forwarded internal token for manager")
		}
	})
}

func doInternalRegistryRequest(t *testing.T, baseURL, token string) int {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/v1/registry/credentials", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

type registryManagerRequestSpy struct {
	mu    sync.Mutex
	calls int
	pathV string
	teamV string
	token string
}

func (s *registryManagerRequestSpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.pathV = r.URL.Path
	s.teamV = r.Header.Get(internalauth.TeamIDHeader)
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
}

func (s *registryManagerRequestSpy) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *registryManagerRequestSpy) path() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pathV
}

func (s *registryManagerRequestSpy) teamID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.teamV
}

func (s *registryManagerRequestSpy) internalToken() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token
}

func newRegistryRouteTestServer(t *testing.T) (string, *internalauth.Generator, *registryManagerRequestSpy, func()) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
	managerSpy := &registryManagerRequestSpy{}
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		managerSpy.record(r)
		w.WriteHeader(http.StatusOK)
	}))

	proxy2Mgr, err := proxy.NewRouter(manager.URL, logger, time.Second)
	if err != nil {
		manager.Close()
		t.Fatalf("create manager proxy: %v", err)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"regional-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	incomingGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{AuthMode: authModeInternal, ManagerURL: manager.URL},
		proxy2Mgr:       proxy2Mgr,
		managerClient:   &client.ManagerClient{},
		authMiddleware:  middleware.NewInternalAuthMiddleware(validator, logger),
		logger:          logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute}),
	}
	server.router = gin.New()
	v1 := server.router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	registry := v1.Group("/registry")
	registry.Use(server.managerUpstreamMiddleware())
	registry.POST("/credentials", server.authMiddleware.RequirePermission(gatewayauthn.PermTemplateWrite), server.getRegistryCredentials)
	gateway := httptest.NewServer(server.router)

	cleanup := func() {
		gateway.Close()
		manager.Close()
	}
	return gateway.URL, incomingGen, managerSpy, cleanup
}

func newInternalRegistryToken(t *testing.T, gen *internalauth.Generator, permissions ...string) string {
	t.Helper()

	token, err := gen.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: permissions,
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	return token
}
