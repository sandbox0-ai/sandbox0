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
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/internal-gateway/pkg/middleware"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestCredentialSourcesRequireDedicatedPermissions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	baseURL, incomingGen, managerSpy, cleanup := newCredentialSourceRouteTestServer(t)
	defer cleanup()

	t.Run("get allowed with read permission", func(t *testing.T) {
		status := doInternalCredentialSourceRequest(t, baseURL, http.MethodGet, "/api/v1/credential-sources/source-a", newCredentialSourceInternalToken(t, incomingGen, gatewayauthn.PermCredentialSourceRead))
		if status != http.StatusOK {
			t.Fatalf("status = %d, want %d", status, http.StatusOK)
		}
		if got := managerSpy.path(); got != "/api/v1/credential-sources/source-a" {
			t.Fatalf("manager path = %q, want %q", got, "/api/v1/credential-sources/source-a")
		}
	})

	t.Run("delete forbidden without delete permission", func(t *testing.T) {
		status := doInternalCredentialSourceRequest(t, baseURL, http.MethodDelete, "/api/v1/credential-sources/source-a", newCredentialSourceInternalToken(t, incomingGen, gatewayauthn.PermCredentialSourceWrite))
		if status != http.StatusForbidden {
			t.Fatalf("status = %d, want %d", status, http.StatusForbidden)
		}
	})
}

func doInternalCredentialSourceRequest(t *testing.T, baseURL, method, path, token string) int {
	t.Helper()

	req, err := http.NewRequest(method, baseURL+path, nil)
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

type credentialSourceManagerRequestSpy struct {
	mu    sync.Mutex
	pathV string
}

func (s *credentialSourceManagerRequestSpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pathV = r.URL.Path
}

func (s *credentialSourceManagerRequestSpy) path() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pathV
}

func newCredentialSourceRouteTestServer(t *testing.T) (string, *internalauth.Generator, *credentialSourceManagerRequestSpy, func()) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
	managerSpy := &credentialSourceManagerRequestSpy{}
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
		Target:             "internal-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"edge-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	incomingGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "edge-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	server := &Server{
		cfg:             &config.InternalGatewayConfig{AuthMode: authModeInternal, ManagerURL: manager.URL},
		proxy2Mgr:       proxy2Mgr,
		managerClient:   &client.ManagerClient{},
		authMiddleware:  middleware.NewInternalAuthMiddleware(validator, logger),
		logger:          logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "internal-gateway", PrivateKey: privateKey, TTL: time.Minute}),
	}
	server.router = gin.New()
	v1 := server.router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	credentialSources := v1.Group("/credential-sources")
	credentialSources.Use(server.managerUpstreamMiddleware())
	credentialSources.GET("/:name", server.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceRead), server.getCredentialSource)
	credentialSources.DELETE("/:name", server.authMiddleware.RequirePermission(gatewayauthn.PermCredentialSourceDelete), server.deleteCredentialSource)
	gateway := httptest.NewServer(server.router)

	cleanup := func() {
		gateway.Close()
		manager.Close()
	}
	return gateway.URL, incomingGen, managerSpy, cleanup
}

func newCredentialSourceInternalToken(t *testing.T, gen *internalauth.Generator, permissions ...string) string {
	t.Helper()

	token, err := gen.Generate("internal-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: permissions,
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	return token
}
