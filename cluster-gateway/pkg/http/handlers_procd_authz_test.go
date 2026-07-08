package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestSandboxProcdRoutesRequireReadWritePermissions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	baseURL, incomingGen, cleanup := newSandboxProcdRoutePermissionTestServer(t)
	defer cleanup()

	tests := []struct {
		name        string
		method      string
		path        string
		permissions []string
	}{
		{
			name:        "context create requires sandbox write",
			method:      http.MethodPost,
			path:        "/api/v1/sandboxes/sb-1/contexts",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "context delete requires sandbox write",
			method:      http.MethodDelete,
			path:        "/api/v1/sandboxes/sb-1/contexts/ctx-1",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "context exec requires sandbox write",
			method:      http.MethodPost,
			path:        "/api/v1/sandboxes/sb-1/contexts/ctx-1/exec",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "context websocket requires sandbox write",
			method:      http.MethodGet,
			path:        "/api/v1/sandboxes/sb-1/contexts/ctx-1/ws",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "context list requires sandbox read",
			method:      http.MethodGet,
			path:        "/api/v1/sandboxes/sb-1/contexts",
			permissions: []string{gatewayauthn.PermSandboxWrite},
		},
		{
			name:        "process create requires sandbox write",
			method:      http.MethodPost,
			path:        "/api/v1/sandboxes/sb-1/processes",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "process delete requires sandbox write",
			method:      http.MethodDelete,
			path:        "/api/v1/sandboxes/sb-1/processes/proc-1",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "process input event requires sandbox write",
			method:      http.MethodPost,
			path:        "/api/v1/sandboxes/sb-1/processes/proc-1/events",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "process event stream requires sandbox read",
			method:      http.MethodGet,
			path:        "/api/v1/sandboxes/sb-1/processes/proc-1/events",
			permissions: []string{gatewayauthn.PermSandboxWrite},
		},
		{
			name:        "file write requires sandbox write",
			method:      http.MethodPost,
			path:        "/api/v1/sandboxes/sb-1/files?path=/tmp/a.txt",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "file delete requires sandbox write",
			method:      http.MethodDelete,
			path:        "/api/v1/sandboxes/sb-1/files?path=/tmp/a.txt",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "file move requires sandbox write",
			method:      http.MethodPost,
			path:        "/api/v1/sandboxes/sb-1/files/move",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
		{
			name:        "file list requires sandbox read",
			method:      http.MethodGet,
			path:        "/api/v1/sandboxes/sb-1/files/list?path=/tmp",
			permissions: []string{gatewayauthn.PermSandboxWrite},
		},
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, baseURL+tt.path, strings.NewReader(`{}`))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set(internalauth.DefaultTokenHeader, newSandboxProcdRouteInternalToken(t, incomingGen, tt.permissions...))

			resp, err := client.Do(req)
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
}

func newSandboxProcdRoutePermissionTestServer(t *testing.T) (string, *internalauth.Generator, func()) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
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
		cfg:            &config.ClusterGatewayConfig{AuthMode: authModeInternal},
		authMiddleware: middleware.NewInternalAuthMiddleware(validator, logger),
		logger:         logger,
	}
	server.router = gin.New()
	v1 := server.router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	sandboxes := v1.Group("/sandboxes")
	server.registerSandboxProcdRoutes(sandboxes)

	gateway := httptest.NewServer(server.router)
	return gateway.URL, incomingGen, gateway.Close
}

func newSandboxProcdRouteInternalToken(t *testing.T, gen *internalauth.Generator, permissions ...string) string {
	t.Helper()

	token, err := gen.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: permissions,
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	return token
}
