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
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type edgeVolumeSyncRequestSpy struct {
	mu     sync.Mutex
	method string
	path   string
	query  string
	token  string
}

func (s *edgeVolumeSyncRequestSpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.method = r.Method
	s.path = r.URL.Path
	s.query = r.URL.RawQuery
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
}

func newEdgeVolumeSyncRouteTestServer(t *testing.T) (string, *Server, *edgeVolumeSyncRequestSpy, func()) {
	t.Helper()

	gin.SetMode(gin.TestMode)

	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	targetSpy := &edgeVolumeSyncRequestSpy{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetSpy.record(r)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"success":true}`)
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
	api.Use(server.injectInternalToken())
	api.Any("/*path", server.clusterGatewayRouter.ProxyToTarget)
	gateway := httptest.NewServer(server.router)

	cleanup := func() {
		gateway.Close()
		target.Close()
	}
	return gateway.URL, server, targetSpy, cleanup
}

func TestVolumeSyncRoutesProxyThroughDefaultClusterGateway(t *testing.T) {
	baseURL, server, targetSpy, cleanup := newEdgeVolumeSyncRouteTestServer(t)
	defer cleanup()

	tests := []struct {
		name     string
		method   string
		path     string
		wantPath string
		wantQ    string
	}{
		{
			name:     "upsert replica",
			method:   http.MethodPut,
			path:     "/api/v1/sandboxvolumes/vol-1/sync/replicas/replica-1",
			wantPath: "/api/v1/sandboxvolumes/vol-1/sync/replicas/replica-1",
		},
		{
			name:     "list changes with query",
			method:   http.MethodGet,
			path:     "/api/v1/sandboxvolumes/vol-1/sync/changes?after=1&limit=5",
			wantPath: "/api/v1/sandboxvolumes/vol-1/sync/changes",
			wantQ:    "after=1&limit=5",
		},
		{
			name:     "replay payload with query",
			method:   http.MethodGet,
			path:     "/api/v1/sandboxvolumes/vol-1/sync/replay-payload?content_ref=sha256:abc",
			wantPath: "/api/v1/sandboxvolumes/vol-1/sync/replay-payload",
			wantQ:    "content_ref=sha256:abc",
		},
		{
			name:     "bootstrap archive with query",
			method:   http.MethodGet,
			path:     "/api/v1/sandboxvolumes/vol-1/sync/bootstrap/archive?snapshot_id=snap-1",
			wantPath: "/api/v1/sandboxvolumes/vol-1/sync/bootstrap/archive",
			wantQ:    "snapshot_id=snap-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
			if err != nil {
				t.Fatalf("issue token pair: %v", err)
			}

			req, err := http.NewRequest(tt.method, baseURL+tt.path, nil)
			if err != nil {
				t.Fatalf("create request: %v", err)
			}
			req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			defer resp.Body.Close()
			_, _ = io.Copy(io.Discard, resp.Body)

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
			}
			if targetSpy.method != tt.method {
				t.Fatalf("method = %q, want %q", targetSpy.method, tt.method)
			}
			if targetSpy.path != tt.wantPath {
				t.Fatalf("path = %q, want %q", targetSpy.path, tt.wantPath)
			}
			if targetSpy.query != tt.wantQ {
				t.Fatalf("query = %q, want %q", targetSpy.query, tt.wantQ)
			}
			if targetSpy.token == "" {
				t.Fatal("expected forwarded internal token")
			}
		})
	}
}
