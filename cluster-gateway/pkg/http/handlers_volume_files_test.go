package http

import (
	"crypto/ed25519"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type volumeFileRequestSpy struct {
	mu     sync.Mutex
	method string
	path   string
	query  string
	teamID string
	token  string
	body   string
}

func (s *volumeFileRequestSpy) record(r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	s.mu.Lock()
	defer s.mu.Unlock()
	s.method = r.Method
	s.path = r.URL.Path
	s.query = r.URL.RawQuery
	s.teamID = r.Header.Get(internalauth.TeamIDHeader)
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
	s.body = string(body)
}

func newVolumeFileRouteTestServer(t *testing.T) (string, *internalauth.Generator, *volumeFileRequestSpy, func()) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
	storageSpy := &volumeFileRequestSpy{}
	storageProxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		storageSpy.record(r)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"success":true}`))
	}))

	proxy2sp, err := proxy.NewRouter(storageProxy.URL, logger, time.Second)
	if err != nil {
		storageProxy.Close()
		t.Fatalf("create storage-proxy proxy: %v", err)
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
		cfg:             &config.ClusterGatewayConfig{AuthMode: authModeInternal, StorageProxyURL: storageProxy.URL},
		proxy2sp:        proxy2sp,
		authMiddleware:  middleware.NewInternalAuthMiddleware(validator, logger),
		logger:          logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute}),
	}
	server.router = gin.New()
	v1 := server.router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	sandboxvolumes := v1.Group("/sandboxvolumes")
	sandboxvolumes.Use(server.storageProxyUpstreamMiddleware())
	{
		files := sandboxvolumes.Group("/:id/files")
		{
			files.GET("", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), server.handleVolumeFileOperation)
			files.POST("", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), server.handleVolumeFileOperation)
			files.DELETE("", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), server.handleVolumeFileOperation)
			files.GET("/watch", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), server.handleVolumeFileWatch)
			files.POST("/move", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileWrite), server.handleVolumeFileMove)
			files.GET("/stat", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), server.handleVolumeFileStat)
			files.GET("/list", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeFileRead), server.handleVolumeFileList)
		}
	}

	gateway := httptest.NewServer(server.router)
	cleanup := func() {
		gateway.Close()
		storageProxy.Close()
	}
	return gateway.URL, incomingGen, storageSpy, cleanup
}

func newStorageProxyRouteInternalToken(t *testing.T, gen *internalauth.Generator, permissions ...string) string {
	t.Helper()

	token, err := gen.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: permissions,
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	return token
}

func TestVolumeFileRoutesProxyToStorageProxy(t *testing.T) {
	gatewayURL, incomingGen, storageSpy, cleanup := newVolumeFileRouteTestServer(t)
	defer cleanup()

	tests := []struct {
		name         string
		method       string
		path         string
		body         string
		permission   string
		wantPath     string
		wantQuery    string
		wantBodyPart string
	}{
		{
			name:       "read file preserves query",
			method:     http.MethodGet,
			path:       "/api/v1/sandboxvolumes/vol-1/files?path=/docs/readme.txt",
			permission: gatewayauthn.PermSandboxVolumeFileRead,
			wantPath:   "/sandboxvolumes/vol-1/files",
			wantQuery:  "path=/docs/readme.txt",
		},
		{
			name:         "write file preserves body",
			method:       http.MethodPost,
			path:         "/api/v1/sandboxvolumes/vol-1/files?path=/docs/readme.txt",
			body:         "hello",
			permission:   gatewayauthn.PermSandboxVolumeFileWrite,
			wantPath:     "/sandboxvolumes/vol-1/files",
			wantQuery:    "path=/docs/readme.txt",
			wantBodyPart: "hello",
		},
		{
			name:       "delete file preserves query",
			method:     http.MethodDelete,
			path:       "/api/v1/sandboxvolumes/vol-1/files?path=/docs/readme.txt",
			permission: gatewayauthn.PermSandboxVolumeFileWrite,
			wantPath:   "/sandboxvolumes/vol-1/files",
			wantQuery:  "path=/docs/readme.txt",
		},
		{
			name:         "move file proxies json body",
			method:       http.MethodPost,
			path:         "/api/v1/sandboxvolumes/vol-1/files/move",
			body:         `{"source":"/a.txt","destination":"/b.txt"}`,
			permission:   gatewayauthn.PermSandboxVolumeFileWrite,
			wantPath:     "/sandboxvolumes/vol-1/files/move",
			wantBodyPart: "/b.txt",
		},
		{
			name:       "stat preserves query",
			method:     http.MethodGet,
			path:       "/api/v1/sandboxvolumes/vol-1/files/stat?path=/docs/readme.txt",
			permission: gatewayauthn.PermSandboxVolumeFileRead,
			wantPath:   "/sandboxvolumes/vol-1/files/stat",
			wantQuery:  "path=/docs/readme.txt",
		},
		{
			name:       "list preserves query",
			method:     http.MethodGet,
			path:       "/api/v1/sandboxvolumes/vol-1/files/list?path=/docs",
			permission: gatewayauthn.PermSandboxVolumeFileRead,
			wantPath:   "/sandboxvolumes/vol-1/files/list",
			wantQuery:  "path=/docs",
		},
		{
			name:       "watch rewrites path",
			method:     http.MethodGet,
			path:       "/api/v1/sandboxvolumes/vol-1/files/watch",
			permission: gatewayauthn.PermSandboxVolumeFileRead,
			wantPath:   "/sandboxvolumes/vol-1/files/watch",
		},
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, gatewayURL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set(internalauth.DefaultTokenHeader, newStorageProxyRouteInternalToken(t, incomingGen, tt.permission))

			resp, err := client.Do(req)
			if err != nil {
				t.Fatalf("do request: %v", err)
			}
			_ = resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
			}
			if storageSpy.method != tt.method {
				t.Fatalf("method = %s, want %s", storageSpy.method, tt.method)
			}
			if storageSpy.path != tt.wantPath {
				t.Fatalf("path = %s, want %s", storageSpy.path, tt.wantPath)
			}
			if storageSpy.query != tt.wantQuery {
				t.Fatalf("query = %q, want %q", storageSpy.query, tt.wantQuery)
			}
			if storageSpy.teamID != "team-1" {
				t.Fatalf("team id = %q, want team-1", storageSpy.teamID)
			}
			if storageSpy.token == "" {
				t.Fatal("expected internal storage-proxy token")
			}
			if tt.wantBodyPart != "" && !strings.Contains(storageSpy.body, tt.wantBodyPart) {
				t.Fatalf("body = %q, want substring %q", storageSpy.body, tt.wantBodyPart)
			}
		})
	}
}
