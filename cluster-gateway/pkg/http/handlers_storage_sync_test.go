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

type storageProxyRequestSpy struct {
	mu     sync.Mutex
	method string
	path   string
	query  string
	teamID string
	token  string
	body   string
}

func (s *storageProxyRequestSpy) record(r *http.Request) {
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

func newStorageProxySyncRouteTestServer(t *testing.T) (string, *internalauth.Generator, *storageProxyRequestSpy, func()) {
	t.Helper()

	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	logger := zap.NewNop()
	storageSpy := &storageProxyRequestSpy{}
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
		sync := sandboxvolumes.Group("/:id/sync")
		{
			replicas := sync.Group("/replicas")
			{
				replicas.PUT("/:replica_id", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), server.upsertSyncReplica)
				replicas.GET("/:replica_id", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), server.getSyncReplica)
				replicas.POST("/:replica_id/changes", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), server.appendSyncReplicaChanges)
				replicas.PUT("/:replica_id/cursor", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), server.updateSyncReplicaCursor)
			}
			sync.POST("/bootstrap", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), server.createSyncBootstrap)
			sync.GET("/bootstrap/archive", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), server.downloadSyncBootstrapArchive)
			sync.GET("/changes", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), server.listSyncChanges)
			conflicts := sync.Group("/conflicts")
			{
				conflicts.GET("", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeRead), server.listSyncConflicts)
				conflicts.PUT("/:conflict_id", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxVolumeWrite), server.resolveSyncConflict)
			}
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

func TestVolumeSyncRoutesProxyToStorageProxy(t *testing.T) {
	gatewayURL, incomingGen, storageSpy, cleanup := newStorageProxySyncRouteTestServer(t)
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
			name:         "upsert replica",
			method:       http.MethodPut,
			path:         "/api/v1/sandboxvolumes/vol-1/sync/replicas/replica-1",
			body:         `{"display_name":"Linux Laptop"}`,
			permission:   gatewayauthn.PermSandboxVolumeWrite,
			wantPath:     "/sandboxvolumes/vol-1/sync/replicas/replica-1",
			wantBodyPart: "Linux Laptop",
		},
		{
			name:         "append replica changes",
			method:       http.MethodPost,
			path:         "/api/v1/sandboxvolumes/vol-1/sync/replicas/replica-1/changes",
			body:         `{"request_id":"req-1","base_seq":0,"changes":[]}`,
			permission:   gatewayauthn.PermSandboxVolumeWrite,
			wantPath:     "/sandboxvolumes/vol-1/sync/replicas/replica-1/changes",
			wantBodyPart: "req-1",
		},
		{
			name:       "list changes preserves query",
			method:     http.MethodGet,
			path:       "/api/v1/sandboxvolumes/vol-1/sync/changes?after=1&limit=10",
			permission: gatewayauthn.PermSandboxVolumeRead,
			wantPath:   "/sandboxvolumes/vol-1/sync/changes",
			wantQuery:  "after=1&limit=10",
		},
		{
			name:       "download bootstrap archive preserves query",
			method:     http.MethodGet,
			path:       "/api/v1/sandboxvolumes/vol-1/sync/bootstrap/archive?snapshot_id=snap-1",
			permission: gatewayauthn.PermSandboxVolumeRead,
			wantPath:   "/sandboxvolumes/vol-1/sync/bootstrap/archive",
			wantQuery:  "snapshot_id=snap-1",
		},
		{
			name:         "resolve conflict",
			method:       http.MethodPut,
			path:         "/api/v1/sandboxvolumes/vol-1/sync/conflicts/conflict-1",
			body:         `{"status":"resolved"}`,
			permission:   gatewayauthn.PermSandboxVolumeWrite,
			wantPath:     "/sandboxvolumes/vol-1/sync/conflicts/conflict-1",
			wantBodyPart: "resolved",
		},
	}

	client := &http.Client{Timeout: 5 * time.Second}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req, err := http.NewRequest(tt.method, gatewayURL+tt.path, strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			req.Header.Set("Content-Type", "application/json")
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
