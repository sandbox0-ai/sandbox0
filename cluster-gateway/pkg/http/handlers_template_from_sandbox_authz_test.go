package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestTemplateFromSandboxRequiresCreateAndSourceReadPermissions(t *testing.T) {
	gin.SetMode(gin.TestMode)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	managerValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         "manager",
		PublicKey:      publicKey,
		AllowedCallers: []string{"cluster-gateway"},
	})
	var (
		managerMu     sync.Mutex
		managerCalls  int
		managerPath   string
		managerClaims *internalauth.Claims
	)
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, validateErr := managerValidator.Validate(r.Header.Get(internalauth.DefaultTokenHeader))
		if validateErr != nil {
			http.Error(w, validateErr.Error(), http.StatusUnauthorized)
			return
		}
		managerMu.Lock()
		managerCalls++
		managerPath = r.URL.Path
		managerClaims = claims
		managerMu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer manager.Close()

	logger := zap.NewNop()
	managerProxy, err := proxy.NewRouter(manager.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create manager proxy: %v", err)
	}
	incomingValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         "cluster-gateway",
		PublicKey:      publicKey,
		AllowedCallers: []string{"regional-gateway"},
	})
	incomingGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	server := &Server{
		proxy2Mgr:       managerProxy,
		authMiddleware:  middleware.NewInternalAuthMiddleware(incomingValidator, logger),
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute}),
		logger:          logger,
	}
	router := gin.New()
	v1 := router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	v1.POST(
		"/templates/from-sandbox",
		server.authMiddleware.RequirePermission(gatewayauthn.PermTemplateCreate),
		server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead),
		server.proxyTemplateFromSandbox,
	)
	gateway := httptest.NewServer(router)
	defer gateway.Close()

	tests := []struct {
		name        string
		permissions []string
		wantStatus  int
	}{
		{
			name:        "both permissions",
			permissions: []string{gatewayauthn.PermTemplateCreate, gatewayauthn.PermSandboxRead},
			wantStatus:  http.StatusOK,
		},
		{
			name:        "create only",
			permissions: []string{gatewayauthn.PermTemplateCreate},
			wantStatus:  http.StatusForbidden,
		},
		{
			name:        "source read only",
			permissions: []string{gatewayauthn.PermSandboxRead},
			wantStatus:  http.StatusForbidden,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := incomingGenerator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
				Permissions: tt.permissions,
			})
			if err != nil {
				t.Fatalf("generate token: %v", err)
			}
			req, err := http.NewRequest(http.MethodPost, gateway.URL+"/api/v1/templates/from-sandbox", nil)
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
			if resp.StatusCode != tt.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tt.wantStatus)
			}
		})
	}

	managerMu.Lock()
	defer managerMu.Unlock()
	if managerCalls != 1 {
		t.Fatalf("manager calls = %d, want 1", managerCalls)
	}
	if managerPath != "/api/v1/templates/from-sandbox" {
		t.Fatalf("manager path = %q, want /api/v1/templates/from-sandbox", managerPath)
	}
	wantPermissions := []string{gatewayauthn.PermTemplateCreate, gatewayauthn.PermSandboxRead}
	if managerClaims == nil || !reflect.DeepEqual(managerClaims.Permissions, wantPermissions) {
		t.Fatalf("manager token permissions = %v, want %v", managerClaims, wantPermissions)
	}
}
