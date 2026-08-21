package http

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type sandboxClaimManagerObservation struct {
	claims *internalauth.Claims
	body   string
	err    error
}

func TestCreateSandboxPreservesSignedIngressAndSLOHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 keypair: %v", err)
	}
	managerValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceManager,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceClusterGateway},
		ClockSkewTolerance: 5 * time.Second,
	})
	observed := make(chan sandboxClaimManagerObservation, 1)
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observation := sandboxClaimManagerObservation{}
		body, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			observation.err = readErr
		} else {
			observation.body = string(body)
			observation.claims, observation.err = managerValidator.Validate(
				r.Header.Get(internalauth.DefaultTokenHeader),
			)
		}
		observed <- observation

		w.Header().Set("Server-Timing", "sandbox0-command-ready;dur=437.250")
		w.Header().Set("Sandbox0-Command-Ready-SLO", "met")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"success":true,"data":{"sandbox_id":"sandbox-1"}}`)
	}))
	defer manager.Close()

	logger := zap.NewNop()
	managerProxy, err := proxy.NewRouter(manager.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create manager proxy: %v", err)
	}
	incomingValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceClusterGateway,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceRegionalGateway},
		ClockSkewTolerance: 5 * time.Second,
	})
	server := &Server{
		cfg:            &config.ClusterGatewayConfig{AuthMode: authModeInternal, ManagerURL: manager.URL},
		proxy2Mgr:      managerProxy,
		managerClient:  &client.ManagerClient{},
		authMiddleware: middleware.NewInternalAuthMiddleware(incomingValidator, logger),
		logger:         logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller: internalauth.ServiceClusterGateway, PrivateKey: privateKey, TTL: time.Minute,
		}),
	}
	server.router = gin.New()
	v1 := server.router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	sandboxes := v1.Group("/sandboxes")
	sandboxes.Use(server.managerUpstreamMiddleware())
	sandboxes.POST("", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxCreate), server.createSandbox)
	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	startedAt := time.Date(2026, time.August, 20, 8, 9, 10, 123456789, time.UTC)
	wantAudit := &internalauth.AuditContext{
		Actor: internalauth.AuditActor{
			Kind:       string(gatewayauthn.PrincipalKindHuman),
			ID:         "user-1",
			UserID:     "user-1",
			AuthMethod: string(gatewayauthn.AuthMethodJWT),
		},
		OperationID:      "operation-regional",
		RequestID:        "request-regional",
		Origin:           internalauth.ServiceRegionalGateway,
		IngressStartedAt: &startedAt,
	}
	incomingToken, err := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: internalauth.ServiceRegionalGateway, PrivateKey: privateKey, TTL: time.Minute,
	}).Generate(internalauth.ServiceClusterGateway, "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{gatewayauthn.PermSandboxCreate},
		Audit:       wantAudit,
	})
	if err != nil {
		t.Fatalf("generate regional token: %v", err)
	}
	requestBody := []byte(`{"template":"default","operation_id":"spoofed","started_at":"2000-01-01T00:00:00Z"}`)
	request, err := http.NewRequest(http.MethodPost, gateway.URL+"/api/v1/sandboxes", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	request.Header.Set(internalauth.DefaultTokenHeader, incomingToken)
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}

	if response.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", response.StatusCode, http.StatusCreated, responseBody)
	}
	if got := response.Header.Get("Server-Timing"); got != "sandbox0-command-ready;dur=437.250" {
		t.Fatalf("Server-Timing = %q", got)
	}
	if got := response.Header.Get("Sandbox0-Command-Ready-SLO"); got != "met" {
		t.Fatalf("Sandbox0-Command-Ready-SLO = %q", got)
	}

	observation := <-observed
	if observation.err != nil {
		t.Fatalf("observe manager request: %v", observation.err)
	}
	if observation.body != string(requestBody) {
		t.Fatalf("manager body = %q, want %q", observation.body, requestBody)
	}
	claims := observation.claims
	if claims == nil || claims.Caller != internalauth.ServiceClusterGateway ||
		claims.Target != internalauth.ServiceManager || claims.TeamID != "team-1" || claims.UserID != "user-1" {
		t.Fatalf("manager claims = %#v, want cluster-gateway delegation for team-1/user-1", claims)
	}
	if claims.Audit == nil || claims.Audit.OperationID != wantAudit.OperationID ||
		claims.Audit.RequestID != wantAudit.RequestID || claims.Audit.Origin != wantAudit.Origin ||
		claims.Audit.Actor != wantAudit.Actor || claims.Audit.IngressStartedAt == nil ||
		!claims.Audit.IngressStartedAt.Equal(startedAt) {
		t.Fatalf("manager audit = %#v, want %#v", claims.Audit, wantAudit)
	}
}
