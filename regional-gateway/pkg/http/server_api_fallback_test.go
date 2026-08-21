package http

import (
	"bytes"
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
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type apiFallbackSpy struct {
	mu     sync.Mutex
	method string
	path   string
	token  string
	teamID string
}

type sandboxClaimFallbackObservation struct {
	claims       *internalauth.Claims
	receivedAt   time.Time
	forwardedJWT string
	body         string
	err          error
}

func (s *apiFallbackSpy) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.method = r.Method
	s.path = r.URL.Path
	s.token = r.Header.Get(internalauth.DefaultTokenHeader)
	s.teamID = r.Header.Get(internalauth.TeamIDHeader)
}

func TestSetupRoutesFallsBackToClusterGatewayForUnmatchedAPIPaths(t *testing.T) {
	gin.SetMode(gin.TestMode)

	spy := &apiFallbackSpy{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy.record(r)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer target.Close()

	gateway, accessToken, _ := newAPIFallbackTestGateway(t, target.URL)

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/workspaces", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set(internalauth.TeamIDHeader, "team-1")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusOK, string(body))
	}
	if spy.method != http.MethodGet {
		t.Fatalf("method = %q, want %q", spy.method, http.MethodGet)
	}
	if spy.path != "/api/v1/workspaces" {
		t.Fatalf("path = %q, want %q", spy.path, "/api/v1/workspaces")
	}
	if spy.token == "" {
		t.Fatal("expected forwarded internal token")
	}
}

func TestSetupRoutesSandboxClaimSignsTrustedIngressAndPreservesSLOHeaders(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const spoofedStartedAt = "2000-01-01T00:00:00Z"
	requestBody := []byte(`{"template":"default","operation_id":"spoofed-operation","started_at":"` + spoofedStartedAt + `"}`)
	observed := make(chan sandboxClaimFallbackObservation, 1)
	var validator *internalauth.Validator
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observation := sandboxClaimFallbackObservation{
			receivedAt:   time.Now().UTC(),
			forwardedJWT: r.Header.Get(internalauth.DefaultTokenHeader),
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			observation.err = err
		} else {
			observation.body = string(body)
			observation.claims, observation.err = validator.Validate(observation.forwardedJWT)
		}
		observed <- observation

		w.Header().Set("Server-Timing", "sandbox0-command-ready;dur=412.500")
		w.Header().Set("Sandbox0-Command-Ready-SLO", "met")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, `{"success":true,"data":{"sandbox_id":"sandbox-1"}}`)
	}))
	defer target.Close()

	gateway, accessToken, publicKey := newAPIFallbackTestGateway(t, target.URL)
	validator = internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             internalauth.ServiceClusterGateway,
		PublicKey:          publicKey,
		AllowedCallers:     []string{internalauth.ServiceRegionalGateway},
		ClockSkewTolerance: 5 * time.Second,
	})

	startedRequestAt := time.Now().UTC()
	req, err := http.NewRequest(http.MethodPost, gateway.URL+"/api/v1/sandboxes", bytes.NewReader(requestBody))
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set(internalauth.TeamIDHeader, "team-1")
	req.Header.Set(internalauth.DefaultTokenHeader, "attacker-controlled-token")
	req.Header.Set("X-Sandbox0-Claim-Ingress-Started-At", spoofedStartedAt)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	completedRequestAt := time.Now().UTC()

	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", resp.StatusCode, http.StatusCreated, responseBody)
	}
	if got := resp.Header.Get("Server-Timing"); got != "sandbox0-command-ready;dur=412.500" {
		t.Fatalf("Server-Timing = %q", got)
	}
	if got := resp.Header.Get("Sandbox0-Command-Ready-SLO"); got != "met" {
		t.Fatalf("Sandbox0-Command-Ready-SLO = %q", got)
	}

	observation := <-observed
	if observation.err != nil {
		t.Fatalf("validate forwarded claim: %v", observation.err)
	}
	if observation.forwardedJWT == "" || observation.forwardedJWT == "attacker-controlled-token" {
		t.Fatalf("forwarded internal token = %q, want a gateway-signed replacement", observation.forwardedJWT)
	}
	if observation.body != string(requestBody) {
		t.Fatalf("forwarded body = %q, want %q", observation.body, requestBody)
	}
	claims := observation.claims
	if claims == nil || claims.TeamID != "team-1" || claims.UserID != "user-1" {
		t.Fatalf("forwarded claims = %#v, want team-1/user-1", claims)
	}
	if claims.Audit == nil || claims.Audit.IngressStartedAt == nil {
		t.Fatalf("forwarded audit = %#v, want signed ingress timestamp", claims.Audit)
	}
	if claims.Audit.Origin != internalauth.ServiceRegionalGateway ||
		claims.Audit.OperationID == "" || claims.Audit.RequestID == "" {
		t.Fatalf("forwarded audit = %#v, want regional origin and correlation IDs", claims.Audit)
	}
	if claims.Audit.OperationID == "spoofed-operation" || claims.Audit.IngressStartedAt.Format(time.RFC3339) == spoofedStartedAt {
		t.Fatalf("forwarded audit trusted client input: %#v", claims.Audit)
	}
	ingressStartedAt := claims.Audit.IngressStartedAt.UTC()
	if ingressStartedAt.Before(startedRequestAt) || ingressStartedAt.After(observation.receivedAt) ||
		ingressStartedAt.After(completedRequestAt) {
		t.Fatalf("signed ingress timestamp = %s, request interval = [%s, %s], upstream received = %s",
			ingressStartedAt, startedRequestAt, completedRequestAt, observation.receivedAt)
	}
}

func newAPIFallbackTestGateway(t *testing.T, targetURL string) (*httptest.Server, string, ed25519.PublicKey) {
	t.Helper()
	logger := zap.NewNop()
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "regional-gateway-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
		TraceExporter: observability.TraceExporterConfig{
			Type: "noop",
		},
	})
	if err != nil {
		t.Fatalf("create observability provider: %v", err)
	}

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	clusterGatewayRouter, err := proxy.NewRouter(targetURL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}

	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: clusterGatewayRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		rateLimiter:          newTestRateLimiter(t),
		requestLogger:        gatewaymiddleware.NewRequestLogger(logger),
		logger:               logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		obsProvider: obsProvider,
		jwtIssuer:   jwtIssuer,
	}
	server.setupRoutes()

	gateway := httptest.NewServer(server.router)
	t.Cleanup(gateway.Close)
	tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	return gateway, tokens.AccessToken, publicKey
}

func TestSetupRoutesWithSchedulerRegistersSandboxRoutesWithoutConflict(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logger := zap.NewNop()
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "regional-gateway-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
		TraceExporter: observability.TraceExporterConfig{
			Type: "noop",
		},
	})
	if err != nil {
		t.Fatalf("create observability provider: %v", err)
	}

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	clusterGatewayTarget := httptest.NewServer(http.NotFoundHandler())
	defer clusterGatewayTarget.Close()
	schedulerTarget := httptest.NewServer(http.NotFoundHandler())
	defer schedulerTarget.Close()

	clusterGatewayRouter, err := proxy.NewRouter(clusterGatewayTarget.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}
	schedulerRouter, err := proxy.NewRouter(schedulerTarget.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create scheduler proxy: %v", err)
	}

	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: clusterGatewayRouter,
		schedulerRouter:      schedulerRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		rateLimiter:          newTestRateLimiter(t),
		requestLogger:        gatewaymiddleware.NewRequestLogger(logger),
		logger:               logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     "regional-gateway",
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		obsProvider: obsProvider,
		jwtIssuer:   jwtIssuer,
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("setupRoutes panicked: %v", r)
		}
	}()

	server.setupRoutes()
}
