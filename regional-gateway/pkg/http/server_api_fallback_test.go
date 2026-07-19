package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	clustermiddleware "github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/tokenbucket"
	"go.uber.org/zap"
)

type apiFallbackSpy struct {
	mu     sync.Mutex
	method string
	path   string
	token  string
	teamID string
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

	spy := &apiFallbackSpy{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		spy.record(r)
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer target.Close()

	clusterGatewayRouter, err := proxy.NewRouter(target.URL, logger, time.Second)
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
		teamQuotaController:  newAllowingTeamQuotaController(logger),
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

	server.setupRoutes()
	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	tokens, err := server.jwtIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/workspaces", nil)
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

func TestAPINoRouteAccountsUploadAndDownloadAtRegionalEdge(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const (
		upload = "sandbox-volume-upload"
		reply  = "sandbox-volume-download"
	)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read proxied request body: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		if string(payload) != upload {
			t.Errorf("proxied request body = %q, want %q", payload, upload)
		}
		_, _ = io.WriteString(w, reply)
	}))
	defer target.Close()

	logger := zap.NewNop()
	controller, rateLimiter, concurrencyLimiter, networkLimiter :=
		newRecordingTeamQuotaController(logger, nil)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server, accessToken := newAPIFallbackRegionalServer(
		t,
		logger,
		target.URL,
		controller,
		privateKey,
	)
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxvolumes/volume-1/files",
		strings.NewReader(upload),
	)
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set(internalauth.TeamIDHeader, "team-1")
	response := httptest.NewRecorder()

	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusOK || response.Body.String() != reply {
		t.Fatalf(
			"response = %d %q, want 200 %q",
			response.Code,
			response.Body.String(),
			reply,
		)
	}
	if rateLimiter.Calls() != 1 {
		t.Fatalf("api_requests admissions = %d, want 1", rateLimiter.Calls())
	}
	if concurrencyLimiter.AcquireCalls() != 1 ||
		concurrencyLimiter.ReleaseCalls() != 1 {
		t.Fatalf(
			"active_request_count calls = acquire %d release %d, want 1 each",
			concurrencyLimiter.AcquireCalls(),
			concurrencyLimiter.ReleaseCalls(),
		)
	}
	if got := networkLimiter.Total(coreteamquota.KeyNetworkIngressBytes); got != int64(len(upload)) {
		t.Fatalf("ingress bytes = %d, want %d", got, len(upload))
	}
	if got := networkLimiter.Total(coreteamquota.KeyNetworkEgressBytes); got != int64(len(reply)) {
		t.Fatalf("egress bytes = %d, want %d", got, len(reply))
	}
}

func TestAPINoRouteRejectsRecognizableLongLivedRequestAtConnectionLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	var targetCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	logger := zap.NewNop()
	controller, rateLimiter, concurrencyLimiter, _ := newRecordingTeamQuotaController(
		logger,
		&coreteamquota.ConcurrencyExceededError{
			TeamID: "team-1",
			Key:    coreteamquota.KeyActiveConnectionCount,
			Limit:  1,
			Used:   1,
		},
	)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server, accessToken := newAPIFallbackRegionalServer(
		t,
		logger,
		target.URL,
		controller,
		privateKey,
	)
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/sandboxes/sandbox-1/sessions/session-1/events/stream",
		nil,
	)
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set(internalauth.TeamIDHeader, "team-1")
	response := httptest.NewRecorder()

	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if targetCalls.Load() != 0 {
		t.Fatalf("fallback target calls = %d, want 0", targetCalls.Load())
	}
	if concurrencyLimiter.AcquireCalls() != 1 {
		t.Fatalf(
			"active_connection_count admissions = %d, want 1",
			concurrencyLimiter.AcquireCalls(),
		)
	}
	if concurrencyLimiter.ReleaseCalls() != 0 {
		t.Fatalf(
			"rejected active_connection_count releases = %d, want 0",
			concurrencyLimiter.ReleaseCalls(),
		)
	}
	if rateLimiter.Calls() != 0 {
		t.Fatalf(
			"api_requests admissions after connection rejection = %d, want 0",
			rateLimiter.Calls(),
		)
	}
}

func TestAPINoRouteAcquiresLeaseForRecognizableLongLivedRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer target.Close()

	logger := zap.NewNop()
	controller, rateLimiter, concurrencyLimiter, _ :=
		newRecordingTeamQuotaController(logger, nil)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	server, accessToken := newAPIFallbackRegionalServer(
		t,
		logger,
		target.URL,
		controller,
		privateKey,
	)
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/sandboxes/sandbox-1/sessions/session-1/events/stream",
		nil,
	)
	request.Header.Set("Authorization", "Bearer "+accessToken)
	request.Header.Set(internalauth.TeamIDHeader, "team-1")
	response := httptest.NewRecorder()

	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204; body=%s", response.Code, response.Body.String())
	}
	if rateLimiter.Calls() != 1 {
		t.Fatalf("api_requests admissions = %d, want 1", rateLimiter.Calls())
	}
	if concurrencyLimiter.AcquireCalls() != 2 ||
		concurrencyLimiter.ReleaseCalls() != 2 {
		t.Fatalf(
			"active connection and request calls = acquire %d release %d, want 2 each",
			concurrencyLimiter.AcquireCalls(),
			concurrencyLimiter.ReleaseCalls(),
		)
	}
}

func TestMultiClusterAPIRequestChargesAtRegionalEntrypointOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logger := zap.NewNop()
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "regional-gateway-team-quota-topology-test",
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
	clusterController, clusterRateLimiter, clusterConcurrencyLimiter, clusterNetworkLimiter :=
		newRecordingTeamQuotaController(logger, nil)
	clusterInternalAuth := clustermiddleware.NewInternalAuthMiddleware(
		internalauth.NewValidator(internalauth.ValidatorConfig{
			Target:             internalauth.ServiceClusterGateway,
			PublicKey:          publicKey,
			AllowedCallers:     []string{internalauth.ServiceRegionalGateway},
			ClockSkewTolerance: 5 * time.Second,
		}),
		logger,
	)
	clusterRouter := gin.New()
	clusterRouter.Use(
		clustermiddleware.NewCompositeAuthMiddleware(
			clusterInternalAuth,
			nil,
			logger,
		).Authenticate(),
	)
	clusterRouter.Use(clusterController.ConsumeForwardedAdmissionProof())
	clusterRouter.Use(clusterController.LimitNetworkTraffic(true))
	clusterRouter.Use(clusterController.AdmitLongLivedConnections(true))
	clusterRouter.Use(clusterController.AdmitActiveRequests(true))
	clusterRouter.Use(clusterController.RateLimitAPIRequests(true))
	clusterRouter.Any("/*path", func(c *gin.Context) {
		payload, err := io.ReadAll(c.Request.Body)
		if err != nil {
			t.Errorf("read cluster request body: %v", err)
			c.Status(http.StatusInternalServerError)
			return
		}
		_, _ = c.Writer.Write(append([]byte("reply:"), payload...))
	})
	clusterTarget := httptest.NewServer(clusterRouter)
	defer clusterTarget.Close()

	clusterGatewayRouter, err := proxy.NewRouter(clusterTarget.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}
	regionalController, regionalRateLimiter, regionalConcurrencyLimiter, regionalNetworkLimiter :=
		newRecordingTeamQuotaController(logger, nil)
	jwtIssuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: clusterGatewayRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		teamQuotaController:  regionalController,
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

	tokens, err := jwtIssuer.IssueTokenPair(
		"user-1",
		"user@example.com",
		"User",
		false,
		[]authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}},
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	const payload = "regional-edge"
	const reply = "reply:" + payload
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxvolumes/volume-1/files",
		strings.NewReader(payload),
	)
	request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	request.Header.Set(internalauth.TeamIDHeader, "team-1")
	request.Header.Set(internalauth.DefaultTokenHeader, "public-spoof")
	recorder := httptest.NewRecorder()

	server.router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK || recorder.Body.String() != reply {
		t.Fatalf(
			"response = %d %q, want 200 %q",
			recorder.Code,
			recorder.Body.String(),
			reply,
		)
	}
	if regionalRateLimiter.Calls() != 1 {
		t.Fatalf("regional api_requests admissions = %d, want 1", regionalRateLimiter.Calls())
	}
	if clusterRateLimiter.Calls() != 0 {
		t.Fatalf("cluster api_requests admissions = %d, want 0 for trusted internal forward", clusterRateLimiter.Calls())
	}
	if regionalConcurrencyLimiter.AcquireCalls() != 1 ||
		regionalConcurrencyLimiter.ReleaseCalls() != 1 ||
		clusterConcurrencyLimiter.AcquireCalls() != 0 {
		t.Fatalf(
			"active_request_count calls = regional acquire %d release %d cluster acquire %d, want 1/1/0",
			regionalConcurrencyLimiter.AcquireCalls(),
			regionalConcurrencyLimiter.ReleaseCalls(),
			clusterConcurrencyLimiter.AcquireCalls(),
		)
	}
	if got := regionalNetworkLimiter.Total(coreteamquota.KeyNetworkIngressBytes); got != int64(len(payload)) {
		t.Fatalf("regional ingress bytes = %d, want %d", got, len(payload))
	}
	if got := regionalNetworkLimiter.Total(coreteamquota.KeyNetworkEgressBytes); got != int64(len(reply)) {
		t.Fatalf("regional egress bytes = %d, want %d", got, len(reply))
	}
	if got := clusterNetworkLimiter.Total(coreteamquota.KeyNetworkIngressBytes); got != 0 {
		t.Fatalf("cluster ingress bytes = %d, want 0 for trusted internal forward", got)
	}
	if got := clusterNetworkLimiter.Total(coreteamquota.KeyNetworkEgressBytes); got != 0 {
		t.Fatalf("cluster egress bytes = %d, want 0 for trusted internal forward", got)
	}
}

func newAPIFallbackRegionalServer(
	t *testing.T,
	logger *zap.Logger,
	targetURL string,
	controller *gatewayteamquota.Controller,
	privateKey ed25519.PrivateKey,
) (*Server, string) {
	t.Helper()

	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "regional-gateway-api-fallback-test",
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
	clusterGatewayRouter, err := proxy.NewRouter(targetURL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}
	issuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: clusterGatewayRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", issuer, logger),
		teamQuotaController:  controller,
		requestLogger:        gatewaymiddleware.NewRequestLogger(logger),
		logger:               logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		obsProvider: obsProvider,
		jwtIssuer:   issuer,
	}
	server.setupRoutes()
	tokens, err := issuer.IssueTokenPair(
		"user-1",
		"user@example.com",
		"User",
		false,
		[]authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}},
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	return server, tokens.AccessToken
}

func TestTeamQuotaAdminRepairBypassesTenantAdmissionButRequiresSystemAdmin(t *testing.T) {
	gin.SetMode(gin.TestMode)

	logger := zap.NewNop()
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "regional-gateway-team-quota-admin-test",
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
	target := httptest.NewServer(http.NotFoundHandler())
	defer target.Close()
	clusterGatewayRouter, err := proxy.NewRouter(target.URL, logger, time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}
	controller, limiter, networkLimiter := newDenyingTeamQuotaController(logger)
	issuer := authn.NewIssuer("regional-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:               gin.New(),
		cfg:                  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		apiKeyRepo:           &apikey.Repository{},
		clusterGatewayRouter: clusterGatewayRouter,
		authMiddleware:       gatewaymiddleware.NewAuthMiddleware(nil, "secret", issuer, logger),
		teamQuotaController:  controller,
		requestLogger:        gatewaymiddleware.NewRequestLogger(logger),
		logger:               logger,
		obsProvider:          obsProvider,
		jwtIssuer:            issuer,
	}
	server.setupRoutes()

	systemTokens, err := issuer.IssueTokenPair(
		"system-admin",
		"system@example.com",
		"System",
		true,
		[]authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}},
	)
	if err != nil {
		t.Fatalf("issue system-admin token: %v", err)
	}
	teamAdminTokens, err := issuer.IssueTokenPair(
		"team-admin",
		"team@example.com",
		"Team",
		false,
		[]authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}},
	)
	if err != nil {
		t.Fatalf("issue team-admin token: %v", err)
	}
	doRequest := func(path, token string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set(internalauth.TeamIDHeader, "team-1")
		recorder := httptest.NewRecorder()
		server.router.ServeHTTP(recorder, request)
		return recorder
	}

	adminResponse := doRequest(
		"/api/v1/teams/team-1/quotas",
		systemTokens.AccessToken,
	)
	if adminResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("admin repair status = %d, want handler-level 503; body=%s", adminResponse.Code, adminResponse.Body.String())
	}
	if limiter.Calls() != 0 {
		t.Fatalf("admin repair consumed tenant bucket; calls = %d", limiter.Calls())
	}
	if got := networkLimiter.Total(coreteamquota.KeyNetworkEgressBytes); got != 0 {
		t.Fatalf("admin repair consumed tenant network quota; bytes = %d", got)
	}

	currentResponse := doRequest("/api/v1/quotas", teamAdminTokens.AccessToken)
	if currentResponse.Code != http.StatusTooManyRequests {
		t.Fatalf("ordinary quota read status = %d, want 429; body=%s", currentResponse.Code, currentResponse.Body.String())
	}
	if limiter.Calls() != 1 {
		t.Fatalf("ordinary request admissions = %d, want 1", limiter.Calls())
	}
	ordinaryEgress := networkLimiter.Total(coreteamquota.KeyNetworkEgressBytes)
	if ordinaryEgress == 0 {
		t.Fatal("ordinary request did not consume tenant network quota")
	}

	limiter.decision = tokenbucket.Decision{Allowed: true, Remaining: 100}
	forbiddenResponse := doRequest(
		"/api/v1/teams/team-1/quotas",
		teamAdminTokens.AccessToken,
	)
	if forbiddenResponse.Code != http.StatusForbidden {
		t.Fatalf("team-admin repair status = %d, want 403; body=%s", forbiddenResponse.Code, forbiddenResponse.Body.String())
	}
	if limiter.Calls() != 2 {
		t.Fatalf("forbidden repair admissions = %d, want tenant request to be charged", limiter.Calls())
	}
	if got := networkLimiter.Total(coreteamquota.KeyNetworkEgressBytes); got <= ordinaryEgress {
		t.Fatalf("forbidden repair network bytes = %d, want more than %d", got, ordinaryEgress)
	}
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
		teamQuotaController:  newAllowingTeamQuotaController(logger),
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
	for _, route := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/v1/quotas"},
		{method: http.MethodGet, path: "/api/v1/teams/:team_id/quotas"},
		{method: http.MethodPut, path: "/api/v1/teams/:team_id/quotas/:key"},
		{method: http.MethodDelete, path: "/api/v1/teams/:team_id/quotas/:key"},
	} {
		if !hasRoute(server.router, route.method, route.path) {
			t.Fatalf("expected Team Quota route %s %s", route.method, route.path)
		}
	}
}
