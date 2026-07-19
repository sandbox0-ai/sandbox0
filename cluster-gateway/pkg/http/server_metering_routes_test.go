package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewayhandlers "github.com/sandbox0-ai/sandbox0/pkg/gateway/http/handlers"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	coreteamquota "github.com/sandbox0-ai/sandbox0/pkg/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/teamquota/guard"
	"go.uber.org/zap"
)

func TestSetupMeteringRoutesMountsInternalEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "")
	server.setupMeteringRoutes()

	if !hasRoute(server.router, "GET", "/internal/v1/metering/status") {
		t.Fatal("expected metering status route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/events") {
		t.Fatal("expected metering events route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/windows") {
		t.Fatal("expected metering windows route to be mounted")
	}
}

func TestSetupRoutesMountsMeteringEndpointsInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/internal/v1/metering/status") {
		t.Fatal("expected metering status route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/events") {
		t.Fatal("expected metering events route to be mounted")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/metering/windows") {
		t.Fatal("expected metering windows route to be mounted")
	}
}

func TestSetupRoutesSkipsControlPlaneEndpointsInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if hasRoute(server.router, "GET", "/internal/v1/sandboxes/:id") {
		t.Fatal("expected public mode to skip internal sandbox metadata route")
	}
	if hasRoute(server.router, "POST", "/internal/v1/sandboxes/:id/resume") {
		t.Fatal("expected public mode to skip internal sandbox resume route")
	}
	if hasRoute(server.router, "PUT", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected public mode to skip internal quota write route")
	}
}

func TestSetupRoutesMountsSandboxObservabilityIngestInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	for _, path := range []string{
		"/internal/v1/sandbox-observability/events",
		"/internal/v1/sandbox-observability/logs",
		"/internal/v1/sandbox-observability/runtime-samples",
	} {
		if !hasRoute(server.router, "POST", path) {
			t.Fatalf("expected public mode to mount sandbox observability ingest route %s", path)
		}
	}
}

func TestSetupRoutesMountsControlPlaneEndpointsInInternalMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/internal/v1/sandboxes/:id") {
		t.Fatal("expected internal mode to mount internal sandbox metadata route")
	}
	if !hasRoute(server.router, "POST", "/internal/v1/sandboxes/:id/resume") {
		t.Fatal("expected internal mode to mount internal sandbox resume route")
	}
	if !hasRoute(server.router, "GET", "/internal/v1/sandboxes/:id/template-source") {
		t.Fatal("expected internal mode to mount sandbox template source route")
	}
	if !hasRoute(server.router, "POST", "/api/v1/templates/from-sandbox") {
		t.Fatal("expected internal mode to mount template-from-sandbox route")
	}
	if hasRoute(server.router, "PUT", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected removed internal quota write proxy route to stay absent")
	}
	if hasRoute(server.router, "DELETE", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected removed internal quota delete proxy route to stay absent")
	}
}

func TestSetupRoutesMountsMetadataEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/metadata") {
		t.Fatal("expected /metadata route to be mounted")
	}
}

func TestSetupRoutesDoesNotMountRemovedSandboxEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	for _, path := range []string{
		"/api/v1/sandboxes/:id/audit/events",
		"/api/v1/sandboxes/:id/audit/events/:event_id/verify",
		"/api/v1/sandboxes/:id/logs",
		"/api/v1/sandboxes/:id/stats",
		"/api/v1/sandboxes/:id/contexts/:ctx_id/stats",
		"/api/v1/sandboxes/:id/observability/metrics",
	} {
		if hasRoute(server.router, "GET", path) {
			t.Fatalf("expected legacy route %s to be removed", path)
		}
	}
}

func TestSetupRoutesMountsSandboxObservabilityEndpoints(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	for _, path := range []string{
		"/api/v1/sandboxes/:id/observability/events",
		"/api/v1/sandboxes/:id/observability/logs",
		"/api/v1/sandboxes/:id/metrics",
		"/api/v1/sandboxes/:id/metrics/catalog",
	} {
		if !hasRoute(server.router, "GET", path) {
			t.Fatalf("expected route %s to be mounted", path)
		}
	}
}

func TestEverySandboxRouteHasExplicitAuditAction(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()
	registered := make(map[string]struct{})
	for _, route := range server.router.Routes() {
		if !strings.HasPrefix(route.Path, "/api/v1/sandboxes") {
			continue
		}
		key := route.Method + " " + route.Path
		registered[key] = struct{}{}
		policy, ok := sandboxAuditRoutePolicies[key]
		if !ok || policy.Action == "" {
			t.Errorf("sandbox route %s has no explicit audit policy", key)
		}
		if route.Method != http.MethodGet && ok && !policy.BufferResponse {
			t.Errorf("sandbox mutation route %s does not buffer its response until canonical audit acknowledgement", key)
		}
	}
	for key, policy := range sandboxAuditRoutePolicies {
		if _, ok := registered[key]; !ok {
			t.Errorf("audit policy %s does not match a registered sandbox route", key)
		}
		if policy.Action == "" {
			t.Errorf("audit policy %s has no action", key)
		}
	}
}

func TestSandboxObservabilityRouteDoesNotRequireManagerUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, authModeInternal)
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.sandboxAuditEntitlements = licensing.NewStaticEntitlements(licensing.FeatureSandboxAudit)
	server.setupRoutes()
	if !hasRoute(server.router, "POST", "/internal/v1/sandbox-observability/events") {
		t.Fatal("expected sandbox observability ingest route to be mounted")
	}

	token, err := generator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxRead, authn.PermSandboxAuditRead},
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb-1/observability/events", nil)
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "sandbox observability backend is disabled") {
		t.Fatalf("response = %s, want disabled backend error", rec.Body.String())
	}
}

func TestInternalAPIWithoutAdmissionProofChargesAPIRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, authModeInternal)
	controller, rateLimiter := newCountingTeamQuotaController(zap.NewNop())
	server.teamQuotaController = controller
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	token, err := generator.Generate(
		internalauth.ServiceClusterGateway,
		"team-1",
		"user-1",
		internalauth.GenerateOptions{
			Permissions: []string{authn.PermSandboxRead},
		},
	)
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/sandboxes/sb-1/metrics/catalog",
		nil,
	)
	request.Header.Set(internalauth.DefaultTokenHeader, token)
	response := httptest.NewRecorder()
	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, want 200; body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	if got := rateLimiter.calls.Load(); got != 1 {
		t.Fatalf("api_requests admissions = %d, want 1", got)
	}
}

func TestInternalAPIWithMatchingAdmissionProofSkipsAPIRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, authModeInternal)
	controller, rateLimiter := newCountingTeamQuotaController(zap.NewNop())
	server.teamQuotaController = controller
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/sandboxes/sb-1/metrics/catalog",
		nil,
	)
	proof, err := internalauth.NewQuotaAdmissionProof(
		internalauth.QuotaAdmissionClassEdgeAdmitted,
		request,
		"team-1",
		"operation-1",
		"request-1",
		internalauth.ServiceRegionalGateway,
		[]coreteamquota.Key{coreteamquota.KeyAPIRequests},
		guard.Version{EnforcementEpoch: 1, RedisGeneration: 1},
	)
	if err != nil {
		t.Fatalf("NewQuotaAdmissionProof() error = %v", err)
	}
	token, err := generator.Generate(
		internalauth.ServiceClusterGateway,
		"team-1",
		"user-1",
		internalauth.GenerateOptions{
			Permissions: []string{authn.PermSandboxRead},
			Audit: &internalauth.AuditContext{
				OperationID: "operation-1",
				RequestID:   "request-1",
				Origin:      internalauth.ServiceRegionalGateway,
			},
			QuotaAdmissionProof: proof,
		},
	)
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	request.Header.Set(internalauth.DefaultTokenHeader, token)
	response := httptest.NewRecorder()
	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, want 200; body=%s",
			response.Code,
			response.Body.String(),
		)
	}
	if got := rateLimiter.calls.Load(); got != 0 {
		t.Fatalf("api_requests admissions = %d, want 0", got)
	}
}

func TestSandboxAuditRouteRequiresEnterpriseFeatureWithoutBlockingOtherSignals(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, authModeInternal)
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	token, err := generator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxRead, authn.PermSandboxAuditRead},
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}

	for _, tc := range []struct {
		path       string
		wantStatus int
		wantBody   string
	}{
		{path: "/api/v1/sandboxes/sb-1/observability/events", wantStatus: http.StatusForbidden, wantBody: "feature_not_licensed"},
		{path: "/api/v1/sandboxes/sb-1/observability/logs", wantStatus: http.StatusServiceUnavailable, wantBody: "sandbox observability backend is disabled"},
		{path: "/api/v1/sandboxes/sb-1/metrics", wantStatus: http.StatusServiceUnavailable, wantBody: "sandbox observability backend is disabled"},
		{path: "/api/v1/sandboxes/sb-1/metrics/catalog", wantStatus: http.StatusOK, wantBody: "sandbox.cpu.utilization"},
	} {
		req := httptest.NewRequest(http.MethodGet, tc.path, nil)
		req.Header.Set(internalauth.DefaultTokenHeader, token)
		rec := httptest.NewRecorder()
		server.router.ServeHTTP(rec, req)

		if rec.Code != tc.wantStatus {
			t.Fatalf("%s status = %d, want %d: %s", tc.path, rec.Code, tc.wantStatus, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), tc.wantBody) {
			t.Fatalf("%s response = %s, want %q", tc.path, rec.Body.String(), tc.wantBody)
		}
	}
}

func TestSandboxRuntimeSampleIngestAcceptsCtldToken(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, authModeInternal)
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"ctld"},
		ClockSkewTolerance: 5 * time.Second,
	})
	server.sandboxObservabilityIngestAuthMiddleware = middleware.NewInternalAuthMiddleware(validator, zap.NewNop())
	server.publicOverloadGuard = newSandboxObservabilityTestOverloadGuard(
		t,
		config.OverloadGuardConfig{},
	)
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "ctld",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	token, err := generator.Generate("cluster-gateway", "team-1", "ctld", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxObservabilityWrite},
	})
	if err != nil {
		t.Fatalf("generate ctld token: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/sandbox-observability/runtime-samples", strings.NewReader(`{"samples":[]}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want backend response after ctld auth: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "permission denied") || strings.Contains(rec.Body.String(), "invalid internal token") {
		t.Fatalf("ctld token was rejected: %s", rec.Body.String())
	}

	auditReq := httptest.NewRequest(http.MethodPost, "/internal/v1/sandbox-observability/events", strings.NewReader(`{"events":[]}`))
	auditReq.Header.Set("Content-Type", "application/json")
	auditReq.Header.Set(internalauth.DefaultTokenHeader, token)
	auditRec := httptest.NewRecorder()
	server.router.ServeHTTP(auditRec, auditReq)
	if auditRec.Code != http.StatusUnauthorized {
		t.Fatalf("audit ingest response = %d %s, want dedicated audit-key rejection", auditRec.Code, auditRec.Body.String())
	}

}

func TestInternalAuthValidatorsKeepControlAndDataPlaneTrustRootsSeparate(t *testing.T) {
	controlPublicKey, controlPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate control-plane key: %v", err)
	}
	dataPublicKey, dataPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate data-plane key: %v", err)
	}
	auditPublicKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate audit key: %v", err)
	}

	controlValidator, ingestValidator, _ := newInternalAuthValidators(
		authModeInternal,
		[]string{"regional-gateway"},
		controlPublicKey,
		dataPublicKey,
		auditPublicKey,
	)
	regionalGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: controlPrivateKey,
		TTL:        time.Minute,
	})
	ctldGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "ctld",
		PrivateKey: dataPrivateKey,
		TTL:        time.Minute,
	})
	regionalToken, err := regionalGenerator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxRead},
	})
	if err != nil {
		t.Fatalf("generate regional token: %v", err)
	}
	ctldToken, err := ctldGenerator.Generate("cluster-gateway", "team-1", "ctld", internalauth.GenerateOptions{
		Permissions: []string{authn.PermSandboxObservabilityWrite},
	})
	if err != nil {
		t.Fatalf("generate ctld token: %v", err)
	}

	if _, err := controlValidator.Validate(regionalToken); err != nil {
		t.Fatalf("control-plane validator rejected regional token: %v", err)
	}
	if _, err := ingestValidator.Validate(ctldToken); err != nil {
		t.Fatalf("ingest validator rejected ctld token: %v", err)
	}
	if _, err := ingestValidator.Validate(regionalToken); err == nil {
		t.Fatal("ingest validator accepted control-plane token")
	}
	if _, err := controlValidator.Validate(ctldToken); err == nil {
		t.Fatal("control-plane validator accepted data-plane token")
	}
}

func TestInternalAuthValidatorsRejectLegacyStorageProxyCaller(t *testing.T) {
	dataPublicKey, dataPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate data-plane key: %v", err)
	}
	_, ingestValidator, _ := newInternalAuthValidators(authModePublic, nil, nil, dataPublicKey, nil)
	legacyGenerator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "storage-proxy",
		PrivateKey: dataPrivateKey,
		TTL:        time.Minute,
	})
	legacyToken, err := legacyGenerator.Generate(
		internalauth.ServiceClusterGateway,
		"team-1",
		"storage-proxy",
		internalauth.GenerateOptions{Permissions: []string{authn.PermSandboxObservabilityWrite}},
	)
	if err != nil {
		t.Fatalf("generate legacy storage-proxy token: %v", err)
	}

	if _, err := ingestValidator.Validate(legacyToken); !errors.Is(err, internalauth.ErrInvalidCaller) {
		t.Fatalf("validate legacy storage-proxy caller error = %v, want ErrInvalidCaller", err)
	}
}

func TestSetupRoutesExposesTeamQuotaPublicAPI(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	if !hasRoute(server.router, "GET", "/api/v1/quotas") {
		t.Fatal("expected current-team quota list route")
	}
	if hasRoute(server.router, "GET", "/api/v1/quotas/:dimension") {
		t.Fatal("expected legacy dimension quota route to be absent")
	}
	if hasRoute(server.router, "PUT", "/internal/v1/teams/:team_id/quotas/:dimension") {
		t.Fatal("expected legacy internal quota proxy to be absent")
	}
}

func TestSetupRoutesExposesAdminTeamQuotaAPIOnlyForPolicyOwner(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, _ := testMeteringRouteServer(t, "public")
	server.cfg.TeamQuota.PolicyOwner = true
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	server.setupRoutes()

	for _, route := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/api/v1/teams/:team_id/quotas"},
		{method: http.MethodPut, path: "/api/v1/teams/:team_id/quotas/:key"},
		{method: http.MethodDelete, path: "/api/v1/teams/:team_id/quotas/:key"},
	} {
		if !hasRoute(server.router, route.method, route.path) {
			t.Fatalf("expected fullmode Team Quota route %s %s", route.method, route.path)
		}
	}

	consumer, _, _ := testMeteringRouteServer(t, "both")
	consumer.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	consumer.obsProvider = newTestMeteringObservability(t)
	consumer.setupRoutes()
	if hasRoute(consumer.router, http.MethodPut, "/api/v1/teams/:team_id/quotas/:key") {
		t.Fatal("expected regional data-plane consumer not to expose Team Quota admin writes")
	}
}

func TestFullmodeAdminQuotaRepairBypassesTenantAdmission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, issuer := testMeteringRouteServer(t, "public")
	server.cfg.TeamQuota.PolicyOwner = true
	server.requestLogger = middleware.NewRequestLogger(zap.NewNop())
	server.obsProvider = newTestMeteringObservability(t)
	rateLimiter := &countingTeamQuotaRateLimiter{}
	networkLimiter := &countingTeamQuotaNetworkLimiter{}
	server.teamQuotaController = gatewayteamquota.NewController(
		nil,
		nil,
		rateLimiter,
		nil,
		zap.NewNop(),
		gatewayteamquota.WithConcurrencyLimiter(allowingTeamQuotaConcurrencyLimiter{}),
		gatewayteamquota.WithNetworkLimiter(networkLimiter),
	)
	server.setupRoutes()

	tokens, err := issuer.IssueTokenPair(
		"system-admin",
		"system@example.com",
		"System",
		true,
		[]authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}},
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	doRequest := func(path string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
		request.Header.Set(internalauth.TeamIDHeader, "team-1")
		recorder := httptest.NewRecorder()
		server.router.ServeHTTP(recorder, request)
		return recorder
	}

	adminResponse := doRequest("/api/v1/teams/team-1/quotas")
	if adminResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("admin repair status = %d, want handler-level 503", adminResponse.Code)
	}
	if rateLimiter.calls.Load() != 0 || networkLimiter.Bytes() != 0 {
		t.Fatalf(
			"admin repair consumed tenant admission: rate=%d bytes=%d",
			rateLimiter.calls.Load(),
			networkLimiter.Bytes(),
		)
	}

	currentResponse := doRequest("/api/v1/quotas")
	if currentResponse.Code != http.StatusServiceUnavailable {
		t.Fatalf("ordinary quota status = %d, want handler-level 503", currentResponse.Code)
	}
	if rateLimiter.calls.Load() != 1 {
		t.Fatalf("ordinary request rate admissions = %d, want 1", rateLimiter.calls.Load())
	}
	if networkLimiter.Bytes() == 0 {
		t.Fatal("ordinary request did not consume tenant network quota")
	}
}

func TestSetupMeteringRoutesDoesNotRequireManagerUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, generator, _ := testMeteringRouteServer(t, "")
	server.setupMeteringRoutes()

	token, err := generator.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSetupMeteringRoutesAllowsSystemAdminInPublicMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, issuer := testMeteringRouteServer(t, "public")
	server.setupMeteringRoutes()

	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", true, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestSetupMeteringRoutesRejectsNonAdminInBothMode(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server, _, issuer := testMeteringRouteServer(t, "both")
	server.setupMeteringRoutes()

	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
}

func testMeteringRouteServer(t *testing.T, authMode string) (*Server, *internalauth.Generator, *authn.Issuer) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 key: %v", err)
	}

	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"regional-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	issuer := authn.NewIssuer("cluster-gateway", "secret", time.Minute, time.Hour)
	publicAuth := gatewaymiddleware.NewAuthMiddleware(nil, "secret", issuer, zap.NewNop())
	internalAuth := middleware.NewInternalAuthMiddleware(validator, zap.NewNop())
	sandboxObservabilityIngestValidator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"netd"},
		ClockSkewTolerance: 5 * time.Second,
	})
	sandboxObservabilityIngestAuth := middleware.NewInternalAuthMiddleware(sandboxObservabilityIngestValidator, zap.NewNop())

	server := &Server{
		router: gin.New(),
		cfg: &config.ClusterGatewayConfig{
			AuthMode:      authMode,
			ClusterID:     "cluster-1",
			GatewayConfig: config.GatewayConfig{RegionID: "aws-us-east-1"},
		},
		authMiddleware:                           internalAuth,
		sandboxAuditIngestAuthMiddleware:         sandboxObservabilityIngestAuth,
		sandboxObservabilityIngestAuthMiddleware: sandboxObservabilityIngestAuth,
		publicAuth:                               publicAuth,
		compositeAuth:                            middleware.NewCompositeAuthMiddleware(internalAuth, publicAuth, zap.NewNop()),
		publicJWT:                                issuer,
		logger:                                   zap.NewNop(),
		meteringHandler:                          gatewayhandlers.NewMeteringHandler(nil, "aws-us-east-1", zap.NewNop()),
		observabilityHandler: gatewayhandlers.NewSandboxObservabilityHandler(
			nil,
			zap.NewNop(),
			gatewayhandlers.WithSandboxObservabilityIngestPolicy(gatewayhandlers.SandboxObservabilityIngestPolicy{
				RegionID:  "aws-us-east-1",
				ClusterID: "cluster-1",
			}),
		),
		sandboxAuditEntitlements: licensing.NewStaticEntitlements(),
		teamQuotaController:      newAllowingTeamQuotaController(zap.NewNop()),
	}

	return server, generator, issuer
}

func hasRoute(router *gin.Engine, method, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}

func newTestMeteringObservability(t *testing.T) *observability.Provider {
	t.Helper()

	provider, err := observability.New(observability.Config{
		ServiceName:    "cluster-gateway-metering-route-test",
		Logger:         zap.NewNop(),
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
	return provider
}
