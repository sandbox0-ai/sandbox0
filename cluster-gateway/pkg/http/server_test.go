package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	gatewayteamquota "github.com/sandbox0-ai/sandbox0/pkg/gateway/teamquota"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	"go.uber.org/zap"
)

type overloadCountingAPIKeyValidator struct {
	calls atomic.Int64
	key   *apikey.APIKey
	err   error
}

func (v *overloadCountingAPIKeyValidator) ValidateAPIKey(
	context.Context,
	string,
) (*apikey.APIKey, error) {
	v.calls.Add(1)
	return v.key, v.err
}

func TestResolveClusterGatewayEntitlements(t *testing.T) {
	publicEntitlements, auditEntitlements, err := resolveClusterGatewayEntitlements(&config.ClusterGatewayConfig{}, false)
	if err != nil {
		t.Fatalf("resolve entitlements: %v", err)
	}
	if !publicEntitlements.Enabled(licensing.FeatureSSO) {
		t.Fatal("expected unconfigured public auth routes to preserve built-in SSO route behavior")
	}
	if auditEntitlements.Enabled(licensing.FeatureSandboxAudit) {
		t.Fatal("did not expect sandbox audit entitlement without explicit configuration")
	}

	_, _, err = resolveClusterGatewayEntitlements(&config.ClusterGatewayConfig{
		SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
	}, false)
	if err == nil || !strings.Contains(err.Error(), "license_file is required") {
		t.Fatalf("error = %v, want required license_file", err)
	}

	licensedConfig := &config.ClusterGatewayConfig{
		LicenseFile:          "test.lic",
		SandboxObservability: config.SandboxObservabilityConfig{AuditEnabled: true},
	}
	publicEntitlements, auditEntitlements, err = resolveClusterGatewayEntitlementsWithLoader(
		licensedConfig,
		false,
		func(string) licensing.Entitlements {
			return licensing.NewStaticEntitlements(licensing.FeatureSandboxAudit)
		},
	)
	if err != nil {
		t.Fatalf("resolve licensed sandbox audit: %v", err)
	}
	if !auditEntitlements.Enabled(licensing.FeatureSandboxAudit) {
		t.Fatal("expected licensed sandbox audit entitlement")
	}
	if !publicEntitlements.Enabled(licensing.FeatureSSO) {
		t.Fatal("expected built-in public auth route behavior to remain unchanged")
	}

	_, _, err = resolveClusterGatewayEntitlementsWithLoader(
		licensedConfig,
		false,
		func(string) licensing.Entitlements {
			return licensing.NewStaticEntitlements(licensing.FeatureSSO)
		},
	)
	if err == nil || !strings.Contains(err.Error(), string(licensing.FeatureSandboxAudit)) {
		t.Fatalf("error = %v, want missing sandbox audit feature", err)
	}
}

func TestRequireUpstream(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("manager upstream blocks when unavailable", func(t *testing.T) {
		server := &Server{
			cfg:    &config.ClusterGatewayConfig{ManagerURL: ""},
			logger: zap.NewNop(),
		}
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes", nil)

		called := false
		engine := gin.New()
		engine.Use(server.managerUpstreamMiddleware())
		engine.GET("/api/v1/sandboxes", func(c *gin.Context) {
			called = true
			c.Status(http.StatusOK)
		})

		engine.ServeHTTP(recorder, req)

		if called {
			t.Fatal("handler should not be called when manager upstream is unavailable")
		}
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("manager storage upstream blocks when unavailable", func(t *testing.T) {
		server := &Server{
			cfg:    &config.ClusterGatewayConfig{ManagerStorageURL: ""},
			logger: zap.NewNop(),
		}
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxvolumes", nil)

		called := false
		engine := gin.New()
		engine.Use(server.managerStorageUpstreamMiddleware())
		engine.GET("/api/v1/sandboxvolumes", func(c *gin.Context) {
			called = true
			c.Status(http.StatusOK)
		})

		engine.ServeHTTP(recorder, req)

		if called {
			t.Fatal("handler should not be called when manager storage upstream is unavailable")
		}
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})
}

func TestSetupPublicRoutesAppliesPublicOverloadGuard(t *testing.T) {
	gin.SetMode(gin.TestMode)

	guard, err := gatewaymiddleware.NewOverloadGuard(
		context.Background(),
		config.OverloadGuardConfig{RequestsPerSecond: 1, Burst: 1},
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("NewOverloadGuard() error = %v", err)
	}
	t.Cleanup(func() { _ = guard.Close() })

	issuer := authn.NewIssuer("cluster-gateway", "secret", time.Minute, time.Hour)
	server := &Server{
		router:              gin.New(),
		cfg:                 &config.ClusterGatewayConfig{AuthMode: authModePublic},
		publicAuth:          gatewaymiddleware.NewAuthMiddleware(nil, "secret", issuer, zap.NewNop()),
		publicJWT:           issuer,
		publicOverloadGuard: guard,
		entitlements:        licensing.NewStaticEntitlements(licensing.FeatureSSO),
		logger:              zap.NewNop(),
	}
	server.setupPublicRoutes()

	first := httptest.NewRecorder()
	server.router.ServeHTTP(
		first,
		httptest.NewRequest(http.MethodGet, "/auth/providers", nil),
	)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d, want %d; body=%s", first.Code, http.StatusOK, first.Body.String())
	}

	second := httptest.NewRecorder()
	server.router.ServeHTTP(
		second,
		httptest.NewRequest(http.MethodGet, "/auth/providers", nil),
	)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf(
			"second status = %d, want %d; body=%s",
			second.Code,
			http.StatusTooManyRequests,
			second.Body.String(),
		)
	}
}

func TestPublicAPIOverloadGuardPrecedesAPIKeyValidationAndHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name      string
		validator *overloadCountingAPIKeyValidator
	}{
		{
			name:      "random API key",
			validator: &overloadCountingAPIKeyValidator{err: errors.New("invalid API key")},
		},
		{
			name: "valid API key",
			validator: &overloadCountingAPIKeyValidator{key: &apikey.APIKey{
				ID:        "key-1",
				TeamID:    "team-1",
				CreatedBy: "user-1",
				Scope:     apikey.ScopeTeam,
			}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			guard, err := gatewaymiddleware.NewOverloadGuard(
				context.Background(),
				config.OverloadGuardConfig{RequestsPerSecond: 1, Burst: 1},
				zap.NewNop(),
			)
			if err != nil {
				t.Fatalf("NewOverloadGuard() error = %v", err)
			}
			t.Cleanup(func() { _ = guard.Close() })
			primePublicOverloadGuard(t, guard)

			server := &Server{publicOverloadGuard: guard}
			router := gin.New()
			api := router.Group("/api/v1")
			server.attachPublicAPIOverloadGuard(api)
			auth := gatewaymiddleware.NewAuthMiddleware(
				tt.validator,
				"secret",
				nil,
				zap.NewNop(),
			)
			api.Use(auth.Authenticate())
			var handlerCalls atomic.Int64
			api.GET("/probe", func(c *gin.Context) {
				handlerCalls.Add(1)
				c.Status(http.StatusNoContent)
			})

			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodGet, "/api/v1/probe", nil)
			request.Header.Set("Authorization", "Bearer s0_probe")
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
			}
			if calls := tt.validator.calls.Load(); calls != 0 {
				t.Fatalf("API key validator calls = %d, want 0", calls)
			}
			if calls := handlerCalls.Load(); calls != 0 {
				t.Fatalf("handler calls = %d, want 0", calls)
			}
		})
	}
}

func primePublicOverloadGuard(
	t *testing.T,
	guard *gatewaymiddleware.OverloadGuard,
) {
	t.Helper()
	router := gin.New()
	router.Use(guard.Admit())
	router.GET("/", func(c *gin.Context) { c.Status(http.StatusNoContent) })
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("prime overload guard status = %d, want 204", recorder.Code)
	}
}

func TestPublicExposureOverloadGuardPrecedesManagerLookup(t *testing.T) {
	gin.SetMode(gin.TestMode)

	guard, err := gatewaymiddleware.NewOverloadGuard(
		context.Background(),
		config.OverloadGuardConfig{RequestsPerSecond: 1, Burst: 1},
		zap.NewNop(),
	)
	if err != nil {
		t.Fatalf("NewOverloadGuard() error = %v", err)
	}
	t.Cleanup(func() { _ = guard.Close() })

	var managerCalls atomic.Int64
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		managerCalls.Add(1)
		spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "not found")
	}))
	t.Cleanup(manager.Close)
	handler := newSandboxServiceExposureTestServerWithManagerURLQuotaAndConfigure(
		t,
		manager.URL,
		newAllowingTeamQuotaController(zap.NewNop()),
		func(server *Server) {
			server.publicOverloadGuard = guard
		},
	)

	first := httptest.NewRecorder()
	firstRequest := httptest.NewRequest(http.MethodGet, "http://gateway/", nil)
	firstRequest.Host = "sb-demo--p3000.aws-us-east-1.sandbox0.app"
	handler.ServeHTTP(first, firstRequest)
	if first.Code != http.StatusNotFound {
		t.Fatalf("first status = %d, want 404; body=%s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	secondRequest := httptest.NewRequest(http.MethodGet, "http://gateway/", nil)
	secondRequest.Host = "sb-demo--p3000.aws-us-east-1.sandbox0.app"
	handler.ServeHTTP(second, secondRequest)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want 429; body=%s", second.Code, second.Body.String())
	}
	if calls := managerCalls.Load(); calls != 1 {
		t.Fatalf("manager lookup calls = %d, want 1", calls)
	}
}

func TestShouldRunTeamQuotaTombstoneCleanup(t *testing.T) {
	controller := &gatewayteamquota.Controller{}
	tests := []struct {
		name   string
		server *Server
		want   bool
	}{
		{
			name: "owned policy controller",
			server: &Server{
				cfg:                     &config.ClusterGatewayConfig{TeamQuota: config.TeamQuotaConfig{PolicyOwner: true}},
				teamQuotaController:     controller,
				ownsTeamQuotaController: true,
			},
			want: true,
		},
		{
			name: "consumer does not prune shared tombstones",
			server: &Server{
				cfg:                     &config.ClusterGatewayConfig{TeamQuota: config.TeamQuotaConfig{PolicyOwner: false}},
				teamQuotaController:     controller,
				ownsTeamQuotaController: true,
			},
		},
		{
			name: "injected controller is not lifecycle owner",
			server: &Server{
				cfg:                     &config.ClusterGatewayConfig{TeamQuota: config.TeamQuotaConfig{PolicyOwner: true}},
				teamQuotaController:     controller,
				ownsTeamQuotaController: false,
			},
		},
		{
			name:   "missing controller",
			server: &Server{cfg: &config.ClusterGatewayConfig{TeamQuota: config.TeamQuotaConfig{PolicyOwner: true}}},
		},
		{
			name:   "missing config",
			server: &Server{teamQuotaController: controller, ownsTeamQuotaController: true},
		},
		{
			name: "nil server",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.server.shouldRunTeamQuotaTombstoneCleanup(); got != test.want {
				t.Fatalf("shouldRunTeamQuotaTombstoneCleanup() = %v, want %v", got, test.want)
			}
		})
	}
}
