package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestSetupPublicRoutesSelfHostedMountsIdentityAndAPIKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testPublicRouteServer(edgeAuthModeSelfHosted)
	server.setupPublicRoutes()

	if !hasRoute(server.router, "POST", "/auth/login") {
		t.Fatal("expected self-hosted mode to mount /auth/login")
	}
	if !hasRoute(server.router, "GET", "/users/me") {
		t.Fatal("expected self-hosted mode to mount /users/me")
	}
	if !hasRoute(server.router, "GET", "/teams") {
		t.Fatal("expected self-hosted mode to mount /teams")
	}
	if !hasRoute(server.router, "GET", "/api-keys") {
		t.Fatal("expected self-hosted mode to mount /api-keys")
	}
	if !hasRoute(server.router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected self-hosted mode to mount SSH key list route")
	}
	if !hasRoute(server.router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected self-hosted mode to mount SSH key create route")
	}
	if !hasRoute(server.router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected self-hosted mode to mount SSH key delete route")
	}
}

func TestSetupPublicRoutesFederatedMountsRegionalStateOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := testPublicRouteServer(edgeAuthModeFederatedGlobal)
	server.setupPublicRoutes()

	if !hasRoute(server.router, "GET", "/api-keys") {
		t.Fatal("expected federated mode to mount /api-keys")
	}
	if !hasRoute(server.router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected federated mode to mount SSH key list route")
	}
	if !hasRoute(server.router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected federated mode to mount SSH key create route")
	}
	if !hasRoute(server.router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected federated mode to mount SSH key delete route")
	}
	if hasRoute(server.router, "POST", "/auth/login") {
		t.Fatal("expected federated mode to omit /auth/login")
	}
	if hasRoute(server.router, "GET", "/users/me") {
		t.Fatal("expected federated mode to omit /users/me")
	}
	if hasRoute(server.router, "POST", "/teams") {
		t.Fatal("expected federated mode to omit /teams")
	}
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

	server := testPublicRouteServer(edgeAuthModeSelfHosted)
	server.publicOverloadGuard = guard
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
			api := router.Group("/api")
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
			request := httptest.NewRequest(http.MethodGet, "/api/probe", nil)
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

func TestPublicAPIOverloadGuardPrecedesNoRouteAPIKeyValidation(t *testing.T) {
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
	primePublicOverloadGuard(t, guard)

	validator := &overloadCountingAPIKeyValidator{key: &apikey.APIKey{
		ID:        "key-1",
		TeamID:    "team-1",
		CreatedBy: "user-1",
		Scope:     apikey.ScopeTeam,
	}}
	server := &Server{
		router:              gin.New(),
		cfg:                 &config.RegionalGatewayConfig{},
		publicOverloadGuard: guard,
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(
			validator,
			"secret",
			nil,
			zap.NewNop(),
		),
	}
	server.setupNoRouteFallback()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/random-path", nil)
	request.Header.Set("Authorization", "Bearer s0_valid")
	server.router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
	}
	if calls := validator.calls.Load(); calls != 0 {
		t.Fatalf("NoRoute API key validator calls = %d, want 0", calls)
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

func TestValidateAcceptedAccessTokenTTL(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *config.RegionalGatewayConfig
		wantErr bool
	}{
		{
			name: "self-hosted issuer may use runtime defaults",
			cfg:  &config.RegionalGatewayConfig{AuthMode: edgeAuthModeSelfHosted},
		},
		{
			name:    "federated verifier requires explicit maximum",
			cfg:     &config.RegionalGatewayConfig{AuthMode: edgeAuthModeFederatedGlobal},
			wantErr: true,
		},
		{
			name: "federated verifier accepts positive maximum",
			cfg: &config.RegionalGatewayConfig{
				AuthMode: edgeAuthModeFederatedGlobal,
				GatewayConfig: config.GatewayConfig{
					JWTAccessTokenTTL: metav1.Duration{Duration: 15 * time.Minute},
				},
			},
		},
		{
			name:    "nil config",
			wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateAcceptedAccessTokenTTL(test.cfg)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateAcceptedAccessTokenTTL() error = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func testPublicRouteServer(mode string) *Server {
	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("test", "secret", time.Minute, time.Hour)
	return &Server{
		router:         gin.New(),
		cfg:            &config.RegionalGatewayConfig{AuthMode: mode},
		apiKeyRepo:     &apikey.Repository{},
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		logger:         logger,
		jwtIssuer:      jwtIssuer,
	}
}

func hasRoute(router *gin.Engine, method, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}
