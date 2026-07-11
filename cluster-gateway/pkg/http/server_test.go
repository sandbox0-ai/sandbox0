package http

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	"go.uber.org/zap"
)

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

	t.Run("storage-proxy upstream blocks when unavailable", func(t *testing.T) {
		server := &Server{
			cfg:    &config.ClusterGatewayConfig{StorageProxyURL: ""},
			logger: zap.NewNop(),
		}
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxvolumes", nil)

		called := false
		engine := gin.New()
		engine.Use(server.storageProxyUpstreamMiddleware())
		engine.GET("/api/v1/sandboxvolumes", func(c *gin.Context) {
			called = true
			c.Status(http.StatusOK)
		})

		engine.ServeHTTP(recorder, req)

		if called {
			t.Fatal("handler should not be called when storage-proxy upstream is unavailable")
		}
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})
}
