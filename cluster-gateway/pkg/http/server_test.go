package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"go.uber.org/zap"
)

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
