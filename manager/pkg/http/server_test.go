package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/network"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"go.uber.org/zap"
)

func TestRequireNetworkPolicyCapability(t *testing.T) {
	server := newTestServerForCapability(t, network.NewNoopProvider())
	recorder := httptest.NewRecorder()
	ctx, engine := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/sb-1/network", nil)

	called := false
	engine.Use(server.requireNetworkPolicyCapability())
	engine.GET("/api/v1/sandboxes/:id/network", func(c *gin.Context) {
		called = true
		c.Status(http.StatusOK)
	})

	engine.ServeHTTP(recorder, ctx.Request)

	if called {
		t.Fatal("handler should not be called when network policy is unsupported")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestRequireTemplateStoreCapability(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/templates", nil)

	called := false
	engine := gin.New()
	engine.Use(server.requireTemplateStoreCapability())
	engine.GET("/api/v1/templates", func(c *gin.Context) {
		called = true
		c.Status(http.StatusOK)
	})

	engine.ServeHTTP(recorder, req)

	if called {
		t.Fatal("handler should not be called when template store is disabled")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestRequireRegistryCapability(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", nil)

	called := false
	engine := gin.New()
	engine.Use(server.requireRegistryCapability())
	engine.POST("/api/v1/registry/credentials", func(c *gin.Context) {
		called = true
		c.Status(http.StatusOK)
	})

	engine.ServeHTTP(recorder, req)

	if called {
		t.Fatal("handler should not be called when registry provider is disabled")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestRequireCredentialSourceCapability(t *testing.T) {
	server := &Server{}
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/credential-sources", nil)

	called := false
	engine := gin.New()
	engine.Use(server.requireCredentialSourceCapability())
	engine.GET("/api/v1/credential-sources", func(c *gin.Context) {
		called = true
		c.Status(http.StatusOK)
	})

	engine.ServeHTTP(recorder, req)

	if called {
		t.Fatal("handler should not be called when credential source store is disabled")
	}
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
	}
}

func TestRequireNetworkPolicyInBody(t *testing.T) {
	t.Run("allows request without network config", func(t *testing.T) {
		server := newTestServerForCapability(t, network.NewNoopProvider())
		body := `{"template":"default","config":{"ttl":300}}`
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		var captured string
		engine := gin.New()
		engine.Use(server.requireNetworkPolicyInBody(func() any { return &claimSandboxCapabilityRequest{} }))
		engine.POST("/api/v1/sandboxes", func(c *gin.Context) {
			data := make([]byte, c.Request.ContentLength)
			n, _ := c.Request.Body.Read(data)
			captured = string(data[:n])
			c.Status(http.StatusCreated)
		})

		engine.ServeHTTP(recorder, req)

		if recorder.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusCreated)
		}
		if captured != body {
			t.Fatalf("captured body = %q, want %q", captured, body)
		}
	})

	t.Run("blocks request with network config when unsupported", func(t *testing.T) {
		server := newTestServerForCapability(t, network.NewNoopProvider())
		body := `{"template":"default","config":{"network":{"mode":"allow_all"}}}`
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		called := false
		engine := gin.New()
		engine.Use(server.requireNetworkPolicyInBody(func() any { return &claimSandboxCapabilityRequest{} }))
		engine.POST("/api/v1/sandboxes", func(c *gin.Context) {
			called = true
			c.Status(http.StatusCreated)
		})

		engine.ServeHTTP(recorder, req)

		if called {
			t.Fatal("handler should not be called when network config is requested without capability")
		}
		if recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusServiceUnavailable)
		}
	})

	t.Run("allows request with network config when supported", func(t *testing.T) {
		server := newTestServerForCapability(t, testProvider("netd"))
		body := `{"config":{"network":{"mode":"allow_all"}}}`
		recorder := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/api/v1/sandboxes/sb-1", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")

		called := false
		engine := gin.New()
		engine.Use(server.requireNetworkPolicyInBody(func() any { return &updateSandboxCapabilityRequest{} }))
		engine.PUT("/api/v1/sandboxes/:id", func(c *gin.Context) {
			called = true
			c.Status(http.StatusOK)
		})

		engine.ServeHTTP(recorder, req)

		if !called {
			t.Fatal("handler should be called when capability is supported")
		}
		if recorder.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
		}
	})
}

func newTestServerForCapability(t *testing.T, provider network.Provider) *Server {
	t.Helper()
	gin.SetMode(gin.TestMode)
	sandboxService := service.NewSandboxService(nil, nil, nil, nil, nil, nil, nil, provider, nil, nil, nil, service.SandboxServiceConfig{}, zap.NewNop(), nil)
	return &Server{sandboxService: sandboxService}
}

func testProvider(name string) network.Provider {
	return fakeProvider{name: name}
}

type fakeProvider struct {
	name string
}

func (p fakeProvider) Name() string                                     { return p.name }
func (p fakeProvider) EnsureBaseline(_ context.Context, _ string) error { return nil }
func (p fakeProvider) ApplySandboxPolicy(_ context.Context, _ network.SandboxPolicyInput) error {
	return nil
}
func (p fakeProvider) RemoveSandboxPolicy(_ context.Context, _, _ string) error { return nil }
