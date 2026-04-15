package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestSandboxLogsFollowDisablesManagerProxyTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)

	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = io.WriteString(w, "stream logs")
	}))
	defer manager.Close()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	logger := zap.NewNop()
	proxy2Mgr, err := proxy.NewRouter(manager.URL, logger, 20*time.Millisecond)
	if err != nil {
		t.Fatalf("create manager proxy: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "cluster-gateway",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"regional-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	incomingGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "regional-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	server := &Server{
		cfg:             &config.ClusterGatewayConfig{AuthMode: authModeInternal, ManagerURL: manager.URL},
		proxy2Mgr:       proxy2Mgr,
		managerClient:   &client.ManagerClient{},
		authMiddleware:  middleware.NewInternalAuthMiddleware(validator, logger),
		logger:          logger,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute}),
	}
	server.router = gin.New()
	v1 := server.router.Group("/api/v1")
	v1.Use(server.authMiddleware.Authenticate())
	sandboxes := v1.Group("/sandboxes")
	sandboxes.Use(server.managerUpstreamMiddleware())
	sandboxes.GET("/:id/logs", server.authMiddleware.RequirePermission(gatewayauthn.PermSandboxRead), server.getSandboxLogs)
	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	token, err := incomingGen.Generate("cluster-gateway", "team-1", "user-1", internalauth.GenerateOptions{
		Permissions: []string{gatewayauthn.PermSandboxRead},
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/sandboxes/sandbox-1/logs?follow=true", nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set(internalauth.DefaultTokenHeader, token)
	resp, err := gateway.Client().Do(req)
	if err != nil {
		t.Fatalf("GET logs: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if string(body) != "stream logs" {
		t.Fatalf("body = %q, want %q", string(body), "stream logs")
	}
}
