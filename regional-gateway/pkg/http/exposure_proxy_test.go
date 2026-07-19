package http

import (
	"context"
	"crypto/ed25519"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestExposureLabelFromHost(t *testing.T) {
	s := &Server{
		cfg: &config.RegionalGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicRootDomain: "sandbox0.app",
				PublicRegionID:   "aws-us-east-1",
			},
		},
	}

	label, ok := s.exposureLabelFromHost("sb-demo--p3000.aws-us-east-1.sandbox0.app")
	if !ok {
		t.Fatalf("expected host to match")
	}
	if label != "sb-demo--p3000" {
		t.Fatalf("unexpected label: %s", label)
	}

	if _, ok := s.exposureLabelFromHost("sb-demo--p3000.sandbox0.app"); ok {
		t.Fatalf("expected host mismatch without region")
	}
}

func TestHandleNoRouteRoutesExposureHostAPIPathToPublicExposure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type forwardedRequest struct {
		path         string
		sandboxID    string
		exposurePort string
		internalJWT  string
	}
	forwarded := make(chan forwardedRequest, 1)
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		forwarded <- forwardedRequest{
			path:         r.URL.Path,
			sandboxID:    r.Header.Get("X-Sandbox-ID"),
			exposurePort: r.Header.Get("X-Exposure-Port"),
			internalJWT:  r.Header.Get(internalauth.DefaultTokenHeader),
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "exposure")
	}))
	defer target.Close()

	clusterGatewayRouter, err := proxy.NewRouter(target.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate internal auth key: %v", err)
	}

	s := &Server{
		router: gin.New(),
		cfg: &config.RegionalGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicExposureEnabled: true,
				PublicRootDomain:      "sandbox0.app",
				PublicRegionID:        "aws-us-east-1",
			},
		},
		clusterGatewayRouter: clusterGatewayRouter,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		logger: zap.NewNop(),
	}
	s.router.NoRoute(s.handleNoRoute)

	gateway := httptest.NewServer(s.router)
	defer gateway.Close()

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/status", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Host = "sb-demo--p3000.aws-us-east-1.sandbox0.app"
	req.Header.Set("X-Sandbox-ID", "sb-victim")
	req.Header.Set("X-Exposure-Port", "4000")
	req.Header.Set(internalauth.DefaultTokenHeader, "forged")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("send request: %v", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusOK, string(body))
	}
	got := <-forwarded
	if got.path != "/api/status" {
		t.Fatalf("forwarded path = %q, want /api/status", got.path)
	}
	if got.sandboxID != "sb-demo" {
		t.Fatalf("X-Sandbox-ID = %q, want sb-demo", got.sandboxID)
	}
	if got.exposurePort != "3000" {
		t.Fatalf("X-Exposure-Port = %q, want 3000", got.exposurePort)
	}
	claims, err := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         internalauth.ServiceClusterGateway,
		PublicKey:      publicKey,
		AllowedCallers: []string{internalauth.ServiceRegionalGateway},
	}).Validate(got.internalJWT)
	if err != nil {
		t.Fatalf("validate signed exposure forward: %v", err)
	}
	if claims.Caller != internalauth.ServiceRegionalGateway || !claims.IsSystem {
		t.Fatalf("forward claims = %#v, want regional-gateway system identity", claims)
	}
}

func TestPublicExposureOverloadGuardPrecedesRegionalProxyOnlyForExposureHost(t *testing.T) {
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

	var proxyCalls atomic.Int64
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxyCalls.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(target.Close)
	clusterGatewayRouter, err := proxy.NewRouter(target.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
	}
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate internal auth key: %v", err)
	}

	server := &Server{
		router: gin.New(),
		cfg: &config.RegionalGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				PublicExposureEnabled: true,
				PublicRootDomain:      "sandbox0.app",
				PublicRegionID:        "aws-us-east-1",
			},
		},
		clusterGatewayRouter: clusterGatewayRouter,
		publicOverloadGuard:  guard,
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceRegionalGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		logger: zap.NewNop(),
	}
	server.setupNoRouteFallback()
	gateway := httptest.NewServer(server.router)
	t.Cleanup(gateway.Close)
	doRequest := func(host, path string) (int, string) {
		t.Helper()
		request, requestErr := http.NewRequest(http.MethodGet, gateway.URL+path, nil)
		if requestErr != nil {
			t.Fatalf("create request: %v", requestErr)
		}
		request.Host = host
		response, requestErr := http.DefaultClient.Do(request)
		if requestErr != nil {
			t.Fatalf("send request: %v", requestErr)
		}
		defer response.Body.Close()
		body, readErr := io.ReadAll(response.Body)
		if readErr != nil {
			t.Fatalf("read response: %v", readErr)
		}
		return response.StatusCode, string(body)
	}

	nonExposureStatus, _ := doRequest("api.example.com", "/missing")
	if nonExposureStatus != http.StatusNotFound {
		t.Fatalf("non-exposure status = %d, want 404", nonExposureStatus)
	}

	firstStatus, firstBody := doRequest(
		"sb-demo--p3000.aws-us-east-1.sandbox0.app",
		"/",
	)
	if firstStatus != http.StatusNoContent {
		t.Fatalf("first exposure status = %d, want 204; body=%s", firstStatus, firstBody)
	}

	secondStatus, secondBody := doRequest(
		"sb-demo--p3000.aws-us-east-1.sandbox0.app",
		"/",
	)
	if secondStatus != http.StatusTooManyRequests {
		t.Fatalf("second exposure status = %d, want 429; body=%s", secondStatus, secondBody)
	}
	if calls := proxyCalls.Load(); calls != 1 {
		t.Fatalf("regional proxy calls = %d, want 1", calls)
	}
}
