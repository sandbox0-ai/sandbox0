package http

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
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

	var gotPath, gotSandboxID, gotExposurePort string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotSandboxID = r.Header.Get("X-Sandbox-ID")
		gotExposurePort = r.Header.Get("X-Exposure-Port")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "exposure")
	}))
	defer target.Close()

	clusterGatewayRouter, err := proxy.NewRouter(target.URL, zap.NewNop(), time.Second)
	if err != nil {
		t.Fatalf("create cluster-gateway proxy: %v", err)
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
	}
	s.router.NoRoute(s.handleNoRoute)

	gateway := httptest.NewServer(s.router)
	defer gateway.Close()

	req, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/status", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Host = "sb-demo--p3000.aws-us-east-1.sandbox0.app"
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
	if gotPath != "/api/status" {
		t.Fatalf("forwarded path = %q, want /api/status", gotPath)
	}
	if gotSandboxID != "sb-demo" {
		t.Fatalf("X-Sandbox-ID = %q, want sb-demo", gotSandboxID)
	}
	if gotExposurePort != "3000" {
		t.Fatalf("X-Exposure-Port = %q, want 3000", gotExposurePort)
	}
}
