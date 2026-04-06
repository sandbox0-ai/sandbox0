package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type stubRegionDirectory struct {
	region *tenantdir.Region
	err    error
}

func (s *stubRegionDirectory) GetRegion(_ context.Context, _ string) (*tenantdir.Region, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.region, nil
}

func TestGlobalGatewayNoRouteProxiesAPIKeyRequestsToRegion(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	var gotAuth string
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer upstream.Close()

	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		regionLookup:  &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: upstream.URL}},
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.router.NoRoute(server.handleNoRoute)
	gw := httptest.NewServer(server.router)
	defer gw.Close()

	req, err := http.NewRequest(http.MethodGet, gw.URL+"/api/v1/templates", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef")
	resp, err := gw.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if gotAuth != "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotPath != "/api/v1/templates" {
		t.Fatalf("path = %q, want /api/v1/templates", gotPath)
	}
}

func TestGlobalGatewayNoRouteLeavesNonAPIKeyRequestsAsNotFound(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.router.NoRoute(server.handleNoRoute)
	gw := httptest.NewServer(server.router)
	defer gw.Close()

	req, err := http.NewRequest(http.MethodGet, gw.URL+"/api/v1/templates", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer user-token")
	resp, err := gw.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
	}
}
