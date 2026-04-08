package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	cachepkg "github.com/sandbox0-ai/sandbox0/pkg/cache"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

type stubRegionDirectory struct {
	region *tenantdir.Region
	err    error
	calls  int
}

func (s *stubRegionDirectory) GetRegion(_ context.Context, _ string) (*tenantdir.Region, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.region, nil
}

func TestGlobalGatewayResolveRoutableRegionCachesLookups(t *testing.T) {
	dir := &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: "https://regional.example"}}
	cache := cachepkg.New[string, tenantdir.Region](cachepkg.Config{MaxSize: 16, TTL: time.Hour})
	defer cache.Close()
	server := &Server{
		logger:       zap.NewNop(),
		regionLookup: dir,
		regionRoutes: cache,
	}

	for i := 0; i < 2; i++ {
		region, err := server.resolveRoutableRegion(context.Background(), "aws-us-east-1")
		if err != nil {
			t.Fatalf("resolve region: %v", err)
		}
		if region.ID != "aws-us-east-1" {
			t.Fatalf("unexpected region: %+v", region)
		}
	}

	if dir.calls != 1 {
		t.Fatalf("expected one backing lookup, got %d", dir.calls)
	}
}

func TestGlobalGatewayResolveRoutableRegionExpiresCache(t *testing.T) {
	dir := &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: "https://regional.example"}}
	cache := cachepkg.New[string, tenantdir.Region](cachepkg.Config{MaxSize: 16, TTL: 10 * time.Millisecond, CleanupInterval: 5 * time.Millisecond})
	defer cache.Close()
	server := &Server{
		logger:       zap.NewNop(),
		regionLookup: dir,
		regionRoutes: cache,
	}

	if _, err := server.resolveRoutableRegion(context.Background(), "aws-us-east-1"); err != nil {
		t.Fatalf("resolve region: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if _, err := server.resolveRoutableRegion(context.Background(), "aws-us-east-1"); err != nil {
		t.Fatalf("resolve region after expiry: %v", err)
	}

	if dir.calls != 2 {
		t.Fatalf("expected expired cache to force a second lookup, got %d", dir.calls)
	}
}

func TestGlobalGatewayResolveRoutableRegionInvalidationClearsCache(t *testing.T) {
	dir := &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: "https://regional.example"}}
	cache := cachepkg.New[string, tenantdir.Region](cachepkg.Config{MaxSize: 16, TTL: time.Hour})
	defer cache.Close()
	server := &Server{
		logger:       zap.NewNop(),
		regionLookup: dir,
		regionRoutes: cache,
	}

	if _, err := server.resolveRoutableRegion(context.Background(), "aws-us-east-1"); err != nil {
		t.Fatalf("resolve region: %v", err)
	}
	server.invalidateRegionRouteCache()
	if _, err := server.resolveRoutableRegion(context.Background(), "aws-us-east-1"); err != nil {
		t.Fatalf("resolve region after invalidation: %v", err)
	}

	if dir.calls != 2 {
		t.Fatalf("expected cache invalidation to force a second lookup, got %d", dir.calls)
	}
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

func TestGlobalGatewayNoRouteProxiesAPIKeyWebSocketRequestsToRegion(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	var gotAuth string
	var gotPath string
	errCh := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			errCh <- err
		}
		if err != nil {
			return
		}
		defer conn.Close()

		messageType, payload, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		if err := conn.WriteMessage(messageType, payload); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
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

	wsURL := "ws" + strings.TrimPrefix(gw.URL, "http") + "/api/v1/stream"
	conn, resp, err := websocket.DefaultDialer.Dial(wsURL, http.Header{
		"Authorization": []string{"Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef"},
	})
	if err != nil {
		statusCode := 0
		if resp != nil {
			statusCode = resp.StatusCode
		}
		t.Fatalf("dial websocket status=%d: %v", statusCode, err)
	}
	defer conn.Close()

	if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
		t.Fatalf("write websocket message: %v", err)
	}
	messageType, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read websocket message: %v", err)
	}
	if messageType != websocket.TextMessage {
		t.Fatalf("message type = %d, want %d", messageType, websocket.TextMessage)
	}
	if string(payload) != "ping" {
		t.Fatalf("payload = %q, want ping", string(payload))
	}
	if gotAuth != "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef" {
		t.Fatalf("Authorization = %q", gotAuth)
	}
	if gotPath != "/api/v1/stream" {
		t.Fatalf("path = %q, want /api/v1/stream", gotPath)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("upstream websocket handling error: %v", err)
	}
}
