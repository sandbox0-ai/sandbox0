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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
	memcachepkg "github.com/sandbox0-ai/sandbox0/pkg/memcache"
	"github.com/sandbox0-ai/sandbox0/pkg/observability"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestGlobalGatewayStartupFailsWithoutSharedRedis(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://sandbox0:sandbox0@127.0.0.1:1/sandbox0?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)

	_, err = NewServer(
		&config.GlobalGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				JWTSecret: "test-secret",
			},
		},
		pool,
		zap.NewNop(),
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "shared overload guard requires redis URL") {
		t.Fatalf("NewServer() error = %v, want shared Redis startup failure", err)
	}
}

func TestGlobalGatewayNewServerClosesOwnedGuardOnLaterFailure(t *testing.T) {
	pool, err := pgxpool.New(
		context.Background(),
		"postgres://sandbox0:sandbox0@127.0.0.1:1/sandbox0?sslmode=disable",
	)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	guard := &countingOverloadGuard{}

	_, err = NewServer(
		&config.GlobalGatewayConfig{
			GatewayConfig: config.GatewayConfig{
				JWTSecret: "test-secret",
				OIDCProviders: []config.OIDCProviderConfig{
					{ID: "test", Enabled: true},
				},
			},
		},
		pool,
		zap.NewNop(),
		nil,
		withOverloadGuardFactoryForTest(func(
			context.Context,
			config.OverloadGuardConfig,
			*zap.Logger,
		) (overloadGuard, error) {
			return guard, nil
		}),
	)
	if err == nil || !strings.Contains(err.Error(), "license_file") {
		t.Fatalf("NewServer() error = %v, want license failure", err)
	}
	if guard.closeCalls != 1 {
		t.Fatalf("guard Close calls = %d, want 1", guard.closeCalls)
	}
}

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

func TestGlobalGatewaySetupRoutesOmitsRegionLocalSSHKeys(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("test", "secret", time.Minute, time.Hour)
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "global-gateway-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
	})
	if err != nil {
		t.Fatalf("create observability provider: %v", err)
	}

	server := &Server{
		router:         gin.New(),
		logger:         logger,
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		requestLogger:  gatewaymiddleware.NewRequestLogger(logger),
		jwtIssuer:      jwtIssuer,
		entitlements:   licensing.NewStaticEntitlements(),
		obsProvider:    obsProvider,
		proxyTimeout:   time.Second,
		regionProxies:  make(map[string]*proxy.Router),
	}
	server.setupRoutes()

	if globalHasRoute(server.router, http.MethodGet, "/users/me/ssh-keys") {
		t.Fatal("expected global-gateway routes to omit region-local SSH key list")
	}
	if globalHasRoute(server.router, http.MethodPost, "/users/me/ssh-keys") {
		t.Fatal("expected global-gateway routes to omit region-local SSH key create")
	}
	if globalHasRoute(server.router, http.MethodDelete, "/users/me/ssh-keys/:id") {
		t.Fatal("expected global-gateway routes to omit region-local SSH key delete")
	}
	if !globalHasRoute(server.router, http.MethodGet, "/users/me") {
		t.Fatal("expected global-gateway routes to include global user profile")
	}
	if !globalHasRoute(server.router, http.MethodGet, "/regions") {
		t.Fatal("expected global-gateway routes to include region directory")
	}
}

func TestGlobalGatewayResolveRoutableRegionCachesLookups(t *testing.T) {
	dir := &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: "https://regional.example"}}
	cache := memcachepkg.New[string, tenantdir.Region](memcachepkg.Config{MaxSize: 16, TTL: time.Hour})
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

func globalHasRoute(router *gin.Engine, method, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}

func TestGlobalGatewayResolveRoutableRegionExpiresCache(t *testing.T) {
	dir := &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: "https://regional.example"}}
	cache := memcachepkg.New[string, tenantdir.Region](memcachepkg.Config{MaxSize: 16, TTL: 10 * time.Millisecond, CleanupInterval: 5 * time.Millisecond})
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
	cache := memcachepkg.New[string, tenantdir.Region](memcachepkg.Config{MaxSize: 16, TTL: time.Hour})
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
	server.registerNoRoute()
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

func TestGlobalGatewayNoRouteOverloadGuardsAPIKeyRegionLookup(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	guard, err := gatewaymiddleware.NewOverloadGuard(context.Background(), config.OverloadGuardConfig{
		RequestsPerSecond: 1,
		Burst:             1,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("create overload guard: %v", err)
	}
	defer guard.Close()
	directory := &stubRegionDirectory{region: &tenantdir.Region{
		ID:                 "aws-us-east-1",
		Enabled:            true,
		RegionalGatewayURL: upstream.URL,
	}}
	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		regionLookup:  directory,
		overloadGuard: guard,
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.registerNoRoute()
	gw := httptest.NewServer(server.router)
	defer gw.Close()

	for i, wantStatus := range []int{http.StatusOK, http.StatusTooManyRequests} {
		req, requestErr := http.NewRequest(http.MethodGet, gw.URL+"/api/v1/templates", nil)
		if requestErr != nil {
			t.Fatalf("create request %d: %v", i, requestErr)
		}
		req.Header.Set("Authorization", "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef")
		resp, requestErr := gw.Client().Do(req)
		if requestErr != nil {
			t.Fatalf("do request %d: %v", i, requestErr)
		}
		resp.Body.Close()
		if resp.StatusCode != wantStatus {
			t.Fatalf("request %d status = %d, want %d", i, resp.StatusCode, wantStatus)
		}
	}
	if directory.calls != 1 {
		t.Fatalf("region lookup calls = %d, want 1", directory.calls)
	}
}

func TestGlobalGatewayNoRouteGuardHoldsMaxInFlightThroughRegionProxy(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	enteredUpstream := make(chan struct{})
	releaseUpstream := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(releaseUpstream)
		}
	}()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(enteredUpstream)
		<-releaseUpstream
		w.WriteHeader(http.StatusNoContent)
	}))
	defer upstream.Close()

	guard, err := gatewaymiddleware.NewOverloadGuard(context.Background(), config.OverloadGuardConfig{
		RequestsPerSecond:      1000,
		Burst:                  1000,
		LocalRequestsPerSecond: 1000,
		LocalBurst:             1000,
		MaxInFlight:            1,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("create overload guard: %v", err)
	}
	defer guard.Close()
	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		regionLookup:  &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: upstream.URL}},
		overloadGuard: guard,
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.registerNoRoute()
	gateway := httptest.NewServer(server.router)
	defer gateway.Close()

	firstStatus := make(chan int, 1)
	go func() {
		request, requestErr := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/templates", nil)
		if requestErr != nil {
			firstStatus <- 0
			return
		}
		request.Header.Set("Authorization", "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef")
		response, requestErr := gateway.Client().Do(request)
		if requestErr != nil {
			firstStatus <- 0
			return
		}
		response.Body.Close()
		firstStatus <- response.StatusCode
	}()

	select {
	case <-enteredUpstream:
	case <-time.After(time.Second):
		t.Fatal("first request did not reach region proxy")
	}

	second, err := http.NewRequest(http.MethodGet, gateway.URL+"/api/v1/templates", nil)
	if err != nil {
		t.Fatalf("create second request: %v", err)
	}
	second.Header.Set("Authorization", "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef")
	secondResponse, err := gateway.Client().Do(second)
	if err != nil {
		t.Fatalf("do second request: %v", err)
	}
	secondResponse.Body.Close()
	if secondResponse.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want %d", secondResponse.StatusCode, http.StatusTooManyRequests)
	}

	close(releaseUpstream)
	released = true
	if status := <-firstStatus; status != http.StatusNoContent {
		t.Fatalf("first status = %d, want %d", status, http.StatusNoContent)
	}
}

func TestGlobalGatewayNoRouteGuardDenialSkipsTokenParseAndRegionLookup(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	upstreamCalls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		upstreamCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	guard, err := gatewaymiddleware.NewOverloadGuard(context.Background(), config.OverloadGuardConfig{
		RequestsPerSecond: 1,
		Burst:             1,
	}, zap.NewNop())
	if err != nil {
		t.Fatalf("create overload guard: %v", err)
	}
	defer guard.Close()

	consume := gin.New()
	consume.GET("/", guard.Admit(), func(c *gin.Context) {
		c.Status(http.StatusOK)
	})
	recorder := httptest.NewRecorder()
	consume.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("consume status = %d, want 200", recorder.Code)
	}

	directory := &stubRegionDirectory{region: &tenantdir.Region{
		ID:                 "aws-us-east-1",
		Enabled:            true,
		RegionalGatewayURL: upstream.URL,
	}}
	parseCalls := 0
	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		regionLookup:  directory,
		overloadGuard: guard,
		parseAPIKeyRegion: func(string) (string, error) {
			parseCalls++
			return "aws-us-east-1", nil
		},
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.registerNoRoute()
	request := httptest.NewRequest(http.MethodGet, "/api/v1/templates", nil)
	request.Header.Set("Authorization", "Bearer s0_"+strings.Repeat("x", 1<<20))
	response := httptest.NewRecorder()
	server.router.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", response.Code)
	}
	if parseCalls != 0 {
		t.Fatalf("API key parse calls = %d, want 0", parseCalls)
	}
	if directory.calls != 0 {
		t.Fatalf("region lookup calls = %d, want 0", directory.calls)
	}
	if upstreamCalls != 0 {
		t.Fatalf("upstream calls = %d, want 0", upstreamCalls)
	}
}

func TestGlobalGatewayRegionsGuardRunsBeforeAuthentication(t *testing.T) {
	gin.SetMode(gin.TestMode)
	logger := zap.NewNop()
	guard, err := gatewaymiddleware.NewOverloadGuard(context.Background(), config.OverloadGuardConfig{
		RequestsPerSecond: 1,
		Burst:             1,
	}, logger)
	if err != nil {
		t.Fatalf("create overload guard: %v", err)
	}
	t.Cleanup(func() { _ = guard.Close() })
	jwtIssuer := authn.NewIssuer("test", "secret", time.Minute, time.Hour)
	obsProvider, err := observability.New(observability.Config{
		ServiceName:    "global-gateway-guard-order-test",
		Logger:         logger,
		DisableTracing: true,
		DisableMetrics: true,
		DisableLogging: true,
	})
	if err != nil {
		t.Fatalf("create observability provider: %v", err)
	}
	server := &Server{
		router:         gin.New(),
		logger:         logger,
		authMiddleware: gatewaymiddleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		overloadGuard:  guard,
		requestLogger:  gatewaymiddleware.NewRequestLogger(logger),
		jwtIssuer:      jwtIssuer,
		entitlements:   licensing.NewStaticEntitlements(),
		obsProvider:    obsProvider,
		proxyTimeout:   time.Second,
		regionProxies:  make(map[string]*proxy.Router),
	}
	server.setupRoutes()

	for _, path := range []string{"/healthz", "/ordinary-not-found"} {
		response := httptest.NewRecorder()
		server.router.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if path == "/healthz" && response.Code != http.StatusOK {
			t.Fatalf("health status = %d, want 200", response.Code)
		}
		if path != "/healthz" && response.Code != http.StatusNotFound {
			t.Fatalf("ordinary missing path status = %d, want 404", response.Code)
		}
	}

	for index, want := range []int{http.StatusUnauthorized, http.StatusTooManyRequests} {
		request := httptest.NewRequest(http.MethodGet, "/regions", nil)
		request.Header.Set("Authorization", "Bearer invalid-token")
		response := httptest.NewRecorder()
		server.router.ServeHTTP(response, request)
		if response.Code != want {
			t.Fatalf("request %d status = %d, want %d", index, response.Code, want)
		}
	}
}

func TestGlobalGatewayNoRouteProxiesCurrentAPIKeyRequestToRegion(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	var gotAuth string
	var gotPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"id":"key_123"}`))
	}))
	defer upstream.Close()

	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		regionLookup:  &stubRegionDirectory{region: &tenantdir.Region{ID: "aws-us-east-1", Enabled: true, RegionalGatewayURL: upstream.URL}},
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.registerNoRoute()
	gw := httptest.NewServer(server.router)
	defer gw.Close()

	req, err := http.NewRequest(http.MethodGet, gw.URL+"/api-keys/current", nil)
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
	if gotPath != "/api-keys/current" {
		t.Fatalf("path = %q, want /api-keys/current", gotPath)
	}
}

type countingOverloadGuard struct {
	closeCalls int
}

func (*countingOverloadGuard) Admit() gin.HandlerFunc {
	return func(c *gin.Context) { c.Next() }
}

func (g *countingOverloadGuard) Close() error {
	g.closeCalls++
	return nil
}

func TestGlobalGatewayNoRouteDoesNotProxyAPIKeyManagementRequests(t *testing.T) {
	gin.SetMode(gin.ReleaseMode)
	server := &Server{
		router:        gin.New(),
		logger:        zap.NewNop(),
		proxyTimeout:  time.Second,
		regionProxies: make(map[string]*proxy.Router),
	}
	server.registerNoRoute()
	gw := httptest.NewServer(server.router)
	defer gw.Close()

	req, err := http.NewRequest(http.MethodPost, gw.URL+"/api-keys", nil)
	if err != nil {
		t.Fatalf("create request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer s0_aws-us-east-1_deadbeefdeadbeefdeadbeefdeadbeef")
	resp, err := gw.Client().Do(req)
	if err != nil {
		t.Fatalf("do request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusNotFound)
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
	server.registerNoRoute()
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
	server.registerNoRoute()
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
