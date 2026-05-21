package http

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/functionruntime"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/functions"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestMatchFunctionRouteChoosesLongestPrefix(t *testing.T) {
	service := mgr.SandboxAppService{
		Ingress: mgr.SandboxAppServiceIngress{
			Public: true,
			Routes: []mgr.SandboxAppServiceRoute{
				{ID: "root", PathPrefix: "/", Methods: []string{"GET"}},
				{ID: "api", PathPrefix: "/api", Methods: []string{"POST"}},
			},
		},
	}

	match := matchFunctionRoute(service, "/api/users", "POST")
	if !match.pathMatched || !match.methodAllowed {
		t.Fatalf("match = %+v, want matched allowed route", match)
	}
	if match.route == nil || match.route.ID != "api" {
		t.Fatalf("route = %+v, want api", match.route)
	}

	disallowed := matchFunctionRoute(service, "/api/users", "GET")
	if !disallowed.pathMatched || disallowed.methodAllowed {
		t.Fatalf("match = %+v, want path matched but method disallowed", disallowed)
	}
}

func TestAuthorizeFunctionRouteBearer(t *testing.T) {
	gin.SetMode(gin.TestMode)
	sum := sha256.Sum256([]byte("secret"))
	route := &mgr.SandboxAppServiceRoute{
		Auth: &mgr.SandboxAppServiceRouteAuth{
			Mode:              mgr.SandboxAppServiceRouteAuthModeBearer,
			BearerTokenSHA256: hex.EncodeToString(sum[:]),
		},
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer secret")
	ctx.Request = req
	authorized, _, _ := authorizeFunctionRoute(ctx, route)
	if !authorized {
		t.Fatal("authorizeFunctionRoute returned false for valid bearer token")
	}

	rec = httptest.NewRecorder()
	ctx, _ = gin.CreateTestContext(rec)
	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	ctx.Request = req
	authorized, reason, status := authorizeFunctionRoute(ctx, route)
	if authorized {
		t.Fatal("authorizeFunctionRoute returned true for invalid bearer token")
	}
	if reason != "auth_failed" || status != http.StatusUnauthorized {
		t.Fatalf("failure = (%q, %d), want auth_failed %d", reason, status, http.StatusUnauthorized)
	}
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestFunctionIngressRequestLimitRejectsLargeContentLength(t *testing.T) {
	gin.SetMode(gin.TestMode)

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodPost, "/", http.NoBody)
	req.ContentLength = defaultFunctionMaxRequestBodyBytes + 1
	ctx.Request = req

	server := &Server{}
	if server.enforceFunctionIngressRequestLimits(ctx, &functions.Function{ID: "fn-1", TeamID: "team-1"}, &functions.Revision{ID: "rev-1"}, "api") {
		t.Fatal("enforceFunctionIngressRequestLimits returned true for oversized body")
	}
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusRequestEntityTooLarge)
	}
}

func TestRewriteFunctionPath(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/api/v1/users", nil)

	rewriteFunctionPath(ctx, "/api", "/")
	if got := ctx.Request.URL.Path; got != "/v1/users" {
		t.Fatalf("path = %q, want /v1/users", got)
	}
}

func TestFunctionRuntimeProxyPath(t *testing.T) {
	path := functionRuntimeProxyPath("sb_test", "api", "/v1/users")
	want := "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/proxy/v1/users"
	if path != want {
		t.Fatalf("path = %q, want %q", path, want)
	}

	root := functionRuntimeProxyPath("sb_test", "api", "")
	if root != "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/proxy/" {
		t.Fatalf("root path = %q", root)
	}
}

func TestFunctionRuntimeProxyUsesClusterGatewayForServing(t *testing.T) {
	gin.SetMode(gin.TestMode)

	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         internalauth.ServiceClusterGateway,
		PublicKey:      publicKey,
		AllowedCallers: []string{internalauth.ServiceFunctionGateway},
	})

	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/proxy/stream" {
			t.Errorf("cluster-gateway path = %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.URL.RawQuery != "q=1" {
			t.Errorf("cluster-gateway raw query = %q", r.URL.RawQuery)
		}
		claims, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader))
		if err != nil {
			t.Errorf("validate internal token: %v", err)
			http.Error(w, "bad token", http.StatusUnauthorized)
			return
		}
		if claims.TeamID != "team-1" || claims.UserID != "user-1" {
			t.Errorf("claims = team %q user %q", claims.TeamID, claims.UserID)
		}
		if got := r.Header.Get("Forwarded"); got != "" {
			t.Errorf("cluster-gateway received Forwarded = %q, want empty", got)
		}
		if got := r.Header.Get("X-Real-IP"); got != "" {
			t.Errorf("cluster-gateway received X-Real-IP = %q, want empty", got)
		}
		if got := r.Header.Get("X-Forwarded-For"); got == "" || strings.Contains(got, "203.0.113.10") {
			t.Errorf("cluster-gateway received X-Forwarded-For = %q, want gateway remote address only", got)
		}
		time.Sleep(120 * time.Millisecond)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: ok\n\n")
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}
	fn := &functions.Function{ID: "fn-1", TeamID: "team-1"}
	rev := &functions.Revision{ID: "rev-1", FunctionID: "fn-1", TeamID: "team-1", CreatedBy: "user-1"}
	sandbox := &mgr.Sandbox{
		ID:           "sb_test",
		TeamID:       "team-1",
		InternalAddr: "http://203.0.113.1:49983",
	}
	service := mgr.SandboxAppService{ID: "api", Port: 3000}
	route := &mgr.SandboxAppServiceRoute{ID: "api", TimeoutSeconds: 1}

	engine := gin.New()
	engine.GET("/*path", func(c *gin.Context) {
		server.proxyFunctionRequestToRuntime(c, fn, rev, sandbox, service, route)
	})
	functionGateway := httptest.NewUnstartedServer(engine)
	functionGateway.Config.WriteTimeout = 50 * time.Millisecond
	functionGateway.Start()
	defer functionGateway.Close()

	client := &http.Client{Timeout: 2 * time.Second}
	req, err := http.NewRequest(http.MethodGet, functionGateway.URL+"/stream?q=1", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	req.Header.Set("Forwarded", "for=203.0.113.10;proto=https")
	req.Header.Set("X-Forwarded-For", "203.0.113.10")
	req.Header.Set("X-Real-IP", "203.0.113.11")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do() error = %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("status = %d, want %d, body = %s", resp.StatusCode, http.StatusOK, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}
	if string(body) != "data: ok\n\n" {
		t.Fatalf("body = %q", string(body))
	}
}

func TestAcquireFunctionRuntimeDoesNotServeSourceSandbox(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	var sourceLookups atomic.Int32
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/sandboxes/source-sandbox" {
			t.Errorf("path = %s, want source sandbox lookup path", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		sourceLookups.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"success":true,"data":{"id":"source-sandbox","team_id":"team-1","status":"running"}}`))
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		functionRepo: functions.NewRepository(nil),
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}

	_, _, err = server.acquireFunctionRuntime(context.Background(), &functions.Function{
		ID:     "fn-1",
		TeamID: "team-1",
	}, &functions.Revision{
		ID:               "rev-1",
		FunctionID:       "fn-1",
		TeamID:           "team-1",
		SourceSandboxID:  "source-sandbox",
		SourceTemplateID: "tmpl-1",
		CreatedBy:        "user-1",
	}, mgr.SandboxAppService{})
	if err == nil || !strings.Contains(err.Error(), "function repository is not configured") {
		t.Fatalf("acquireFunctionRuntime() error = %v, want revision runtime path", err)
	}
	if got := sourceLookups.Load(); got != 0 {
		t.Fatalf("source sandbox lookups = %d, want 0", got)
	}
}

func TestClaimFunctionSandboxIncludesRuntimeOwnerMetadata(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	var got mgr.ClaimRequest
	var gotHeader http.Header
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/sandboxes" {
			t.Errorf("path = %s, want sandbox claim path", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Errorf("decode claim request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		gotHeader = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"success":true,"data":{"sandbox_id":"sb-runtime","status":"starting","procd_address":"127.0.0.1:8080","pod_name":"sb-runtime","template":"tmpl-1"}}`))
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}

	_, err = server.claimFunctionSandboxViaClusterGateway(context.Background(), &functions.Function{
		ID:     "fn-1",
		TeamID: "team-1",
	}, &functions.Revision{
		ID:               "rev-1",
		FunctionID:       "fn-1",
		TeamID:           "team-1",
		SourceTemplateID: "tmpl-1",
		CreatedBy:        "user-1",
	}, mgr.SandboxAppService{}, "inst-1")
	if err != nil {
		t.Fatalf("claimFunctionSandboxViaClusterGateway() error = %v", err)
	}
	if got.Metadata != nil {
		t.Fatal("claim request metadata should not be serialized in public JSON body")
	}
	metadata := functionruntime.FromHeaders(gotHeader)
	if metadata == nil {
		t.Fatal("runtime metadata headers are missing")
	}
	if metadata.OwnerKind != functionruntime.OwnerKind {
		t.Fatalf("owner kind header = %q, want %q", metadata.OwnerKind, functionruntime.OwnerKind)
	}
	if metadata.FunctionID != "fn-1" {
		t.Fatalf("function id header = %q, want fn-1", metadata.FunctionID)
	}
	if metadata.FunctionRevisionID != "rev-1" {
		t.Fatalf("function revision id header = %q, want rev-1", metadata.FunctionRevisionID)
	}
	if metadata.FunctionRuntimeInstanceID != "inst-1" {
		t.Fatalf("function runtime instance id header = %q, want inst-1", metadata.FunctionRuntimeInstanceID)
	}
}

func TestDecodeFunctionContextResponseAcceptsGatewayEnvelope(t *testing.T) {
	out, err := decodeFunctionContextResponse(strings.NewReader(`{"success":true,"data":{"id":"ctx-a","running":true}}`))
	if err != nil {
		t.Fatalf("decodeFunctionContextResponse() error = %v", err)
	}
	if out.ID != "ctx-a" || !out.Running || out.Paused {
		t.Fatalf("decoded context = %+v, want running ctx-a", out)
	}
}

func TestDecodeFunctionContextResponseAcceptsRawContextBody(t *testing.T) {
	out, err := decodeFunctionContextResponse(strings.NewReader(`{"id":"ctx-a","paused":true}`))
	if err != nil {
		t.Fatalf("decodeFunctionContextResponse() error = %v", err)
	}
	if out.ID != "ctx-a" || out.Running || !out.Paused {
		t.Fatalf("decoded context = %+v, want paused ctx-a", out)
	}
}

func TestDecodeFunctionContextListResponseAcceptsGatewayEnvelope(t *testing.T) {
	out, err := decodeFunctionContextListResponse(strings.NewReader(`{"success":true,"data":{"contexts":[{"id":"ctx-a","type":"cmd","alias":"api","running":true}]}}`))
	if err != nil {
		t.Fatalf("decodeFunctionContextListResponse() error = %v", err)
	}
	if len(out.Contexts) != 1 || out.Contexts[0].ID != "ctx-a" || out.Contexts[0].Type != "cmd" || out.Contexts[0].Alias != "api" || !out.Contexts[0].Running {
		t.Fatalf("decoded contexts = %+v, want running cmd ctx-a", out.Contexts)
	}
}

func TestDecodeFunctionContextListResponseAcceptsRawListBody(t *testing.T) {
	out, err := decodeFunctionContextListResponse(strings.NewReader(`{"contexts":[{"id":"ctx-a","type":"cmd","alias":"api","paused":true}]}`))
	if err != nil {
		t.Fatalf("decodeFunctionContextListResponse() error = %v", err)
	}
	if len(out.Contexts) != 1 || out.Contexts[0].ID != "ctx-a" || out.Contexts[0].Type != "cmd" || out.Contexts[0].Alias != "api" || !out.Contexts[0].Paused {
		t.Fatalf("decoded contexts = %+v, want paused cmd ctx-a", out.Contexts)
	}
}

func TestSelectFunctionWarmProcessContextRequiresCMD(t *testing.T) {
	contexts := []functionContextResponse{
		{ID: "ctx-repl", Type: "repl", Alias: "node", Running: true},
		{ID: "ctx-api", Type: "cmd", Alias: "api", Running: true},
	}

	selected, err := selectFunctionWarmProcessContext(contexts, "api")
	if err != nil {
		t.Fatalf("selectFunctionWarmProcessContext() error = %v", err)
	}
	if selected.ID != "ctx-api" {
		t.Fatalf("selected context = %+v, want ctx-api", selected)
	}

	if _, err := selectFunctionWarmProcessContext(contexts, "node"); err == nil {
		t.Fatal("selectFunctionWarmProcessContext() error = nil, want repl rejection")
	}
}

func TestSelectFunctionWarmProcessContextRejectsStoppedContext(t *testing.T) {
	_, err := selectFunctionWarmProcessContext([]functionContextResponse{
		{ID: "ctx-api", Type: "cmd", Alias: "api"},
	}, "api")
	if err == nil {
		t.Fatal("selectFunctionWarmProcessContext() error = nil, want stopped context rejection")
	}
}

func TestFunctionRuntimeContextCanServeWarmProcess(t *testing.T) {
	service := mgr.SandboxAppService{
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:            mgr.SandboxAppServiceRuntimeWarmProcess,
			WarmProcessName: "api",
		},
	}
	if !functionRuntimeContextCanServe(&functionContextResponse{ID: "ctx-api", Type: "cmd", Alias: "api", Running: true}, service) {
		t.Fatal("running cmd warm process should serve")
	}
	if functionRuntimeContextCanServe(&functionContextResponse{ID: "ctx-api", Type: "repl", Alias: "api", Running: true}, service) {
		t.Fatal("repl warm process should not serve function traffic")
	}
}

type fakeRuntimeRestoreLocker struct {
	resources []string
}

func (f *fakeRuntimeRestoreLocker) WithExclusive(ctx context.Context, resource string, fn func(context.Context) error) error {
	f.resources = append(f.resources, resource)
	return fn(ctx)
}

func TestWithRevisionRuntimeDistributedLockUsesRevisionResource(t *testing.T) {
	locker := &fakeRuntimeRestoreLocker{}
	server := &Server{runtimeRestoreLocks: locker}

	called := false
	if err := server.withRevisionRuntimeDistributedLock(context.Background(), " rev-1 ", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("withRevisionRuntimeDistributedLock() error = %v", err)
	}

	if !called {
		t.Fatal("callback was not called")
	}
	if len(locker.resources) != 1 || locker.resources[0] != "function-runtime-restore:rev-1" {
		t.Fatalf("resources = %v, want function runtime restore revision key", locker.resources)
	}
}

func TestWithRevisionRuntimeDistributedLockFallsBackWithoutLocker(t *testing.T) {
	server := &Server{}
	called := false

	if err := server.withRevisionRuntimeDistributedLock(context.Background(), "rev-1", func(context.Context) error {
		called = true
		return nil
	}); err != nil {
		t.Fatalf("withRevisionRuntimeDistributedLock() error = %v", err)
	}

	if !called {
		t.Fatal("callback was not called")
	}
}

func TestWaitForFunctionServiceReadinessUsesClusterGatewayTCPFallback(t *testing.T) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:         internalauth.ServiceClusterGateway,
		PublicKey:      publicKey,
		AllowedCallers: []string{internalauth.ServiceFunctionGateway},
	})

	var requests int32
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/readiness" {
			t.Errorf("path = %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("health_path"); got != "" {
			t.Errorf("health_path = %q, want empty TCP fallback", got)
		}
		claims, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader))
		if err != nil {
			t.Errorf("validate internal token: %v", err)
			http.Error(w, "bad token", http.StatusUnauthorized)
			return
		}
		if claims.TeamID != "team-1" || claims.UserID != "user-1" {
			t.Errorf("claims = team %q user %q", claims.TeamID, claims.UserID)
		}
		if atomic.AddInt32(&requests, 1) == 1 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	service := mgr.SandboxAppService{ID: "api"}
	if err := server.waitForFunctionServiceReadiness(ctx, "sb_test", "team-1", "user-1", service); err != nil {
		t.Fatalf("waitForFunctionServiceReadiness() error = %v", err)
	}
	if got := atomic.LoadInt32(&requests); got < 2 {
		t.Fatalf("readiness requests = %d, want retry before success", got)
	}
}

func TestWaitForFunctionServiceReadinessPassesHealthPathToClusterGateway(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	var requests int32
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/functions/runtime/sandboxes/sb_test/services/api/readiness" {
			t.Errorf("path = %s", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if got := r.URL.Query().Get("health_path"); got != "/ready" {
			t.Errorf("health_path = %q, want /ready", got)
		}
		if atomic.AddInt32(&requests, 1) == 1 {
			http.Error(w, "starting", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	service := mgr.SandboxAppService{
		ID:          "api",
		HealthCheck: &mgr.SandboxAppServiceHealth{Path: "/ready"},
	}
	if err := server.waitForFunctionServiceReadiness(ctx, "sb_test", "team-1", "user-1", service); err != nil {
		t.Fatalf("waitForFunctionServiceReadiness() error = %v", err)
	}
	if got := atomic.LoadInt32(&requests); got < 2 {
		t.Fatalf("readiness requests = %d, want retry before success", got)
	}
}

func TestWaitForFunctionServiceReadinessFailure(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = server.waitForFunctionServiceReadiness(ctx, "sb_test", "team-1", "user-1", mgr.SandboxAppService{ID: "api"})
	if err == nil {
		t.Fatal("waitForFunctionServiceReadiness() error = nil, want readiness failure")
	}
	if !strings.Contains(err.Error(), "HTTP 503") || !strings.Contains(err.Error(), "did not pass") {
		t.Fatalf("error = %q, want readable readiness failure", err.Error())
	}
}

func TestWaitForFunctionServiceReadinessTimeout(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}
	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
		logger:     zap.NewNop(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err = server.waitForFunctionServiceReadiness(ctx, "sb_test", "team-1", "user-1", mgr.SandboxAppService{ID: "api"})
	if err == nil {
		t.Fatal("waitForFunctionServiceReadiness() error = nil, want timeout")
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "did not pass") {
		t.Fatalf("error = %q, want readiness timeout with context deadline", err.Error())
	}
}

func TestStartFunctionServiceRuntimeWarmProcessUsesExistingCMDContext(t *testing.T) {
	_, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate ed25519 keypair: %v", err)
	}

	clusterGateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
			http.Error(w, "unexpected method", http.StatusInternalServerError)
			return
		}
		if r.URL.Path != "/api/v1/sandboxes/sb_test/contexts" {
			t.Errorf("path = %s, want /api/v1/sandboxes/sb_test/contexts", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Header.Get(internalauth.DefaultTokenHeader) == "" {
			t.Error("internal auth header is empty")
			http.Error(w, "missing token", http.StatusUnauthorized)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"contexts":[{"id":"ctx-api","type":"cmd","alias":"api","running":true}]}`))
	}))
	defer clusterGateway.Close()

	server := &Server{
		cfg: &config.FunctionGatewayConfig{
			DefaultClusterGatewayURL: clusterGateway.URL,
		},
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{
			Caller:     internalauth.ServiceFunctionGateway,
			PrivateKey: privateKey,
			TTL:        time.Minute,
		}),
		httpClient: clusterGateway.Client(),
	}
	contextID, err := server.startFunctionServiceRuntime(context.Background(), "sb_test", "team-1", "user-1", mgr.SandboxAppService{
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:            mgr.SandboxAppServiceRuntimeWarmProcess,
			WarmProcessName: "api",
		},
	})
	if err != nil {
		t.Fatalf("startFunctionServiceRuntime() error = %v", err)
	}
	if contextID != "ctx-api" {
		t.Fatalf("contextID = %q, want ctx-api", contextID)
	}
}

func TestClaimMountsFromRevisionUsesPreparedVolume(t *testing.T) {
	server := &Server{}
	mounts, err := server.claimMountsFromRevision(&functions.Revision{
		RestoreMounts: []functions.RestoreMount{{
			SandboxVolumeID:       "revision-volume",
			SourceSandboxVolumeID: "source-volume",
			SnapshotID:            "snapshot-1",
			MountPoint:            "/workspace/data",
		}},
	})
	if err != nil {
		t.Fatalf("claimMountsFromRevision() error = %v", err)
	}
	if len(mounts) != 1 {
		t.Fatalf("mount count = %d, want 1", len(mounts))
	}
	if mounts[0].SandboxVolumeID != "revision-volume" || mounts[0].MountPoint != "/workspace/data" {
		t.Fatalf("mount = %+v, want prepared revision volume", mounts[0])
	}
}

func TestClaimMountsFromRevisionRequiresPreparedVolume(t *testing.T) {
	server := &Server{}
	_, err := server.claimMountsFromRevision(&functions.Revision{
		RestoreMounts: []functions.RestoreMount{{
			SnapshotID: "snapshot-1",
			MountPoint: "/workspace/data",
		}},
	})
	if err == nil {
		t.Fatal("claimMountsFromRevision() error = nil, want missing volume error")
	}
}

func TestFunctionDomainAPIRouteUsesFunctionDispatch(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{
		router: gin.New(),
		cfg: &config.FunctionGatewayConfig{
			FunctionRootDomain: "sandbox0.test",
			FunctionRegionID:   "us-east-1",
		},
		functionRepo: functions.NewRepository(nil),
		logger:       zap.NewNop(),
	}
	server.router.NoRoute(server.handleNoRoute)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/functions", nil)
	req.Host = "demo.us-east-1.sandbox0.test"
	rec := httptest.NewRecorder()
	server.router.ServeHTTP(rec, req)

	var body struct {
		Error struct {
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if rec.Code != http.StatusNotFound || body.Error.Message != "function not found" {
		t.Fatalf("response = %d %q, want function dispatch not platform API 404", rec.Code, body.Error.Message)
	}
}
