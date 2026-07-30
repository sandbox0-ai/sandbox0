package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	"go.uber.org/zap"
)

func TestCreateContextHonorsDisabledUpstreamTimeout(t *testing.T) {
	gin.SetMode(gin.TestMode)

	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/contexts" {
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
		time.Sleep(50 * time.Millisecond)
		_ = spec.WriteSuccess(w, http.StatusCreated, map[string]any{"id": "ctx-1"})
	}))
	defer procd.Close()

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = spec.WriteSuccess(w, http.StatusOK, mgr.Sandbox{
			ID:           "sb-1",
			TeamID:       "team-a",
			UserID:       "user-a",
			InternalAddr: procd.URL,
			Status:       mgr.SandboxStatusRunning,
		})
	}))
	defer manager.Close()

	cfg := &config.ClusterGatewayConfig{}
	cfg.ProxyTimeout.Duration = 10 * time.Millisecond
	server := &Server{
		cfg:             cfg,
		managerClient:   client.NewManagerClient(manager.URL, tokenGen, zap.NewNop(), time.Second),
		internalAuthGen: tokenGen,
		logger:          zap.NewNop(),
		httpClient:      &http.Client{Timeout: cfg.ProxyTimeout.Duration},
	}

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Params = gin.Params{{Key: "id", Value: "sb-1"}}
	req := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/sandboxes/sb-1/contexts",
		strings.NewReader(`{"type":"cmd","cmd":{"command":["sh","-lc","true"]},"wait_until_done":true}`),
	)
	req = proxy.WithUpstreamTimeoutDisabledRequest(req)
	authCtx := &gatewayauthn.AuthContext{TeamID: "team-a", UserID: "user-a"}
	ctx.Set("auth_context", authCtx)
	ctx.Request = req.WithContext(gatewayauthn.WithAuthContext(req.Context(), authCtx))

	server.createContext(ctx)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusCreated, rec.Body.String())
	}
}

func TestGetProcdURLFetchesManagerForEachRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	managerURL, managerSpy, tokenGen, cleanup := newGetProcdURLTestManager(t)
	defer cleanup()

	server := &Server{
		managerClient: client.NewManagerClient(managerURL, tokenGen, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}

	addr, rec := mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("first team A status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := addr.String(); got != "http://127.0.0.1:7777" {
		t.Fatalf("first team A procd url = %q, want %q", got, "http://127.0.0.1:7777")
	}

	addr, rec = mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("second team A status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := addr.String(); got != "http://127.0.0.1:7777" {
		t.Fatalf("second team A procd url = %q, want %q", got, "http://127.0.0.1:7777")
	}

	addr, rec = mustGetProcdURL(t, server, "team-b", "user-b", "sb-1")
	if addr != nil {
		t.Fatalf("team B expected nil addr, got %q", addr.String())
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("team B status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := managerSpy.teamIDs(); len(got) != 3 || got[0] != "team-a" || got[1] != "team-a" || got[2] != "team-b" {
		t.Fatalf("manager team ids = %#v, want [team-a team-a team-b]", got)
	}
}

func TestGetProcdURLReturnsNotFoundForMissingSandbox(t *testing.T) {
	gin.SetMode(gin.TestMode)

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/sandboxes/missing-sandbox" {
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
		_ = spec.WriteError(w, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
	}))
	defer manager.Close()

	server := &Server{
		managerClient: client.NewManagerClient(manager.URL, tokenGen, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}

	addr, rec := mustGetProcdURL(t, server, "team-a", "user-a", "missing-sandbox")

	if addr != nil {
		t.Fatalf("addr = %v, want nil", addr)
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestGetProcdURLRechecksPausedStateAfterSuccessfulAccess(t *testing.T) {
	gin.SetMode(gin.TestMode)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "manager",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"cluster-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})

	var getCalls int
	var resumeCalls int
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader)); err != nil {
			t.Fatalf("validate token: %v", err)
		}
		switch {
		case r.Method == http.MethodGet:
			getCalls++
			sandbox := mgr.Sandbox{
				ID:           "sb-1",
				TeamID:       "team-a",
				UserID:       "user-a",
				InternalAddr: "http://127.0.0.1:7777",
				Status:       mgr.SandboxStatusRunning,
				AutoResume:   true,
			}
			if getCalls == 2 {
				sandbox.Paused = true
				sandbox.Status = mgr.SandboxStatusPaused
				sandbox.InternalAddr = ""
			}
			if getCalls >= 3 {
				sandbox.Paused = false
				sandbox.Status = mgr.SandboxStatusRunning
				sandbox.InternalAddr = "http://127.0.0.1:7777"
			}
			_ = spec.WriteSuccess(w, http.StatusOK, sandbox)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-1/resume":
			resumeCalls++
			_ = spec.WriteSuccess(w, http.StatusOK, mgr.ResumeSandboxResponse{
				SandboxID: "sb-1",
				Resumed:   true,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false})
		}
	}))
	defer manager.Close()

	server := &Server{
		managerClient: client.NewManagerClient(manager.URL, tokenGen, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}

	addr, _ := mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if addr == nil || addr.String() != "http://127.0.0.1:7777" {
		t.Fatalf("first addr = %v, want http://127.0.0.1:7777", addr)
	}

	addr, _ = mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if addr == nil || addr.String() != "http://127.0.0.1:7777" {
		t.Fatalf("second addr = %v, want http://127.0.0.1:7777", addr)
	}
	if getCalls != 3 {
		t.Fatalf("getCalls = %d, want 3", getCalls)
	}
	if resumeCalls != 1 {
		t.Fatalf("resumeCalls = %d, want 1", resumeCalls)
	}
}

func TestGetProcdURLPausedSandboxReturnsWakingUp(t *testing.T) {
	gin.SetMode(gin.TestMode)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "manager",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"cluster-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})

	var resumeCalls int
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader)); err != nil {
			t.Fatalf("validate token: %v", err)
		}
		switch {
		case r.Method == http.MethodGet:
			_ = spec.WriteSuccess(w, http.StatusOK, mgr.Sandbox{
				ID:           "sb-1",
				TeamID:       "team-a",
				UserID:       "user-a",
				InternalAddr: "",
				Status:       mgr.SandboxStatusPaused,
				Paused:       true,
				AutoResume:   true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-1/resume":
			resumeCalls++
			_ = spec.WriteSuccess(w, http.StatusOK, mgr.ResumeSandboxResponse{
				SandboxID: "sb-1",
				Resumed:   true,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false})
		}
	}))
	defer manager.Close()

	server := &Server{
		managerClient: client.NewManagerClient(manager.URL, tokenGen, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}

	addr, rec := mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if addr != nil {
		t.Fatalf("addr = %v, want nil", addr)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertGatewayError(t, rec, spec.CodeUnavailable, "sandbox is waking up")
	if resumeCalls != 1 {
		t.Fatalf("resumeCalls = %d, want 1", resumeCalls)
	}
}

func TestGetProcdURLReportsDefinitiveResumeFailure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "manager",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"cluster-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})

	var resumeCalls int
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader)); err != nil {
			t.Fatalf("validate token: %v", err)
		}
		switch {
		case r.Method == http.MethodGet:
			_ = spec.WriteSuccess(w, http.StatusOK, mgr.Sandbox{
				ID:         "sb-1",
				TeamID:     "team-a",
				UserID:     "user-a",
				Status:     mgr.SandboxStatusPaused,
				Paused:     true,
				AutoResume: true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-1/resume":
			resumeCalls++
			_ = spec.WriteError(w, http.StatusGatewayTimeout, spec.CodeUnavailable, "runtime initialization timed out")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer manager.Close()

	server := &Server{
		managerClient: client.NewManagerClient(manager.URL, tokenGen, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}

	addr, rec := mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if addr != nil {
		t.Fatalf("addr = %v, want nil", addr)
	}
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
	assertGatewayError(t, rec, spec.CodeSandboxResumeFailed, "sandbox resume failed")
	if resumeCalls != 1 {
		t.Fatalf("resumeCalls = %d, want 1", resumeCalls)
	}
}

func assertGatewayError(t *testing.T, rec *httptest.ResponseRecorder, code string, message string) {
	t.Helper()

	var response spec.Response
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode gateway response: %v", err)
	}
	if response.Error == nil {
		t.Fatal("gateway response error is nil")
	}
	if response.Error.Code != code {
		t.Fatalf("error code = %q, want %q", response.Error.Code, code)
	}
	if response.Error.Message != message {
		t.Fatalf("error message = %q, want %q", response.Error.Message, message)
	}
}

func TestSandboxRuntimeMissing(t *testing.T) {
	tests := []struct {
		name    string
		sandbox *mgr.Sandbox
		want    bool
	}{
		{
			name:    "running with address",
			sandbox: &mgr.Sandbox{Status: mgr.SandboxStatusRunning, InternalAddr: "http://127.0.0.1:7777"},
			want:    false,
		},
		{
			name:    "running without address",
			sandbox: &mgr.Sandbox{Status: mgr.SandboxStatusRunning},
			want:    true,
		},
		{
			name:    "starting with stale address",
			sandbox: &mgr.Sandbox{Status: mgr.SandboxStatusStarting, InternalAddr: "http://127.0.0.1:7777"},
			want:    true,
		},
		{
			name:    "paused with stale address",
			sandbox: &mgr.Sandbox{Status: mgr.SandboxStatusPaused, InternalAddr: "http://127.0.0.1:7777"},
			want:    true,
		},
		{
			name:    "failed with stale address",
			sandbox: &mgr.Sandbox{Status: mgr.SandboxStatusFailed, InternalAddr: "http://127.0.0.1:7777"},
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sandboxRuntimeMissing(tt.sandbox); got != tt.want {
				t.Fatalf("sandboxRuntimeMissing() = %t, want %t", got, tt.want)
			}
		})
	}
}

func mustGetProcdURL(t *testing.T, server *Server, teamID, userID, sandboxID string) (*url.URL, *httptest.ResponseRecorder) {
	t.Helper()

	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/sandboxes/"+sandboxID+"/contexts", nil)
	ctx.Request = req

	authCtx := &gatewayauthn.AuthContext{
		TeamID: teamID,
		UserID: userID,
	}
	ctx.Set("auth_context", authCtx)
	ctx.Request = ctx.Request.WithContext(gatewayauthn.WithAuthContext(ctx.Request.Context(), authCtx))

	addr, err := server.getProcdURL(ctx, sandboxID)
	if err != nil {
		return nil, rec
	}
	return addr, rec
}

type getProcdURLManagerSpy struct {
	mu    sync.Mutex
	teams []string
}

func (s *getProcdURLManagerSpy) add(teamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.teams = append(s.teams, teamID)
}

func (s *getProcdURLManagerSpy) teamIDs() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.teams))
	copy(out, s.teams)
	return out
}

func newGetProcdURLTestManager(t *testing.T) (string, *getProcdURLManagerSpy, *internalauth.Generator, func()) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate keypair: %v", err)
	}
	validator := internalauth.NewValidator(internalauth.ValidatorConfig{
		Target:             "manager",
		PublicKey:          publicKey,
		AllowedCallers:     []string{"cluster-gateway"},
		ClockSkewTolerance: 5 * time.Second,
	})
	tokenGen := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	spy := &getProcdURLManagerSpy{}

	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		claims, err := validator.Validate(r.Header.Get(internalauth.DefaultTokenHeader))
		if err != nil {
			t.Errorf("validate token: %v", err)
			_ = spec.WriteError(w, http.StatusUnauthorized, spec.CodeUnauthorized, "invalid token")
			return
		}

		spy.add(claims.TeamID)
		_ = spec.WriteSuccess(w, http.StatusOK, mgr.Sandbox{
			ID:           "sb-1",
			TeamID:       "team-a",
			UserID:       "user-a",
			InternalAddr: "http://127.0.0.1:7777",
			Status:       mgr.SandboxStatusRunning,
		})
	}))

	cleanup := func() {
		manager.Close()
	}
	return manager.URL, spy, tokenGen, cleanup
}
