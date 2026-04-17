package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

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
				sandbox.PowerState = mgr.SandboxPowerState{
					Desired:            mgr.SandboxPowerStatePaused,
					DesiredGeneration:  3,
					Observed:           mgr.SandboxPowerStatePaused,
					ObservedGeneration: 3,
					Phase:              mgr.SandboxPowerPhaseStable,
				}
			}
			_ = spec.WriteSuccess(w, http.StatusOK, sandbox)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-1/resume":
			resumeCalls++
			_ = spec.WriteSuccess(w, http.StatusOK, mgr.ResumeSandboxResponse{
				SandboxID: "sb-1",
				Resumed:   true,
				PowerState: mgr.SandboxPowerState{
					Desired:            mgr.SandboxPowerStateActive,
					DesiredGeneration:  4,
					Observed:           mgr.SandboxPowerStateActive,
					ObservedGeneration: 4,
					Phase:              mgr.SandboxPowerPhaseStable,
				},
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
	if getCalls != 2 {
		t.Fatalf("getCalls = %d, want 2", getCalls)
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
				InternalAddr: "http://127.0.0.1:7777",
				Status:       mgr.SandboxStatusRunning,
				Paused:       false,
				AutoResume:   true,
				PowerState: mgr.SandboxPowerState{
					Desired:            mgr.SandboxPowerStatePaused,
					DesiredGeneration:  3,
					Observed:           mgr.SandboxPowerStatePaused,
					ObservedGeneration: 3,
					Phase:              mgr.SandboxPowerPhaseStable,
				},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-1/resume":
			resumeCalls++
			_ = spec.WriteSuccess(w, http.StatusOK, mgr.ResumeSandboxResponse{
				SandboxID: "sb-1",
				Resumed:   true,
				PowerState: mgr.SandboxPowerState{
					Desired:            mgr.SandboxPowerStateActive,
					DesiredGeneration:  4,
					Observed:           mgr.SandboxPowerStateActive,
					ObservedGeneration: 4,
					Phase:              mgr.SandboxPowerPhaseStable,
				},
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
		t.Fatalf("addr = %v, want http://127.0.0.1:7777", addr)
	}
	if resumeCalls != 1 {
		t.Fatalf("resumeCalls = %d, want 1", resumeCalls)
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
