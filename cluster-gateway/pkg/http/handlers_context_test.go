package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/cache"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestGetProcdURLCacheIsScopedByTeam(t *testing.T) {
	gin.SetMode(gin.TestMode)

	managerURL, managerSpy, tokenGen, cleanup := newGetProcdURLTestManager(t)
	defer cleanup()

	server := &Server{
		managerClient: client.NewManagerClient(managerURL, tokenGen, zap.NewNop(), time.Second),
		sandboxAddrCache: cache.New[sandboxAddrCacheKey, *url.URL](cache.Config{
			MaxSize:         16,
			TTL:             time.Minute,
			CleanupInterval: time.Minute,
		}),
		logger: zap.NewNop(),
	}
	defer server.sandboxAddrCache.Close()

	addr, rec := mustGetProcdURL(t, server, "team-a", "user-a", "sb-1")
	if rec.Code != http.StatusOK {
		t.Fatalf("team A status = %d, want %d", rec.Code, http.StatusOK)
	}
	if got := addr.String(); got != "http://127.0.0.1:7777" {
		t.Fatalf("team A procd url = %q, want %q", got, "http://127.0.0.1:7777")
	}

	addr, rec = mustGetProcdURL(t, server, "team-b", "user-b", "sb-1")
	if addr != nil {
		t.Fatalf("team B expected nil addr, got %q", addr.String())
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("team B status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if got := managerSpy.teamIDs(); len(got) != 2 || got[0] != "team-a" || got[1] != "team-b" {
		t.Fatalf("manager team ids = %#v, want [team-a team-b]", got)
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
