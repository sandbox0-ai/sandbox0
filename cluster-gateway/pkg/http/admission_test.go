package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/infra-operator/api/config"
	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/admission"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type runtimeAdmissionStore struct {
	record admission.Record
	found  bool
	err    error
}

func (s *runtimeAdmissionStore) Get(context.Context, string) (admission.Record, bool, error) {
	return s.record, s.found, s.err
}

func (s *runtimeAdmissionStore) Put(
	context.Context,
	string,
	admission.Update,
) (admission.PutResult, error) {
	return admission.PutResult{}, nil
}

func TestEnforceRuntimeStartAdmission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("self hosted without projection", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", nil)
		server := &Server{cfg: &config.ClusterGatewayConfig{}}
		if !server.enforceRuntimeStartAdmission(ctx, "team") {
			t.Fatalf("self-hosted admission was rejected: %s", recorder.Body.String())
		}
	})

	t.Run("hosted missing projection", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/", nil)
		cfg := &config.ClusterGatewayConfig{}
		cfg.AdmissionRequireState = true
		server := &Server{
			cfg:            cfg,
			admissionStore: &runtimeAdmissionStore{},
			logger:         zap.NewNop(),
		}
		if server.enforceRuntimeStartAdmission(ctx, "team") ||
			recorder.Code != http.StatusServiceUnavailable {
			t.Fatalf("missing admission status = %d body=%s", recorder.Code, recorder.Body.String())
		}
	})
}

func TestContextAutoResumeHonorsAdmission(t *testing.T) {
	pausedSandbox := &mgr.Sandbox{
		ID:         "sb-demo",
		TeamID:     "team-a",
		UserID:     "user-a",
		Status:     mgr.SandboxStatusPaused,
		Paused:     true,
		AutoResume: true,
	}
	var resumed atomic.Bool
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/sandboxes/sb-demo":
			_ = spec.WriteSuccess(w, http.StatusOK, pausedSandbox)
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/sandboxes/sb-demo/resume":
			resumed.Store(true)
			_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"sandbox_id": "sb-demo"})
		default:
			t.Fatalf("unexpected manager request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer manager.Close()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	server := &Server{
		cfg: &config.ClusterGatewayConfig{},
		admissionStore: &runtimeAdmissionStore{
			found: true,
			record: admission.Record{
				State:   admission.StateRestricted,
				Version: 9,
				Reason:  "insufficient_credits",
			},
		},
		managerClient: client.NewManagerClient(manager.URL, generator, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}

	address, recorder := mustGetProcdURL(t, server, "team-a", "user-a", "sb-demo")
	if address != nil ||
		recorder.Code != http.StatusForbidden ||
		resumed.Load() {
		t.Fatalf(
			"context admission address=%v status=%d resumed=%v body=%s",
			address,
			recorder.Code,
			resumed.Load(),
			recorder.Body.String(),
		)
	}
}

func TestInternalResumeHonorsAdmission(t *testing.T) {
	managerCalled := false
	manager := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		managerCalled = true
	}))
	defer manager.Close()
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	generator := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller:     "cluster-gateway",
		PrivateKey: privateKey,
		TTL:        time.Minute,
	})
	server := &Server{
		cfg: &config.ClusterGatewayConfig{},
		admissionStore: &runtimeAdmissionStore{
			found:  true,
			record: admission.Record{State: admission.StateRestricted, Version: 3},
		},
		managerClient: client.NewManagerClient(manager.URL, generator, zap.NewNop(), time.Second),
		logger:        zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Params = gin.Params{{Key: "id", Value: "sb-demo"}}
	request := httptest.NewRequest(http.MethodPost, "/internal/v1/sandboxes/sb-demo/resume", nil)
	authContext := &gatewayauthn.AuthContext{TeamID: "team-a", UserID: "user-a"}
	ctx.Set("auth_context", authContext)
	ctx.Request = request.WithContext(gatewayauthn.WithAuthContext(request.Context(), authContext))

	server.resumeInternalSandbox(ctx)

	if recorder.Code != http.StatusForbidden || managerCalled {
		t.Fatalf(
			"internal resume status=%d manager_called=%v body=%s",
			recorder.Code,
			managerCalled,
			recorder.Body.String(),
		)
	}
	if !strings.Contains(recorder.Body.String(), spec.CodeAdmissionRestricted) {
		t.Fatalf("internal resume error = %s", recorder.Body.String())
	}
}
