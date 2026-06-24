package http

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	mgr "github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestSandboxServiceRuntimeEnvVarsUsesServiceRuntimeType(t *testing.T) {
	service := &mgr.SandboxAppService{
		ID:   "web",
		Port: 3000,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeNextJS,
			EnvVars: map[string]string{"APP_ENV": "test"},
		},
	}

	env := sandboxServiceRuntimeEnvVars(service)
	if env[sandboxServiceRuntimeTypeEnv] != mgr.SandboxAppServiceRuntimeNextJS {
		t.Fatalf("runtime env = %q, want nextjs", env[sandboxServiceRuntimeTypeEnv])
	}
	if env[sandboxServiceRuntimeServiceIDEnv] != "web" || env[sandboxServiceRuntimePortEnv] != "3000" || env["APP_ENV"] != "test" {
		t.Fatalf("env = %#v, want service metadata and runtime vars", env)
	}
}

func TestEnsureSandboxServiceNextJSRuntimeCallsProcd(t *testing.T) {
	var got procdEnsureNextJSRuntimeRequest
	procd := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/services/nextjs/ensure" {
			t.Fatalf("unexpected procd request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatalf("decode procd request: %v", err)
		}
		_ = spec.WriteSuccess(w, http.StatusOK, map[string]any{"running": true})
	}))
	defer procd.Close()

	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	server := &Server{
		httpClient:      procd.Client(),
		internalAuthGen: internalauth.NewGenerator(internalauth.GeneratorConfig{Caller: "cluster-gateway", PrivateKey: privateKey, TTL: time.Minute}),
	}
	sandbox := &mgr.Sandbox{ID: "sb-1", TeamID: "team-1", InternalAddr: procd.URL}
	service := &mgr.SandboxAppService{
		ID:   "web",
		Port: 3000,
		Runtime: &mgr.SandboxAppServiceRuntime{
			Type:    mgr.SandboxAppServiceRuntimeNextJS,
			CWD:     "/workspace/app",
			EnvVars: map[string]string{"APP_ENV": "test"},
		},
	}

	if err := server.ensureSandboxServiceNextJSRuntime(t.Context(), sandbox, service); err != nil {
		t.Fatalf("ensureSandboxServiceNextJSRuntime: %v", err)
	}
	if got.ServiceID != "web" || got.Port != 3000 || got.CWD != "/workspace/app" {
		t.Fatalf("request = %#v, want service metadata", got)
	}
	if got.EnvVars[sandboxServiceRuntimeTypeEnv] != mgr.SandboxAppServiceRuntimeNextJS || got.EnvVars["APP_ENV"] != "test" {
		t.Fatalf("env vars = %#v, want nextjs runtime env", got.EnvVars)
	}
}
