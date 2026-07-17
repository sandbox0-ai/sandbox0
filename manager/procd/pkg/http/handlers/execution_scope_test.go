package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/execution"
	"github.com/sandbox0-ai/sandbox0/manager/procd/pkg/session"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
)

func TestExecutionScopeResolveRequiresNetdSandboxIdentity(t *testing.T) {
	handler := NewExecutionScopeHandler(nil, func() string { return "sandbox-1" })
	tests := []struct {
		name   string
		claims *internalauth.Claims
	}{
		{
			name: "wrong caller",
			claims: &internalauth.Claims{
				Caller:    internalauth.ServiceManager,
				TeamID:    "team-1",
				SandboxID: "sandbox-1",
			},
		},
		{
			name: "wrong sandbox",
			claims: &internalauth.Claims{
				Caller:    internalauth.ServiceNetd,
				TeamID:    "team-1",
				SandboxID: "sandbox-2",
			},
		},
		{
			name: "missing team",
			claims: &internalauth.Claims{
				Caller:    internalauth.ServiceNetd,
				SandboxID: "sandbox-1",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/api/v1/execution-scopes/resolve?transport=tcp&local_port=40000", nil)
			request = request.WithContext(internalauth.WithClaims(request.Context(), tt.claims))
			response := httptest.NewRecorder()
			handler.Resolve(response, request)
			if response.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", response.Code, http.StatusForbidden)
			}
		})
	}
}

func TestExecutionScopeResolveAcceptsCompleteSocketTuple(t *testing.T) {
	handler := NewExecutionScopeHandler(
		execution.NewResolver(t.TempDir(), func() []session.ExecutionScopeRoot { return nil }),
		func() string { return "sandbox-1" },
	)
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/execution-scopes/resolve?transport=tcp&local_ip=10.0.0.2&local_port=40000&remote_ip=8.8.8.8&remote_port=443",
		nil,
	)
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		Caller:    internalauth.ServiceNetd,
		TeamID:    "team-1",
		SandboxID: "sandbox-1",
	}))
	response := httptest.NewRecorder()
	handler.Resolve(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
}

func TestExecutionScopeResolveRequiresCompleteSocketTuple(t *testing.T) {
	handler := NewExecutionScopeHandler(nil, func() string { return "sandbox-1" })
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/v1/execution-scopes/resolve?transport=tcp&local_port=40000&remote_ip=8.8.8.8&remote_port=443",
		nil,
	)
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		Caller:    internalauth.ServiceNetd,
		TeamID:    "team-1",
		SandboxID: "sandbox-1",
	}))
	response := httptest.NewRecorder()
	handler.Resolve(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
