package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/egressauth"
	egressauthruntime "github.com/sandbox0-ai/sandbox0/pkg/egressauth/runtime"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

type egressAuthBindingStore struct {
	record        *egressauth.BindingRecord
	sourceVersion *egressauth.CredentialSourceVersion
}

func (s *egressAuthBindingStore) GetBindings(context.Context, string, string) (*egressauth.BindingRecord, error) {
	return s.record, nil
}

func (s *egressAuthBindingStore) UpsertBindings(context.Context, *egressauth.BindingRecord) error {
	return nil
}

func (s *egressAuthBindingStore) DeleteBindings(context.Context, string, string) error {
	return nil
}

func (s *egressAuthBindingStore) GetSourceByRef(context.Context, string, string) (*egressauth.CredentialSource, error) {
	return nil, nil
}

func (s *egressAuthBindingStore) GetSourceVersion(context.Context, int64, int64) (*egressauth.CredentialSourceVersion, error) {
	return s.sourceVersion, nil
}

func TestResolveEgressAuthAllowsNetdCaller(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		logger: zap.NewNop(),
		egressAuthService: service.NewEgressAuthService(service.EgressAuthServiceConfig{
			DefaultResolveTTL: time.Minute,
			StaticAuth: []egressauthruntime.StaticAuthConfig{{
				AuthRef: "example-api",
				Headers: map[string]string{"Authorization": "Bearer static"},
				TTL:     time.Minute,
			}},
		}, &egressAuthBindingStore{}, zap.NewNop()),
	}

	reqBody, err := json.Marshal(egressauth.ResolveRequest{
		SandboxID:   "sbx-123",
		TeamID:      "team-123",
		AuthRef:     "example-api",
		Destination: "api.example.com",
		Protocol:    "http",
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/egress-auth/resolve", bytes.NewReader(reqBody))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		Caller:   "netd",
		Target:   "manager",
		Audience: "manager",
		IsSystem: true,
	}))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	srv.resolveEgressAuth(ctx)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	resp, apiErr, err := spec.DecodeResponse[egressauth.ResolveResponse](recorder.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", apiErr)
	}
	if got := resp.Headers["Authorization"]; got != "Bearer static" {
		t.Fatalf("authorization header = %q", got)
	}
}

func TestResolveEgressAuthRejectsNonNetdCaller(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := &Server{
		logger:            zap.NewNop(),
		egressAuthService: service.NewEgressAuthService(service.EgressAuthServiceConfig{}, nil, zap.NewNop()),
	}

	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/egress-auth/resolve", bytes.NewReader([]byte(`{"authRef":"example-api"}`)))
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{
		Caller:   "cluster-gateway",
		Target:   "manager",
		Audience: "manager",
	}))
	req.Header.Set("Content-Type", "application/json")
	ctx.Request = req

	srv.resolveEgressAuth(ctx)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusForbidden)
	}
}
