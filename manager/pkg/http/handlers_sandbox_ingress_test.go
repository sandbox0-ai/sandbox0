package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestSandboxClaimIngressStartedAtUsesSignedClaimsOnly(t *testing.T) {
	want := time.Date(2026, time.August, 20, 8, 9, 10, 123456789, time.FixedZone("test", 8*60*60))
	claims := &internalauth.Claims{Audit: &internalauth.AuditContext{IngressStartedAt: &want}}
	got := sandboxClaimIngressStartedAt(claims)
	if !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("started at = %s (%s), want %s in UTC", got, got.Location(), want)
	}
	if got := sandboxClaimIngressStartedAt(nil); !got.IsZero() {
		t.Fatalf("nil claims start = %s, want zero", got)
	}
}

type recordingSandboxClaimer struct {
	request *service.ClaimRequest
}

func (r *recordingSandboxClaimer) ClaimSandbox(_ context.Context, request *service.ClaimRequest) (*service.ClaimResponse, error) {
	r.request = request
	return &service.ClaimResponse{SandboxID: "sandbox-1"}, nil
}

func TestClaimSandboxPropagatesSignedOperationIdentityOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	startedAt := time.Date(2026, 8, 20, 4, 5, 6, 0, time.FixedZone("offset", 8*60*60))
	claimer := &recordingSandboxClaimer{}
	server := &Server{sandboxClaimer: claimer, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes?operation_id=spoofed", strings.NewReader(
		`{"template":"default","operation_id":"spoofed","started_at":"2000-01-01T00:00:00Z"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		TeamID: "team-1", UserID: "user-1",
		Audit: &internalauth.AuditContext{OperationID: "operation-signed", IngressStartedAt: &startedAt},
	}))
	ginContext.Request = request

	server.claimSandbox(ginContext)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if claimer.request == nil || claimer.request.OperationID != "operation-signed" ||
		!claimer.request.StartedAt.Equal(startedAt.UTC()) {
		t.Fatalf("claim request = %#v, want signed operation and ingress time", claimer.request)
	}
}

func TestSandboxClaimOperationIDUsesSignedClaimsOnly(t *testing.T) {
	claims := &internalauth.Claims{Audit: &internalauth.AuditContext{OperationID: " operation-1 "}}
	if got := sandboxClaimOperationID(claims); got != "operation-1" {
		t.Fatalf("operation ID = %q, want operation-1", got)
	}
	if got := sandboxClaimOperationID(nil); got != "" {
		t.Fatalf("nil operation ID = %q, want empty", got)
	}
}

type recordingSandboxForker struct {
	sourceID string
	teamID   string
	userID   string
	request  *service.ForkSandboxRequest
}

func (r *recordingSandboxForker) ForkSandbox(
	_ context.Context,
	sourceID, teamID, userID string,
	request *service.ForkSandboxRequest,
) (*service.ForkSandboxResponse, error) {
	r.sourceID, r.teamID, r.userID = sourceID, teamID, userID
	copy := *request
	r.request = &copy
	return &service.ForkSandboxResponse{SourceSandboxID: sourceID}, nil
}

func TestForkSandboxUsesRuntimeBackendAndSignedOperation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	startedAt := time.Date(2026, 8, 20, 5, 6, 7, 0, time.FixedZone("offset", 8*60*60))
	forker := &recordingSandboxForker{}
	server := &Server{sandboxForker: forker, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Params = gin.Params{{Key: "id", Value: "sandbox-source"}}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes/sandbox-source/fork?operation_id=spoofed",
		strings.NewReader(`{"operation_id":"spoofed","config":{"ttl":30}}`))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		TeamID: "team-1", UserID: "user-1",
		Audit: &internalauth.AuditContext{OperationID: "operation-signed", IngressStartedAt: &startedAt},
	}))
	ginContext.Request = request

	server.forkSandbox(ginContext)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if forker.sourceID != "sandbox-source" || forker.teamID != "team-1" || forker.userID != "user-1" ||
		forker.request == nil || forker.request.OperationID != "operation-signed" ||
		!forker.request.StartedAt.Equal(startedAt.UTC()) || forker.request.Config == nil ||
		forker.request.Config.TTL == nil || *forker.request.Config.TTL != 30 {
		t.Fatalf("fork backend request = source=%q team=%q user=%q request=%+v",
			forker.sourceID, forker.teamID, forker.userID, forker.request)
	}
}
