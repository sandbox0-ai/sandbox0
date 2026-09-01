package http

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
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
	return &service.ClaimResponse{
		SandboxID: "sandbox-1", CommandReadyDuration: 375 * time.Millisecond, CommandReadyWithinSLO: true,
	}, nil
}

func TestClaimSandboxPropagatesSignedOperationIdentityOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	startedAt := time.Date(2026, 8, 20, 4, 5, 6, 0, time.FixedZone("offset", 8*60*60))
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 keypair: %v", err)
	}
	token, err := internalauth.NewGenerator(internalauth.GeneratorConfig{
		Caller: internalauth.ServiceClusterGateway, PrivateKey: privateKey, TTL: time.Minute,
	}).Generate(internalauth.ServiceManager, "team-1", "user-1", internalauth.GenerateOptions{
		Audit: &internalauth.AuditContext{OperationID: "operation-signed", IngressStartedAt: &startedAt},
	})
	if err != nil {
		t.Fatalf("generate internal token: %v", err)
	}
	claimer := &recordingSandboxClaimer{}
	server := &Server{
		sandboxClaimer: claimer,
		authValidator: internalauth.NewValidator(internalauth.ValidatorConfig{
			Target:             internalauth.ServiceManager,
			PublicKey:          publicKey,
			AllowedCallers:     []string{internalauth.ServiceClusterGateway},
			ClockSkewTolerance: 5 * time.Second,
		}),
		logger: zap.NewNop(),
	}
	router := gin.New()
	router.POST("/api/v1/sandboxes", server.authMiddleware(), server.claimSandbox)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes?operation_id=spoofed", strings.NewReader(
		`{"template":"default","operation_id":"spoofed","started_at":"2000-01-01T00:00:00Z"}`,
	))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(internalauth.DefaultTokenHeader, token)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if claimer.request == nil || claimer.request.OperationID != "operation-signed" ||
		!claimer.request.StartedAt.Equal(startedAt.UTC()) {
		t.Fatalf("claim request = %#v, want signed operation and ingress time", claimer.request)
	}
	if got := recorder.Header().Get("Server-Timing"); got != "sandbox0-command-ready;dur=375.000" {
		t.Fatalf("Server-Timing = %q", got)
	}
	if got := recorder.Header().Get("Sandbox0-Command-Ready-SLO"); got != "met" {
		t.Fatalf("SLO header = %q", got)
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

type recordingSandboxRootFSService struct {
	SandboxRootFSService
	sandboxID string
	teamID    string
	request   *service.CreateSandboxRootFSSnapshotRequest
}

func (r *recordingSandboxRootFSService) CreateSandboxRootFSSnapshot(
	_ context.Context,
	sandboxID, teamID string,
	request *service.CreateSandboxRootFSSnapshotRequest,
) (*service.SandboxRootFSSnapshot, error) {
	r.sandboxID, r.teamID = sandboxID, teamID
	copy := *request
	r.request = &copy
	return &service.SandboxRootFSSnapshot{ID: "snapshot-1", SandboxID: sandboxID}, nil
}

func TestCreateSandboxRootFSSnapshotUsesSignedOperationIdentity(t *testing.T) {
	gin.SetMode(gin.TestMode)
	startedAt := time.Date(2026, 8, 21, 7, 8, 9, 123456000, time.FixedZone("offset", 8*60*60))
	rootFS := &recordingSandboxRootFSService{}
	server := &Server{sandboxRootFS: rootFS, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Params = gin.Params{{Key: "id", Value: "sandbox-source"}}
	request := httptest.NewRequest(http.MethodPost,
		"/api/v1/sandboxes/sandbox-source/snapshots?operation_id=spoofed",
		strings.NewReader(`{"name":"checkpoint","operation_id":"spoofed","started_at":"2000-01-01T00:00:00Z"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		TeamID: "team-1", UserID: "user-1",
		Audit: &internalauth.AuditContext{OperationID: "operation-signed", IngressStartedAt: &startedAt},
	}))
	ginContext.Request = request

	server.createSandboxRootFSSnapshot(ginContext)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusCreated, recorder.Body.String())
	}
	if rootFS.sandboxID != "sandbox-source" || rootFS.teamID != "team-1" || rootFS.request == nil ||
		rootFS.request.Name != "checkpoint" || rootFS.request.OperationID != "operation-signed" ||
		!rootFS.request.StartedAt.Equal(startedAt.UTC()) {
		t.Fatalf("rootfs snapshot request = sandbox=%q team=%q request=%+v",
			rootFS.sandboxID, rootFS.teamID, rootFS.request)
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

type recordingSandboxRootFSRebaser struct {
	sandboxID string
	teamID    string
	request   *service.RebaseSandboxRootFSRequest
}

func (r *recordingSandboxRootFSRebaser) RebaseSandboxRootFS(
	_ context.Context,
	sandboxID, teamID string,
	request *service.RebaseSandboxRootFSRequest,
) (*service.RebaseSandboxRootFSResponse, error) {
	r.sandboxID, r.teamID = sandboxID, teamID
	copy := *request
	r.request = &copy
	return &service.RebaseSandboxRootFSResponse{SandboxID: sandboxID}, nil
}

func TestRebaseSandboxRootFSUsesSignedOperationAndPUTBackend(t *testing.T) {
	gin.SetMode(gin.TestMode)
	startedAt := time.Date(2026, 8, 21, 5, 6, 7, 123456000, time.FixedZone("offset", 8*60*60))
	rebaser := &recordingSandboxRootFSRebaser{}
	server := &Server{sandboxRootFSRebaser: rebaser, logger: zap.NewNop()}
	recorder := httptest.NewRecorder()
	ginContext, _ := gin.CreateTestContext(recorder)
	ginContext.Params = gin.Params{{Key: "id", Value: "sandbox-source"}}
	request := httptest.NewRequest(http.MethodPut,
		"/api/v1/sandboxes/sandbox-source/rootfs/rebase?operation_id=spoofed",
		strings.NewReader(`{"target_base_artifact_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","rollback_ttl":3600,"operation_id":"spoofed"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		TeamID: "team-1", UserID: "user-1",
		Audit: &internalauth.AuditContext{OperationID: "operation-signed", IngressStartedAt: &startedAt},
	}))
	ginContext.Request = request

	server.rebaseSandboxRootFS(ginContext)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusOK, recorder.Body.String())
	}
	if rebaser.sandboxID != "sandbox-source" || rebaser.teamID != "team-1" || rebaser.request == nil ||
		rebaser.request.OperationID != "operation-signed" || !rebaser.request.StartedAt.Equal(startedAt.UTC()) ||
		rebaser.request.RollbackTTL == nil || *rebaser.request.RollbackTTL != 3600 {
		t.Fatalf("rebase backend request = sandbox=%q team=%q request=%+v",
			rebaser.sandboxID, rebaser.teamID, rebaser.request)
	}
}
