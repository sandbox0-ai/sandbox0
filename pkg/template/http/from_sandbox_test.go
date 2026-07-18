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
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type fromSandboxBuildStore struct {
	templatestore.TemplateBuildStore
	getByIdempotencyKey func(context.Context, string, string, string) (*template.Template, error)
	create              func(context.Context, *template.Template, *template.TemplateBuild) (*template.Template, bool, error)
}

func (s *fromSandboxBuildStore) GetTemplateByIdempotencyKey(ctx context.Context, scope, teamID, key string) (*template.Template, error) {
	if s.getByIdempotencyKey == nil {
		return nil, nil
	}
	return s.getByIdempotencyKey(ctx, scope, teamID, key)
}

func (s *fromSandboxBuildStore) CreateTemplateBuild(ctx context.Context, tpl *template.Template, build *template.TemplateBuild) (*template.Template, bool, error) {
	if s.create == nil {
		return tpl, true, nil
	}
	return s.create(ctx, tpl, build)
}

type fromSandboxSourceResolver struct {
	calls   int
	resolve func(context.Context, string, string) (*template.SandboxTemplateSource, error)
}

func (r *fromSandboxSourceResolver) ResolveSandboxTemplateSource(ctx context.Context, sandboxID, teamID string) (*template.SandboxTemplateSource, error) {
	r.calls++
	if r.resolve == nil {
		return nil, template.ErrTemplateSourceNotFound
	}
	return r.resolve(ctx, sandboxID, teamID)
}

func TestCreateTemplateFromSandboxRejectsUnknownFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "top level",
			body: `{"template_id":"demo","sandbox_id":"source","mainContainer":{"image":"unsafe"}}`,
		},
		{
			name: "spec override",
			body: `{"template_id":"demo","sandbox_id":"source","spec_overrides":{"network":{"mode":"allow-all"}}}`,
		},
		{
			name: "pool override",
			body: `{"template_id":"demo","sandbox_id":"source","spec_overrides":{"pool":{"minIdle":0,"maxIdle":1,"burst":2}}}`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			resolver := &fromSandboxSourceResolver{}
			handler := &Handler{
				BuildStore:     &fromSandboxBuildStore{},
				SourceResolver: resolver,
				Logger:         zap.NewNop(),
			}
			router := gin.New()
			router.Use(withClaims(&internalauth.Claims{TeamID: "team-1", UserID: "user-1"}))
			router.POST("/api/v1/templates/from-sandbox", handler.CreateTemplateFromSandbox)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/templates/from-sandbox", bytes.NewBufferString(tt.body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
			if resolver.calls != 0 {
				t.Fatalf("source resolver calls = %d, want 0", resolver.calls)
			}
		})
	}
}

func TestCreateTemplateFromSandboxIdempotentReplayDoesNotResolveSource(t *testing.T) {
	t.Parallel()

	sandboxID, err := naming.SandboxName("default", "source-template", "abcde")
	if err != nil {
		t.Fatalf("SandboxName() error = %v", err)
	}
	request := TemplateFromSandboxRequest{
		TemplateID: "derived",
		SandboxID:  sandboxID,
	}
	requestHash, err := templateFromSandboxRequestHash(request)
	if err != nil {
		t.Fatalf("templateFromSandboxRequestHash() error = %v", err)
	}
	startedAt := time.Now().UTC()
	existing := &template.Template{
		TemplateID:          request.TemplateID,
		Scope:               naming.ScopeTeam,
		TeamID:              "team-1",
		Spec:                validTemplateSpec(),
		CreationRequestHash: requestHash,
		Status: &v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State:     v1alpha1.TemplateCreationStateCreating,
				Stage:     v1alpha1.TemplateCreationStageCapturing,
				StartedAt: metav1Time(startedAt),
			},
		},
	}
	buildStore := &fromSandboxBuildStore{
		getByIdempotencyKey: func(_ context.Context, scope, teamID, key string) (*template.Template, error) {
			if scope != naming.ScopeTeam || teamID != "team-1" || key != "request-1" {
				t.Fatalf("unexpected idempotency lookup: scope=%q team=%q key=%q", scope, teamID, key)
			}
			return existing, nil
		},
		create: func(context.Context, *template.Template, *template.TemplateBuild) (*template.Template, bool, error) {
			t.Fatal("CreateTemplateBuild must not run for a replay")
			return nil, false, nil
		},
	}
	resolver := &fromSandboxSourceResolver{}
	handler := &Handler{
		BuildStore:     buildStore,
		SourceResolver: resolver,
		Logger:         zap.NewNop(),
	}
	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID:      "team-1",
		UserID:      "user-1",
		Permissions: []string{gatewayauthn.PermTemplateCreate, gatewayauthn.PermSandboxRead},
	}))
	router.POST("/api/v1/templates/from-sandbox", handler.CreateTemplateFromSandbox)

	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates/from-sandbox", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "request-1")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if resolver.calls != 0 {
		t.Fatalf("source resolver calls = %d, want 0", resolver.calls)
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
}

func TestCreateTemplateFromSandboxReplayAfterManualUpdateReturnsCurrentTemplate(t *testing.T) {
	t.Parallel()

	sandboxID, err := naming.SandboxName("default", "source-template", "abcde")
	if err != nil {
		t.Fatalf("SandboxName() error = %v", err)
	}
	request := TemplateFromSandboxRequest{
		TemplateID: "derived",
		SandboxID:  sandboxID,
	}
	requestHash, err := templateFromSandboxRequestHash(request)
	if err != nil {
		t.Fatalf("templateFromSandboxRequestHash() error = %v", err)
	}
	current := &template.Template{
		TemplateID:          request.TemplateID,
		Scope:               naming.ScopeTeam,
		TeamID:              "team-1",
		Spec:                validTemplateSpec(),
		CreationRequestHash: requestHash,
	}
	current.Spec.MainContainer.Image = "ubuntu:24.04"
	buildStore := &fromSandboxBuildStore{
		getByIdempotencyKey: func(context.Context, string, string, string) (*template.Template, error) {
			return current, nil
		},
	}
	resolver := &fromSandboxSourceResolver{}
	handler := &Handler{
		BuildStore:     buildStore,
		SourceResolver: resolver,
		Logger:         zap.NewNop(),
	}
	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID:      "team-1",
		UserID:      "user-1",
		Permissions: []string{gatewayauthn.PermTemplateCreate, gatewayauthn.PermSandboxRead},
	}))
	router.POST("/api/v1/templates/from-sandbox", handler.CreateTemplateFromSandbox)

	body, err := json.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates/from-sandbox", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "old-request")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusAccepted, rec.Body.String())
	}
	if resolver.calls != 0 {
		t.Fatalf("source resolver calls = %d, want 0", resolver.calls)
	}
}

func TestCreateTemplateFromSandboxRequiresCreateAndSourceReadPermissions(t *testing.T) {
	t.Parallel()

	sandboxID, err := naming.SandboxName("default", "source-template", "abcde")
	if err != nil {
		t.Fatalf("SandboxName() error = %v", err)
	}
	body, err := json.Marshal(TemplateFromSandboxRequest{
		TemplateID: "derived",
		SandboxID:  sandboxID,
	})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	tests := []struct {
		name        string
		permissions []string
	}{
		{
			name:        "create permission only",
			permissions: []string{gatewayauthn.PermTemplateCreate},
		},
		{
			name:        "source read permission only",
			permissions: []string{gatewayauthn.PermSandboxRead},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resolver := &fromSandboxSourceResolver{}
			handler := &Handler{
				BuildStore:     &fromSandboxBuildStore{},
				SourceResolver: resolver,
				Logger:         zap.NewNop(),
			}
			router := gin.New()
			router.Use(withClaims(&internalauth.Claims{
				TeamID:      "team-1",
				UserID:      "user-1",
				Permissions: tt.permissions,
			}))
			router.POST("/api/v1/templates/from-sandbox", handler.CreateTemplateFromSandbox)

			req := httptest.NewRequest(http.MethodPost, "/api/v1/templates/from-sandbox", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
			if resolver.calls != 0 {
				t.Fatalf("source resolver calls = %d, want 0", resolver.calls)
			}
		})
	}
}

func metav1Time(value time.Time) *metav1.Time {
	out := metav1.NewTime(value)
	return &out
}
