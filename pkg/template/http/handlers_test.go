package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	v1alpha1 "github.com/sandbox0-ai/sandbox0/pkg/sandboxspec"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	"go.uber.org/zap"
)

type testTemplateStore struct {
	getTemplateFn        func(context.Context, string, string, string) (*template.Template, error)
	getTemplateForTeamFn func(context.Context, string, string) (*template.Template, error)
	listVisibleFn        func(context.Context, string) ([]*template.Template, error)
	createCalled         bool
	updateCalled         bool
	createdOrUpdatedID   string
	createdScope         string
	createdTeamID        string
	updatedScope         string
	updatedTeamID        string
	createdOrUpdatedSpec v1alpha1.TemplateSpec
}

func (s *testTemplateStore) CreateTemplate(_ context.Context, tpl *template.Template) error {
	s.createCalled = true
	s.createdOrUpdatedID = tpl.TemplateID
	s.createdScope = tpl.Scope
	s.createdTeamID = tpl.TeamID
	s.createdOrUpdatedSpec = tpl.Spec
	return nil
}

func (s *testTemplateStore) GetTemplate(ctx context.Context, scope, teamID, templateID string) (*template.Template, error) {
	if s.getTemplateFn != nil {
		return s.getTemplateFn(ctx, scope, teamID, templateID)
	}
	return nil, nil
}

func (s *testTemplateStore) GetTemplateForTeam(ctx context.Context, teamID, templateID string) (*template.Template, error) {
	if s.getTemplateForTeamFn != nil {
		return s.getTemplateForTeamFn(ctx, teamID, templateID)
	}
	return nil, nil
}

func (s *testTemplateStore) ListTemplates(context.Context) ([]*template.Template, error) {
	return nil, nil
}
func (s *testTemplateStore) ListVisibleTemplates(ctx context.Context, teamID string) ([]*template.Template, error) {
	if s.listVisibleFn != nil {
		return s.listVisibleFn(ctx, teamID)
	}
	return nil, nil
}
func (s *testTemplateStore) UpdateTemplate(_ context.Context, tpl *template.Template) error {
	s.updateCalled = true
	s.createdOrUpdatedID = tpl.TemplateID
	s.updatedScope = tpl.Scope
	s.updatedTeamID = tpl.TeamID
	s.createdOrUpdatedSpec = tpl.Spec
	return nil
}
func (s *testTemplateStore) DeleteTemplate(context.Context, string, string, string) error { return nil }

func TestCreateTemplatePersistsRuntimeNeutralSpec(t *testing.T) {
	store := &testTemplateStore{}
	handler := &Handler{Store: store, Logger: zap.NewNop()}
	router := templateTestRouter(http.MethodPost, "/api/v1/templates", handler.CreateTemplate,
		&internalauth.Claims{TeamID: "team-1", UserID: "user-1"})

	body := `{
		"template_id":"demo",
		"spec":{
			"description":"runtime neutral",
			"mainContainer":{"image":"registry.example/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","env":[{"name":"A","value":"B"}],"resources":{"memory":"4Gi","ephemeralStorage":"768Mi"}},
			"network":{"mode":"block-all","egress":{"allowedCidrs":["192.0.2.0/24"]}}
		}
	}`
	response := performTemplateRequest(router, http.MethodPost, "/api/v1/templates", body)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d; body=%s", response.Code, http.StatusCreated, response.Body.String())
	}
	if !store.createCalled || store.createdScope != naming.ScopeTeam || store.createdTeamID != "team-1" {
		t.Fatalf("create state = (%t,%q,%q)", store.createCalled, store.createdScope, store.createdTeamID)
	}
	if got := store.createdOrUpdatedSpec.MainContainer.Resources.CPU; got != "1" {
		t.Fatalf("derived CPU = %q, want 1", got)
	}
	if got := store.createdOrUpdatedSpec.MainContainer.Resources.EphemeralStorage; got != "768Mi" {
		t.Fatalf("ephemeral storage = %q, want 768Mi", got)
	}
	if strings.Contains(response.Body.String(), `"cpu"`) {
		t.Fatalf("public response leaked platform-derived CPU: %s", response.Body.String())
	}
}

func TestCreateTemplateRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		name, field, fragment string
	}{
		{name: "top level", field: `"unknown":true`, fragment: `unknown field \"unknown\"`},
		{name: "container", field: `"mainContainer":{"image":"ubuntu:22.04","unknown":true,"resources":{"memory":"4Gi"}}`, fragment: `unknown field \"unknown\"`},
		{name: "resources", field: `"mainContainer":{"image":"ubuntu:22.04","resources":{"memory":"4Gi","unknown":"1"}}`, fragment: `unknown field \"unknown\"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &testTemplateStore{}
			handler := &Handler{Store: store, Logger: zap.NewNop()}
			router := templateTestRouter(http.MethodPost, "/api/v1/templates", handler.CreateTemplate,
				&internalauth.Claims{TeamID: "team-1"})
			main := `"mainContainer":{"image":"ubuntu:22.04","resources":{"memory":"4Gi"}}`
			if strings.HasPrefix(test.field, `"mainContainer"`) {
				main = test.field
				test.field = ""
			}
			body := `{"template_id":"demo","spec":{` + main
			if test.field != "" {
				body += `,` + test.field
			}
			body += `}}`
			response := performTemplateRequest(router, http.MethodPost, "/api/v1/templates", body)
			if response.Code != http.StatusBadRequest || !strings.Contains(response.Body.String(), test.fragment) {
				t.Fatalf("status=%d body=%s, want %q", response.Code, response.Body.String(), test.fragment)
			}
			if store.createCalled {
				t.Fatal("unknown field reached persistence")
			}
		})
	}
}

func TestCreateTemplateRejectsExplicitCPUAndOutOfRangeMemory(t *testing.T) {
	tests := []struct {
		name, resources, fragment string
	}{
		{name: "CPU", resources: `{"cpu":"1","memory":"4Gi"}`, fragment: "resources.cpu is not supported"},
		{name: "memory", resources: `{"memory":"32Gi"}`, fragment: "16Gi"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &testTemplateStore{}
			handler := &Handler{Store: store, Logger: zap.NewNop()}
			router := templateTestRouter(http.MethodPost, "/api/v1/templates", handler.CreateTemplate,
				&internalauth.Claims{TeamID: "team-1"})
			body := `{"template_id":"demo","spec":{"mainContainer":{"image":"registry.example/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","resources":` + test.resources + `}}}`
			response := performTemplateRequest(router, http.MethodPost, "/api/v1/templates", body)
			if response.Code != http.StatusBadRequest && response.Code != http.StatusForbidden {
				t.Fatalf("status = %d; body=%s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Body.String(), test.fragment) {
				t.Fatalf("body=%s, want %q", response.Body.String(), test.fragment)
			}
		})
	}
}

func TestCreateTemplateRejectsMutableOrUnnormalizedImage(t *testing.T) {
	for name, image := range map[string]string{
		"tag":       "docker.io/library/python:3.12",
		"shorthand": "python@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	} {
		t.Run(name, func(t *testing.T) {
			store := &testTemplateStore{}
			handler := &Handler{Store: store, Logger: zap.NewNop()}
			router := templateTestRouter(http.MethodPost, "/api/v1/templates", handler.CreateTemplate,
				&internalauth.Claims{TeamID: "team-1"})
			body := `{"template_id":"demo","spec":{"mainContainer":{"image":` +
				strconv.Quote(image) + `,"resources":{"memory":"4Gi"}}}}`
			response := performTemplateRequest(router, http.MethodPost, "/api/v1/templates", body)
			if response.Code != http.StatusBadRequest ||
				!strings.Contains(response.Body.String(), "digest-pinned SHA-256") {
				t.Fatalf("status = %d; body=%s", response.Code, response.Body.String())
			}
			if store.createCalled {
				t.Fatal("invalid image reached persistence")
			}
		})
	}
}

func TestUpdateTemplateDerivesCPU(t *testing.T) {
	store := &testTemplateStore{getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
		return &template.Template{TemplateID: "demo", Scope: naming.ScopeTeam, TeamID: "team-1", Spec: validTemplateSpec()}, nil
	}}
	handler := &Handler{Store: store, Logger: zap.NewNop(), ResourcePolicy: template.NewResourcePolicy("2Gi", "16Gi")}
	router := templateTestRouter(http.MethodPut, "/api/v1/templates/:id", handler.UpdateTemplate,
		&internalauth.Claims{TeamID: "team-1", UserID: "user-1"})
	body := `{"spec":{"mainContainer":{"image":"registry.example/runtime@sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","resources":{"memory":"4Gi"}}}}`
	response := performTemplateRequest(router, http.MethodPut, "/api/v1/templates/demo", body)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; body=%s", response.Code, response.Body.String())
	}
	if !store.updateCalled || store.createdOrUpdatedSpec.MainContainer.Resources.CPU != "2" {
		t.Fatalf("update=%t CPU=%q", store.updateCalled, store.createdOrUpdatedSpec.MainContainer.Resources.CPU)
	}
}

func TestSystemTemplateWithoutTeamUsesPublicScope(t *testing.T) {
	store := &testTemplateStore{}
	handler := &Handler{Store: store, Logger: zap.NewNop()}
	router := templateTestRouter(http.MethodPost, "/api/v1/templates", handler.CreateTemplate,
		&internalauth.Claims{IsSystem: true})
	body := `{"template_id":"builtin","spec":{"mainContainer":{"image":"registry.example/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","resources":{"memory":"4Gi"}}}}`
	response := performTemplateRequest(router, http.MethodPost, "/api/v1/templates", body)
	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d; body=%s", response.Code, response.Body.String())
	}
	if store.createdScope != naming.ScopePublic || store.createdTeamID != "" {
		t.Fatalf("scope/team = %q/%q", store.createdScope, store.createdTeamID)
	}
}

func TestValidateTemplateSpecRejectsInvalidNetwork(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*v1alpha1.TemplateSpec)
	}{
		{name: "network mode", mutate: func(spec *v1alpha1.TemplateSpec) { spec.Network = &v1alpha1.SandboxNetworkPolicy{Mode: "invalid"} }},
		{name: "CIDR", mutate: func(spec *v1alpha1.TemplateSpec) {
			spec.Network = &v1alpha1.SandboxNetworkPolicy{Mode: v1alpha1.NetworkModeBlockAll, Egress: &v1alpha1.NetworkEgressPolicy{AllowedCIDRs: []string{"invalid"}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := validTemplateSpec()
			test.mutate(&spec)
			if err := validateTemplateSpec(spec); err == nil {
				t.Fatal("validateTemplateSpec() error = nil")
			}
		})
	}
}

func TestValidateTemplateSpecRejectsUnsafeRootFSSize(t *testing.T) {
	for _, value := range []string{"299Mi", "314572801", "2Ti"} {
		t.Run(value, func(t *testing.T) {
			spec := validTemplateSpec()
			spec.MainContainer.Resources.EphemeralStorage = value
			if err := validateTemplateSpec(spec); err == nil {
				t.Fatal("unsafe RootFS size was accepted")
			}
		})
	}
}

func TestValidateTemplateClaimNameBudgetAllowsMaximumTemplateID(t *testing.T) {
	if err := validateTemplateClaimNameBudget(strings.Repeat("a", 255), validTemplateSpec()); err != nil {
		t.Fatalf("validateTemplateClaimNameBudget() error = %v", err)
	}
}

func withClaims(claims *internalauth.Claims) gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request = c.Request.WithContext(internalauth.WithClaims(c.Request.Context(), claims))
		c.Next()
	}
}

func validTemplateSpec() v1alpha1.TemplateSpec {
	return v1alpha1.TemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image:     "registry.example/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			Resources: v1alpha1.ResourceQuota{CPU: "1", Memory: "4Gi", EphemeralStorage: "8Gi"},
		},
	}
}

func templateTestRouter(method, path string, handler gin.HandlerFunc, claims *internalauth.Claims) *gin.Engine {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(withClaims(claims))
	router.Handle(method, path, handler)
	return router
}

func performTemplateRequest(router http.Handler, method, path, body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	router.ServeHTTP(response, request)
	return response
}
