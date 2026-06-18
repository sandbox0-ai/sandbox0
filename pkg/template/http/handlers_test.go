package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/api/resource"

	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
)

type testTemplateStore struct {
	getTemplateFn        func(ctx context.Context, scope, teamID, templateID string) (*template.Template, error)
	getTemplateForTeamFn func(ctx context.Context, teamID, templateID string) (*template.Template, error)
	listVisibleFn        func(ctx context.Context, teamID string) ([]*template.Template, error)
	createCalled         bool
	updateCalled         bool
	createdOrUpdatedID   string
	createdScope         string
	createdTeamID        string
	updatedScope         string
	updatedTeamID        string
	createdOrUpdatedSpec v1alpha1.SandboxTemplateSpec
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

type testTemplateStatsProvider struct {
	stats *TemplateStats
	err   error
}

func (p *testTemplateStatsProvider) GetTemplateStats(context.Context) (*TemplateStats, error) {
	if p.err != nil {
		return nil, p.err
	}
	return p.stats, nil
}

func (s *testTemplateStore) UpdateTemplate(_ context.Context, tpl *template.Template) error {
	s.updateCalled = true
	s.createdOrUpdatedID = tpl.TemplateID
	s.updatedScope = tpl.Scope
	s.updatedTeamID = tpl.TeamID
	s.createdOrUpdatedSpec = tpl.Spec
	return nil
}

func (s *testTemplateStore) DeleteTemplate(context.Context, string, string, string) error {
	return nil
}

func TestCreateTemplate_RejectsPrivilegedFieldsForRegularTeam(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1},
			"pod":{"serviceAccountName":"custom-sa"}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rec.Code)
	}
	if store.createCalled {
		t.Fatalf("expected create not called for forbidden request")
	}
}

func TestCreateTemplate_RejectsImagePullPolicyForRegularTeam(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{
				"image":"ubuntu:22.04",
				"imagePullPolicy":"Always",
				"resources":{"cpu":"1","memory":"4Gi"}
			},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rec.Code)
	}
	if store.createCalled {
		t.Fatalf("expected create not called for forbidden request")
	}
}

func TestCreateTemplate_RejectsPrivateImageFromDifferentTeam(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{}
	h := &Handler{
		Store:                store,
		PrivateRegistryHosts: []string{"registry.internal.svc:5000"},
		Logger:               zap.NewNop(),
	}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"image":"registry.internal.svc:5000/t-other/my-app:v1","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rec.Code)
	}
	if store.createCalled {
		t.Fatalf("expected create not called for forbidden request")
	}
}

func TestCreateTemplate_AllowsTeamScopedPrivateImage(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return nil, nil
		},
	}
	h := &Handler{
		Store:                store,
		PrivateRegistryHosts: []string{"registry.internal.svc:5000"},
		Logger:               zap.NewNop(),
	}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	prefix := naming.TeamImageRepositoryPrefix("team-1")
	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"image":"registry.internal.svc:5000/` + prefix + `/my-app:v1","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if !store.createCalled {
		t.Fatalf("expected create to be called")
	}
}

func TestCreateTemplate_AllowsNetworkForRegularTeam(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return nil, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1},
			"network":{"mode":"block-all"}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if !store.createCalled {
		t.Fatalf("expected create to be called")
	}
}

func TestCreateTemplate_PreservesEphemeralStorage(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return nil, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi","ephemeralStorage":"768Mi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if !store.createCalled {
		t.Fatalf("expected create to be called")
	}
	got := store.createdOrUpdatedSpec.MainContainer.Resources.EphemeralStorage
	if got.Cmp(resource.MustParse("768Mi")) != 0 {
		t.Fatalf("ephemeralStorage = %s, want 768Mi", got.String())
	}
}

func TestCreateTemplate_AllowsPrivilegedFieldForSystemToken(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return nil, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID:   "team-1",
		UserID:   "system",
		IsSystem: true,
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{
				"image":"ubuntu:22.04",
				"resources":{"cpu":"1","memory":"4Gi"},
				"securityContext":{"runAsUser":1000}
			},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if !store.createCalled {
		t.Fatalf("expected create to be called")
	}
}

func TestCreateTemplate_SystemWithoutTeamCreatesPublicTemplate(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(_ context.Context, scope, teamID, templateID string) (*template.Template, error) {
			if scope != naming.ScopePublic || teamID != "" || templateID != "demo" {
				t.Fatalf("GetTemplate scope/team/id = %q/%q/%q, want public//demo", scope, teamID, templateID)
			}
			return nil, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		UserID:   "system",
		IsSystem: true,
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d", http.StatusCreated, rec.Code)
	}
	if !store.createCalled {
		t.Fatalf("expected create to be called")
	}
	if store.createdScope != naming.ScopePublic || store.createdTeamID != "" {
		t.Fatalf("created scope/team = %q/%q, want public/empty", store.createdScope, store.createdTeamID)
	}
}

func TestGetTemplate_SystemWithoutTeamReadsPublicTemplate(t *testing.T) {
	t.Parallel()

	calledGetTemplate := false
	calledGetTemplateForTeam := false
	store := &testTemplateStore{
		getTemplateFn: func(_ context.Context, scope, teamID, templateID string) (*template.Template, error) {
			calledGetTemplate = true
			if scope != naming.ScopePublic || teamID != "" || templateID != "demo" {
				t.Fatalf("GetTemplate scope/team/id = %q/%q/%q, want public//demo", scope, teamID, templateID)
			}
			return &template.Template{TemplateID: templateID, Scope: scope, TeamID: teamID, Spec: validTemplateSpec()}, nil
		},
		getTemplateForTeamFn: func(context.Context, string, string) (*template.Template, error) {
			calledGetTemplateForTeam = true
			return nil, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		UserID:   "system",
		IsSystem: true,
	}))
	router.GET("/api/v1/templates/:id", h.GetTemplate)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/templates/demo", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if !calledGetTemplate {
		t.Fatalf("expected public GetTemplate to be called")
	}
	if calledGetTemplateForTeam {
		t.Fatalf("did not expect team fallback lookup for system public template")
	}
}

func TestCreateTemplate_RejectsMissingMainContainerImage(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return nil, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"mainContainer":{"resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
	if store.createCalled {
		t.Fatalf("expected create not called for invalid request")
	}
}

func TestCreateTemplate_RejectsUnsupportedDocumentedSpecFields(t *testing.T) {
	t.Parallel()

	fields := map[string]string{
		"lifecycle":    `"lifecycle":{"defaultTTL":60}`,
		"public":       `"public":true`,
		"allowedTeams": `"allowedTeams":["team-1"]`,
	}
	for field, fieldJSON := range fields {
		field := field
		fieldJSON := fieldJSON
		t.Run(field, func(t *testing.T) {
			t.Parallel()
			store := &testTemplateStore{
				getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
					return nil, nil
				},
			}
			h := &Handler{Store: store, Logger: zap.NewNop()}

			router := gin.New()
			router.Use(withClaims(&internalauth.Claims{
				TeamID: "team-1",
				UserID: "user-1",
			}))
			router.POST("/api/v1/templates", h.CreateTemplate)

			body := []byte(`{
				"template_id":"demo",
				"spec":{
					"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
					"pool":{"minIdle":0,"maxIdle":1},
					` + fieldJSON + `
				}
			}`)
			req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), "spec."+field+" is not supported") {
				t.Fatalf("response = %s, want unsupported field error", rec.Body.String())
			}
			if store.createCalled {
				t.Fatalf("expected create not called for unsupported spec field")
			}
		})
	}
}

func TestUpdateTemplate_RejectsInvalidPoolRange(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return &template.Template{
				TemplateID: "demo",
				Scope:      "team",
				TeamID:     "team-1",
				Spec: v1alpha1.SandboxTemplateSpec{
					MainContainer: v1alpha1.ContainerSpec{
						Image: "ubuntu:22.04",
						Resources: v1alpha1.ResourceQuota{
							CPU:    resource.MustParse("1"),
							Memory: resource.MustParse("4Gi"),
						},
					},
					Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
				},
			}, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.PUT("/api/v1/templates/:id", h.UpdateTemplate)

	body := []byte(`{
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":2,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/templates/demo", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, rec.Code)
	}
	if store.updateCalled {
		t.Fatalf("expected update not called for invalid request")
	}
}

func TestUpdateTemplate_RejectsUnsupportedDocumentedSpecFields(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return &template.Template{TemplateID: "demo", Scope: "team", TeamID: "team-1", Spec: validTemplateSpec()}, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.PUT("/api/v1/templates/:id", h.UpdateTemplate)

	body := []byte(`{
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1},
			"allowedTeams":["team-1"]
		}
	}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/templates/demo", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "spec.allowedTeams is not supported") {
		t.Fatalf("response = %s, want unsupported field error", rec.Body.String())
	}
	if store.updateCalled {
		t.Fatalf("expected update not called for unsupported spec field")
	}
}

func TestUpdateTemplate_SystemWithoutTeamUpdatesPublicTemplate(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(_ context.Context, scope, teamID, templateID string) (*template.Template, error) {
			if scope != naming.ScopePublic || teamID != "" || templateID != "demo" {
				t.Fatalf("GetTemplate scope/team/id = %q/%q/%q, want public//demo", scope, teamID, templateID)
			}
			return &template.Template{TemplateID: templateID, Scope: scope, TeamID: teamID, Spec: validTemplateSpec()}, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		UserID:   "system",
		IsSystem: true,
	}))
	router.PUT("/api/v1/templates/:id", h.UpdateTemplate)

	body := []byte(`{
		"spec":{
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/templates/demo", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}
	if !store.updateCalled {
		t.Fatalf("expected update to be called")
	}
	if store.updatedScope != naming.ScopePublic || store.updatedTeamID != "" {
		t.Fatalf("updated scope/team = %q/%q, want public/empty", store.updatedScope, store.updatedTeamID)
	}
}

func TestUpdateTemplate_RejectsImagePullPolicyForRegularTeam(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{
		getTemplateFn: func(context.Context, string, string, string) (*template.Template, error) {
			return &template.Template{
				TemplateID: "demo",
				Scope:      "team",
				TeamID:     "team-1",
				Spec: v1alpha1.SandboxTemplateSpec{
					MainContainer: v1alpha1.ContainerSpec{
						Image: "ubuntu:22.04",
						Resources: v1alpha1.ResourceQuota{
							CPU:    resource.MustParse("1"),
							Memory: resource.MustParse("4Gi"),
						},
					},
					Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
				},
			}, nil
		},
	}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.PUT("/api/v1/templates/:id", h.UpdateTemplate)

	body := []byte(`{
		"spec":{
			"mainContainer":{
				"image":"ubuntu:22.04",
				"imagePullPolicy":"Always",
				"resources":{"cpu":"1","memory":"4Gi"}
			},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPut, "/api/v1/templates/demo", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected status %d, got %d", http.StatusForbidden, rec.Code)
	}
	if store.updateCalled {
		t.Fatalf("expected update not called for forbidden request")
	}
}

func TestCreateTemplate_RejectsUnclaimableNameBudget(t *testing.T) {
	t.Parallel()

	store := &testTemplateStore{}
	h := &Handler{Store: store, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{IsSystem: true}))
	router.POST("/api/v1/templates", h.CreateTemplate)

	clusterID := strings.Repeat("a", naming.ClusterIDMaxLen+1)
	body := []byte(`{
		"template_id":"demo",
		"spec":{
			"clusterId":"` + clusterID + `",
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"4Gi"}},
			"pool":{"minIdle":0,"maxIdle":1}
		}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/templates", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "cannot generate claimable sandbox names") {
		t.Fatalf("expected claimable name budget error, got %s", rec.Body.String())
	}
	if store.createCalled {
		t.Fatalf("expected create not called for unclaimable name budget")
	}
}

func TestValidateTemplateSpecForClaims_WildcardPermissionRejected(t *testing.T) {
	t.Parallel()

	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("4Gi"),
			},
			SecurityContext: &v1alpha1.SecurityContext{
				RunAsUser: ptrInt64(1000),
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	err := validateTemplateSpecForClaims(spec, &internalauth.Claims{
		Permissions: []string{"*"},
	})
	if err == nil {
		t.Fatalf("expected wildcard permission to be rejected")
	}
}

func TestValidateTemplateClaimNameBudget_AllowsMaxLengthTemplateID(t *testing.T) {
	t.Parallel()

	err := validateTemplateClaimNameBudget(naming.ScopeTeam, "team-1", strings.Repeat("a", 255), validTemplateSpec())
	if err != nil {
		t.Fatalf("validateTemplateClaimNameBudget: %v", err)
	}
}

func TestValidateTemplateClaimNameBudget_RejectsUnclaimableClusterID(t *testing.T) {
	t.Parallel()

	spec := validTemplateSpec()
	clusterID := strings.Repeat("a", naming.ClusterIDMaxLen+1)
	spec.ClusterId = &clusterID
	err := validateTemplateClaimNameBudget(naming.ScopePublic, "", "demo", spec)
	if err == nil {
		t.Fatalf("expected unclaimable naming budget error")
	}
	if !strings.Contains(err.Error(), "cannot generate claimable sandbox names") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateTemplateSpec_StrictValidation(t *testing.T) {
	t.Parallel()

	newSpec := func() v1alpha1.SandboxTemplateSpec {
		return v1alpha1.SandboxTemplateSpec{
			MainContainer: v1alpha1.ContainerSpec{
				Image: "ubuntu:22.04",
				Resources: v1alpha1.ResourceQuota{
					CPU:    resource.MustParse("1"),
					Memory: resource.MustParse("4Gi"),
				},
			},
			Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
		}
	}

	cases := []struct {
		name    string
		mutate  func(*v1alpha1.SandboxTemplateSpec)
		wantErr string
	}{
		{
			name: "reject missing cpu",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.MainContainer.Resources.CPU = resource.MustParse("0")
			},
			wantErr: "spec.mainContainer.resources.cpu must be > 0",
		},
		{
			name: "reject negative ephemeral storage",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.MainContainer.Resources.EphemeralStorage = resource.MustParse("-1Gi")
			},
			wantErr: "spec.mainContainer.resources.ephemeralStorage must be >= 0",
		},
		{
			name: "reject invalid network mode",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: "deny-all",
				}
			},
			wantErr: "spec.network.mode must be one of: allow-all, block-all",
		},
		{
			name: "reject invalid cidr",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{
						AllowedCIDRs: []string{"not-a-cidr"},
					},
				}
			},
			wantErr: "spec.network.egress.allowedCidrs[0] must be valid CIDR",
		},
		{
			name: "reject invalid port range",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{
						AllowedPorts: []v1alpha1.PortSpec{
							{Port: 1000, EndPort: ptrInt32(999)},
						},
					},
				}
			},
			wantErr: "spec.network.egress.allowedPorts[0].endPort must be between port and 65535",
		},
		{
			name: "reject invalid traffic rule cidr",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{
						TrafficRules: []v1alpha1.TrafficRule{{
							Name:   "bad-cidr",
							Action: v1alpha1.TrafficRuleActionAllow,
							CIDRs:  []string{"not-a-cidr"},
						}},
					},
				}
			},
			wantErr: `spec.network: egress traffic rule "bad-cidr" is invalid: invalid CIDR "not-a-cidr"`,
		},
		{
			name: "reject invalid traffic rule action",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{
						TrafficRules: []v1alpha1.TrafficRule{{
							Name:    "bad-action",
							Action:  "pass",
							Domains: []string{"example.com"},
						}},
					},
				}
			},
			wantErr: `spec.network: egress traffic rule "bad-action" has unsupported action "pass"`,
		},
		{
			name: "reject mixed legacy and traffic rules",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{
						AllowedCIDRs: []string{"10.0.0.0/8"},
						TrafficRules: []v1alpha1.TrafficRule{{
							Name:   "allow-private",
							Action: v1alpha1.TrafficRuleActionAllow,
							CIDRs:  []string{"10.0.0.0/8"},
						}},
					},
				}
			},
			wantErr: "spec.network: egress trafficRules cannot be combined with legacy allowed*/denied* fields",
		},
		{
			name: "reject dangling credential rule",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Network = &v1alpha1.SandboxNetworkPolicy{
					Mode: v1alpha1.NetworkModeBlockAll,
					Egress: &v1alpha1.NetworkEgressPolicy{
						CredentialRules: []v1alpha1.EgressCredentialRule{{
							Name:          "missing",
							CredentialRef: "missing-ref",
							Protocol:      v1alpha1.EgressAuthProtocolHTTP,
						}},
					},
				}
			},
			wantErr: `spec.network: egress rule credentialRef "missing-ref" not found`,
		},
		{
			name: "reject invalid emptyDir mount path",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Pod = &v1alpha1.PodSpecOverride{
					EmptyDirMounts: []v1alpha1.EmptyDirMountSpec{{MountPath: "/var/lib/../docker"}},
				}
			},
			wantErr: "spec.pod.emptyDirMounts[0].mountPath is invalid",
		},
		{
			name: "reject reserved emptyDir mount path",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Pod = &v1alpha1.PodSpecOverride{
					EmptyDirMounts: []v1alpha1.EmptyDirMountSpec{{MountPath: "/config/docker"}},
				}
			},
			wantErr: "spec.pod.emptyDirMounts[0].mountPath uses reserved path \"/config\"",
		},
		{
			name: "reject emptyDir mount colliding with volume mount",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.VolumeMounts = []v1alpha1.VolumeMountSpec{{Name: "cache", MountPath: "/cache"}}
				s.Pod = &v1alpha1.PodSpecOverride{
					EmptyDirMounts: []v1alpha1.EmptyDirMountSpec{{MountPath: "/cache"}},
				}
			},
			wantErr: "spec.pod.emptyDirMounts[0].mountPath \"/cache\" duplicates spec.volumeMounts[0].mountPath",
		},
		{
			name: "reject non-positive emptyDir size limit",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				sizeLimit := resource.MustParse("0")
				s.Pod = &v1alpha1.PodSpecOverride{
					EmptyDirMounts: []v1alpha1.EmptyDirMountSpec{{MountPath: "/var/lib/docker", SizeLimit: &sizeLimit}},
				}
			},
			wantErr: "spec.pod.emptyDirMounts[0].sizeLimit must be > 0",
		},
		{
			name: "reject empty added capability",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.MainContainer.SecurityContext = &v1alpha1.SecurityContext{
					Capabilities: &v1alpha1.Capabilities{Add: []string{"SYS_ADMIN", " "}},
				}
			},
			wantErr: "spec.mainContainer.securityContext.capabilities.add[1] is required",
		},
		{
			name: "reject localhost seccomp without profile",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.MainContainer.SecurityContext = &v1alpha1.SecurityContext{
					SeccompProfile: &v1alpha1.SeccompProfile{Type: v1alpha1.SeccompProfileTypeLocalhost},
				}
			},
			wantErr: "spec.mainContainer.securityContext.seccompProfile.localhostProfile is required when type is Localhost",
		},
		{
			name: "reject non-localhost apparmor with profile",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.MainContainer.SecurityContext = &v1alpha1.SecurityContext{
					AppArmorProfile: &v1alpha1.AppArmorProfile{
						Type:             v1alpha1.AppArmorProfileTypeRuntimeDefault,
						LocalhostProfile: ptrString("custom-profile"),
					},
				}
			},
			wantErr: "spec.mainContainer.securityContext.appArmorProfile.localhostProfile must be omitted unless type is Localhost",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			spec := newSpec()
			tc.mutate(&spec)

			err := validateTemplateSpec(spec)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if got := err.Error(); got != tc.wantErr && !bytes.Contains([]byte(got), []byte(tc.wantErr)) {
				t.Fatalf("expected error containing %q, got %q", tc.wantErr, got)
			}
		})
	}
}

func TestValidateTemplateSpec_AllowsExpandedSecurityContext(t *testing.T) {
	t.Parallel()

	spec := validTemplateSpec()
	spec.MainContainer.SecurityContext = &v1alpha1.SecurityContext{
		Privileged:               ptrBool(true),
		RunAsUser:                ptrInt64(0),
		RunAsGroup:               ptrInt64(0),
		RunAsNonRoot:             ptrBool(false),
		ReadOnlyRootFilesystem:   ptrBool(false),
		AllowPrivilegeEscalation: ptrBool(true),
		Capabilities: &v1alpha1.Capabilities{
			Add:  []string{"SYS_ADMIN", "NET_ADMIN"},
			Drop: []string{"NET_RAW"},
		},
		SeccompProfile: &v1alpha1.SeccompProfile{
			Type: v1alpha1.SeccompProfileTypeUnconfined,
		},
		AppArmorProfile: &v1alpha1.AppArmorProfile{
			Type: v1alpha1.AppArmorProfileTypeRuntimeDefault,
		},
	}

	if err := validateTemplateSpec(spec); err != nil {
		t.Fatalf("validateTemplateSpec: %v", err)
	}
	if err := validateTemplateSpecForClaims(spec, &internalauth.Claims{IsSystem: true}); err != nil {
		t.Fatalf("expected system token to allow expanded security context, got %v", err)
	}
}

func TestValidateTemplateSpec_AllowsEmptyDirMounts(t *testing.T) {
	t.Parallel()

	sizeLimit := resource.MustParse("20Gi")
	spec := validTemplateSpec()
	spec.Pod = &v1alpha1.PodSpecOverride{
		EmptyDirMounts: []v1alpha1.EmptyDirMountSpec{{
			MountPath: "/var/lib/docker",
			SizeLimit: &sizeLimit,
		}},
	}

	if err := validateTemplateSpec(spec); err != nil {
		t.Fatalf("validateTemplateSpec: %v", err)
	}
}

func TestValidateTemplateSpecForClaims_RequiresSystemIdentityForEmptyDirMounts(t *testing.T) {
	t.Parallel()

	spec := validTemplateSpec()
	sizeLimit := resource.MustParse("20Gi")
	spec.Pod = &v1alpha1.PodSpecOverride{
		EmptyDirMounts: []v1alpha1.EmptyDirMountSpec{{
			MountPath: "/var/lib/docker",
			SizeLimit: &sizeLimit,
		}},
	}

	err := validateTemplateSpecForClaims(spec, &internalauth.Claims{TeamID: "team-1"})
	if err == nil || err.Error() != "spec.pod requires system identity" {
		t.Fatalf("expected team token to reject pod emptyDir mounts, got %v", err)
	}
	if err := validateTemplateSpecForClaims(spec, &internalauth.Claims{IsSystem: true}); err != nil {
		t.Fatalf("expected system token to allow pod emptyDir mounts, got %v", err)
	}
}

func TestValidateTemplateSpecForClaims_RejectsMismatchedMainResources(t *testing.T) {
	t.Parallel()

	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	err := validateTemplateSpecForClaims(spec, &internalauth.Claims{TeamID: "team-1"})
	if err == nil {
		t.Fatal("expected aggregate resource ratio to be rejected")
	}
	if got := err.Error(); !strings.Contains(got, "team-owned template total memory must equal total cpu") {
		t.Fatalf("unexpected error %q", got)
	}
}

func TestValidateTemplateSpecForClaims_RejectsSystemOwnedMismatchedMainResources(t *testing.T) {
	t.Parallel()

	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	claims := &internalauth.Claims{IsSystem: true}
	err := validateTemplateSpecForClaims(spec, claims)
	if err == nil {
		t.Fatal("expected system token to reject resource ratio mismatch")
	}
	if got := err.Error(); !strings.Contains(got, "system template total memory must equal total cpu") {
		t.Fatalf("unexpected error %q", got)
	}
}

func TestGetTemplate_IncludesPoolStatus(t *testing.T) {
	t.Parallel()

	namespace, err := naming.TemplateNamespaceForTeam("team-1")
	if err != nil {
		t.Fatalf("resolve team namespace: %v", err)
	}

	store := &testTemplateStore{
		getTemplateForTeamFn: func(context.Context, string, string) (*template.Template, error) {
			return &template.Template{
				TemplateID: "demo",
				Scope:      "team",
				TeamID:     "team-1",
			}, nil
		},
	}
	statusProvider := &testTemplateStatsProvider{
		stats: &TemplateStats{
			Templates: []TemplateStat{
				{
					TemplateID:  naming.TemplateNameForCluster("team", "team-1", "demo"),
					Namespace:   namespace,
					IdleCount:   3,
					ActiveCount: 7,
				},
			},
		},
	}
	h := &Handler{Store: store, StatsProvider: statusProvider, Logger: zap.NewNop()}

	router := gin.New()
	router.Use(withClaims(&internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	router.GET("/api/v1/templates/:id", h.GetTemplate)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/templates/demo", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, rec.Code)
	}

	var resp struct {
		Success bool `json:"success"`
		Data    struct {
			Status struct {
				IdleCount   int32 `json:"idleCount"`
				ActiveCount int32 `json:"activeCount"`
			} `json:"status"`
		} `json:"data"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success response")
	}
	if resp.Data.Status.IdleCount != 3 || resp.Data.Status.ActiveCount != 7 {
		t.Fatalf("unexpected status payload: idle=%d active=%d", resp.Data.Status.IdleCount, resp.Data.Status.ActiveCount)
	}
}

func withClaims(claims *internalauth.Claims) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

func validTemplateSpec() v1alpha1.SandboxTemplateSpec {
	return v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("4Gi"),
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}
}

func ptrInt64(v int64) *int64 {
	return &v
}

func ptrBool(v bool) *bool {
	return &v
}

func ptrString(v string) *string {
	return &v
}

func ptrInt32(v int32) *int32 {
	return &v
}
