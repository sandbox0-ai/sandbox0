package http

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"go.uber.org/zap"

	"github.com/sandbox0-ai/infra/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/infra/pkg/internalauth"
	"github.com/sandbox0-ai/infra/pkg/template"
)

type testTemplateStore struct {
	getTemplateFn      func(ctx context.Context, scope, teamID, templateID string) (*template.Template, error)
	createCalled       bool
	updateCalled       bool
	createdOrUpdatedID string
}

func (s *testTemplateStore) CreateTemplate(_ context.Context, tpl *template.Template) error {
	s.createCalled = true
	s.createdOrUpdatedID = tpl.TemplateID
	return nil
}

func (s *testTemplateStore) GetTemplate(ctx context.Context, scope, teamID, templateID string) (*template.Template, error) {
	if s.getTemplateFn != nil {
		return s.getTemplateFn(ctx, scope, teamID, templateID)
	}
	return nil, nil
}

func (s *testTemplateStore) GetTemplateForTeam(context.Context, string, string) (*template.Template, error) {
	return nil, nil
}

func (s *testTemplateStore) ListTemplates(context.Context) ([]*template.Template, error) {
	return nil, nil
}

func (s *testTemplateStore) ListVisibleTemplates(context.Context, string) ([]*template.Template, error) {
	return nil, nil
}

func (s *testTemplateStore) UpdateTemplate(_ context.Context, tpl *template.Template) error {
	s.updateCalled = true
	s.createdOrUpdatedID = tpl.TemplateID
	return nil
}

func (s *testTemplateStore) DeleteTemplate(context.Context, string, string, string) error {
	return nil
}

func TestCreateTemplate_RejectsPrivilegedFieldsForRegularTeam(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
			"pool":{"minIdle":0,"maxIdle":1,"autoScale":false},
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

func TestCreateTemplate_AllowsNetworkForRegularTeam(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
			"pool":{"minIdle":0,"maxIdle":1,"autoScale":false},
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

func TestCreateTemplate_AllowsPrivilegedFieldForSystemToken(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

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
				"resources":{"cpu":"1","memory":"1Gi"},
				"securityContext":{"runAsUser":1000}
			},
			"pool":{"minIdle":0,"maxIdle":1,"autoScale":false}
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

func TestUpdateTemplate_RejectsPrivilegedFieldsForRegularTeam(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

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
							Memory: resource.MustParse("1Gi"),
						},
					},
					Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1, AutoScale: false},
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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
			"pool":{"minIdle":0,"maxIdle":1,"autoScale":false},
			"sidecars":[{"name":"helper","image":"busybox","command":["sleep","3600"]}]
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

func TestValidateTemplateSpecForClaims_WildcardPermission(t *testing.T) {
	t.Parallel()

	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
			SecurityContext: &v1alpha1.SecurityContext{
				RunAsUser: ptrInt64(1000),
			},
		},
		Sidecars: []corev1.Container{{Name: "helper", Image: "busybox"}},
		Pool:     v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1, AutoScale: false},
	}

	err := validateTemplateSpecForClaims(spec, &internalauth.Claims{
		Permissions: []string{"*"},
	})
	if err != nil {
		t.Fatalf("expected wildcard permission to pass, got error: %v", err)
	}
}

func withClaims(claims *internalauth.Claims) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := internalauth.WithClaims(c.Request.Context(), claims)
		c.Request = c.Request.WithContext(ctx)
		c.Next()
	}
}

func ptrInt64(v int64) *int64 {
	return &v
}
