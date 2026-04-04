package http

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/intstr"

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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
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
				"resources":{"cpu":"1","memory":"1Gi"}
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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
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
				"resources":{"cpu":"1","memory":"1Gi"},
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
			"mainContainer":{"resources":{"cpu":"1","memory":"1Gi"}},
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
							Memory: resource.MustParse("1Gi"),
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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
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

func TestUpdateTemplate_AllowsSidecarsForRegularTeam(t *testing.T) {
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
							Memory: resource.MustParse("1Gi"),
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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
			"pool":{"minIdle":0,"maxIdle":1},
			"sidecars":[{"name":"helper","image":"busybox","command":["sleep","3600"]}]
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
		t.Fatalf("expected update called for sidecar request")
	}
}

func TestCreateTemplate_AllowsSidecarsForRegularTeam(t *testing.T) {
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
			"mainContainer":{"image":"ubuntu:22.04","resources":{"cpu":"1","memory":"1Gi"}},
			"pool":{"minIdle":0,"maxIdle":1},
			"sidecars":[{
				"name":"codex",
				"image":"busybox",
				"command":["sh","-lc","sleep 3600"],
				"readinessProbe":{"exec":{"command":["test","-f","/tmp/ready"]}}
			}]
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
		t.Fatalf("expected create called for sidecar request")
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
							Memory: resource.MustParse("1Gi"),
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
				"resources":{"cpu":"1","memory":"1Gi"}
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

func TestValidateTemplateSpecForClaims_WildcardPermissionRejected(t *testing.T) {
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
		Pool:     v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	err := validateTemplateSpecForClaims(spec, &internalauth.Claims{
		Permissions: []string{"*"},
	})
	if err == nil {
		t.Fatalf("expected wildcard permission to be rejected")
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
					Memory: resource.MustParse("1Gi"),
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
			name: "reject sidecar without name",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Sidecars = []corev1.Container{
					{Image: "busybox"},
				}
			},
			wantErr: "spec.sidecars[0].name is required",
		},
		{
			name: "reject sidecar probe without handler",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Sidecars = []corev1.Container{
					{
						Name:           "helper",
						Image:          "busybox",
						ReadinessProbe: &corev1.Probe{},
					},
				}
			},
			wantErr: "spec.sidecars[0].readinessProbe must define exactly one handler",
		},
		{
			name: "reject sidecar probe with multiple handlers",
			mutate: func(s *v1alpha1.SandboxTemplateSpec) {
				s.Sidecars = []corev1.Container{
					{
						Name:  "helper",
						Image: "busybox",
						ReadinessProbe: &corev1.Probe{
							ProbeHandler: corev1.ProbeHandler{
								Exec:    &corev1.ExecAction{Command: []string{"true"}},
								HTTPGet: &corev1.HTTPGetAction{Path: "/ready", Port: intstr.FromInt32(8080)},
							},
						},
					},
				}
			},
			wantErr: "spec.sidecars[0].readinessProbe must define exactly one handler",
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

func TestValidateTemplateSpecForClaims_AllowsClaimableSidecars(t *testing.T) {
	t.Parallel()

	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
		Sidecars: []corev1.Container{
			{
				Name:    "codex",
				Image:   "busybox",
				Command: []string{"sh", "-lc", "sleep 3600"},
				ReadinessProbe: &corev1.Probe{
					ProbeHandler: corev1.ProbeHandler{
						Exec: &corev1.ExecAction{Command: []string{"test", "-f", "/tmp/ready"}},
					},
				},
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	if err := validateTemplateSpecForClaims(spec, &internalauth.Claims{TeamID: "team-1"}); err != nil {
		t.Fatalf("expected sidecars to be allowed, got %v", err)
	}
}

func TestValidateTemplateSpecForClaims_RejectsPrivilegedSidecarFields(t *testing.T) {
	t.Parallel()

	privileged := true
	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
		Sidecars: []corev1.Container{
			{
				Name:  "codex",
				Image: "busybox",
				SecurityContext: &corev1.SecurityContext{
					Privileged: &privileged,
				},
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	err := validateTemplateSpecForClaims(spec, &internalauth.Claims{TeamID: "team-1"})
	if err == nil {
		t.Fatal("expected privileged sidecar field to be rejected")
	}
	if got := err.Error(); got != "spec.sidecars[0].securityContext.privileged requires system identity" {
		t.Fatalf("unexpected error %q", got)
	}
}

func TestValidateTemplateSpecForClaims_AllowsSystemOwnedSidecarFields(t *testing.T) {
	t.Parallel()

	privileged := true
	policy := "Always"
	spec := v1alpha1.SandboxTemplateSpec{
		MainContainer: v1alpha1.ContainerSpec{
			Image: "ubuntu:22.04",
			Resources: v1alpha1.ResourceQuota{
				CPU:    resource.MustParse("1"),
				Memory: resource.MustParse("1Gi"),
			},
		},
		Sidecars: []corev1.Container{
			{
				Name:            "codex",
				Image:           "busybox",
				ImagePullPolicy: corev1.PullPolicy(policy),
				SecurityContext: &corev1.SecurityContext{
					Privileged: &privileged,
				},
			},
		},
		Pool: v1alpha1.PoolStrategy{MinIdle: 0, MaxIdle: 1},
	}

	claims := &internalauth.Claims{IsSystem: true}
	if err := validateTemplateSpecForClaims(spec, claims); err != nil {
		t.Fatalf("expected system token to allow sidecar fields, got %v", err)
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

func ptrInt64(v int64) *int64 {
	return &v
}

func ptrInt32(v int32) *int32 {
	return &v
}
