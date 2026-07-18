package http

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatehttp "github.com/sandbox0-ai/sandbox0/pkg/template/http"
)

func TestSetupRoutesMountsTemplateFromSandboxEndpoint(t *testing.T) {
	gin.SetMode(gin.TestMode)
	server := &Server{
		router:          gin.New(),
		templateHandler: &templatehttp.Handler{},
	}
	server.setupRoutes()

	for _, route := range server.router.Routes() {
		if route.Method == http.MethodPost && route.Path == "/api/v1/templates/from-sandbox" {
			return
		}
	}
	t.Fatal("expected POST /api/v1/templates/from-sandbox route")
}

func TestCreateSandboxRejectsCreatingTemplateBeforeClusterSelection(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tpl := newRoutingTemplate("derived")
	tpl.Status = &v1alpha1.SandboxTemplateStatus{
		Creation: &v1alpha1.TemplateCreationStatus{
			State: v1alpha1.TemplateCreationStateCreating,
			Stage: v1alpha1.TemplateCreationStageReconciling,
		},
	}
	server := newRoutingTestServer(tpl, nil, nil, &fakeRoutingReconciler{})
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Request = c.Request.WithContext(internalauth.WithClaims(c.Request.Context(), &internalauth.Claims{
			TeamID: "team-a",
			UserID: "user-a",
		}))
		c.Next()
	})
	router.POST("/api/v1/sandboxes", server.createSandbox)

	body, err := json.Marshal(map[string]string{"template": "derived"})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusConflict, recorder.Body.String())
	}
	if got := recorder.Header().Get("Retry-After"); got != "1" {
		t.Fatalf("Retry-After = %q, want 1", got)
	}
	var response spec.Response
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if response.Success || response.Error == nil || response.Error.Code != spec.CodeTemplateNotReady {
		t.Fatalf("response = %#v, want template_not_ready", response)
	}
	if selected, _, _, err := server.selectClusterForTemplate(newRoutingContext(), "derived", "team-a"); selected != nil || err != template.ErrTemplateNotReady {
		t.Fatalf("selectClusterForTemplate() = (%v, %v), want (nil, ErrTemplateNotReady)", selected, err)
	}
}
