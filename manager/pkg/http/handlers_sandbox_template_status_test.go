package http

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/apis/sandbox0/v1alpha1"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/naming"
	"github.com/sandbox0-ai/sandbox0/pkg/template"
	templatestore "github.com/sandbox0-ai/sandbox0/pkg/template/store"
	"go.uber.org/zap"
)

type managerTemplateStatusStore struct {
	templatestore.TemplateStore
	tpl *template.Template
}

func (s *managerTemplateStatusStore) GetTemplateForTeam(_ context.Context, teamID, templateID string) (*template.Template, error) {
	if s.tpl == nil || s.tpl.TemplateID != templateID {
		return nil, nil
	}
	if s.tpl.Scope == naming.ScopePublic || s.tpl.TeamID == teamID {
		return s.tpl, nil
	}
	return nil, nil
}

func TestClaimSandboxRejectsCreatingTemplateBeforeDataPlaneClaim(t *testing.T) {
	gin.SetMode(gin.TestMode)

	store := &managerTemplateStatusStore{tpl: &template.Template{
		TemplateID: "derived",
		Scope:      naming.ScopeTeam,
		TeamID:     "team-1",
		Status: &v1alpha1.SandboxTemplateStatus{
			Creation: &v1alpha1.TemplateCreationStatus{
				State: v1alpha1.TemplateCreationStateCreating,
				Stage: v1alpha1.TemplateCreationStagePublishing,
			},
		},
	}}
	server := &Server{
		templateStore:        store,
		templateStoreEnabled: true,
		logger:               zap.NewNop(),
	}
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	request := httptest.NewRequest(http.MethodPost, "/api/v1/sandboxes", strings.NewReader(`{"template":"DERIVED"}`))
	request.Header.Set("Content-Type", "application/json")
	request = request.WithContext(internalauth.WithClaims(request.Context(), &internalauth.Claims{
		TeamID: "team-1",
		UserID: "user-1",
	}))
	ctx.Request = request

	server.claimSandbox(ctx)

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
}
