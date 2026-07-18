package http

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	gatewayauthn "github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	templatehttp "github.com/sandbox0-ai/sandbox0/pkg/template/http"
	"go.uber.org/zap"
)

func TestManagerTemplateFromSandboxEndpointRequiresBothPermissions(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name        string
		permissions []string
	}{
		{name: "create only", permissions: []string{gatewayauthn.PermTemplateCreate}},
		{name: "source read only", permissions: []string{gatewayauthn.PermSandboxRead}},
	} {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			server := &Server{
				templateHandler: &templatehttp.Handler{Logger: zap.NewNop()},
			}
			router := gin.New()
			router.Use(managerTemplateClaims("team-1", tt.permissions))
			router.POST("/api/v1/templates/from-sandbox", server.createTemplateFromSandbox)

			req := httptest.NewRequest(
				http.MethodPost,
				"/api/v1/templates/from-sandbox",
				bytes.NewBufferString(`{"template_id":"derived","sandbox_id":"rs-default-source-abcde"}`),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
}

func TestManagerTemplateSourceEndpointRequiresSandboxRead(t *testing.T) {
	t.Parallel()

	server := &Server{logger: zap.NewNop()}
	router := gin.New()
	router.Use(managerTemplateClaims("team-1", []string{gatewayauthn.PermTemplateCreate}))
	router.GET("/internal/v1/sandboxes/:id/template-source", server.getSandboxTemplateSourceInternal)

	req := httptest.NewRequest(http.MethodGet, "/internal/v1/sandboxes/rs-default-source-abcde/template-source", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d; body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
	}
}

func managerTemplateClaims(teamID string, permissions []string) gin.HandlerFunc {
	return func(c *gin.Context) {
		claims := &internalauth.Claims{
			TeamID:      teamID,
			UserID:      "user-1",
			Permissions: permissions,
		}
		c.Request = c.Request.WithContext(internalauth.WithClaims(c.Request.Context(), claims))
		c.Next()
	}
}
