package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/registry"
	"github.com/sandbox0-ai/sandbox0/manager/pkg/service"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestGetRegistryCredentialsReturnsBadRequestForInvalidTarget(t *testing.T) {
	gin.SetMode(gin.TestMode)

	server := &Server{
		registryService: service.NewRegistryService(registryProviderFunc(func(context.Context, registry.PushCredentialsRequest) (*registry.Credential, error) {
			return nil, registry.ErrInvalidTargetImage
		}), zap.NewNop()),
		logger: zap.NewNop(),
	}
	router := gin.New()
	router.POST("/api/v1/registry/credentials", server.getRegistryCredentials)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/registry/credentials", strings.NewReader(`{"targetImage":"t-other/probe:v1"}`))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(internalauth.WithClaims(req.Context(), &internalauth.Claims{TeamID: "team-1"}))
	rec := httptest.NewRecorder()

	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusBadRequest, rec.Body.String())
	}
}

type registryProviderFunc func(context.Context, registry.PushCredentialsRequest) (*registry.Credential, error)

func (f registryProviderFunc) GetPushCredentials(ctx context.Context, req registry.PushCredentialsRequest) (*registry.Credential, error) {
	return f(ctx, req)
}
