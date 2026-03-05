package http

import (
	stdhttp "net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/licensing"
)

func TestRequireFeature_AllowsLicensedRequest(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.GET(
		"/protected",
		RequireFeature(licensing.NewStaticEntitlements(licensing.FeatureSSO), licensing.FeatureSSO, nil),
		func(c *gin.Context) {
			spec.JSONSuccess(c, stdhttp.StatusOK, gin.H{"ok": true})
		},
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(stdhttp.MethodGet, "/protected", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != stdhttp.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequireFeature_DeniesUnlicensedRequest(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.GET(
		"/protected",
		RequireFeature(licensing.NewStaticEntitlements(), licensing.FeatureSSO, nil),
		func(c *gin.Context) {
			t.Fatalf("handler should not be invoked when feature is not licensed")
		},
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(stdhttp.MethodGet, "/protected", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != stdhttp.StatusForbidden {
		t.Fatalf("expected 403, got %d body=%s", rec.Code, rec.Body.String())
	}

	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil {
		t.Fatalf("expected api error response")
	}
	if apiErr.Code != spec.CodeNotLicensed {
		t.Fatalf("expected code %q, got %q", spec.CodeNotLicensed, apiErr.Code)
	}
}
