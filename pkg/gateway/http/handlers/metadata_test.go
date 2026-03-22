package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestGatewayMetadata(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.GET("/metadata", GatewayMetadata("global-gateway", GatewayModeGlobal))

	req := httptest.NewRequest(http.MethodGet, "/metadata", nil)
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected response body")
	}
}
