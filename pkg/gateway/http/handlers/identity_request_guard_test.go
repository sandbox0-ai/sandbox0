package handlers

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
)

func TestPublicIdentityAndRegionHandlersRejectOversizedJSONBeforeDependencies(t *testing.T) {
	gin.SetMode(gin.TestMode)

	tests := []struct {
		name    string
		path    string
		handler gin.HandlerFunc
		limit   int
	}{
		{
			name:    "login",
			path:    "/auth/login",
			handler: (&AuthHandler{}).Login,
			limit:   identity.MaxIdentityRequestBodyBytes,
		},
		{
			name:    "register",
			path:    "/auth/register",
			handler: (&AuthHandler{}).Register,
			limit:   identity.MaxIdentityRequestBodyBytes,
		},
		{
			name:    "region create",
			path:    "/regions",
			handler: (&RegionHandler{}).CreateRegion,
			limit:   maxRegionRequestBodyBytes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			router := gin.New()
			router.POST(test.path, test.handler)

			request := httptest.NewRequest(
				http.MethodPost,
				test.path,
				bytes.NewReader(bytes.Repeat([]byte("x"), test.limit+1)),
			)
			request.Header.Set("Content-Type", "application/json")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusRequestEntityTooLarge {
				t.Fatalf("status = %d, want %d; body=%s", recorder.Code, http.StatusRequestEntityTooLarge, recorder.Body.String())
			}
		})
	}
}
