package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

type stubTenantResolver struct {
	activeTeam *tenantdir.ActiveTeam
	err        error
}

func (s *stubTenantResolver) ResolveActiveTeam(_ context.Context, _, _ string) (*tenantdir.ActiveTeam, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.activeTeam, nil
}

func TestTenantHandlerIssueRegionToken(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	handler := NewTenantHandler(
		&stubTenantResolver{activeTeam: &tenantdir.ActiveTeam{
			UserID:             "user-1",
			TeamID:             "team-1",
			TeamRole:           "admin",
			HomeRegionID:       "aws/us-east-1",
			RegionalGatewayURL: "https://use1.example.com",
		}},
		authn.NewIssuer("global-gateway", "test-secret", time.Minute, time.Hour),
		5*time.Minute,
		zap.NewNop(),
	)

	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod:    authn.AuthMethodJWT,
			UserID:        "user-1",
			TeamID:        "team-1",
			TeamRole:      "admin",
			IsSystemAdmin: true,
		})
		c.Next()
	})
	router.POST("/auth/region-token", handler.IssueRegionToken)

	req := httptest.NewRequest(http.MethodPost, "/auth/region-token", strings.NewReader(`{}`))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	var response struct {
		Data IssueRegionTokenResponse `json:"data"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.RegionID != "aws/us-east-1" {
		t.Fatalf("expected region id, got %q", response.Data.RegionID)
	}
	if response.Data.RegionalGatewayURL != "https://use1.example.com" {
		t.Fatalf("expected regional gateway url, got %q", response.Data.RegionalGatewayURL)
	}
	if response.Data.Token == "" {
		t.Fatal("expected region token to be issued")
	}
}
