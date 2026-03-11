package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"go.uber.org/zap"
)

func TestAuthMiddleware_JWTAccessToken(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/v1/templates", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req

	middleware := NewAuthMiddleware(nil, "test-secret", issuer, zap.NewNop())
	authCtx, err := middleware.AuthenticateRequest(ctx)
	if err != nil {
		t.Fatalf("authenticate request: %v", err)
	}
	if authCtx.UserID != "user-1" || authCtx.TeamID != "team-1" {
		t.Fatalf("unexpected auth context: user=%s team=%s", authCtx.UserID, authCtx.TeamID)
	}
}

func TestAuthMiddleware_JWTRefreshTokenRejected(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "team-1", "admin", "user@example.com", "User", false)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/v1/templates", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.RefreshToken)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req

	middleware := NewAuthMiddleware(nil, "test-secret", issuer, zap.NewNop())
	if _, err := middleware.AuthenticateRequest(ctx); err == nil {
		t.Fatalf("expected refresh token to be rejected")
	}
}

func TestAuthMiddleware_JWTRegionTokenAcceptedForMatchingRegion(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("global-directory", "test-secret", time.Minute, time.Hour)
	regionToken, _, err := issuer.IssueRegionToken("user-1", "team-1", "admin", "aws/us-east-1", false, time.Minute)
	if err != nil {
		t.Fatalf("issue region token: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/v1/templates", nil)
	req.Header.Set("Authorization", "Bearer "+regionToken)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req

	middleware := NewAuthMiddleware(
		nil,
		"test-secret",
		issuer,
		zap.NewNop(),
		WithJWTValidationMode(JWTValidationModeRegion),
		WithRequiredRegionID("aws/us-east-1"),
	)
	authCtx, err := middleware.AuthenticateRequest(ctx)
	if err != nil {
		t.Fatalf("authenticate request: %v", err)
	}
	if authCtx.UserID != "user-1" || authCtx.TeamID != "team-1" {
		t.Fatalf("unexpected auth context: user=%s team=%s", authCtx.UserID, authCtx.TeamID)
	}
}

func TestAuthMiddleware_JWTRegionTokenRejectedForWrongRegion(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("global-directory", "test-secret", time.Minute, time.Hour)
	regionToken, _, err := issuer.IssueRegionToken("user-1", "team-1", "admin", "aws/us-west-2", false, time.Minute)
	if err != nil {
		t.Fatalf("issue region token: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/v1/templates", nil)
	req.Header.Set("Authorization", "Bearer "+regionToken)
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req

	middleware := NewAuthMiddleware(
		nil,
		"test-secret",
		issuer,
		zap.NewNop(),
		WithJWTValidationMode(JWTValidationModeRegion),
		WithRequiredRegionID("aws/us-east-1"),
	)
	if _, err := middleware.AuthenticateRequest(ctx); err == nil {
		t.Fatal("expected region mismatch to be rejected")
	}
}

func TestAuthMiddleware_RequireJWTAuth(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	tests := []struct {
		name       string
		authCtx    *authn.AuthContext
		wantStatus int
	}{
		{
			name: "jwt user allowed",
			authCtx: &authn.AuthContext{
				AuthMethod: authn.AuthMethodJWT,
				UserID:     "user-1",
				TeamID:     "team-1",
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "api key rejected",
			authCtx: &authn.AuthContext{
				AuthMethod: authn.AuthMethodAPIKey,
				TeamID:     "team-1",
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "jwt without user rejected",
			authCtx: &authn.AuthContext{
				AuthMethod: authn.AuthMethodJWT,
				TeamID:     "team-1",
			},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "missing auth context rejected",
			authCtx:    nil,
			wantStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewAuthMiddleware(nil, "test-secret", nil, zap.NewNop())
			engine := gin.New()
			engine.Use(func(c *gin.Context) {
				if tt.authCtx != nil {
					c.Set("auth_context", tt.authCtx)
				}
				c.Next()
			})
			engine.Use(m.RequireJWTAuth())
			engine.GET("/teams", func(c *gin.Context) {
				c.Status(http.StatusNoContent)
			})

			req := httptest.NewRequest(http.MethodGet, "/teams", nil)
			rec := httptest.NewRecorder()
			engine.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("unexpected status: got %d want %d", rec.Code, tt.wantStatus)
			}
			if tt.wantStatus == http.StatusUnauthorized {
				var body map[string]string
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
					t.Fatalf("decode response: %v", err)
				}
				if body["error"] != "this API requires a user access token (human login); API keys are not supported" {
					t.Fatalf("unexpected error message: %q", body["error"])
				}
			}
		})
	}
}
