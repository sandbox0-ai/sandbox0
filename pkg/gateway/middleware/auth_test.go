package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestAuthMiddleware_JWTAccessToken(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
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
	if authCtx.UserID != "user-1" || authCtx.TeamID != "" {
		t.Fatalf("unexpected auth context: user=%s team=%s", authCtx.UserID, authCtx.TeamID)
	}
}

func TestAuthMiddleware_JWTRefreshTokenRejected(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"}})
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

func TestAuthMiddleware_JWTAccessTokenExplicitTeamHeader(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	homeRegionID := "aws-us-east-1"
	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-2", TeamRole: "developer", HomeRegionID: homeRegionID}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/v1/templates", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	req.Header.Set(internalauth.TeamIDHeader, "team-2")
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req

	middleware := NewAuthMiddleware(
		nil,
		"test-secret",
		issuer,
		zap.NewNop(),
		WithRequiredTeamRegionID(homeRegionID),
	)
	authCtx, err := middleware.AuthenticateRequest(ctx)
	if err != nil {
		t.Fatalf("authenticate request: %v", err)
	}
	if authCtx.TeamID != "team-2" || authCtx.TeamRole != "developer" {
		t.Fatalf("unexpected auth context: team=%s role=%s", authCtx.TeamID, authCtx.TeamRole)
	}
	if !authCtx.HasPermission(authn.PermSandboxCreate) {
		t.Fatal("expected developer permissions to be populated from selected team membership")
	}
}

func TestAuthMiddleware_JWTAccessTokenExplicitTeamHeaderRequiredForTeamlessToken(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, nil)
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
	if authCtx.TeamID != "" || authCtx.TeamRole != "" {
		t.Fatalf("expected teamless auth context, got team=%q role=%q", authCtx.TeamID, authCtx.TeamRole)
	}
}

func TestAuthMiddleware_JWTAccessTokenExplicitTeamHeaderRejectsWrongRegion(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{{TeamID: "team-2", TeamRole: "developer", HomeRegionID: "aws-us-west-2"}})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	req := httptest.NewRequest("GET", "/api/v1/templates", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	req.Header.Set(internalauth.TeamIDHeader, "team-2")
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = req

	middleware := NewAuthMiddleware(
		nil,
		"test-secret",
		issuer,
		zap.NewNop(),
		WithRequiredTeamRegionID("aws-us-east-1"),
	)
	if _, err := middleware.AuthenticateRequest(ctx); err != ErrSelectedTeamWrongRegion {
		t.Fatalf("authenticate request error = %v, want %v", err, ErrSelectedTeamWrongRegion)
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

func TestAuthMiddleware_RequireSystemAdmin(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	tests := []struct {
		name       string
		authCtx    *authn.AuthContext
		wantStatus int
	}{
		{
			name: "system admin allowed",
			authCtx: &authn.AuthContext{
				AuthMethod:    authn.AuthMethodJWT,
				UserID:        "user-1",
				TeamID:        "team-1",
				IsSystemAdmin: true,
			},
			wantStatus: http.StatusNoContent,
		},
		{
			name: "team admin rejected",
			authCtx: &authn.AuthContext{
				AuthMethod: authn.AuthMethodJWT,
				UserID:     "user-1",
				TeamID:     "team-1",
				TeamRole:   "admin",
			},
			wantStatus: http.StatusForbidden,
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
			engine.Use(m.RequireSystemAdmin())
			engine.GET("/internal/v1/metering/status", func(c *gin.Context) {
				c.Status(http.StatusNoContent)
			})

			req := httptest.NewRequest(http.MethodGet, "/internal/v1/metering/status", nil)
			rec := httptest.NewRecorder()
			engine.ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("unexpected status: got %d want %d", rec.Code, tt.wantStatus)
			}
		})
	}
}
