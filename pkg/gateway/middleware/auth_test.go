package middleware

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	gatewayjwt "github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/jwt"
	"go.uber.org/zap"
)

func TestAuthMiddleware_JWTAccessToken(t *testing.T) {
	t.Setenv("GIN_MODE", "release")

	issuer := gatewayjwt.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
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

	issuer := gatewayjwt.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
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
