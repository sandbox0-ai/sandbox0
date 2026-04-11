package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	gatewaymiddleware "github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestCompositeAuthMiddlewareFallsBackToPublicWithoutInternalValidator(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.TestMode)

	issuer := authn.NewIssuer("cluster-gateway", "test-secret", time.Minute, time.Hour)
	tokens, err := issuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{
		{TeamID: "team-1", TeamRole: "admin", HomeRegionID: "aws-us-east-1"},
	})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	composite := NewCompositeAuthMiddleware(
		NewInternalAuthMiddleware(nil, zap.NewNop()),
		gatewaymiddleware.NewAuthMiddleware(nil, "test-secret", issuer, zap.NewNop()),
		zap.NewNop(),
	)

	engine := gin.New()
	engine.Use(composite.Authenticate())
	engine.GET("/", func(c *gin.Context) {
		authCtx := GetAuthContext(c)
		if authCtx == nil {
			t.Fatal("missing auth context")
		}
		c.JSON(http.StatusOK, gin.H{
			"auth_method": authCtx.AuthMethod,
			"team_id":     authCtx.TeamID,
			"user_id":     authCtx.UserID,
		})
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	req.Header.Set(internalauth.DefaultTokenHeader, "bogus-internal-token")
	recorder := httptest.NewRecorder()

	engine.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", recorder.Code, http.StatusOK, recorder.Body.String())
	}

	var body struct {
		AuthMethod string `json:"auth_method"`
		TeamID     string `json:"team_id"`
		UserID     string `json:"user_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.AuthMethod != string(authn.AuthMethodJWT) {
		t.Fatalf("auth_method = %q, want %q", body.AuthMethod, authn.AuthMethodJWT)
	}
	if body.TeamID != "team-1" {
		t.Fatalf("team_id = %q, want %q", body.TeamID, "team-1")
	}
	if body.UserID != "user-1" {
		t.Fatalf("user_id = %q, want %q", body.UserID, "user-1")
	}
}
