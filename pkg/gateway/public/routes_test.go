package public

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

func TestRegisterRoutesMountsSelfHostedPublicSurface(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterRoutes(router, testDeps())

	if !hasRoute(router, "POST", "/auth/login") {
		t.Fatal("expected full public routes to include /auth/login")
	}
	if !hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected full public routes to include /users/me")
	}
	if !hasRoute(router, "GET", "/teams") {
		t.Fatal("expected full public routes to include /teams")
	}
	if !hasRoute(router, "PUT", "/teams/:id/owner") {
		t.Fatal("expected full public routes to include team owner transfer")
	}
	if !hasRoute(router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected full public routes to include SSH key list")
	}
	if !hasRoute(router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected full public routes to include SSH key create")
	}
	if !hasRoute(router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected full public routes to include SSH key delete")
	}
	if !hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected full public routes to include /api-keys")
	}
}

func TestRegisterIdentityRoutesAppliesPlatformAdmissionExactlyOnce(t *testing.T) {
	gin.SetMode(gin.TestMode)

	deps := testDeps()
	admissionCalls := 0
	deps.IdentityRequestAdmission = func(c *gin.Context) {
		admissionCalls++
		c.Next()
	}
	router := gin.New()
	RegisterIdentityRoutes(router, deps)

	for _, request := range []*http.Request{
		httptest.NewRequest(http.MethodGet, "/auth/providers", nil),
		httptest.NewRequest(http.MethodGet, "/teams", nil),
	} {
		before := admissionCalls
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, request)
		if admissionCalls != before+1 {
			t.Fatalf("%s admission calls = %d, want %d", request.URL.Path, admissionCalls, before+1)
		}
	}
}

func TestRegisterIdentityRoutesOmitsRegionalStateRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterIdentityRoutes(router, testDeps())

	if hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected identity routes to omit /api-keys")
	}
	if hasRoute(router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected identity routes to omit SSH key list")
	}
	if hasRoute(router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected identity routes to omit SSH key create")
	}
	if hasRoute(router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected identity routes to omit SSH key delete")
	}
	if !hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected identity routes to include /users/me")
	}
	if !hasRoute(router, "GET", "/teams") {
		t.Fatal("expected identity routes to include /teams")
	}
	if !hasRoute(router, "PUT", "/teams/:id/owner") {
		t.Fatal("expected identity routes to include team owner transfer")
	}
}

func TestRegisterAPIKeyRoutesOnlyMountsRegionalAPIKeySurface(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterAPIKeyRoutes(router, testDeps())

	if !hasRoute(router, "GET", "/api-keys") {
		t.Fatal("expected regional routes to include /api-keys")
	}
	if !hasRoute(router, "GET", "/api-keys/current") {
		t.Fatal("expected regional routes to include /api-keys/current")
	}
	if hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected regional API key routes to omit /users/me")
	}
	if hasRoute(router, "POST", "/auth/login") {
		t.Fatal("expected regional API key routes to omit /auth/login")
	}
}

func TestRegisterAPIKeyRoutesRejectViewerJWTForManagement(t *testing.T) {
	gin.SetMode(gin.TestMode)

	deps := testDeps()
	router := gin.New()
	RegisterAPIKeyRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair("user-1", "viewer@example.com", "Viewer", false, []authn.TeamGrant{
		{TeamID: "team-1", TeamRole: "viewer", HomeRegionID: "aws-us-east-1"},
	})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	tests := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodGet, path: "/api-keys"},
		{method: http.MethodPost, path: "/api-keys", body: `{"name":"viewer-key","roles":["viewer"]}`},
		{method: http.MethodDelete, path: "/api-keys/key-1"},
		{method: http.MethodPost, path: "/api-keys/key-1/deactivate"},
	}

	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			if tt.body != "" {
				req.Header.Set("Content-Type", "application/json")
			}
			req.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
			req.Header.Set(internalauth.TeamIDHeader, "team-1")

			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d body=%s", rec.Code, http.StatusForbidden, rec.Body.String())
			}
		})
	}
}

func TestRegisterUserSSHKeyRoutesMountsSSHKeysOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	RegisterUserSSHKeyRoutes(router, testDeps())

	if !hasRoute(router, "GET", "/users/me/ssh-keys") {
		t.Fatal("expected SSH key routes to include list")
	}
	if !hasRoute(router, "POST", "/users/me/ssh-keys") {
		t.Fatal("expected SSH key routes to include create")
	}
	if !hasRoute(router, "DELETE", "/users/me/ssh-keys/:id") {
		t.Fatal("expected SSH key routes to include delete")
	}
	if hasRoute(router, "GET", "/users/me") {
		t.Fatal("expected SSH key routes to omit full user profile")
	}
	if hasRoute(router, "POST", "/auth/login") {
		t.Fatal("expected SSH key routes to omit /auth/login")
	}
}

func TestRegisterRoutesAdmitsEveryAttributedTeamSurface(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const (
		targetTeamID   = "11111111-1111-4111-8111-111111111111"
		selectedTeamID = "22222222-2222-4222-8222-222222222222"
		targetUserID   = "33333333-3333-4333-8333-333333333333"
	)
	deps := testDeps()
	admissionCalls := 0
	activeRequestCalls := 0
	trafficCalls := 0
	deps.TeamTrafficAdmission = func(c *gin.Context) {
		assertRequestTeamID(t, c, targetTeamID)
		trafficCalls++
		c.Next()
	}
	deps.TeamRequestAdmission = func(c *gin.Context) {
		assertRequestTeamID(t, c, targetTeamID)
		admissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}
	deps.TeamActiveRequestAdmission = func(c *gin.Context) {
		assertRequestTeamID(t, c, targetTeamID)
		activeRequestCalls++
		c.Next()
	}
	router := gin.New()
	RegisterRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair(targetUserID, "user@example.com", "User", false, []authn.TeamGrant{
		{TeamID: targetTeamID, TeamRole: "viewer", HomeRegionID: "aws-us-east-1"},
		{TeamID: selectedTeamID, TeamRole: "admin", HomeRegionID: "aws-us-east-1"},
	})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	routes := []struct {
		method string
		path   string
		teamID string
	}{
		{method: http.MethodGet, path: "/teams/" + targetTeamID},
		{method: http.MethodPut, path: "/teams/" + targetTeamID, teamID: selectedTeamID},
		{method: http.MethodDelete, path: "/teams/" + targetTeamID},
		{method: http.MethodPut, path: "/teams/" + targetTeamID + "/owner"},
		{method: http.MethodGet, path: "/teams/" + targetTeamID + "/members"},
		{method: http.MethodPost, path: "/teams/" + targetTeamID + "/members"},
		{
			method: http.MethodPut,
			path:   "/teams/" + targetTeamID + "/members/44444444-4444-4444-8444-444444444444",
		},
		{
			method: http.MethodDelete,
			path:   "/teams/" + targetTeamID + "/members/44444444-4444-4444-8444-444444444444",
		},
		{method: http.MethodGet, path: "/users/me/ssh-keys", teamID: targetTeamID},
		{method: http.MethodGet, path: "/api-keys/current", teamID: targetTeamID},
	}
	for _, route := range routes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			beforeAdmission := admissionCalls
			beforeActiveRequests := activeRequestCalls
			beforeTraffic := trafficCalls
			request := httptest.NewRequest(route.method, route.path, nil)
			request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
			if route.teamID != "" {
				request.Header.Set(internalauth.TeamIDHeader, route.teamID)
			}
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusTooManyRequests {
				t.Fatalf("status = %d, want admission response; body=%s", recorder.Code, recorder.Body.String())
			}
			if admissionCalls != beforeAdmission+1 {
				t.Fatalf("admission calls = %d, want %d", admissionCalls, beforeAdmission+1)
			}
			if trafficCalls != beforeTraffic+1 {
				t.Fatalf("traffic admission calls = %d, want %d", trafficCalls, beforeTraffic+1)
			}
			if activeRequestCalls != beforeActiveRequests+1 {
				t.Fatalf(
					"active request admission calls = %d, want %d",
					activeRequestCalls,
					beforeActiveRequests+1,
				)
			}
		})
	}

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/auth/providers", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("anonymous auth status = %d, want 200; body=%s", recorder.Code, recorder.Body.String())
	}
	if admissionCalls != len(routes) {
		t.Fatalf("anonymous auth unexpectedly consumed admission; calls = %d", admissionCalls)
	}
	if trafficCalls != len(routes) {
		t.Fatalf("anonymous auth unexpectedly consumed traffic admission; calls = %d", trafficCalls)
	}
	if activeRequestCalls != len(routes) {
		t.Fatalf(
			"anonymous auth unexpectedly consumed active request admission; calls = %d",
			activeRequestCalls,
		)
	}

	invalidDeletion := httptest.NewRequest(http.MethodDelete, "/teams/team-1", nil)
	invalidDeletion.Header.Set("Authorization", "Bearer invalid")
	invalidDeletion.Header.Set(internalauth.TeamIDHeader, "team-1")
	invalidRecorder := httptest.NewRecorder()
	router.ServeHTTP(invalidRecorder, invalidDeletion)
	if invalidRecorder.Code != http.StatusUnauthorized {
		t.Fatalf("invalid deletion auth status = %d, want 401", invalidRecorder.Code)
	}
	if admissionCalls != len(routes) {
		t.Fatalf("invalid deletion auth consumed admission; calls = %d", admissionCalls)
	}
	if trafficCalls != len(routes) {
		t.Fatalf("invalid deletion auth consumed traffic admission; calls = %d", trafficCalls)
	}
	if activeRequestCalls != len(routes) {
		t.Fatalf("invalid deletion auth consumed active request admission; calls = %d", activeRequestCalls)
	}
}

func TestCreateTeamBootstrapDoesNotRequireSelectedTeamAdmission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	deps := testDeps()
	admissionCalls := 0
	deps.TeamRequestAdmission = func(c *gin.Context) {
		admissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}
	router := gin.New()
	RegisterIdentityRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair(
		"33333333-3333-4333-8333-333333333333",
		"user@example.com",
		"User",
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, "/teams", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
	}
	if admissionCalls != 0 {
		t.Fatalf("bootstrap request consumed team admission; calls = %d", admissionCalls)
	}
}

func TestTargetTeamAdmissionRejectsTargetMissingFromSignedGrants(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const targetTeamID = "11111111-1111-4111-8111-111111111111"
	deps := testDeps()
	admissionCalls := 0
	deps.TeamRequestAdmission = func(c *gin.Context) {
		admissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}
	router := gin.New()
	RegisterIdentityRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair(
		"33333333-3333-4333-8333-333333333333",
		"user@example.com",
		"User",
		false,
		nil,
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/teams/"+targetTeamID, nil)
	request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", recorder.Code, recorder.Body.String())
	}
	if admissionCalls != 0 {
		t.Fatalf("unsigned target consumed team admission; calls = %d", admissionCalls)
	}
}

func TestTargetTeamAdmissionRejectsGrantOwnedByAnotherRegion(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const targetTeamID = "11111111-1111-4111-8111-111111111111"
	deps := testDeps()
	deps.AuthMiddleware = middleware.NewAuthMiddleware(
		nil,
		"secret",
		deps.JWTIssuer,
		deps.Logger,
		middleware.WithRequiredTeamRegionID("aws-us-west-2"),
	)
	admissionCalls := 0
	deps.TeamRequestAdmission = func(c *gin.Context) {
		admissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}
	router := gin.New()
	RegisterIdentityRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair(
		"33333333-3333-4333-8333-333333333333",
		"user@example.com",
		"User",
		false,
		[]authn.TeamGrant{{
			TeamID:       targetTeamID,
			TeamRole:     "admin",
			HomeRegionID: "aws-us-east-1",
		}},
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	request := httptest.NewRequest(http.MethodGet, "/teams/"+targetTeamID, nil)
	request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403; body=%s", recorder.Code, recorder.Body.String())
	}
	if admissionCalls != 0 {
		t.Fatalf("wrong-region target consumed team admission; calls = %d", admissionCalls)
	}
}

func TestCurrentAPIKeyAdmissionExplicitlyExemptsPlatformKey(t *testing.T) {
	gin.SetMode(gin.TestMode)

	admissionCalls := 0
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod:    authn.AuthMethodAPIKey,
			APIKeyID:      "platform-key",
			TeamID:        "caller-supplied-team",
			IsSystemAdmin: true,
		})
		c.Next()
	})
	router.Use(admitCurrentAPIKeyRequest(func(c *gin.Context) {
		admissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}))
	router.GET("/api-keys/current", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api-keys/current", nil))
	if recorder.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", recorder.Code)
	}
	if admissionCalls != 0 {
		t.Fatalf("admission calls = %d, want 0", admissionCalls)
	}
}

func TestCurrentAPIKeyAdmissionRejectsUnattributedNonPlatformRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	admissionCalls := 0
	router := gin.New()
	router.Use(func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{
			AuthMethod: authn.AuthMethodJWT,
			UserID:     "user-1",
		})
		c.Next()
	})
	router.Use(admitCurrentAPIKeyRequest(func(c *gin.Context) {
		admissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}))
	router.GET("/api-keys/current", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api-keys/current", nil))

	if recorder.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401; body=%s", recorder.Code, recorder.Body.String())
	}
	if admissionCalls != 0 {
		t.Fatalf("admission calls = %d, want 0", admissionCalls)
	}
}

func TestTeamOwnedRoutesRejectMissingSelectedTeamBeforeAdmission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	deps := testDeps()
	admissionCalls := 0
	deps.TeamTrafficAdmission = func(c *gin.Context) {
		admissionCalls++
		c.Next()
	}
	deps.TeamActiveRequestAdmission = func(c *gin.Context) {
		admissionCalls++
		c.Next()
	}
	deps.TeamRequestAdmission = func(c *gin.Context) {
		admissionCalls++
		c.Next()
	}
	router := gin.New()
	RegisterRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair(
		"33333333-3333-4333-8333-333333333333",
		"user@example.com",
		"User",
		false,
		[]authn.TeamGrant{
			{
				TeamID:   "11111111-1111-4111-8111-111111111111",
				TeamRole: "admin",
			},
			{
				TeamID:   "22222222-2222-4222-8222-222222222222",
				TeamRole: "admin",
			},
		},
	)
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}

	for _, route := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/users/me/ssh-keys"},
		{method: http.MethodGet, path: "/api-keys"},
	} {
		t.Run(route.path, func(t *testing.T) {
			request := httptest.NewRequest(route.method, route.path, nil)
			request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
			recorder := httptest.NewRecorder()

			router.ServeHTTP(recorder, request)

			if recorder.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
	if admissionCalls != 0 {
		t.Fatalf("missing-team requests reached team admission; calls = %d", admissionCalls)
	}
}

func TestRegisterIdentityRoutesDeletionUsesNormalTeamAdmission(t *testing.T) {
	gin.SetMode(gin.TestMode)

	deps := testDeps()
	teamAdmissionCalls := 0
	identityAdmissionCalls := 0
	deps.TeamRequestAdmission = func(c *gin.Context) {
		teamAdmissionCalls++
		c.AbortWithStatus(http.StatusTooManyRequests)
	}
	deps.IdentityRequestAdmission = func(c *gin.Context) {
		identityAdmissionCalls++
		c.Next()
	}
	deps.TeamDeletionUnavailableReason = "team deletion requires home-region coordination"

	const teamID = "11111111-1111-1111-1111-111111111111"
	router := gin.New()
	RegisterIdentityRoutes(router, deps)

	tokens, err := deps.JWTIssuer.IssueTokenPair("user-1", "user@example.com", "User", false, []authn.TeamGrant{
		{TeamID: teamID, TeamRole: "admin", HomeRegionID: "aws-us-east-1"},
	})
	if err != nil {
		t.Fatalf("issue token pair: %v", err)
	}
	request := httptest.NewRequest(http.MethodDelete, "/teams/"+teamID, nil)
	request.Header.Set("Authorization", "Bearer "+tokens.AccessToken)
	request.Header.Set(internalauth.TeamIDHeader, teamID)
	recorder := httptest.NewRecorder()

	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", recorder.Code, recorder.Body.String())
	}
	if teamAdmissionCalls != 1 {
		t.Fatalf("team admission calls = %d, want 1", teamAdmissionCalls)
	}
	if identityAdmissionCalls != 1 {
		t.Fatalf(
			"identity admission calls = %d, want 1",
			identityAdmissionCalls,
		)
	}
}

func assertRequestTeamID(t *testing.T, c *gin.Context, want string) {
	t.Helper()
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		t.Fatal("admission request has no authentication context")
	}
	if authCtx.TeamID != want {
		t.Fatalf("admission team ID = %q, want %q", authCtx.TeamID, want)
	}
	if authCtx.TeamRole != "viewer" {
		t.Fatalf("admission team role = %q, want target grant role viewer", authCtx.TeamRole)
	}
	if authCtx.HasPermission(authn.PermAPIKeyManage) {
		t.Fatal("target viewer grant unexpectedly retained selected-team admin permission")
	}
	if requestAuthCtx := authn.FromContext(c.Request.Context()); requestAuthCtx == nil ||
		requestAuthCtx.TeamID != want {
		t.Fatalf("request context team ID is not attributed to %q", want)
	}
}

func testDeps() Deps {
	logger := zap.NewNop()
	jwtIssuer := authn.NewIssuer("test", "secret", time.Minute, time.Hour)
	return Deps{
		APIKeyRepo:     &apikey.Repository{},
		AuthMiddleware: middleware.NewAuthMiddleware(nil, "secret", jwtIssuer, logger),
		JWTIssuer:      jwtIssuer,
		Logger:         logger,
	}
}

func hasRoute(router *gin.Engine, method, path string) bool {
	for _, route := range router.Routes() {
		if route.Method == method && route.Path == path {
			return true
		}
	}
	return false
}
