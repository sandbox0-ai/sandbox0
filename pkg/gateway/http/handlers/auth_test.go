package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

type mockAuthRepository struct {
	users              map[string]*identity.User
	teams              map[string]*identity.Team
	refreshTokens      map[string]*identity.RefreshToken
	webLoginCodes      map[string]*identity.WebLoginCode
	deviceAuthSessions map[string]*identity.DeviceAuthSession
	teamMembers        map[string]*identity.TeamMember
	teamGrantCalls     int
	createCalls        int
}

func newMockAuthRepository() *mockAuthRepository {
	return &mockAuthRepository{
		users:              map[string]*identity.User{},
		teams:              map[string]*identity.Team{},
		refreshTokens:      map[string]*identity.RefreshToken{},
		webLoginCodes:      map[string]*identity.WebLoginCode{},
		deviceAuthSessions: map[string]*identity.DeviceAuthSession{},
		teamMembers:        map[string]*identity.TeamMember{},
	}
}

func (m *mockAuthRepository) CreateRefreshToken(_ context.Context, token *identity.RefreshToken) error {
	if _, exists := m.refreshTokens[token.TokenHash]; exists {
		return errors.New("duplicate refresh token hash")
	}
	copyToken := *token
	m.refreshTokens[token.TokenHash] = &copyToken
	m.createCalls++
	return nil
}

func (m *mockAuthRepository) ValidateRefreshToken(_ context.Context, tokenHash string) (*identity.RefreshToken, error) {
	token, ok := m.refreshTokens[tokenHash]
	if !ok {
		return nil, identity.ErrTokenNotFound
	}
	if token.Revoked {
		return nil, identity.ErrTokenRevoked
	}
	if time.Now().After(token.ExpiresAt) {
		return nil, identity.ErrTokenExpired
	}
	return token, nil
}

func (m *mockAuthRepository) RevokeAllUserRefreshTokens(_ context.Context, userID string) error {
	for _, token := range m.refreshTokens {
		if token.UserID == userID {
			token.Revoked = true
		}
	}
	return nil
}

func (m *mockAuthRepository) CreateWebLoginCode(_ context.Context, code *identity.WebLoginCode) error {
	if _, exists := m.webLoginCodes[code.CodeHash]; exists {
		return errors.New("duplicate web login code hash")
	}
	copyCode := *code
	if copyCode.ID == "" {
		copyCode.ID = "web-login-code-1"
	}
	m.webLoginCodes[copyCode.CodeHash] = &copyCode
	code.ID = copyCode.ID
	return nil
}

func (m *mockAuthRepository) ConsumeWebLoginCode(_ context.Context, codeHash, returnURL string) (*identity.WebLoginCode, error) {
	code, ok := m.webLoginCodes[codeHash]
	if !ok || code.ReturnURL != returnURL || code.ConsumedAt != nil || time.Now().After(code.ExpiresAt) {
		return nil, identity.ErrWebLoginCodeNotFound
	}
	now := time.Now()
	code.ConsumedAt = &now
	copyCode := *code
	return &copyCode, nil
}

func (m *mockAuthRepository) GetUserByID(_ context.Context, id string) (*identity.User, error) {
	user, ok := m.users[id]
	if !ok {
		return nil, errors.New("user not found")
	}
	return user, nil
}

func (m *mockAuthRepository) ListTeamGrantsByUserID(_ context.Context, userID string) ([]identity.TeamGrantRecord, error) {
	m.teamGrantCalls++
	grants := make([]identity.TeamGrantRecord, 0)
	for _, member := range m.teamMembers {
		if member.UserID != userID {
			continue
		}
		grant := identity.TeamGrantRecord{
			TeamID:   member.TeamID,
			TeamRole: member.Role,
		}
		if team, ok := m.teams[member.TeamID]; ok {
			grant.HomeRegionID = team.HomeRegionID
		}
		grants = append(grants, grant)
	}
	return grants, nil
}

func (m *mockAuthRepository) CreateDeviceAuthSession(_ context.Context, session *identity.DeviceAuthSession) error {
	copySession := *session
	if copySession.ID == "" {
		copySession.ID = "device-session-1"
	}
	m.deviceAuthSessions[copySession.ID] = &copySession
	session.ID = copySession.ID
	return nil
}

func (m *mockAuthRepository) GetDeviceAuthSessionByID(_ context.Context, id string) (*identity.DeviceAuthSession, error) {
	session, ok := m.deviceAuthSessions[id]
	if !ok {
		return nil, identity.ErrDeviceAuthSessionNotFound
	}
	if session.ConsumedAt != nil {
		return nil, identity.ErrDeviceAuthSessionConsumed
	}
	if time.Now().After(session.ExpiresAt) {
		return nil, identity.ErrDeviceAuthSessionExpired
	}
	copySession := *session
	return &copySession, nil
}

func (m *mockAuthRepository) MarkDeviceAuthSessionConsumed(_ context.Context, id string) error {
	session, ok := m.deviceAuthSessions[id]
	if !ok {
		return identity.ErrDeviceAuthSessionNotFound
	}
	now := time.Now()
	session.ConsumedAt = &now
	return nil
}

func TestAuthHandler_RefreshToken_SucceedsWithPersistedToken(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := newMockAuthRepository()
	user := &identity.User{
		ID:      "user-1",
		Email:   "user@example.com",
		Name:    "User",
		IsAdmin: false,
	}
	repo.users[user.ID] = user

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, nil)
	if err != nil {
		t.Fatalf("issue initial token pair: %v", err)
	}
	if err := repo.CreateRefreshToken(context.Background(), &identity.RefreshToken{
		UserID:    user.ID,
		TokenHash: authn.HashRefreshToken(initialTokens.RefreshToken),
		ExpiresAt: initialTokens.RefreshExpiresAt,
	}); err != nil {
		t.Fatalf("persist initial refresh token: %v", err)
	}

	handler := &AuthHandler{
		repo:      repo,
		jwtIssuer: issuer,
		logger:    zap.NewNop(),
	}

	router := gin.New()
	router.POST("/auth/refresh", handler.RefreshToken)

	rec := httptest.NewRecorder()
	reqBody := map[string]string{"refresh_token": initialTokens.RefreshToken}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	data, apiErr, err := spec.DecodeResponse[LoginResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", *apiErr)
	}
	if data.AccessToken == "" || data.RefreshToken == "" {
		t.Fatalf("expected new tokens in response")
	}
	if repo.createCalls != 2 {
		t.Fatalf("expected 2 create calls (seed + refresh), got %d", repo.createCalls)
	}
}

func TestAuthHandler_BuildTeamGrantsUsesSingleRepositoryCall(t *testing.T) {
	repo := newMockAuthRepository()
	homeRegionA := "aws-us-east-1"
	homeRegionB := "aws-eu-west-1"
	repo.teams["team-b"] = &identity.Team{ID: "team-b", HomeRegionID: &homeRegionB}
	repo.teams["team-a"] = &identity.Team{ID: "team-a", HomeRegionID: &homeRegionA}
	repo.teamMembers["team-b:user-1"] = &identity.TeamMember{TeamID: "team-b", UserID: "user-1", Role: "viewer"}
	repo.teamMembers["team-a:user-1"] = &identity.TeamMember{TeamID: "team-a", UserID: "user-1", Role: "admin"}

	handler := &AuthHandler{repo: repo, logger: zap.NewNop()}
	grants, err := handler.buildTeamGrants(context.Background(), "user-1")
	if err != nil {
		t.Fatalf("build team grants: %v", err)
	}
	if repo.teamGrantCalls != 1 {
		t.Fatalf("expected exactly one team grant query, got %d", repo.teamGrantCalls)
	}
	if len(grants) != 2 {
		t.Fatalf("expected 2 grants, got %d", len(grants))
	}
	if grants[0].TeamID != "team-a" || grants[0].TeamRole != "admin" || grants[0].HomeRegionID != homeRegionA {
		t.Fatalf("unexpected first grant: %+v", grants[0])
	}
	if grants[1].TeamID != "team-b" || grants[1].TeamRole != "viewer" || grants[1].HomeRegionID != homeRegionB {
		t.Fatalf("unexpected second grant: %+v", grants[1])
	}
}

func TestAuthHandler_RefreshToken_IncludesImplicitSingleTeamContext(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := newMockAuthRepository()
	user := &identity.User{
		ID:      "user-1",
		Email:   "user@example.com",
		Name:    "User",
		IsAdmin: false,
	}
	repo.users[user.ID] = user
	repo.teamMembers["team-1:user-1"] = &identity.TeamMember{
		ID:     "member-1",
		TeamID: "team-1",
		UserID: "user-1",
		Role:   "admin",
	}
	homeRegionID := "aws-us-east-1"
	repo.teams["team-1"] = &identity.Team{ID: "team-1", HomeRegionID: &homeRegionID}

	issuer := authn.NewIssuer("global-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: homeRegionID}})
	if err != nil {
		t.Fatalf("issue initial token pair: %v", err)
	}
	if err := repo.CreateRefreshToken(context.Background(), &identity.RefreshToken{
		UserID:    user.ID,
		TokenHash: authn.HashRefreshToken(initialTokens.RefreshToken),
		ExpiresAt: initialTokens.RefreshExpiresAt,
	}); err != nil {
		t.Fatalf("persist initial refresh token: %v", err)
	}

	handler := &AuthHandler{
		repo:      repo,
		jwtIssuer: issuer,
		logger:    zap.NewNop(),
	}

	router := gin.New()
	router.POST("/auth/refresh", handler.RefreshToken)

	rec := httptest.NewRecorder()
	reqBody := map[string]string{"refresh_token": initialTokens.RefreshToken}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	data, apiErr, err := spec.DecodeResponse[LoginResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", *apiErr)
	}
	claims, err := issuer.ValidateAccessToken(data.AccessToken)
	if err != nil {
		t.Fatalf("validate access token: %v", err)
	}
	if len(claims.TeamGrants) != 1 || claims.TeamGrants[0].HomeRegionID != homeRegionID {
		t.Fatalf("expected single team grant with home region, got %+v", claims.TeamGrants)
	}
}

func TestAuthHandler_RefreshToken_OmitsTeamContextWhenUserHasMultipleTeams(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := newMockAuthRepository()
	user := &identity.User{
		ID:      "user-1",
		Email:   "user@example.com",
		Name:    "User",
		IsAdmin: false,
	}
	repo.users[user.ID] = user
	repo.teamMembers["team-1:user-1"] = &identity.TeamMember{
		ID:     "member-1",
		TeamID: "team-1",
		UserID: "user-1",
		Role:   "admin",
	}
	repo.teamMembers["team-2:user-1"] = &identity.TeamMember{
		ID:     "member-2",
		TeamID: "team-2",
		UserID: "user-1",
		Role:   "viewer",
	}
	homeRegionA := "aws-us-east-1"
	homeRegionB := "aws-eu-west-1"
	repo.teams["team-1"] = &identity.Team{ID: "team-1", HomeRegionID: &homeRegionA}
	repo.teams["team-2"] = &identity.Team{ID: "team-2", HomeRegionID: &homeRegionB}

	issuer := authn.NewIssuer("global-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, []authn.TeamGrant{{TeamID: "team-1", TeamRole: "admin", HomeRegionID: homeRegionA}, {TeamID: "team-2", TeamRole: "viewer", HomeRegionID: homeRegionB}})
	if err != nil {
		t.Fatalf("issue initial token pair: %v", err)
	}
	if err := repo.CreateRefreshToken(context.Background(), &identity.RefreshToken{
		UserID:    user.ID,
		TokenHash: authn.HashRefreshToken(initialTokens.RefreshToken),
		ExpiresAt: initialTokens.RefreshExpiresAt,
	}); err != nil {
		t.Fatalf("persist initial refresh token: %v", err)
	}

	handler := &AuthHandler{
		repo:      repo,
		jwtIssuer: issuer,
		logger:    zap.NewNop(),
	}

	router := gin.New()
	router.POST("/auth/refresh", handler.RefreshToken)

	rec := httptest.NewRecorder()
	reqBody := map[string]string{"refresh_token": initialTokens.RefreshToken}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	data, apiErr, err := spec.DecodeResponse[LoginResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", *apiErr)
	}
	claims, err := issuer.ValidateAccessToken(data.AccessToken)
	if err != nil {
		t.Fatalf("validate access token: %v", err)
	}
	if len(claims.TeamGrants) != 2 {
		t.Fatalf("expected multi-team refresh token to include team grants, got %+v", claims.TeamGrants)
	}
}

func TestAuthHandler_LogoutRevocation_BlocksRefresh(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := newMockAuthRepository()
	user := &identity.User{
		ID:      "user-1",
		Email:   "user@example.com",
		Name:    "User",
		IsAdmin: false,
	}
	repo.users[user.ID] = user

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, nil)
	if err != nil {
		t.Fatalf("issue initial token pair: %v", err)
	}
	if err := repo.CreateRefreshToken(context.Background(), &identity.RefreshToken{
		UserID:    user.ID,
		TokenHash: authn.HashRefreshToken(initialTokens.RefreshToken),
		ExpiresAt: initialTokens.RefreshExpiresAt,
	}); err != nil {
		t.Fatalf("persist initial refresh token: %v", err)
	}

	handler := &AuthHandler{
		repo:      repo,
		jwtIssuer: issuer,
		logger:    zap.NewNop(),
	}

	router := gin.New()
	router.POST("/auth/logout", func(c *gin.Context) {
		c.Set("auth_context", &authn.AuthContext{UserID: user.ID})
		handler.Logout(c)
	})
	router.POST("/auth/refresh", handler.RefreshToken)

	logoutReq := httptest.NewRequest(http.MethodPost, "/auth/logout", nil)
	logoutRec := httptest.NewRecorder()
	router.ServeHTTP(logoutRec, logoutReq)
	if logoutRec.Code != http.StatusOK {
		t.Fatalf("expected logout 200, got %d body=%s", logoutRec.Code, logoutRec.Body.String())
	}

	rec := httptest.NewRecorder()
	reqBody := map[string]string{"refresh_token": initialTokens.RefreshToken}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 after logout revocation, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAuthHandler_RefreshToken_FailsWhenTokenNeverPersisted(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	repo := newMockAuthRepository()
	user := &identity.User{
		ID:      "user-1",
		Email:   "user@example.com",
		Name:    "User",
		IsAdmin: false,
	}
	repo.users[user.ID] = user

	issuer := authn.NewIssuer("regional-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, user.Email, user.Name, user.IsAdmin, nil)
	if err != nil {
		t.Fatalf("issue initial token pair: %v", err)
	}

	handler := &AuthHandler{
		repo:      repo,
		jwtIssuer: issuer,
		logger:    zap.NewNop(),
	}

	router := gin.New()
	router.POST("/auth/refresh", handler.RefreshToken)

	rec := httptest.NewRecorder()
	reqBody := map[string]string{"refresh_token": initialTokens.RefreshToken}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/auth/refresh", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for non-persisted refresh token, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestAuthHandler_OIDCLogin_ReturnsNotFoundWithoutOIDCManager(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	handler := &AuthHandler{
		logger: zap.NewNop(),
	}

	router := gin.New()
	router.GET("/auth/oidc/:provider/login", handler.OIDCLogin)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth/oidc/example/login", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d body=%s", rec.Code, rec.Body.String())
	}

	_, apiErr, err := spec.DecodeResponse[map[string]any](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr == nil {
		t.Fatalf("expected api error")
	}
	if apiErr.Code != spec.CodeNotFound {
		t.Fatalf("expected code %q, got %q", spec.CodeNotFound, apiErr.Code)
	}
}

func TestAuthHandler_GetAuthProviders_HandlesNilBuiltinAndDisabledSSO(t *testing.T) {
	t.Setenv("GIN_MODE", "release")
	gin.SetMode(gin.ReleaseMode)

	handler := &AuthHandler{
		logger: zap.NewNop(),
	}

	router := gin.New()
	router.GET("/auth/providers", handler.GetAuthProviders)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/auth/providers", nil)
	router.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}

	type authProvidersResponse struct {
		Providers []map[string]any `json:"providers"`
	}
	data, apiErr, err := spec.DecodeResponse[authProvidersResponse](rec.Body)
	if err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if apiErr != nil {
		t.Fatalf("unexpected api error: %+v", *apiErr)
	}
	if len(data.Providers) != 0 {
		t.Fatalf("expected no providers, got %d", len(data.Providers))
	}
}
