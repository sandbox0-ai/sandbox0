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
	users         map[string]*identity.User
	refreshTokens map[string]*identity.RefreshToken
	createCalls   int
}

func newMockAuthRepository() *mockAuthRepository {
	return &mockAuthRepository{
		users:         map[string]*identity.User{},
		refreshTokens: map[string]*identity.RefreshToken{},
	}
}

func (m *mockAuthRepository) CreateRefreshToken(_ context.Context, token *identity.RefreshToken) error {
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

func (m *mockAuthRepository) GetUserByID(_ context.Context, id string) (*identity.User, error) {
	user, ok := m.users[id]
	if !ok {
		return nil, errors.New("user not found")
	}
	return user, nil
}

func (m *mockAuthRepository) GetTeamMember(_ context.Context, _, _ string) (*identity.TeamMember, error) {
	return nil, errors.New("team member not found")
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

	issuer := authn.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, "", "", user.Email, user.Name, user.IsAdmin)
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

	issuer := authn.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, "", "", user.Email, user.Name, user.IsAdmin)
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

	issuer := authn.NewIssuer("edge-gateway", "test-secret", time.Minute, time.Hour)
	initialTokens, err := issuer.IssueTokenPair(user.ID, "", "", user.Email, user.Name, user.IsAdmin)
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
