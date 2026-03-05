package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/jwt"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/infra/pkg/gateway/db"
	"github.com/sandbox0-ai/infra/pkg/gateway/middleware"
	"github.com/sandbox0-ai/infra/pkg/gateway/spec"
	"github.com/sandbox0-ai/infra/pkg/license"
	"go.uber.org/zap"
)

// AuthHandler handles authentication endpoints
type AuthHandler struct {
	repo            authRepository
	builtinProvider *builtin.Provider
	oidcManager     *oidc.Manager
	ssoEnabled      bool
	jwtIssuer       *jwt.Issuer
	logger          *zap.Logger
}

type authRepository interface {
	CreateRefreshToken(ctx context.Context, token *db.RefreshToken) error
	ValidateRefreshToken(ctx context.Context, tokenHash string) (*db.RefreshToken, error)
	RevokeAllUserRefreshTokens(ctx context.Context, userID string) error
	GetUserByID(ctx context.Context, id string) (*db.User, error)
	GetTeamMember(ctx context.Context, teamID, userID string) (*db.TeamMember, error)
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(
	repo *db.Repository,
	builtinProvider *builtin.Provider,
	oidcManager *oidc.Manager,
	ssoEnabled bool,
	jwtIssuer *jwt.Issuer,
	logger *zap.Logger,
) *AuthHandler {
	return &AuthHandler{
		repo:            repo,
		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		ssoEnabled:      ssoEnabled,
		jwtIssuer:       jwtIssuer,
		logger:          logger,
	}
}

// LoginRequest is the request body for login
type LoginRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required"`
}

// LoginResponse is the response for login
type LoginResponse struct {
	AccessToken  string        `json:"access_token"`
	RefreshToken string        `json:"refresh_token"`
	ExpiresAt    int64         `json:"expires_at"`
	User         *UserResponse `json:"user"`
}

// Login handles email/password login
func (h *AuthHandler) Login(c *gin.Context) {
	var req LoginRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	user, err := h.builtinProvider.Authenticate(c.Request.Context(), req.Email, req.Password)
	if err != nil {
		h.logger.Warn("Login failed",
			zap.String("email", req.Email),
			zap.Error(err),
		)

		status := http.StatusUnauthorized
		if errors.Is(err, builtin.ErrBuiltInAuthDisabled) {
			status = http.StatusForbidden
		}

		code := spec.CodeUnauthorized
		if status == http.StatusForbidden {
			code = spec.CodeForbidden
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	// Get default team ID for token
	teamID := ""
	if user.DefaultTeamID != nil {
		teamID = *user.DefaultTeamID
	}
	teamRole, err := h.resolveTeamRole(c.Request.Context(), teamID, user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve team role for login", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamID, teamRole)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to issue tokens")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresAt:    tokens.ExpiresAt.Unix(),
		User:         NewUserResponse(user),
	})
}

// RegisterRequest is the request body for registration
type RegisterRequest struct {
	Email    string `json:"email" binding:"required,email"`
	Password string `json:"password" binding:"required,min=8"`
	Name     string `json:"name" binding:"required"`
}

// Register handles user registration
func (h *AuthHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	user, err := h.builtinProvider.Register(c.Request.Context(), req.Email, req.Password, req.Name)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, builtin.ErrRegistrationDisabled) || errors.Is(err, builtin.ErrBuiltInAuthDisabled) {
			status = http.StatusForbidden
		}
		if errors.Is(err, builtin.ErrEmailAlreadyExists) {
			status = http.StatusConflict
		}

		code := spec.CodeBadRequest
		if status == http.StatusForbidden {
			code = spec.CodeForbidden
		} else if status == http.StatusConflict {
			code = spec.CodeConflict
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	// Get default team ID
	teamID := ""
	if user.DefaultTeamID != nil {
		teamID = *user.DefaultTeamID
	}
	teamRole, err := h.resolveTeamRole(c.Request.Context(), teamID, user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve team role for register", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamID, teamRole)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to issue tokens")
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresAt:    tokens.ExpiresAt.Unix(),
		User:         NewUserResponse(user),
	})
}

// RefreshRequest is the request body for token refresh
type RefreshRequest struct {
	RefreshToken string `json:"refresh_token" binding:"required"`
}

// RefreshToken refreshes an access token
func (h *AuthHandler) RefreshToken(c *gin.Context) {
	var req RefreshRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	// Validate refresh token
	claims, err := h.jwtIssuer.ValidateRefreshToken(req.RefreshToken)
	if err != nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "invalid refresh token")
		return
	}

	tokenHash := jwt.HashRefreshToken(req.RefreshToken)
	storedToken, err := h.repo.ValidateRefreshToken(c.Request.Context(), tokenHash)
	if err != nil || storedToken.UserID != claims.UserID {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "invalid refresh token")
		return
	}

	// Get user
	user, err := h.repo.GetUserByID(c.Request.Context(), claims.UserID)
	if err != nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "user not found")
		return
	}

	// Get default team ID
	teamID := ""
	if user.DefaultTeamID != nil {
		teamID = *user.DefaultTeamID
	}
	teamRole, err := h.resolveTeamRole(c.Request.Context(), teamID, user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve team role for refresh", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	// Issue new tokens
	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamID, teamRole)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to issue tokens")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresAt:    tokens.ExpiresAt.Unix(),
		User:         NewUserResponse(user),
	})
}

// ChangePasswordRequest is the request body for password change
type ChangePasswordRequest struct {
	OldPassword string `json:"old_password" binding:"required"`
	NewPassword string `json:"new_password" binding:"required,min=8"`
}

// ChangePassword handles password change
func (h *AuthHandler) ChangePassword(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	var req ChangePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	err := h.builtinProvider.ChangePassword(c.Request.Context(), authCtx.UserID, req.OldPassword, req.NewPassword)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, builtin.ErrInvalidCredentials) {
			status = http.StatusUnauthorized
		}
		code := spec.CodeBadRequest
		if status == http.StatusUnauthorized {
			code = spec.CodeUnauthorized
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	// Optionally revoke all refresh tokens
	_ = h.repo.RevokeAllUserRefreshTokens(c.Request.Context(), authCtx.UserID)

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "password changed successfully"})
}

// Logout handles user logout
func (h *AuthHandler) Logout(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "logged out"})
		return
	}

	// Revoke all refresh tokens for the user
	_ = h.repo.RevokeAllUserRefreshTokens(c.Request.Context(), authCtx.UserID)

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "logged out"})
}

func (h *AuthHandler) resolveTeamRole(ctx context.Context, teamID, userID string) (string, error) {
	if teamID == "" {
		return "", nil
	}
	member, err := h.repo.GetTeamMember(ctx, teamID, userID)
	if err != nil {
		return "", err
	}
	return member.Role, nil
}

// OIDCLogin initiates OIDC login
func (h *AuthHandler) OIDCLogin(c *gin.Context) {
	if !h.ssoEnabled {
		spec.JSONError(c, http.StatusForbidden, spec.CodeNotLicensed, fmt.Sprintf("feature %q is not licensed", license.FeatureSSO))
		return
	}
	if h.oidcManager == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, oidc.ErrProviderNotFound.Error())
		return
	}

	providerID := c.Param("provider")
	returnURL := c.Query("return_url")
	if returnURL == "" {
		returnURL = "/"
	}

	authURL, err := h.oidcManager.GenerateAuthURL(providerID, returnURL)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, oidc.ErrProviderNotFound) {
			status = http.StatusNotFound
		}
		code := spec.CodeBadRequest
		if status == http.StatusNotFound {
			code = spec.CodeNotFound
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	c.Redirect(http.StatusFound, authURL)
}

// OIDCCallback handles OIDC callback
func (h *AuthHandler) OIDCCallback(c *gin.Context) {
	if !h.ssoEnabled {
		spec.JSONError(c, http.StatusForbidden, spec.CodeNotLicensed, fmt.Sprintf("feature %q is not licensed", license.FeatureSSO))
		return
	}
	if h.oidcManager == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, oidc.ErrProviderNotFound.Error())
		return
	}

	providerID := c.Param("provider")
	code := c.Query("code")
	state := c.Query("state")

	if code == "" || state == "" {
		errorMsg := c.Query("error")
		if errorMsg == "" {
			errorMsg = "missing code or state"
		}
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, errorMsg)
		return
	}

	user, returnURL, err := h.oidcManager.HandleCallback(c.Request.Context(), providerID, code, state)
	if err != nil {
		h.logger.Warn("OIDC callback failed",
			zap.String("provider", providerID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, err.Error())
		return
	}

	// Get default team ID
	teamID := ""
	if user.DefaultTeamID != nil {
		teamID = *user.DefaultTeamID
	}
	teamRole, err := h.resolveTeamRole(c.Request.Context(), teamID, user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve team role for OIDC login", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamID, teamRole)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to issue tokens")
		return
	}

	if isLocalReturnURL(returnURL) {
		redirectURL, err := buildCLIReturnURL(returnURL, tokens)
		if err != nil {
			h.logger.Warn("Failed to build OIDC CLI redirect URL", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to complete oidc login")
			return
		}
		c.Redirect(http.StatusFound, redirectURL)
		return
	}

	// Return tokens as JSON (frontend should handle redirect)
	spec.JSONSuccess(c, http.StatusOK, LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresAt:    tokens.ExpiresAt.Unix(),
		User:         NewUserResponse(user),
	})
}

// GetAuthProviders returns available auth providers
func (h *AuthHandler) GetAuthProviders(c *gin.Context) {
	providers := make([]gin.H, 0)

	// Add OIDC providers when licensed.
	if h.ssoEnabled && h.oidcManager != nil {
		for _, info := range h.oidcManager.ListProviderInfo() {
			providers = append(providers, gin.H{
				"id":   info.ID,
				"name": info.Name,
				"type": "oidc",
			})
		}
	}

	// Add built-in provider if enabled.
	// Keep it after OIDC providers so server-side OIDC config can be the default login path for CLI.
	if h.builtinProvider != nil && h.builtinProvider.IsEnabled() {
		providers = append(providers, gin.H{
			"id":   "builtin",
			"name": "Email & Password",
			"type": "builtin",
		})
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"providers": providers})
}

func (h *AuthHandler) issueAndPersistTokenPair(ctx context.Context, user *db.User, teamID, teamRole string) (*jwt.TokenPair, error) {
	tokens, err := h.jwtIssuer.IssueTokenPair(
		user.ID,
		teamID,
		teamRole,
		user.Email,
		user.Name,
		user.IsAdmin,
	)
	if err != nil {
		return nil, err
	}
	if err := h.persistRefreshToken(ctx, user.ID, tokens); err != nil {
		return nil, err
	}
	return tokens, nil
}

func (h *AuthHandler) persistRefreshToken(ctx context.Context, userID string, tokens *jwt.TokenPair) error {
	return h.repo.CreateRefreshToken(ctx, &db.RefreshToken{
		UserID:    userID,
		TokenHash: jwt.HashRefreshToken(tokens.RefreshToken),
		ExpiresAt: tokens.RefreshExpiresAt,
	})
}

func isLocalReturnURL(raw string) bool {
	if raw == "" {
		return false
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func buildCLIReturnURL(raw string, tokens *jwt.TokenPair) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	q := u.Query()
	q.Set("access_token", tokens.AccessToken)
	q.Set("refresh_token", tokens.RefreshToken)
	q.Set("expires_unix", fmt.Sprintf("%d", tokens.ExpiresAt.Unix()))
	u.RawQuery = q.Encode()
	return u.String(), nil
}
