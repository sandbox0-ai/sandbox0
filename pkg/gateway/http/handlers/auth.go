package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/jwt"
	"github.com/sandbox0-ai/infra/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/infra/pkg/gateway/db"
	"github.com/sandbox0-ai/infra/pkg/gateway/middleware"
	"go.uber.org/zap"
)

// AuthHandler handles authentication endpoints
type AuthHandler struct {
	repo            *db.Repository
	builtinProvider *builtin.Provider
	oidcManager     *oidc.Manager
	jwtIssuer       *jwt.Issuer
	logger          *zap.Logger
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(
	repo *db.Repository,
	builtinProvider *builtin.Provider,
	oidcManager *oidc.Manager,
	jwtIssuer *jwt.Issuer,
	logger *zap.Logger,
) *AuthHandler {
	return &AuthHandler{
		repo:            repo,
		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
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

		c.JSON(status, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusForbidden, gin.H{"error": "user is not a member of the selected team"})
		return
	}

	// Issue tokens
	tokens, err := h.jwtIssuer.IssueTokenPair(
		user.ID,
		teamID,
		teamRole,
		user.Email,
		user.Name,
		user.IsAdmin,
	)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to issue tokens"})
		return
	}

	c.JSON(http.StatusOK, LoginResponse{
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
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

		c.JSON(status, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusForbidden, gin.H{"error": "user is not a member of the selected team"})
		return
	}

	// Issue tokens
	tokens, err := h.jwtIssuer.IssueTokenPair(
		user.ID,
		teamID,
		teamRole,
		user.Email,
		user.Name,
		user.IsAdmin,
	)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to issue tokens"})
		return
	}

	c.JSON(http.StatusCreated, LoginResponse{
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
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	// Validate refresh token
	claims, err := h.jwtIssuer.ValidateRefreshToken(req.RefreshToken)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "invalid refresh token"})
		return
	}

	// Get user
	user, err := h.repo.GetUserByID(c.Request.Context(), claims.UserID)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "user not found"})
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
		c.JSON(http.StatusForbidden, gin.H{"error": "user is not a member of the selected team"})
		return
	}

	// Issue new tokens
	tokens, err := h.jwtIssuer.IssueTokenPair(
		user.ID,
		teamID,
		teamRole,
		user.Email,
		user.Name,
		user.IsAdmin,
	)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to issue tokens"})
		return
	}

	c.JSON(http.StatusOK, LoginResponse{
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
		c.JSON(http.StatusUnauthorized, gin.H{"error": "not authenticated"})
		return
	}

	var req ChangePasswordRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request body"})
		return
	}

	err := h.builtinProvider.ChangePassword(c.Request.Context(), authCtx.UserID, req.OldPassword, req.NewPassword)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, builtin.ErrInvalidCredentials) {
			status = http.StatusUnauthorized
		}
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}

	// Optionally revoke all refresh tokens
	_ = h.repo.RevokeAllUserRefreshTokens(c.Request.Context(), authCtx.UserID)

	c.JSON(http.StatusOK, gin.H{"message": "password changed successfully"})
}

// Logout handles user logout
func (h *AuthHandler) Logout(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil {
		c.JSON(http.StatusOK, gin.H{"message": "logged out"})
		return
	}

	// Revoke all refresh tokens for the user
	_ = h.repo.RevokeAllUserRefreshTokens(c.Request.Context(), authCtx.UserID)

	c.JSON(http.StatusOK, gin.H{"message": "logged out"})
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
		c.JSON(status, gin.H{"error": err.Error()})
		return
	}

	c.Redirect(http.StatusFound, authURL)
}

// OIDCCallback handles OIDC callback
func (h *AuthHandler) OIDCCallback(c *gin.Context) {
	providerID := c.Param("provider")
	code := c.Query("code")
	state := c.Query("state")

	if code == "" || state == "" {
		errorMsg := c.Query("error")
		if errorMsg == "" {
			errorMsg = "missing code or state"
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": errorMsg})
		return
	}

	user, err := h.oidcManager.HandleCallback(c.Request.Context(), providerID, code, state)
	if err != nil {
		h.logger.Warn("OIDC callback failed",
			zap.String("provider", providerID),
			zap.Error(err),
		)
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
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
		c.JSON(http.StatusForbidden, gin.H{"error": "user is not a member of the selected team"})
		return
	}

	// Issue tokens
	tokens, err := h.jwtIssuer.IssueTokenPair(
		user.ID,
		teamID,
		teamRole,
		user.Email,
		user.Name,
		user.IsAdmin,
	)
	if err != nil {
		h.logger.Error("Failed to issue tokens", zap.Error(err))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to issue tokens"})
		return
	}

	// Return tokens as JSON (frontend should handle redirect)
	c.JSON(http.StatusOK, LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresAt:    tokens.ExpiresAt.Unix(),
		User:         NewUserResponse(user),
	})
}

// GetAuthProviders returns available auth providers
func (h *AuthHandler) GetAuthProviders(c *gin.Context) {
	providers := make([]gin.H, 0)

	// Add built-in provider if enabled
	if h.builtinProvider.IsEnabled() {
		providers = append(providers, gin.H{
			"id":   "builtin",
			"name": "Email & Password",
			"type": "builtin",
		})
	}

	// Add OIDC providers
	for _, info := range h.oidcManager.ListProviderInfo() {
		providers = append(providers, gin.H{
			"id":   info.ID,
			"name": info.Name,
			"type": "oidc",
		})
	}

	c.JSON(http.StatusOK, gin.H{"providers": providers})
}
