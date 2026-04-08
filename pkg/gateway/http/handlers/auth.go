package handlers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/builtin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/auth/oidc"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

// AuthHandler handles authentication endpoints
type AuthHandler struct {
	repo                      authRepository
	builtinProvider           *builtin.Provider
	oidcManager               *oidc.Manager
	jwtIssuer                 *authn.Issuer
	regionLookup              TeamRegionLookup
	requireHomeRegionOnCreate bool
	logger                    *zap.Logger
}

// AuthHandlerOption configures AuthHandler behavior.
type AuthHandlerOption func(*AuthHandler)

// WithCreateHomeRegionRequiredForAuth requires explicit home_region_id for auth-driven team creation flows.
func WithCreateHomeRegionRequiredForAuth(regionLookup TeamRegionLookup) AuthHandlerOption {
	return func(h *AuthHandler) {
		h.requireHomeRegionOnCreate = true
		h.regionLookup = regionLookup
	}
}

type authRepository interface {
	CreateRefreshToken(ctx context.Context, token *identity.RefreshToken) error
	ValidateRefreshToken(ctx context.Context, tokenHash string) (*identity.RefreshToken, error)
	RevokeAllUserRefreshTokens(ctx context.Context, userID string) error
	GetUserByID(ctx context.Context, id string) (*identity.User, error)
	ListTeamGrantsByUserID(ctx context.Context, userID string) ([]identity.TeamGrantRecord, error)
	CreateDeviceAuthSession(ctx context.Context, session *identity.DeviceAuthSession) error
	GetDeviceAuthSessionByID(ctx context.Context, id string) (*identity.DeviceAuthSession, error)
	MarkDeviceAuthSessionConsumed(ctx context.Context, id string) error
}

// NewAuthHandler creates a new auth handler
func NewAuthHandler(
	repo *identity.Repository,
	builtinProvider *builtin.Provider,
	oidcManager *oidc.Manager,
	jwtIssuer *authn.Issuer,
	logger *zap.Logger,
	opts ...AuthHandlerOption,
) *AuthHandler {
	handler := &AuthHandler{
		repo:            repo,
		builtinProvider: builtinProvider,
		oidcManager:     oidcManager,
		jwtIssuer:       jwtIssuer,
		logger:          logger,
	}
	for _, opt := range opts {
		opt(handler)
	}
	return handler
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

// DeviceLoginStartResponse is returned when a device flow is initiated.
type DeviceLoginStartResponse struct {
	DeviceLoginID           string `json:"device_login_id"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete,omitempty"`
	ExpiresAt               int64  `json:"expires_at"`
	IntervalSeconds         int    `json:"interval_seconds"`
}

// DeviceLoginPollRequest identifies the pending device login.
type DeviceLoginPollRequest struct {
	DeviceLoginID string `json:"device_login_id" binding:"required"`
}

// DeviceLoginPollResponse returns pending or completed device login state.
type DeviceLoginPollResponse struct {
	Status          string         `json:"status"`
	IntervalSeconds int            `json:"interval_seconds,omitempty"`
	ExpiresAt       int64          `json:"expires_at,omitempty"`
	Login           *LoginResponse `json:"login,omitempty"`
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

	teamGrants, err := h.buildTeamGrants(c.Request.Context(), user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve login team context", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamGrants)
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
	Email        string  `json:"email" binding:"required,email"`
	Password     string  `json:"password" binding:"required,min=8"`
	Name         string  `json:"name" binding:"required"`
	HomeRegionID *string `json:"home_region_id"`
}

// Register handles user registration
func (h *AuthHandler) Register(c *gin.Context) {
	var req RegisterRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	homeRegionID := normalizeOptionalString(req.HomeRegionID)
	if err := validateNormalizedHomeRegionID(homeRegionID); err != nil {
		status, code, message := resolveHomeRegionValidationError(err)
		spec.JSONError(c, status, code, message)
		return
	}
	if h.requireHomeRegionOnCreate {
		if err := validateRequiredRoutableHomeRegion(c.Request.Context(), h.regionLookup, homeRegionID); err != nil {
			status, code, message := resolveHomeRegionValidationError(err)
			if status == http.StatusInternalServerError {
				h.logger.Error("Failed to resolve home region", zap.Error(err))
			}
			spec.JSONError(c, status, code, message)
			return
		}
	}

	user, err := h.builtinProvider.Register(c.Request.Context(), req.Email, req.Password, req.Name, homeRegionID)
	if err != nil {
		status := http.StatusBadRequest
		if errors.Is(err, builtin.ErrRegistrationDisabled) || errors.Is(err, builtin.ErrBuiltInAuthDisabled) {
			status = http.StatusForbidden
		}
		if errors.Is(err, builtin.ErrEmailAlreadyExists) {
			status = http.StatusConflict
		}

		code := spec.CodeBadRequest
		switch status {
		case http.StatusForbidden:
			code = spec.CodeForbidden
		case http.StatusConflict:
			code = spec.CodeConflict
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	teamGrants, err := h.buildTeamGrants(c.Request.Context(), user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve register team context", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamGrants)
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

	tokenHash := authn.HashRefreshToken(req.RefreshToken)
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

	teamGrants, err := h.buildTeamGrants(c.Request.Context(), user.ID)
	if err != nil {
		h.logger.Warn("Failed to resolve refresh team context", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "user is not a member of the selected team")
		return
	}

	// Issue new tokens
	tokens, err := h.issueAndPersistTokenPair(c.Request.Context(), user, teamGrants)
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

func (h *AuthHandler) buildTeamGrants(ctx context.Context, userID string) ([]authn.TeamGrant, error) {
	records, err := h.repo.ListTeamGrantsByUserID(ctx, userID)
	if err != nil {
		return nil, err
	}
	grants := make([]authn.TeamGrant, 0, len(records))
	for _, record := range records {
		grant := authn.TeamGrant{
			TeamID:   record.TeamID,
			TeamRole: record.TeamRole,
		}
		if record.HomeRegionID != nil {
			grant.HomeRegionID = strings.TrimSpace(*record.HomeRegionID)
		}
		grants = append(grants, grant)
	}
	sort.Slice(grants, func(i, j int) bool {
		return grants[i].TeamID < grants[j].TeamID
	})
	return grants, nil
}

// OIDCLogin initiates OIDC login
func (h *AuthHandler) OIDCLogin(c *gin.Context) {
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
		status := http.StatusUnauthorized
		apiCode := spec.CodeUnauthorized
		switch {
		case errors.Is(err, tenantdir.ErrRegionNotFound), errors.Is(err, oidc.ErrHomeRegionNotRoutable):
			status = http.StatusBadRequest
			apiCode = spec.CodeBadRequest
		}
		spec.JSONError(c, status, apiCode, err.Error())
		return
	}

	loginResponse, tokens, err := h.buildLoginResponse(c.Request.Context(), user)
	if err != nil {
		h.logger.Warn("Failed to complete OIDC login", zap.Error(err))
		status := http.StatusInternalServerError
		code := spec.CodeInternal
		if errors.Is(err, errUserNotMemberOfSelectedTeam) {
			status = http.StatusForbidden
			code = spec.CodeForbidden
		}
		spec.JSONError(c, status, code, err.Error())
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
	spec.JSONSuccess(c, http.StatusOK, loginResponse)
}

// GetAuthProviders returns available auth providers
func (h *AuthHandler) GetAuthProviders(c *gin.Context) {
	providers := make([]gin.H, 0)

	// Add OIDC providers when available.
	if h.oidcManager != nil {
		for _, info := range h.oidcManager.ListProviderInfo() {
			entry := gin.H{
				"id":                    info.ID,
				"name":                  info.Name,
				"type":                  "oidc",
				"browser_login_enabled": info.BrowserLoginEnabled,
				"device_login_enabled":  info.DeviceLoginEnabled,
			}
			if info.ExternalAuthPortalURL != "" {
				entry["external_auth_portal_url"] = info.ExternalAuthPortalURL
			}
			providers = append(providers, entry)
		}
	}

	// Add built-in provider if enabled.
	// Keep it after OIDC providers so server-side OIDC config can be the default login path for CLI.
	if h.builtinProvider != nil && h.builtinProvider.IsEnabled() {
		providers = append(providers, gin.H{
			"id":                    "builtin",
			"name":                  "Email & Password",
			"type":                  "builtin",
			"browser_login_enabled": false,
			"device_login_enabled":  false,
		})
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"providers": providers})
}

// OIDCDeviceStart initiates a hosted device login flow for the selected OIDC provider.
func (h *AuthHandler) OIDCDeviceStart(c *gin.Context) {
	if h.oidcManager == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, oidc.ErrProviderNotFound.Error())
		return
	}

	providerID := c.Param("provider")
	deviceAuth, err := h.oidcManager.StartDeviceAuthorization(c.Request.Context(), providerID)
	if err != nil {
		status := http.StatusBadRequest
		code := spec.CodeBadRequest
		switch {
		case errors.Is(err, oidc.ErrProviderNotFound):
			status = http.StatusNotFound
			code = spec.CodeNotFound
		case errors.Is(err, oidc.ErrDeviceFlowNotSupported):
			status = http.StatusNotFound
			code = spec.CodeNotFound
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	session := &identity.DeviceAuthSession{
		Provider:                providerID,
		DeviceCode:              deviceAuth.DeviceCode,
		UserCode:                deviceAuth.UserCode,
		VerificationURI:         deviceAuth.VerificationURI,
		VerificationURIComplete: deviceAuth.VerificationURIComplete,
		IntervalSeconds:         deviceAuth.Interval,
		ExpiresAt:               time.Now().Add(time.Duration(deviceAuth.ExpiresIn) * time.Second),
	}
	if err := h.repo.CreateDeviceAuthSession(c.Request.Context(), session); err != nil {
		h.logger.Error("Failed to persist device auth session", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to start device login")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, DeviceLoginStartResponse{
		DeviceLoginID:           session.ID,
		UserCode:                session.UserCode,
		VerificationURI:         session.VerificationURI,
		VerificationURIComplete: session.VerificationURIComplete,
		ExpiresAt:               session.ExpiresAt.Unix(),
		IntervalSeconds:         session.IntervalSeconds,
	})
}

// OIDCDevicePoll polls a pending device login session until it completes.
func (h *AuthHandler) OIDCDevicePoll(c *gin.Context) {
	if h.oidcManager == nil {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, oidc.ErrProviderNotFound.Error())
		return
	}

	var req DeviceLoginPollRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	session, err := h.repo.GetDeviceAuthSessionByID(c.Request.Context(), req.DeviceLoginID)
	if err != nil {
		status := http.StatusUnauthorized
		code := spec.CodeUnauthorized
		switch {
		case errors.Is(err, identity.ErrDeviceAuthSessionNotFound):
			status = http.StatusNotFound
			code = spec.CodeNotFound
		case errors.Is(err, identity.ErrDeviceAuthSessionExpired), errors.Is(err, identity.ErrDeviceAuthSessionConsumed):
			status = http.StatusUnauthorized
			code = spec.CodeUnauthorized
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}
	if session.Provider != c.Param("provider") {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "device login does not match provider")
		return
	}

	user, err := h.oidcManager.PollDeviceAuthorization(c.Request.Context(), session.Provider, session.DeviceCode)
	if err != nil {
		switch {
		case errors.Is(err, oidc.ErrDeviceAuthorizationPending):
			spec.JSONSuccess(c, http.StatusOK, DeviceLoginPollResponse{
				Status:          "pending",
				IntervalSeconds: session.IntervalSeconds,
				ExpiresAt:       session.ExpiresAt.Unix(),
			})
			return
		case errors.Is(err, oidc.ErrDeviceAuthorizationSlowDown):
			spec.JSONSuccess(c, http.StatusOK, DeviceLoginPollResponse{
				Status:          "slow_down",
				IntervalSeconds: session.IntervalSeconds + 5,
				ExpiresAt:       session.ExpiresAt.Unix(),
			})
			return
		case errors.Is(err, oidc.ErrDeviceAuthorizationDeclined), errors.Is(err, oidc.ErrDeviceAuthorizationExpired):
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, err.Error())
			return
		default:
			h.logger.Warn("Device auth poll failed", zap.Error(err))
			spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, err.Error())
			return
		}
	}

	if err := h.repo.MarkDeviceAuthSessionConsumed(c.Request.Context(), session.ID); err != nil {
		h.logger.Warn("Failed to consume device auth session", zap.Error(err))
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, err.Error())
		return
	}

	loginResponse, _, err := h.buildLoginResponse(c.Request.Context(), user)
	if err != nil {
		h.logger.Warn("Failed to issue device login tokens", zap.Error(err))
		status := http.StatusInternalServerError
		code := spec.CodeInternal
		if errors.Is(err, errUserNotMemberOfSelectedTeam) {
			status = http.StatusForbidden
			code = spec.CodeForbidden
		}
		spec.JSONError(c, status, code, err.Error())
		return
	}

	spec.JSONSuccess(c, http.StatusOK, DeviceLoginPollResponse{
		Status: "completed",
		Login:  loginResponse,
	})
}

var errUserNotMemberOfSelectedTeam = errors.New("user is not a member of the selected team")

func (h *AuthHandler) buildLoginResponse(ctx context.Context, user *identity.User) (*LoginResponse, *authn.TokenPair, error) {
	teamGrants, err := h.buildTeamGrants(ctx, user.ID)
	if err != nil {
		return nil, nil, errUserNotMemberOfSelectedTeam
	}

	tokens, err := h.issueAndPersistTokenPair(ctx, user, teamGrants)
	if err != nil {
		return nil, nil, fmt.Errorf("issue tokens: %w", err)
	}
	return &LoginResponse{
		AccessToken:  tokens.AccessToken,
		RefreshToken: tokens.RefreshToken,
		ExpiresAt:    tokens.ExpiresAt.Unix(),
		User:         NewUserResponse(user),
	}, tokens, nil
}

func (h *AuthHandler) issueAndPersistTokenPair(ctx context.Context, user *identity.User, teamGrants []authn.TeamGrant) (*authn.TokenPair, error) {
	tokens, err := h.jwtIssuer.IssueTokenPair(
		user.ID,
		user.Email,
		user.Name,
		user.IsAdmin,
		teamGrants,
	)
	if err != nil {
		return nil, err
	}
	if err := h.persistRefreshToken(ctx, user.ID, tokens); err != nil {
		return nil, err
	}
	return tokens, nil
}

func (h *AuthHandler) persistRefreshToken(ctx context.Context, userID string, tokens *authn.TokenPair) error {
	return h.repo.CreateRefreshToken(ctx, &identity.RefreshToken{
		UserID:    userID,
		TokenHash: authn.HashRefreshToken(tokens.RefreshToken),
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

func buildCLIReturnURL(raw string, tokens *authn.TokenPair) (string, error) {
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
