package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/apikey"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// APIKeyHandler handles API key endpoints
type APIKeyHandler struct {
	keys     apiKeyStore
	identity *identity.Repository
	logger   *zap.Logger
	regionID string
}

type apiKeyStore interface {
	CreateAPIKey(ctx context.Context, teamID, regionID, userID, name, scope string, roles []string, expiresAt time.Time) (*apikey.APIKey, string, error)
	GetAPIKeysByTeamID(ctx context.Context, teamID string) ([]*apikey.APIKey, error)
	GetAPIKeysByUserID(ctx context.Context, userID string) ([]*apikey.APIKey, error)
	GetAPIKeyByID(ctx context.Context, id string) (*apikey.APIKey, error)
	DeleteAPIKey(ctx context.Context, id string) error
	DeactivateAPIKey(ctx context.Context, id string) error
}

// NewAPIKeyHandler creates a new API key handler
func NewAPIKeyHandler(keys *apikey.Repository, identityRepo *identity.Repository, regionID string, logger *zap.Logger) *APIKeyHandler {
	return &APIKeyHandler{
		keys:     keys,
		identity: identityRepo,
		regionID: strings.TrimSpace(regionID),
		logger:   logger,
	}
}

// ListAPIKeys returns all API keys for the current user's team
func (h *APIKeyHandler) ListAPIKeys(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	// Get keys for the current team or user
	var keys []*apikey.APIKey
	var err error

	if authCtx.TeamID != "" {
		keys, err = h.keys.GetAPIKeysByTeamID(c.Request.Context(), authCtx.TeamID)
	} else {
		keys, err = h.keys.GetAPIKeysByUserID(c.Request.Context(), authCtx.UserID)
	}

	if err != nil {
		h.logger.Error("Failed to get API keys", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get API keys")
		return
	}
	keys = filterVisibleAPIKeys(authCtx, keys)

	spec.JSONSuccess(c, http.StatusOK, gin.H{"api_keys": keys})
}

// CreateAPIKeyRequest is the request body for creating an API key
type CreateAPIKeyRequest struct {
	Name      string   `json:"name" binding:"required"`
	Scope     string   `json:"scope"`
	Roles     []string `json:"roles"`
	ExpiresIn string   `json:"expires_in"` // e.g., "30d", "90d", "365d", "never"
}

// APIKeyResponse is the response after creating an API key
type CreateAPIKeyResponse struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Scope     string    `json:"scope"`
	Roles     []string  `json:"roles"`
	TeamID    string    `json:"team_id"`
	Key       string    `json:"key,omitempty"` // Only returned on creation
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// CurrentAPIKeyResponse is returned when an API key introspects itself.
type CurrentAPIKeyResponse struct {
	ID          string    `json:"id"`
	TeamID      string    `json:"team_id"`
	CreatedBy   string    `json:"created_by"`
	Scope       string    `json:"scope"`
	Roles       []string  `json:"roles"`
	Permissions []string  `json:"permissions"`
	IsActive    bool      `json:"is_active"`
	ExpiresAt   time.Time `json:"expires_at"`
}

// GetCurrentAPIKey introspects the currently authenticated API key.
func (h *APIKeyHandler) GetCurrentAPIKey(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.APIKeyID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "api key authentication required")
		return
	}

	key, err := h.keys.GetAPIKeyByID(c.Request.Context(), authCtx.APIKeyID)
	if err != nil {
		if errors.Is(err, apikey.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "API key not found")
			return
		}
		h.logger.Error("Failed to introspect current API key", zap.Error(err), zap.String("api_key_id", authCtx.APIKeyID))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to introspect API key")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{
		"api_key": &CurrentAPIKeyResponse{
			ID:          key.ID,
			TeamID:      key.TeamID,
			CreatedBy:   key.CreatedBy,
			Scope:       key.Scope,
			Roles:       key.Roles,
			Permissions: append([]string(nil), authCtx.Permissions...),
			IsActive:    key.IsActive,
			ExpiresAt:   key.ExpiresAt,
		},
	})
}

// CreateAPIKey creates a new API key
func (h *APIKeyHandler) CreateAPIKey(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	if authCtx.TeamID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "no team selected")
		return
	}

	var req CreateAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	// Parse expiration
	expiresAt := time.Now().AddDate(0, 0, 90) // Default 90 days
	switch req.ExpiresIn {
	case "30d":
		expiresAt = time.Now().AddDate(0, 0, 30)
	case "90d":
		expiresAt = time.Now().AddDate(0, 0, 90)
	case "180d":
		expiresAt = time.Now().AddDate(0, 0, 180)
	case "365d":
		expiresAt = time.Now().AddDate(1, 0, 0)
	case "never":
		expiresAt = time.Now().AddDate(100, 0, 0) // ~100 years
	}

	scope, err := normalizeCreateAPIKeyScope(req.Scope)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
		return
	}

	var roles []string
	if scope == apikey.ScopePlatform {
		if !canManagePlatformAPIKeys(authCtx) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "platform API keys require system admin user access")
			return
		}
		if len(req.Roles) > 0 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "platform API keys do not support roles")
			return
		}
		roles = []string{}
	} else {
		// Default roles if not provided
		roles, err = normalizeCreateAPIKeyRoles(req.Roles)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		if !canGrantAPIKeyRoles(authCtx, roles) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "cannot create API key with roles that grant permissions outside the caller's permissions")
			return
		}
	}

	regionID := h.regionID
	if regionID == "" {
		team, err := h.identity.GetTeamByID(c.Request.Context(), authCtx.TeamID)
		if err != nil {
			h.logger.Error("Failed to get team for API key creation", zap.Error(err), zap.String("team_id", authCtx.TeamID))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to resolve team home region")
			return
		}
		if team.HomeRegionID == nil || strings.TrimSpace(*team.HomeRegionID) == "" {
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "team home region is not configured")
			return
		}
		regionID = strings.TrimSpace(*team.HomeRegionID)
	}

	// Create API key
	key, keyValue, err := h.keys.CreateAPIKey(
		c.Request.Context(),
		authCtx.TeamID,
		regionID,
		authCtx.UserID,
		req.Name,
		scope,
		roles,
		expiresAt,
	)
	if err != nil {
		h.logger.Error("Failed to create API key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create API key")
		return
	}

	// Return with the actual key value (only shown once)
	response := &CreateAPIKeyResponse{
		ID:        key.ID,
		Name:      key.Name,
		Scope:     key.Scope,
		Roles:     key.Roles,
		TeamID:    key.TeamID,
		Key:       keyValue, // Full key, only shown at creation
		ExpiresAt: key.ExpiresAt,
		CreatedAt: key.CreatedAt,
	}

	spec.JSONSuccess(c, http.StatusCreated, response)
}

// DeleteAPIKey deletes an API key
func (h *APIKeyHandler) DeleteAPIKey(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	keyID := c.Param("id")

	// Get the key to verify ownership
	key, err := h.keys.GetAPIKeyByID(c.Request.Context(), keyID)
	if err != nil {
		if errors.Is(err, apikey.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "API key not found")
			return
		}
		h.logger.Error("Failed to get API key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get API key")
		return
	}

	if key.Scope == apikey.ScopePlatform && !canManagePlatformAPIKeys(authCtx) {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not authorized to delete this API key")
		return
	}

	// Verify the key belongs to the user's team
	if key.TeamID != authCtx.TeamID {
		// Check if user is member of the key's team
		_, err := h.identity.GetTeamMember(c.Request.Context(), key.TeamID, authCtx.UserID)
		if err != nil {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not authorized to delete this API key")
			return
		}
	}

	if err := h.keys.DeleteAPIKey(c.Request.Context(), keyID); err != nil {
		h.logger.Error("Failed to delete API key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete API key")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "API key deleted"})
}

// DeactivateAPIKey deactivates an API key without deleting it
func (h *APIKeyHandler) DeactivateAPIKey(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	keyID := c.Param("id")

	// Get the key to verify ownership
	key, err := h.keys.GetAPIKeyByID(c.Request.Context(), keyID)
	if err != nil {
		if errors.Is(err, apikey.ErrNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "API key not found")
			return
		}
		h.logger.Error("Failed to get API key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get API key")
		return
	}

	if key.Scope == apikey.ScopePlatform && !canManagePlatformAPIKeys(authCtx) {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not authorized to deactivate this API key")
		return
	}

	// Verify the key belongs to the user's team
	if key.TeamID != authCtx.TeamID {
		_, err := h.identity.GetTeamMember(c.Request.Context(), key.TeamID, authCtx.UserID)
		if err != nil {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not authorized to deactivate this API key")
			return
		}
	}

	if err := h.keys.DeactivateAPIKey(c.Request.Context(), keyID); err != nil {
		h.logger.Error("Failed to deactivate API key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to deactivate API key")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "API key deactivated"})
}

func normalizeCreateAPIKeyScope(scope string) (string, error) {
	normalized, ok := apikey.NormalizeScope(scope)
	if !ok {
		return "", errors.New("scope must be team or platform")
	}
	return normalized, nil
}

func normalizeCreateAPIKeyRoles(roles []string) ([]string, error) {
	if len(roles) == 0 {
		return []string{"developer"}, nil
	}

	normalized := make([]string, 0, len(roles))
	seen := make(map[string]struct{}, len(roles))
	for _, role := range roles {
		role = strings.TrimSpace(role)
		if role == "" {
			return nil, errors.New("role cannot be empty")
		}
		if _, ok := authn.RolePermissions[role]; !ok {
			return nil, errors.New("unsupported role: " + role)
		}
		if _, ok := seen[role]; ok {
			continue
		}
		seen[role] = struct{}{}
		normalized = append(normalized, role)
	}
	if len(normalized) == 0 {
		return nil, errors.New("at least one role is required")
	}
	return normalized, nil
}

func canGrantAPIKeyRoles(authCtx *authn.AuthContext, roles []string) bool {
	if authCtx == nil {
		return false
	}

	callerPermissions := append([]string(nil), authCtx.Permissions...)
	if len(callerPermissions) == 0 && authCtx.TeamRole != "" {
		callerPermissions = authn.ExpandRolePermissions(authCtx.TeamRole)
	}
	if hasWildcardPermission(callerPermissions) {
		return true
	}

	caller := &authn.AuthContext{Permissions: callerPermissions}
	for _, permission := range authn.ExpandRolesPermissions(roles) {
		if !caller.HasPermission(permission) {
			return false
		}
	}
	return true
}

func canManagePlatformAPIKeys(authCtx *authn.AuthContext) bool {
	return authCtx != nil && authCtx.AuthMethod == authn.AuthMethodJWT && authCtx.IsSystemAdmin
}

func filterVisibleAPIKeys(authCtx *authn.AuthContext, keys []*apikey.APIKey) []*apikey.APIKey {
	if len(keys) == 0 {
		return keys
	}
	filtered := make([]*apikey.APIKey, 0, len(keys))
	for _, key := range keys {
		if key == nil {
			continue
		}
		if key.Scope == apikey.ScopePlatform && !canManagePlatformAPIKeys(authCtx) {
			continue
		}
		filtered = append(filtered, key)
	}
	return filtered
}

func hasWildcardPermission(permissions []string) bool {
	for _, permission := range permissions {
		if permission == "*" || permission == "*:*" {
			return true
		}
	}
	return false
}
