package handlers

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/db"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"go.uber.org/zap"
)

// UserResponse is the API response for a user (without sensitive fields)
type UserResponse struct {
	ID            string    `json:"id"`
	Email         string    `json:"email"`
	Name          string    `json:"name"`
	AvatarURL     string    `json:"avatar_url,omitempty"`
	DefaultTeamID *string   `json:"default_team_id,omitempty"`
	EmailVerified bool      `json:"email_verified"`
	IsAdmin       bool      `json:"is_admin"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// NewUserResponse creates a UserResponse from a db.User
func NewUserResponse(u *db.User) *UserResponse {
	if u == nil {
		return nil
	}
	return &UserResponse{
		ID:            u.ID,
		Email:         u.Email,
		Name:          u.Name,
		AvatarURL:     u.AvatarURL,
		DefaultTeamID: u.DefaultTeamID,
		EmailVerified: u.EmailVerified,
		IsAdmin:       u.IsAdmin,
		CreatedAt:     u.CreatedAt,
		UpdatedAt:     u.UpdatedAt,
	}
}

// UserHandler handles user endpoints
type UserHandler struct {
	repo   *db.Repository
	logger *zap.Logger
}

// NewUserHandler creates a new user handler
func NewUserHandler(repo *db.Repository, logger *zap.Logger) *UserHandler {
	return &UserHandler{
		repo:   repo,
		logger: logger,
	}
}

// GetCurrentUser returns the current authenticated user
func (h *UserHandler) GetCurrentUser(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	user, err := h.repo.GetUserByID(c.Request.Context(), authCtx.UserID)
	if err != nil {
		h.logger.Error("Failed to get user", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get user")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, NewUserResponse(user))
}

// UpdateUserRequest is the request body for updating user
type UpdateUserRequest struct {
	Name          string  `json:"name"`
	AvatarURL     string  `json:"avatar_url"`
	DefaultTeamID *string `json:"default_team_id"`
}

// UpdateCurrentUser updates the current authenticated user
func (h *UserHandler) UpdateCurrentUser(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	var req UpdateUserRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	user, err := h.repo.GetUserByID(c.Request.Context(), authCtx.UserID)
	if err != nil {
		h.logger.Error("Failed to get user", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get user")
		return
	}

	// Update fields
	if req.Name != "" {
		user.Name = req.Name
	}
	if req.AvatarURL != "" {
		user.AvatarURL = req.AvatarURL
	}
	if req.DefaultTeamID != nil {
		// Verify user is member of the team
		_, err := h.repo.GetTeamMember(c.Request.Context(), *req.DefaultTeamID, user.ID)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "you are not a member of this team")
			return
		}
		user.DefaultTeamID = req.DefaultTeamID
	}

	if err := h.repo.UpdateUser(c.Request.Context(), user); err != nil {
		h.logger.Error("Failed to update user", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update user")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, NewUserResponse(user))
}

// GetUserIdentities returns the OIDC identities for the current user
func (h *UserHandler) GetUserIdentities(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	identities, err := h.repo.GetUserIdentitiesByUserID(c.Request.Context(), authCtx.UserID)
	if err != nil {
		h.logger.Error("Failed to get identities", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get identities")
		return
	}

	// Remove raw claims for privacy
	type IdentityResponse struct {
		ID        string `json:"id"`
		Provider  string `json:"provider"`
		CreatedAt int64  `json:"created_at"`
	}

	response := make([]IdentityResponse, len(identities))
	for i, id := range identities {
		response[i] = IdentityResponse{
			ID:        id.ID,
			Provider:  id.Provider,
			CreatedAt: id.CreatedAt.Unix(),
		}
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"identities": response})
}

// DeleteUserIdentity removes an OIDC identity from the current user
func (h *UserHandler) DeleteUserIdentity(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	identityID := c.Param("id")

	// Verify the identity belongs to this user
	identities, err := h.repo.GetUserIdentitiesByUserID(c.Request.Context(), authCtx.UserID)
	if err != nil {
		h.logger.Error("Failed to get identities", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get identities")
		return
	}

	found := false
	for _, id := range identities {
		if id.ID == identityID {
			found = true
			break
		}
	}

	if !found {
		spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "identity not found")
		return
	}

	// Ensure user has at least one auth method (password or identity)
	user, err := h.repo.GetUserByID(c.Request.Context(), authCtx.UserID)
	if err != nil {
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get user")
		return
	}

	if user.PasswordHash == "" && len(identities) <= 1 {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cannot remove last authentication method")
		return
	}

	if err := h.repo.DeleteUserIdentity(c.Request.Context(), identityID); err != nil {
		h.logger.Error("Failed to delete identity", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete identity")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "identity removed"})
}
