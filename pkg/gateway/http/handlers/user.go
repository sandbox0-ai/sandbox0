package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
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
	EmailVerified bool      `json:"email_verified"`
	IsAdmin       bool      `json:"is_admin"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// NewUserResponse creates a UserResponse from an identity.User.
func NewUserResponse(u *identity.User) *UserResponse {
	if u == nil {
		return nil
	}
	return &UserResponse{
		ID:            u.ID,
		Email:         u.Email,
		Name:          u.Name,
		AvatarURL:     u.AvatarURL,
		EmailVerified: u.EmailVerified,
		IsAdmin:       u.IsAdmin,
		CreatedAt:     u.CreatedAt,
		UpdatedAt:     u.UpdatedAt,
	}
}

type userRepository interface {
	GetUserByID(ctx context.Context, id string) (*identity.User, error)
	UpdateUser(ctx context.Context, user *identity.User) error
	GetUserIdentitiesByUserID(ctx context.Context, userID string) ([]*identity.UserIdentity, error)
	DeleteUserIdentity(ctx context.Context, id string) error
	CreateUserSSHPublicKey(ctx context.Context, key *identity.UserSSHPublicKey) error
	ListUserSSHPublicKeysByTeamAndUserID(ctx context.Context, teamID, userID string) ([]*identity.UserSSHPublicKey, error)
	DeleteUserSSHPublicKeyByTeamAndUserID(ctx context.Context, teamID, userID, keyID string) error
}

// UserHandler handles user endpoints
type UserHandler struct {
	repo   userRepository
	logger *zap.Logger
}

// NewUserHandler creates a new user handler
func NewUserHandler(repo userRepository, logger *zap.Logger) *UserHandler {
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
	Name      string `json:"name"`
	AvatarURL string `json:"avatar_url"`
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

// SSHPublicKeyResponse is the API response for one user-managed SSH key.
type SSHPublicKeyResponse struct {
	ID                string    `json:"id"`
	Name              string    `json:"name"`
	PublicKey         string    `json:"public_key"`
	KeyType           string    `json:"key_type"`
	FingerprintSHA256 string    `json:"fingerprint_sha256"`
	Comment           string    `json:"comment,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// CreateSSHPublicKeyRequest is the request body for uploading one SSH public key.
type CreateSSHPublicKeyRequest struct {
	Name      string `json:"name" binding:"required"`
	PublicKey string `json:"public_key" binding:"required"`
}

func newSSHPublicKeyResponse(key *identity.UserSSHPublicKey) *SSHPublicKeyResponse {
	if key == nil {
		return nil
	}
	return &SSHPublicKeyResponse{
		ID:                key.ID,
		Name:              key.Name,
		PublicKey:         key.PublicKey,
		KeyType:           key.KeyType,
		FingerprintSHA256: key.FingerprintSHA256,
		Comment:           key.Comment,
		CreatedAt:         key.CreatedAt,
		UpdatedAt:         key.UpdatedAt,
	}
}

// ListUserSSHPublicKeys returns the current user's uploaded SSH public keys.
func (h *UserHandler) ListUserSSHPublicKeys(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}
	if strings.TrimSpace(authCtx.TeamID) == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "no team selected")
		return
	}

	keys, err := h.repo.ListUserSSHPublicKeysByTeamAndUserID(c.Request.Context(), authCtx.TeamID, authCtx.UserID)
	if err != nil {
		h.logger.Error("Failed to list ssh public keys", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to list ssh public keys")
		return
	}

	response := make([]SSHPublicKeyResponse, 0, len(keys))
	for _, key := range keys {
		response = append(response, *newSSHPublicKeyResponse(key))
	}
	spec.JSONSuccess(c, http.StatusOK, gin.H{"ssh_keys": response})
}

// CreateUserSSHPublicKey stores one SSH public key for the current user.
func (h *UserHandler) CreateUserSSHPublicKey(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}
	if strings.TrimSpace(authCtx.TeamID) == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "no team selected")
		return
	}

	var req CreateSSHPublicKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	name := strings.TrimSpace(req.Name)
	if name == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "ssh public key name is required")
		return
	}

	publicKey, keyType, fingerprint, comment, err := identity.NormalizeAuthorizedSSHPublicKey(req.PublicKey)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid ssh public key")
		return
	}

	key := &identity.UserSSHPublicKey{
		TeamID:            authCtx.TeamID,
		UserID:            authCtx.UserID,
		Name:              name,
		PublicKey:         publicKey,
		KeyType:           keyType,
		FingerprintSHA256: fingerprint,
		Comment:           comment,
	}
	if err := h.repo.CreateUserSSHPublicKey(c.Request.Context(), key); err != nil {
		if errors.Is(err, identity.ErrSSHPublicKeyAlreadyExists) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "ssh public key already exists")
			return
		}
		h.logger.Error("Failed to create ssh public key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create ssh public key")
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, newSSHPublicKeyResponse(key))
}

// DeleteUserSSHPublicKey deletes one SSH public key from the current user.
func (h *UserHandler) DeleteUserSSHPublicKey(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}
	if strings.TrimSpace(authCtx.TeamID) == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "no team selected")
		return
	}

	keyID := strings.TrimSpace(c.Param("id"))
	if keyID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "ssh public key id is required")
		return
	}

	if err := h.repo.DeleteUserSSHPublicKeyByTeamAndUserID(c.Request.Context(), authCtx.TeamID, authCtx.UserID, keyID); err != nil {
		if errors.Is(err, identity.ErrSSHPublicKeyNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "ssh public key not found")
			return
		}
		h.logger.Error("Failed to delete ssh public key", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete ssh public key")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "ssh public key removed"})
}
