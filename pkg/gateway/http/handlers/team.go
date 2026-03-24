package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/identity"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

type teamRepository interface {
	GetTeamsByUserID(ctx context.Context, userID string) ([]*identity.Team, error)
	CreateTeam(ctx context.Context, team *identity.Team) error
	GetTeamMember(ctx context.Context, teamID, userID string) (*identity.TeamMember, error)
	GetTeamByID(ctx context.Context, id string) (*identity.Team, error)
	UpdateTeam(ctx context.Context, team *identity.Team) error
	DeleteTeam(ctx context.Context, id string) error
	GetTeamMembers(ctx context.Context, teamID string) ([]*identity.TeamMemberWithUser, error)
	GetUserByEmail(ctx context.Context, email string) (*identity.User, error)
	AddTeamMember(ctx context.Context, member *identity.TeamMember) error
	UpdateTeamMemberRole(ctx context.Context, teamID, userID, role string) error
	RemoveTeamMember(ctx context.Context, teamID, userID string) error
}

// TeamRegionLookup resolves region directory entries for team validation.
type TeamRegionLookup interface {
	GetRegion(ctx context.Context, regionID string) (*tenantdir.Region, error)
}

// TeamHandler handles team endpoints
type TeamHandler struct {
	repo                      teamRepository
	logger                    *zap.Logger
	requireHomeRegionOnCreate bool
	regionLookup              TeamRegionLookup
}

// TeamHandlerOption configures TeamHandler behavior.
type TeamHandlerOption func(*TeamHandler)

// WithCreateHomeRegionRequired requires create requests to include a valid, routable home region.
func WithCreateHomeRegionRequired(regionLookup TeamRegionLookup) TeamHandlerOption {
	return func(h *TeamHandler) {
		h.requireHomeRegionOnCreate = true
		h.regionLookup = regionLookup
	}
}

// NewTeamHandler creates a new team handler.
func NewTeamHandler(repo teamRepository, logger *zap.Logger, opts ...TeamHandlerOption) *TeamHandler {
	handler := &TeamHandler{
		repo:   repo,
		logger: logger,
	}
	for _, opt := range opts {
		opt(handler)
	}
	return handler
}

// ListTeams returns all teams the current user belongs to
func (h *TeamHandler) ListTeams(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teams, err := h.repo.GetTeamsByUserID(c.Request.Context(), authCtx.UserID)
	if err != nil {
		h.logger.Error("Failed to get teams", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get teams")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"teams": teams})
}

// CreateTeamRequest is the request body for creating a team
type CreateTeamRequest struct {
	Name         string  `json:"name" binding:"required"`
	Slug         string  `json:"slug"`
	HomeRegionID *string `json:"home_region_id"`
}

// CreateTeam creates a new team
func (h *TeamHandler) CreateTeam(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	var req CreateTeamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	homeRegionID := normalizeOptionalString(req.HomeRegionID)
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

	team := &identity.Team{
		Name:         req.Name,
		Slug:         req.Slug,
		OwnerID:      &authCtx.UserID,
		HomeRegionID: homeRegionID,
	}

	if err := h.repo.CreateTeam(c.Request.Context(), team); err != nil {
		if errors.Is(err, identity.ErrTeamAlreadyExists) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team with this slug already exists")
			return
		}
		h.logger.Error("Failed to create team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to create team")
		return
	}

	// Add creator as admin member
	member := &identity.TeamMember{
		TeamID: team.ID,
		UserID: authCtx.UserID,
		Role:   "admin",
	}
	if err := h.repo.AddTeamMember(c.Request.Context(), member); err != nil {
		h.logger.Warn("Failed to add creator as member", zap.Error(err))
	}

	spec.JSONSuccess(c, http.StatusCreated, team)
}

// GetTeam returns a specific team
func (h *TeamHandler) GetTeam(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")

	// Verify user is member of the team
	_, err := h.repo.GetTeamMember(c.Request.Context(), teamID, authCtx.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not a member of this team")
			return
		}
		h.logger.Error("Failed to check membership", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check membership")
		return
	}

	team, err := h.repo.GetTeamByID(c.Request.Context(), teamID)
	if err != nil {
		if errors.Is(err, identity.ErrTeamNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "team not found")
			return
		}
		h.logger.Error("Failed to get team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get team")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, team)
}

// UpdateTeamRequest is the request body for updating a team
type UpdateTeamRequest struct {
	Name         string  `json:"name"`
	Slug         string  `json:"slug"`
	HomeRegionID *string `json:"home_region_id"`
}

// UpdateTeam updates a team
func (h *TeamHandler) UpdateTeam(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")

	// Verify user is admin of the team
	member, err := h.repo.GetTeamMember(c.Request.Context(), teamID, authCtx.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not a member of this team")
			return
		}
		h.logger.Error("Failed to check membership", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check membership")
		return
	}

	if member.Role != "admin" {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "only admins can update team")
		return
	}

	var req UpdateTeamRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	team, err := h.repo.GetTeamByID(c.Request.Context(), teamID)
	if err != nil {
		if errors.Is(err, identity.ErrTeamNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "team not found")
			return
		}
		h.logger.Error("Failed to get team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get team")
		return
	}

	if req.Name != "" {
		team.Name = req.Name
	}
	if req.Slug != "" {
		team.Slug = req.Slug
	}
	if req.HomeRegionID != nil {
		nextHomeRegionID := normalizeOptionalString(req.HomeRegionID)
		if !sameOptionalString(team.HomeRegionID, nextHomeRegionID) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team home region cannot be changed after creation")
			return
		}
	}

	if err := h.repo.UpdateTeam(c.Request.Context(), team); err != nil {
		if errors.Is(err, identity.ErrTeamAlreadyExists) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team with this slug already exists")
			return
		}
		h.logger.Error("Failed to update team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update team")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, team)
}

func sameOptionalString(a, b *string) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil || b == nil:
		return false
	default:
		return *a == *b
	}
}

// DeleteTeam deletes a team
func (h *TeamHandler) DeleteTeam(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")

	// Verify user is owner of the team
	team, err := h.repo.GetTeamByID(c.Request.Context(), teamID)
	if err != nil {
		if errors.Is(err, identity.ErrTeamNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "team not found")
			return
		}
		h.logger.Error("Failed to get team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get team")
		return
	}

	if team.OwnerID == nil || *team.OwnerID != authCtx.UserID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "only team owner can delete team")
		return
	}

	if err := h.repo.DeleteTeam(c.Request.Context(), teamID); err != nil {
		h.logger.Error("Failed to delete team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete team")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "team deleted"})
}

// ListTeamMembers returns all members of a team
func (h *TeamHandler) ListTeamMembers(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")

	// Verify user is member of the team
	_, err := h.repo.GetTeamMember(c.Request.Context(), teamID, authCtx.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not a member of this team")
			return
		}
		h.logger.Error("Failed to check membership", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check membership")
		return
	}

	members, err := h.repo.GetTeamMembers(c.Request.Context(), teamID)
	if err != nil {
		h.logger.Error("Failed to get members", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get members")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"members": members})
}

// AddTeamMemberRequest is the request body for adding a team member
type AddTeamMemberRequest struct {
	Email string `json:"email" binding:"required,email"`
	Role  string `json:"role" binding:"required,oneof=admin developer viewer"`
}

// AddTeamMember adds a member to a team
func (h *TeamHandler) AddTeamMember(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")

	// Verify user is admin of the team
	member, err := h.repo.GetTeamMember(c.Request.Context(), teamID, authCtx.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not a member of this team")
			return
		}
		h.logger.Error("Failed to check membership", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check membership")
		return
	}

	if member.Role != "admin" {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "only admins can add members")
		return
	}

	var req AddTeamMemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	// Find user by email
	user, err := h.repo.GetUserByEmail(c.Request.Context(), req.Email)
	if err != nil {
		if errors.Is(err, identity.ErrUserNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "user not found")
			return
		}
		h.logger.Error("Failed to find user", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to find user")
		return
	}

	// Add member
	newMember := &identity.TeamMember{
		TeamID: teamID,
		UserID: user.ID,
		Role:   req.Role,
	}

	if err := h.repo.AddTeamMember(c.Request.Context(), newMember); err != nil {
		if errors.Is(err, identity.ErrAlreadyMember) {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "user is already a member")
			return
		}
		h.logger.Error("Failed to add member", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to add member")
		return
	}

	spec.JSONSuccess(c, http.StatusCreated, newMember)
}

// UpdateTeamMemberRequest is the request body for updating a team member
type UpdateTeamMemberRequest struct {
	Role string `json:"role" binding:"required,oneof=admin developer viewer"`
}

// UpdateTeamMember updates a team member's role
func (h *TeamHandler) UpdateTeamMember(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")
	userID := c.Param("userId")

	// Verify user is admin of the team
	member, err := h.repo.GetTeamMember(c.Request.Context(), teamID, authCtx.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not a member of this team")
			return
		}
		h.logger.Error("Failed to check membership", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check membership")
		return
	}

	if member.Role != "admin" {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "only admins can update members")
		return
	}

	var req UpdateTeamMemberRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	if err := h.repo.UpdateTeamMemberRole(c.Request.Context(), teamID, userID, req.Role); err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "member not found")
			return
		}
		h.logger.Error("Failed to update member", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to update member")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "member updated"})
}

func normalizeOptionalString(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

var errHomeRegionLookupUnavailable = errors.New("home region lookup unavailable")
var errHomeRegionRequired = errors.New("home_region_id is required")
var errHomeRegionNotRoutable = errors.New("home region is not routable")

func validateRequiredRoutableHomeRegion(ctx context.Context, regionLookup TeamRegionLookup, homeRegionID *string) error {
	if homeRegionID == nil {
		return errHomeRegionRequired
	}
	if regionLookup == nil {
		return errHomeRegionLookupUnavailable
	}

	region, err := regionLookup.GetRegion(ctx, *homeRegionID)
	if err != nil {
		if errors.Is(err, tenantdir.ErrRegionNotFound) {
			return tenantdir.ErrRegionNotFound
		}
		return err
	}
	if !region.Enabled || strings.TrimSpace(region.RegionalGatewayURL) == "" {
		return errHomeRegionNotRoutable
	}
	return nil
}

func resolveHomeRegionValidationError(err error) (int, string, string) {
	switch {
	case errors.Is(err, errHomeRegionRequired):
		return http.StatusBadRequest, spec.CodeBadRequest, "home_region_id is required"
	case errors.Is(err, tenantdir.ErrRegionNotFound):
		return http.StatusBadRequest, spec.CodeBadRequest, "home region not found"
	case errors.Is(err, errHomeRegionNotRoutable):
		return http.StatusBadRequest, spec.CodeBadRequest, "home region is not routable"
	case errors.Is(err, errHomeRegionLookupUnavailable):
		return http.StatusInternalServerError, spec.CodeInternal, "failed to resolve home region"
	default:
		return http.StatusInternalServerError, spec.CodeInternal, "failed to resolve home region"
	}
}

// ValidateInitUserHomeRegion verifies the configured init user's home region for global-gateway bootstrap.
func ValidateInitUserHomeRegion(ctx context.Context, regionLookup TeamRegionLookup, homeRegionID string) error {
	if strings.TrimSpace(homeRegionID) == "" {
		return errHomeRegionRequired
	}
	return validateRequiredRoutableHomeRegion(ctx, regionLookup, &homeRegionID)
}

// RemoveTeamMember removes a member from a team
func (h *TeamHandler) RemoveTeamMember(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")
	userID := c.Param("userId")

	// Verify user is admin of the team (or removing themselves)
	member, err := h.repo.GetTeamMember(c.Request.Context(), teamID, authCtx.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "not a member of this team")
			return
		}
		h.logger.Error("Failed to check membership", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check membership")
		return
	}

	if member.Role != "admin" && userID != authCtx.UserID {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "only admins can remove other members")
		return
	}

	// Check if this is the last admin
	if userID == authCtx.UserID && member.Role == "admin" {
		members, err := h.repo.GetTeamMembers(c.Request.Context(), teamID)
		if err != nil {
			h.logger.Error("Failed to get members", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check team admins")
			return
		}

		adminCount := 0
		for _, m := range members {
			if m.Role == "admin" {
				adminCount++
			}
		}

		if adminCount <= 1 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cannot remove the last admin")
			return
		}
	}

	if err := h.repo.RemoveTeamMember(c.Request.Context(), teamID, userID); err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "member not found")
			return
		}
		h.logger.Error("Failed to remove member", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to remove member")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "member removed"})
}
