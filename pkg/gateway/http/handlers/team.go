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
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/teamresources"
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
	TransferTeamOwner(ctx context.Context, teamID, userID string) (*identity.Team, error)
	GetTeamMembers(ctx context.Context, teamID string) ([]*identity.TeamMemberWithUser, error)
	SearchTeamMembers(ctx context.Context, teamID, query string) ([]*identity.TeamMemberWithUser, error)
	GetUserByEmail(ctx context.Context, email string) (*identity.User, error)
	AddTeamMember(ctx context.Context, member *identity.TeamMember) error
	UpdateTeamMemberRole(ctx context.Context, teamID, userID, role string) error
	RemoveTeamMember(ctx context.Context, teamID, userID string) error
}

// TeamDeletePreflight checks for resources that must be cleared before deleting a team.
type TeamDeletePreflight interface {
	GetTeamResourceInventory(ctx context.Context, teamID string) (*teamresources.Inventory, error)
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
	deletePreflight           TeamDeletePreflight
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

// WithTeamDeletePreflight configures the resource inventory used by team deletion.
func WithTeamDeletePreflight(preflight TeamDeletePreflight) TeamHandlerOption {
	return func(h *TeamHandler) {
		h.deletePreflight = preflight
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
	name := strings.TrimSpace(req.Name)
	if name == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team name is required")
		return
	}
	slug, err := normalizeExplicitTeamSlug(req.Slug)
	if err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
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

	team := &identity.Team{
		Name:         name,
		Slug:         slug,
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
	if rejectInvalidUUID(c, "team id", teamID) {
		return
	}

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
	if rejectInvalidUUID(c, "team id", teamID) {
		return
	}

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
		name := strings.TrimSpace(req.Name)
		if name == "" {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team name is required")
			return
		}
		team.Name = name
	}
	if req.Slug != "" {
		slug, err := normalizeExplicitTeamSlug(req.Slug)
		if err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, err.Error())
			return
		}
		team.Slug = slug
	}
	if req.HomeRegionID != nil {
		nextHomeRegionID := normalizeOptionalString(req.HomeRegionID)
		if err := validateNormalizedHomeRegionID(nextHomeRegionID); err != nil {
			status, code, message := resolveHomeRegionValidationError(err)
			spec.JSONError(c, status, code, message)
			return
		}
		currentHomeRegionID := normalizeOptionalString(team.HomeRegionID)
		if !sameOptionalString(currentHomeRegionID, nextHomeRegionID) {
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

func normalizeExplicitTeamSlug(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	slug := strings.TrimSpace(value)
	if slug == "" {
		return "", errors.New("team slug cannot be whitespace")
	}
	if !isValidTeamSlug(slug) {
		return "", errors.New("team slug must contain only lowercase letters, numbers, and single hyphens")
	}
	return slug, nil
}

func isValidTeamSlug(slug string) bool {
	previousHyphen := false
	for i, r := range slug {
		switch {
		case r >= 'a' && r <= 'z':
			previousHyphen = false
		case r >= '0' && r <= '9':
			previousHyphen = false
		case r == '-':
			if i == 0 || previousHyphen {
				return false
			}
			previousHyphen = true
		default:
			return false
		}
	}
	return slug != "" && !previousHyphen
}

// DeleteTeam deletes a team
func (h *TeamHandler) DeleteTeam(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")
	if rejectInvalidUUID(c, "team id", teamID) {
		return
	}

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

	if h.deletePreflight != nil {
		inventory, err := h.deletePreflight.GetTeamResourceInventory(c.Request.Context(), teamID)
		if err != nil {
			h.logger.Error("Failed to check team resources before deletion", zap.Error(err), zap.String("team_id", teamID))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check team resources")
			return
		}
		if inventory.HasBlockingResources() {
			spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team has resources that must be removed before deletion", inventory)
			return
		}
	}

	if err := h.repo.DeleteTeam(c.Request.Context(), teamID); err != nil {
		h.logger.Error("Failed to delete team", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to delete team")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"message": "team deleted"})
}

// TransferTeamOwnerRequest is the request body for transferring team ownership.
type TransferTeamOwnerRequest struct {
	UserID string `json:"user_id" binding:"required"`
}

// TransferTeamOwner transfers team ownership to an existing team member.
func (h *TeamHandler) TransferTeamOwner(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")
	if rejectInvalidUUID(c, "team id", teamID) {
		return
	}

	var req TransferTeamOwnerRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}
	req.UserID = strings.TrimSpace(req.UserID)
	if req.UserID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "user_id is required")
		return
	}
	if rejectInvalidUUID(c, "user_id", req.UserID) {
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
	if !isTeamOwner(team, authCtx.UserID) {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "only team owner can transfer ownership")
		return
	}

	team, err = h.repo.TransferTeamOwner(c.Request.Context(), teamID, req.UserID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "member not found")
			return
		}
		if errors.Is(err, identity.ErrTeamNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "team not found")
			return
		}
		h.logger.Error("Failed to transfer team owner", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to transfer team owner")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, team)
}

// ListTeamMembers returns all members of a team
func (h *TeamHandler) ListTeamMembers(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")
	if rejectInvalidUUID(c, "team id", teamID) {
		return
	}

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

	query := strings.TrimSpace(c.Query("query"))
	var members []*identity.TeamMemberWithUser
	if query == "" {
		members, err = h.repo.GetTeamMembers(c.Request.Context(), teamID)
	} else {
		members, err = h.repo.SearchTeamMembers(c.Request.Context(), teamID, query)
	}
	if err != nil {
		h.logger.Error("Failed to get members", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get members")
		return
	}
	if members == nil {
		members = []*identity.TeamMemberWithUser{}
	}

	spec.JSONSuccess(c, http.StatusOK, gin.H{"members": members})
}

// AddTeamMemberRequest is the request body for adding a team member
type AddTeamMemberRequest struct {
	Email string `json:"email" binding:"required,email"`
	Role  string `json:"role" binding:"required,oneof=admin developer builder viewer"`
}

// AddTeamMember adds a member to a team
func (h *TeamHandler) AddTeamMember(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	teamID := c.Param("id")
	if rejectInvalidUUID(c, "team id", teamID) {
		return
	}

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
	Role string `json:"role" binding:"required,oneof=admin developer builder viewer"`
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
	if rejectInvalidUUID(c, "team id", teamID) || rejectInvalidUUID(c, "user id", userID) {
		return
	}

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
	targetMember, err := h.repo.GetTeamMember(c.Request.Context(), teamID, userID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "member not found")
			return
		}
		h.logger.Error("Failed to get target member", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get member")
		return
	}
	if isTeamOwner(team, userID) && req.Role != "admin" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team owner must remain an admin")
		return
	}
	if targetMember.Role == "admin" && req.Role != "admin" {
		members, err := h.repo.GetTeamMembers(c.Request.Context(), teamID)
		if err != nil {
			h.logger.Error("Failed to get members", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check team admins")
			return
		}
		if countTeamAdmins(members) <= 1 {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cannot remove the last admin")
			return
		}
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

func rejectInvalidUUID(c *gin.Context, field, value string) bool {
	if isValidUUID(value) {
		return false
	}
	spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, field+" must be a valid UUID")
	return true
}

var errHomeRegionLookupUnavailable = errors.New("home region lookup unavailable")
var errHomeRegionRequired = errors.New("home_region_id is required")
var errHomeRegionInvalidFormat = errors.New("home region id must use provider-region format")
var errHomeRegionNotRoutable = errors.New("home region is not routable")

func validateNormalizedHomeRegionID(homeRegionID *string) error {
	if homeRegionID == nil {
		return nil
	}
	if !tenantdir.IsNormalizedRegionID(*homeRegionID) {
		return errHomeRegionInvalidFormat
	}
	return nil
}

func validateRequiredRoutableHomeRegion(ctx context.Context, regionLookup TeamRegionLookup, homeRegionID *string) error {
	if homeRegionID == nil || strings.TrimSpace(*homeRegionID) == "" {
		return errHomeRegionRequired
	}
	if err := validateNormalizedHomeRegionID(homeRegionID); err != nil {
		return err
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
	case errors.Is(err, errHomeRegionInvalidFormat):
		return http.StatusBadRequest, spec.CodeBadRequest, "home_region_id must use provider-region format"
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
	if rejectInvalidUUID(c, "team id", teamID) || rejectInvalidUUID(c, "user id", userID) {
		return
	}

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
	if isTeamOwner(team, userID) {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "cannot remove team owner")
		return
	}

	targetMember, err := h.repo.GetTeamMember(c.Request.Context(), teamID, userID)
	if err != nil {
		if errors.Is(err, identity.ErrMemberNotFound) {
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "member not found")
			return
		}
		h.logger.Error("Failed to get target member", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to get member")
		return
	}

	if targetMember.Role == "admin" {
		members, err := h.repo.GetTeamMembers(c.Request.Context(), teamID)
		if err != nil {
			h.logger.Error("Failed to get members", zap.Error(err))
			spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to check team admins")
			return
		}
		if countTeamAdmins(members) <= 1 {
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

func isTeamOwner(team *identity.Team, userID string) bool {
	return team != nil && team.OwnerID != nil && *team.OwnerID == strings.TrimSpace(userID)
}

func countTeamAdmins(members []*identity.TeamMemberWithUser) int {
	count := 0
	for _, member := range members {
		if member != nil && member.Role == "admin" {
			count++
		}
	}
	return count
}
