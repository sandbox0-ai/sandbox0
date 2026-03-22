package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/tenantdir"
	"go.uber.org/zap"
)

type tenantResolver interface {
	ResolveActiveTeam(ctx context.Context, userID, teamID string) (*tenantdir.ActiveTeam, error)
}

// TenantHandler handles active-team and regional token exchange endpoints.
type TenantHandler struct {
	resolver       tenantResolver
	jwtIssuer      *authn.Issuer
	regionTokenTTL time.Duration
	logger         *zap.Logger
}

// IssueRegionTokenRequest requests a region-scoped token for the selected team.
type IssueRegionTokenRequest struct {
	TeamID string `json:"team_id"`
}

// IssueRegionTokenResponse is returned when a region-scoped token is issued.
type IssueRegionTokenResponse struct {
	RegionID           string `json:"region_id"`
	RegionalGatewayURL string `json:"regional_gateway_url,omitempty"`
	Token              string `json:"token"`
	ExpiresAt          int64  `json:"expires_at"`
}

// NewTenantHandler creates a new tenant handler.
func NewTenantHandler(resolver tenantResolver, jwtIssuer *authn.Issuer, regionTokenTTL time.Duration, logger *zap.Logger) *TenantHandler {
	return &TenantHandler{
		resolver:       resolver,
		jwtIssuer:      jwtIssuer,
		regionTokenTTL: regionTokenTTL,
		logger:         logger,
	}
}

// GetActiveTeam resolves the user's active team and routing information.
func (h *TenantHandler) GetActiveTeam(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	activeTeam, err := h.resolver.ResolveActiveTeam(c.Request.Context(), authCtx.UserID, c.Query("team_id"))
	if err != nil {
		h.writeResolveError(c, err)
		return
	}
	if strings.TrimSpace(activeTeam.HomeRegionID) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "active team has no home region")
		return
	}
	if strings.TrimSpace(activeTeam.RegionalGatewayURL) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "active team home region is not routable")
		return
	}
	spec.JSONSuccess(c, http.StatusOK, activeTeam)
}

// IssueRegionToken exchanges a global user token for a short-lived region token.
func (h *TenantHandler) IssueRegionToken(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	var req IssueRegionTokenRequest
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&req); err != nil {
			spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
			return
		}
	}

	activeTeam, err := h.resolver.ResolveActiveTeam(c.Request.Context(), authCtx.UserID, req.TeamID)
	if err != nil {
		h.writeResolveError(c, err)
		return
	}
	if strings.TrimSpace(activeTeam.HomeRegionID) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "active team has no home region")
		return
	}
	if strings.TrimSpace(activeTeam.RegionalGatewayURL) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "active team home region is not routable")
		return
	}

	token, expiry, err := h.jwtIssuer.IssueRegionToken(
		authCtx.UserID,
		activeTeam.TeamID,
		activeTeam.TeamRole,
		activeTeam.HomeRegionID,
		authCtx.IsSystemAdmin,
		h.regionTokenTTL,
	)
	if err != nil {
		h.logger.Error("Failed to issue region token", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to issue region token")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, IssueRegionTokenResponse{
		RegionID:           activeTeam.HomeRegionID,
		RegionalGatewayURL: activeTeam.RegionalGatewayURL,
		Token:              token,
		ExpiresAt:          expiry.Unix(),
	})
}

func (h *TenantHandler) writeResolveError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, tenantdir.ErrNoActiveTeam):
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "no active team selected")
	case errors.Is(err, tenantdir.ErrRegionNotFound):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "active team home region is not routable")
	default:
		h.logger.Warn("Failed to resolve active team", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "failed to resolve active team")
	}
}
