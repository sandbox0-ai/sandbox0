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
	ResolveTeamAccess(ctx context.Context, userID, teamID string) (*tenantdir.TeamAccess, error)
}

// TenantHandler handles explicit team-to-region token exchange.
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

// RegionalSessionResponse carries region-scoped access session data.
type RegionalSessionResponse struct {
	RegionID           string `json:"region_id"`
	RegionalGatewayURL string `json:"regional_gateway_url,omitempty"`
	Token              string `json:"token"`
	ExpiresAt          int64  `json:"expires_at"`
}

// IssueRegionTokenResponse is returned when a region-scoped token is issued.
type IssueRegionTokenResponse = RegionalSessionResponse

// NewTenantHandler creates a new tenant handler.
func NewTenantHandler(resolver tenantResolver, jwtIssuer *authn.Issuer, regionTokenTTL time.Duration, logger *zap.Logger) *TenantHandler {
	return &TenantHandler{
		resolver:       resolver,
		jwtIssuer:      jwtIssuer,
		regionTokenTTL: regionTokenTTL,
		logger:         logger,
	}
}

// IssueRegionToken exchanges a user token for a short-lived region token scoped to an explicit team.
func (h *TenantHandler) IssueRegionToken(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	if authCtx == nil || authCtx.UserID == "" {
		spec.JSONError(c, http.StatusUnauthorized, spec.CodeUnauthorized, "not authenticated")
		return
	}

	var req IssueRegionTokenRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "invalid request body")
		return
	}

	teamAccess, err := h.resolver.ResolveTeamAccess(c.Request.Context(), authCtx.UserID, req.TeamID)
	if err != nil {
		h.writeResolveError(c, err)
		return
	}
	if strings.TrimSpace(teamAccess.HomeRegionID) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team has no home region")
		return
	}
	if strings.TrimSpace(teamAccess.RegionalGatewayURL) == "" {
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team home region is not routable")
		return
	}

	normalizedRegionID := strings.TrimSpace(teamAccess.HomeRegionID)
	token, expiry, err := h.jwtIssuer.IssueRegionToken(
		authCtx.UserID,
		teamAccess.TeamID,
		teamAccess.TeamRole,
		normalizedRegionID,
		authCtx.IsSystemAdmin,
		h.regionTokenTTL,
	)
	if err != nil {
		h.logger.Error("Failed to issue region token", zap.Error(err))
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "failed to issue region token")
		return
	}

	spec.JSONSuccess(c, http.StatusOK, IssueRegionTokenResponse{
		RegionID:           normalizedRegionID,
		RegionalGatewayURL: teamAccess.RegionalGatewayURL,
		Token:              token,
		ExpiresAt:          expiry.Unix(),
	})
}

func (h *TenantHandler) writeResolveError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, tenantdir.ErrTeamRequired):
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "team_id is required")
	case errors.Is(err, tenantdir.ErrRegionNotFound):
		spec.JSONError(c, http.StatusConflict, spec.CodeConflict, "team home region is not routable")
	default:
		h.logger.Warn("Failed to resolve team access", zap.Error(err))
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "team not accessible")
	}
}
