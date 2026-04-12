package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// === Internal API Handlers (for scheduler) ===

// getClusterSummary proxies cluster summary request to manager
func (s *Server) getClusterSummary(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	claims := internalauth.ClaimsFromContext(c.Request.Context())

	// Generate internal token for manager
	perms := s.cfg.SchedulerPermissions
	if len(perms) == 0 {
		perms = []string{"*:*"}
	}
	internalToken, err := s.generateManagerToken(authCtx, claims, perms)
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	// Set headers
	c.Request.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)

	// Rewrite path for manager
	c.Request.URL.Path = "/internal/v1/cluster/summary"

	// Forward to manager
	s.proxy2Mgr.ProxyToTarget(c)
}

// getTemplateStats proxies template stats request to manager
func (s *Server) getTemplateStats(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	claims := internalauth.ClaimsFromContext(c.Request.Context())

	// Generate internal token for manager
	perms := s.cfg.SchedulerPermissions
	if len(perms) == 0 {
		perms = []string{"*:*"}
	}
	internalToken, err := s.generateManagerToken(authCtx, claims, perms)
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	// Set headers
	c.Request.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)

	// Rewrite path for manager
	c.Request.URL.Path = "/internal/v1/templates/stats"

	// Forward to manager
	s.proxy2Mgr.ProxyToTarget(c)
}

// proxyInternalTemplateRequest forwards scheduler template sync requests to manager.
func (s *Server) proxyInternalTemplateRequest(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	claims := internalauth.ClaimsFromContext(c.Request.Context())

	perms := s.cfg.SchedulerPermissions
	if len(perms) == 0 {
		perms = []string{"*:*"}
	}
	internalToken, err := s.generateManagerToken(authCtx, claims, perms)
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", authCtx.TeamID),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	c.Request.Header.Set(internalauth.TeamIDHeader, authCtx.TeamID)
	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)

	// Preserve the incoming internal template path and body.
	s.proxy2Mgr.ProxyToTarget(c)
}

func (s *Server) generateManagerToken(authCtx *authn.AuthContext, claims *internalauth.Claims, permissions []string) (string, error) {
	opts := internalauth.GenerateOptions{
		Permissions: permissions,
	}
	if claims != nil && claims.IsSystem {
		return s.internalAuthGen.GenerateSystem("manager", opts)
	}
	if authCtx != nil && authCtx.IsSystemAdmin && strings.TrimSpace(authCtx.TeamID) == "" {
		return s.internalAuthGen.GenerateSystem("manager", opts)
	}

	teamID := ""
	userID := ""
	if authCtx != nil {
		teamID = authCtx.TeamID
		userID = authCtx.UserID
	}
	return s.internalAuthGen.Generate("manager", teamID, userID, opts)
}
