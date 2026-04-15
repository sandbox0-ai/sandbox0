package http

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

var errManagerTokenTeamIDRequired = errors.New("team_id is required for team-scoped manager token")

type managerTokenOptions struct {
	systemScopeForSystemAdmin bool
}

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
	return s.generateManagerTokenWithOptions(authCtx, claims, permissions, managerTokenOptions{})
}

func (s *Server) generateTemplateManagerToken(authCtx *authn.AuthContext, claims *internalauth.Claims, permissions []string) (string, error) {
	return s.generateManagerTokenWithOptions(authCtx, claims, permissions, managerTokenOptions{
		systemScopeForSystemAdmin: true,
	})
}

func (s *Server) generateManagerTokenWithOptions(authCtx *authn.AuthContext, claims *internalauth.Claims, permissions []string, tokenOpts managerTokenOptions) (string, error) {
	opts := internalauth.GenerateOptions{
		Permissions: permissions,
	}
	if authCtx != nil {
		opts.UserID = authCtx.UserID
	}

	teamID := ""
	userID := ""
	if authCtx != nil {
		teamID = strings.TrimSpace(authCtx.TeamID)
		userID = authCtx.UserID
	}
	if shouldGenerateSystemManagerToken(authCtx, claims, teamID, tokenOpts) {
		return s.internalAuthGen.GenerateSystem("manager", opts)
	}
	if teamID == "" {
		return "", errManagerTokenTeamIDRequired
	}
	return s.internalAuthGen.Generate("manager", teamID, userID, opts)
}

func shouldGenerateSystemManagerToken(authCtx *authn.AuthContext, claims *internalauth.Claims, teamID string, opts managerTokenOptions) bool {
	if claims != nil && claims.IsSystem {
		return true
	}
	if authCtx == nil || !authCtx.IsSystemAdmin {
		return false
	}
	if teamID == "" {
		return true
	}
	return opts.systemScopeForSystemAdmin && authCtx.AuthMethod == authn.AuthMethodAPIKey
}
