package http

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/authn"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"go.uber.org/zap"
)

// === Internal API Handlers (for scheduler) ===

// getClusterSummary proxies cluster summary request to manager
func (s *Server) getClusterSummary(c *gin.Context) {
	s.proxySchedulerManagerRequest(c, "/internal/v1/cluster/summary")
}

// getTemplateStats proxies template stats request to manager
func (s *Server) getTemplateStats(c *gin.Context) {
	s.proxySchedulerManagerRequest(c, "/internal/v1/templates/stats")
}

// proxyInternalTemplateRequest forwards scheduler template sync requests to manager.
func (s *Server) proxyInternalTemplateRequest(c *gin.Context) {
	s.proxyInternalManagerRequest(c)
}

// proxyInternalManagerRequest forwards a trusted control-plane request to the
// manager while preserving its internal path and caller team context.
func (s *Server) proxyInternalManagerRequest(c *gin.Context) {
	s.proxySchedulerManagerRequest(c, "")
}

// proxySchedulerManagerRequest forwards a scheduler request to the manager.
// An empty managerPath preserves the incoming internal path.
func (s *Server) proxySchedulerManagerRequest(c *gin.Context, managerPath string) {
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

	if managerPath != "" {
		c.Request.URL.Path = managerPath
	}

	s.proxy2Mgr.ProxyToTarget(c)
}

func (s *Server) proxyInternalSystemQuotaRequest(c *gin.Context) {
	s.proxyInternalSystemManagerRequest(c)
}

// proxyInternalSystemPauseRequest forwards a trusted billing enforcement
// request to manager. Billing callers are system identities and must never be
// able to select an arbitrary team-scoped user identity.
func (s *Server) proxyInternalSystemPauseRequest(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	authCtx := middleware.GetAuthContext(c)
	if (claims == nil || !claims.IsSystemToken()) && (authCtx == nil || !authCtx.IsSystemAdmin) {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system token is required")
		return
	}
	s.forwardInternalSystemManagerRequest(c, claims, authCtx)
}

func (s *Server) proxyInternalSystemManagerRequest(c *gin.Context) {
	claims := internalauth.ClaimsFromContext(c.Request.Context())
	if claims == nil || !claims.IsSystemToken() {
		spec.JSONError(c, http.StatusForbidden, spec.CodeForbidden, "system token is required")
		return
	}
	authCtx := middleware.GetAuthContext(c)
	s.forwardInternalSystemManagerRequest(c, claims, authCtx)
}

func (s *Server) forwardInternalSystemManagerRequest(c *gin.Context, claims *internalauth.Claims, authCtx *authn.AuthContext) {
	// The caller was authenticated as a system identity before reaching this
	// point. Reissue a system token for manager instead of translating public
	// billing credentials into a team-scoped manager token.
	internalToken, err := s.internalAuthGen.GenerateSystem("manager", internalauth.GenerateOptions{
		Permissions: []string{"*:*"},
	})
	if err != nil {
		s.logger.Error("Failed to generate internal token for manager",
			zap.String("team_id", c.Param("team_id")),
			zap.Error(err),
		)
		spec.JSONError(c, http.StatusInternalServerError, spec.CodeInternal, "internal authentication failed")
		return
	}

	c.Request.Header.Set(internalauth.DefaultTokenHeader, internalToken)
	s.proxy2Mgr.ProxyToTarget(c)
}

func (s *Server) generateManagerToken(authCtx *authn.AuthContext, claims *internalauth.Claims, permissions []string) (string, error) {
	opts := internalauth.GenerateOptions{
		Permissions: permissions,
	}
	if claims != nil && claims.IsSystem {
		return s.internalAuthGen.GenerateSystem("manager", opts)
	}
	if authCtx != nil && authCtx.IsSystemAdmin && (strings.TrimSpace(authCtx.TeamID) == "" || authCtx.AuthMethod == authn.AuthMethodAPIKey) {
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
