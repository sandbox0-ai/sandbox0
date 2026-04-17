package http

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/client"
	"github.com/sandbox0-ai/sandbox0/cluster-gateway/pkg/middleware"
	"github.com/sandbox0-ai/sandbox0/pkg/gateway/spec"
	"github.com/sandbox0-ai/sandbox0/pkg/internalauth"
	"github.com/sandbox0-ai/sandbox0/pkg/proxy"
	sharedssh "github.com/sandbox0-ai/sandbox0/pkg/sshgateway"
	"go.uber.org/zap"
)

// === Sandbox Management Handlers (→ Manager) ===

// proxyToManager proxies a request to manager with internal authentication
func (s *Server) proxyToManager(c *gin.Context) {
	authCtx := middleware.GetAuthContext(c)
	claims := internalauth.ClaimsFromContext(c.Request.Context())

	// Generate internal token for manager
	internalToken, err := s.generateManagerToken(authCtx, claims, nil)
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

	// Forward to manager
	s.proxy2Mgr.ProxyToTarget(c)
}

// createSandbox creates a new sandbox
func (s *Server) createSandbox(c *gin.Context) {
	// Rewrite path for manager
	c.Request.URL.Path = "/api/v1/sandboxes"

	s.proxyToManager(c)
}

// listSandboxes lists all sandboxes for the authenticated team
func (s *Server) listSandboxes(c *gin.Context) {
	s.proxyToManager(c)
}

// getSandbox gets a sandbox by ID
func (s *Server) getSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	authCtx := middleware.GetAuthContext(c)
	sandbox, err := s.managerClient.GetSandbox(c.Request.Context(), sandboxID, authCtx.UserID, authCtx.TeamID)
	if err != nil {
		s.logger.Warn("Failed to get sandbox from manager",
			zap.String("sandbox_id", sandboxID),
			zap.String("team_id", authCtx.TeamID),
			zap.String("user_id", authCtx.UserID),
			zap.Error(err),
		)
		switch {
		case errors.Is(err, client.ErrSandboxNotFound):
			spec.JSONError(c, http.StatusNotFound, spec.CodeNotFound, "sandbox not found")
		default:
			spec.JSONError(c, http.StatusBadGateway, spec.CodeUnavailable, "sandbox unavailable")
		}
		return
	}

	payload := sharedssh.SandboxToAPI(sandbox, sharedssh.BuildConnectionInfo(s.cfg.SSHEndpointHost, s.cfg.SSHEndpointPort, sandbox.ID))
	spec.JSONSuccess(c, http.StatusOK, payload)
}

// getSandboxStatus gets sandbox status
func (s *Server) getSandboxStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/status"

	s.proxyToManager(c)
}

// getSandboxLogs gets sandbox pod logs
func (s *Server) getSandboxLogs(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	// Rewrite path to manager API
	c.Request.URL.Path = "/api/v1/sandboxes/" + sandboxID + "/logs"
	if sandboxLogsFollowRequested(c) {
		c.Request = proxy.WithUpstreamTimeoutDisabledRequest(c.Request)
		c.Request.Header.Set("Accept", "text/plain")
	}

	s.proxyToManager(c)
}

func sandboxLogsFollowRequested(c *gin.Context) bool {
	follow, err := strconv.ParseBool(c.Query("follow"))
	return err == nil && follow
}

// updateSandbox updates sandbox configuration
func (s *Server) updateSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)
}

// deleteSandbox deletes a sandbox
func (s *Server) deleteSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)
}

// pauseSandbox pauses a sandbox
func (s *Server) pauseSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)
}

// resumeSandbox resumes a paused sandbox
func (s *Server) resumeSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)
}

// refreshSandbox refreshes sandbox TTL
func (s *Server) refreshSandbox(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	s.proxyToManager(c)
}

// === Sandbox Volume Mount Handlers (→ Procd) ===

// mountSandboxVolume mounts a volume in the sandbox
func (s *Server) mountSandboxVolume(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/sandboxvolumes/mount"
	s.proxyToProcd(c, procdURL)
}

// unmountSandboxVolume unmounts a volume from the sandbox
func (s *Server) unmountSandboxVolume(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/sandboxvolumes/unmount"
	s.proxyToProcd(c, procdURL)
}

// getSandboxVolumeStatus gets the status of mounted volumes
func (s *Server) getSandboxVolumeStatus(c *gin.Context) {
	sandboxID := c.Param("id")
	if sandboxID == "" {
		spec.JSONError(c, http.StatusBadRequest, spec.CodeBadRequest, "sandbox_id is required")
		return
	}

	procdURL, err := s.getProcdURL(c, sandboxID)
	if err != nil {
		return
	}

	c.Request.URL.Path = "/api/v1/sandboxvolumes/status"
	s.proxyToProcd(c, procdURL)
}
